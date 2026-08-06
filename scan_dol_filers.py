#!/usr/bin/env python3
"""
Scan DOL EFAST2 bulk data for a form year: find MA ESOP filers missing from our
database, verify stored fields against the authoritative source, and optionally
import new filers (used to start tracking a new form year, e.g. 2025).

What it does per --year:
  1. Downloads F_5500_{year}_Latest.zip and F_5500_SF_{year}_Latest.zip
     (cached in /tmp/dol; --no-download reuses the cache).
  2. Filters to MA sponsors whose TYPE_PENSION_BNFT_CD contains 2O or 2P
     (ESOP / leveraged ESOP) — same rule as the existing dataset.
  3. Diffs against form5500_filings for that filing_year, bucketing:
       NEW (EIN never seen in DB) / RETURNING (EIN known from other years)
       / POSSIBLE CROSS-EIN DUPE (name matches an existing row for the year —
       these were deliberately de-duplicated before; do not blindly re-add).
  4. Verifies common rows field-by-field: participants, active participants,
     city, NAICS (only when both present), and financials with the repo's
     precedence (Schedule H > Schedule I > 5500-SF). Schedule H/I values come
     from the filtered files in data/form5500/ (saved by refetch_dol_financials);
     SF values come from the fresh SF download.
  5. --import-new: backs up the DB, inserts ONLY the NEW/RETURNING bucket rows
     (never the possible dupes), enriches their financials from that year's
     Schedule H/I/SF where available, and leaves form5500_annual_summary
     untouched so existing dashboard pages keep their 2014-2024 scope.

Usage:
    python3 scan_dol_filers.py --year 2024
    python3 scan_dol_filers.py --year 2025 --import-new
    python3 scan_dol_filers.py --year 2025 --no-download --import-new
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import re
import shutil
import sqlite3
import ssl
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")
SCHEDULE_DIR = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500")
CACHE_DIR = "/tmp/dol"

MAIN_VARIANTS = ["{y}/Latest/F_5500_{y}_Latest", "{y}/All/F_5500_{y}_All"]
SF_VARIANTS = ["{y}/Latest/F_5500_SF_{y}_Latest", "{y}/All/F_5500_SF_{y}_All"]
SCH_VARIANTS = {
    "sch_h": ["{y}/Latest/F_SCH_H_{y}_Latest", "{y}/All/F_SCH_H_{y}_All"],
    "sch_i": ["{y}/Latest/F_SCH_I_{y}_Latest", "{y}/All/F_SCH_I_{y}_All"],
}

# Money fields verified/enriched, in DB-column order.
MONEY_FIELDS = ["total_assets", "total_liabilities", "employer_contributions",
                "participant_contributions", "benefits_paid", "net_income"]

# Schedule I uses its own SMALL_* names (same map as refetch_dol_financials).
SCH_I_FIELDS = {
    "total_assets": ["SMALL_TOT_ASSETS_EOY_AMT"],
    "total_liabilities": ["SMALL_TOT_LIABILITIES_EOY_AMT"],
    "employer_contributions": ["SMALL_EMPLR_CONTRIB_INCOME_AMT"],
    "participant_contributions": ["SMALL_PARTICIPANT_CONTRIB_AMT"],
    "benefits_paid": ["SMALL_TOT_DISTRIB_BNFT_AMT"],
    "net_income": ["SMALL_NET_INCOME_AMT"],
}


# EINs whose ESOP-coded filings are deliberately NOT tracked, keyed by normalized
# (leading-zero-stripped) EIN. These are large publicly traded employers whose
# ordinary 401(k)/savings plan carries an ESOP code (2O) purely because company
# stock is one investment option alongside 2J/2K — confirmed by reading the raw
# F_5500 PLAN_NAME. They are not closely held employee-owned companies, and each
# would add 1,900-4,400 participants, materially overstating MA employee
# ownership. Reviewed and excluded 2026-08-06; listed here so a later
# `--import-new` run cannot silently re-add them. See FORM5500_METHODOLOGY.
EXCLUDED_EINS: dict[str, str] = {
    "42271897": "Cabot Corporation (NYSE: CBT) - 'CABOT 401(K) PLAN', 2O alongside 2J/2K",
    "43234558": "Waters Technologies (NYSE: WAT) - 'THE WATERS EMPLOYEE INVESTMENT PLAN'",
    "880706021": "Crane NXT, Co. (NYSE: CXT) - 'CRANE NXT, CO. SAVINGS AND INVESTMENT PLAN'",
}


def norm_key(ein, pn) -> tuple[str, str]:
    e = re.sub(r"\D", "", str(ein or "")).lstrip("0")
    p = re.sub(r"\D", "", str(pn or "")).lstrip("0") or "1"
    return e, p


def norm_name(s) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", str(s or "").lower())
    stop = {"inc", "incorporated", "corp", "corporation", "llc", "co", "company",
            "the", "and", "of", "employee", "stock", "ownership", "plan", "trust", "esop"}
    return " ".join(t for t in s.split() if t not in stop)


def _download(url: str, dest: str) -> bool:
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=300, context=ctx) as resp, open(dest, "wb") as f:
            shutil.copyfileobj(resp, f)
        return True
    except Exception as exc:
        print(f"    ! {url.rsplit('/', 1)[-1]}: {exc}")
        if os.path.exists(dest):
            os.remove(dest)
        return False


def fetch_zip_rows(year: int, variants: list[str], no_download: bool):
    """Yield csv.DictReader rows from the first available bulk zip, or None."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    for variant in variants:
        rel = variant.format(y=year)
        dest = os.path.join(CACHE_DIR, rel.replace("/", "_") + ".zip")
        if not os.path.exists(dest):
            if no_download:
                continue
            url = f"https://askebsa.dol.gov/FOIA%20Files/{rel}.zip"
            print(f"    downloading {rel}.zip ...")
            if not _download(url, dest):
                continue
        try:
            zf = zipfile.ZipFile(dest)
            inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig", errors="replace"))
        except Exception as exc:
            print(f"    ! bad zip {dest}: {exc}")
            os.remove(dest)
    return None


def pick_col(headers: list[str], candidates: list[str]) -> str | None:
    """Find a column by candidate names, tolerating an SF_ prefix."""
    up = {h.upper().strip(): h for h in headers}
    for cand in candidates:
        for name in (cand, "SF_" + cand):
            if name in up:
                return up[name]
    return None


def to_num(v):
    s = str(v or "").strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def extract_ma_esops(reader, source: str) -> dict:
    """Filter a main-form/SF reader to MA ESOP rows keyed by (ein, pn)."""
    headers = reader.fieldnames or []
    col = {
        "ein": pick_col(headers, ["SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "SPONS_EIN", "EIN"]),
        "pn": pick_col(headers, ["SPONS_DFE_PN", "PLAN_NUM", "LAST_RPT_PLAN_NUM", "PN"]),
        "state": pick_col(headers, ["SPONS_DFE_MAIL_US_STATE", "SPONS_US_STATE",
                                    "SPONS_DFE_LOC_US_STATE", "SPONSOR_DFE_MAIL_US_STATE"]),
        "codes": pick_col(headers, ["TYPE_PENSION_BNFT_CODE", "TYPE_PENSION_BNFT_CD",
                                    "TYPE_PENSION_BENEFIT_CD"]),
        "sponsor": pick_col(headers, ["SPONSOR_DFE_NAME", "SPONS_DFE_NAME", "SPONS_NAME",
                                      "SPONS_SIGNED_NAME"]),
        "city": pick_col(headers, ["SPONS_DFE_MAIL_US_CITY", "SPONS_US_CITY", "SPONS_DFE_LOC_US_CITY"]),
        "zip": pick_col(headers, ["SPONS_DFE_MAIL_US_ZIP", "SPONS_US_ZIP", "SPONS_DFE_LOC_US_ZIP"]),
        "plan_name": pick_col(headers, ["PLAN_NAME", "PLAN_NM"]),
        "entity": pick_col(headers, ["TYPE_PLAN_ENTITY_CD", "TYPE_PLAN_ENTITY"]),
        "tot": pick_col(headers, ["TOT_PARTCP_BOY_CNT", "TOT_PARTCP_EOY_CNT", "TOT_PARTCP_CNT"]),
        "act": pick_col(headers, ["TOT_ACTIVE_PARTCP_CNT", "TOT_ACT_PARTCP_BOY_CNT",
                                  "TOT_ACT_PARTCP_EOY_CNT", "TOT_ACT_PARTCP_CNT",
                                  "ACT_PARTCP_CNT"]),
        "naics": pick_col(headers, ["BUSINESS_CD", "BUSINESS_CODE", "NAICS_CD"]),
        "eff": pick_col(headers, ["PLAN_EFF_DATE", "PLAN_EFFECTIVE_DT"]),
        "yr_begin": pick_col(headers, ["FORM_PLAN_YEAR_BEGIN_DATE", "PLAN_YEAR_BEGIN_DATE"]),
        "yr_end": pick_col(headers, ["FORM_TAX_PRD", "PLAN_YEAR_END_DATE"]),
        "received": pick_col(headers, ["DATE_RECEIVED", "FILING_DATE"]),
    }
    missing = [k for k, v in col.items() if v is None and k in ("ein", "state", "codes")]
    if missing:
        print(f"    ! {source}: required columns not found: {missing}")
        return {}
    out, scanned = {}, 0
    for row in reader:
        scanned += 1
        if str(row.get(col["state"], "")).strip().upper() != "MA":
            continue
        codes = str(row.get(col["codes"], "") or "").upper()
        # 2O (ESOP) / 2P (leveraged ESOP) are the primary markers. 2Q is also
        # ESOP-specific: it normally accompanies 2P on leveraged plans, but a
        # plan winding down or restating can file carrying 2Q alone (2024 MA:
        # Janis Research "2Q", OEM Connect "2Q3D" — both unambiguously ESOPs by
        # plan name and filing history). Matching 2Q keeps those visible to the
        # scan instead of silently dropping them from coverage checks.
        if not any(c in codes for c in ("2O", "2P", "2Q")):
            continue
        key = norm_key(row.get(col["ein"]), row.get(col["pn"]) if col["pn"] else "1")
        rec = {
            "source": source,
            "ein_raw": str(row.get(col["ein"], "")).strip(),
            "pn_raw": str(row.get(col["pn"], "")).strip() if col["pn"] else "001",
            "codes": codes,
            "sponsor_name": str(row.get(col["sponsor"], "") or "").strip() if col["sponsor"] else "",
            "sponsor_city": str(row.get(col["city"], "") or "").strip() if col["city"] else "",
            "sponsor_zip": str(row.get(col["zip"], "") or "").strip() if col["zip"] else "",
            "plan_name": str(row.get(col["plan_name"], "") or "").strip() if col["plan_name"] else "",
            "type_plan_entity": str(row.get(col["entity"], "") or "").strip() if col["entity"] else "",
            "total_participants": to_num(row.get(col["tot"])) if col["tot"] else None,
            "active_participants": to_num(row.get(col["act"])) if col["act"] else None,
            "naics_code": str(row.get(col["naics"], "") or "").strip() if col["naics"] else "",
            "plan_eff_date": str(row.get(col["eff"], "") or "").strip() if col["eff"] else "",
            "yr_begin": str(row.get(col["yr_begin"], "") or "").strip() if col["yr_begin"] else "",
            "yr_end": str(row.get(col["yr_end"], "") or "").strip() if col["yr_end"] else "",
            "received": str(row.get(col["received"], "") or "").strip() if col["received"] else "",
        }
        # SF carries its own financials (used when no Schedule H/I exists).
        if source == "5500-SF":
            for db_field, cands in fa.SCHEDULE_FINANCIAL_FIELDS.items():
                c = pick_col(headers, cands)
                if c:
                    rec["sf_" + db_field] = to_num(row.get(c))
        out[key] = rec
    print(f"    {source}: scanned {scanned:,} rows -> {len(out)} MA ESOP plans")
    return out


def index_schedule(reader, fields: dict, keep_keys=None) -> dict:
    """Index a Schedule H/I reader by (ein, pn) -> {db_field: value}."""
    headers = reader.fieldnames or []
    ein_c = pick_col(headers, ["SCH_H_EIN", "SCH_I_EIN", "SPONS_DFE_EIN", "EIN"])
    pn_c = pick_col(headers, ["SCH_H_PN", "SCH_I_PLAN_NUM", "SCH_I_PN", "PLAN_NUM", "PN"])
    if not ein_c:
        return {}
    fmap = {db: pick_col(headers, cands) for db, cands in fields.items()}
    out = {}
    for row in reader:
        key = norm_key(row.get(ein_c), row.get(pn_c) if pn_c else "1")
        if keep_keys is not None and key not in keep_keys:
            continue
        out[key] = {db: to_num(row.get(c)) for db, c in fmap.items() if c}
    return out


def load_schedule(year: int, kind: str, fields: dict, keep_keys, no_download: bool) -> dict:
    """Use the filtered CSV in data/form5500 when present, else the DOL bulk zip."""
    path = os.path.join(SCHEDULE_DIR, f"f_{kind}_{year}.csv")
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8-sig") as f:
            return index_schedule(csv.DictReader(f), fields)
    reader = fetch_zip_rows(year, SCH_VARIANTS[kind], no_download)
    if reader is None:
        return {}
    return index_schedule(reader, fields, keep_keys)


def authoritative_money(key, sch_h, sch_i, bulk_rec):
    """Repo precedence: Schedule H > Schedule I > 5500-SF."""
    if key in sch_h and any(v is not None for v in sch_h[key].values()):
        return sch_h[key], "SCH_H"
    if key in sch_i and any(v is not None for v in sch_i[key].values()):
        return sch_i[key], "SCH_I"
    sf = {f: bulk_rec.get("sf_" + f) for f in MONEY_FIELDS}
    if any(v is not None for v in sf.values()):
        return sf, "SF"
    return {}, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--import-new", action="store_true",
                    help="insert NEW/RETURNING filers into the DB (backs up first)")
    ap.add_argument("--no-download", action="store_true", help="reuse /tmp/dol cache")
    args = ap.parse_args()
    y = args.year

    print(f"=== DOL form-year {y}: download & filter ===")
    bulk = {}
    main_reader = fetch_zip_rows(y, [v for v in MAIN_VARIANTS], args.no_download)
    if main_reader:
        bulk.update(extract_ma_esops(main_reader, "F_5500"))
    sf_reader = fetch_zip_rows(y, [v for v in SF_VARIANTS], args.no_download)
    if sf_reader:
        for k, v in extract_ma_esops(sf_reader, "5500-SF").items():
            bulk.setdefault(k, v)  # full form wins if a plan somehow appears in both
    if not bulk:
        print(f"No MA ESOP rows found in DOL bulk data for {y} "
              f"(file may not be published yet).")
        sys.exit(0 if not main_reader and not sf_reader else 1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    db_year = {norm_key(r["ein"], r["plan_num"]): dict(r) for r in conn.execute(
        "SELECT * FROM form5500_filings WHERE filing_year=? AND is_esop=1", (y,))}
    db_eins_all = {norm_key(e, "1")[0] for (e,) in conn.execute(
        "SELECT DISTINCT ein FROM form5500_filings")}
    db_names_year = {norm_name(r["sponsor_name"]): r["ein"] for r in db_year.values()}

    new_rows, returning, dupes, excluded = [], [], [], []
    for key, rec in bulk.items():
        if key in db_year:
            continue
        if key[0] in EXCLUDED_EINS:
            excluded.append((key, rec, EXCLUDED_EINS[key[0]]))
            continue
        nm = norm_name(rec["sponsor_name"])
        same_name = db_names_year.get(nm)
        if same_name and norm_key(same_name, "1")[0] != key[0]:
            dupes.append((key, rec, same_name))
        elif key[0] in db_eins_all:
            returning.append((key, rec))
        else:
            new_rows.append((key, rec))
    gone = [k for k in db_year if k not in bulk]

    print(f"\n=== {y} coverage vs DB ===")
    print(f"  DOL bulk MA ESOP plans : {len(bulk)}")
    print(f"  in DB for {y}          : {len(db_year)}")
    print(f"  NEW (EIN never in DB)  : {len(new_rows)}")
    print(f"  RETURNING (known EIN, no {y} row): {len(returning)}")
    print(f"  possible cross-EIN dupes (NOT importable): {len(dupes)}")
    print(f"  deliberately excluded (see EXCLUDED_EINS): {len(excluded)}")
    print(f"  in DB but not in bulk  : {len(gone)}")
    for key, rec, why in excluded:
        print(f"    EXCLUDED: {rec['sponsor_name']} EIN={rec['ein_raw']} - {why}")
    for key, rec in new_rows:
        print(f"    NEW: {rec['sponsor_name']} ({rec['sponsor_city']}) EIN={rec['ein_raw']} "
              f"PN={rec['pn_raw']} partcp={rec['total_participants']} src={rec['source']}")
    for key, rec in returning:
        print(f"    RETURNING: {rec['sponsor_name']} ({rec['sponsor_city']}) EIN={rec['ein_raw']} src={rec['source']}")
    for key, rec, other in dupes:
        print(f"    DUPE?: {rec['sponsor_name']} EIN={rec['ein_raw']} matches existing {y} EIN={other}")
    for k in gone:
        r = db_year[k]
        print(f"    DB-only: {r['sponsor_name']} EIN={r['ein']} PN={r['plan_num']} "
              f"(amended away / withdrawn / different form year?)")

    # ── field verification for rows present in both ──
    bulk_keys = set(bulk)
    sch_h = load_schedule(y, "sch_h", fa.SCHEDULE_FINANCIAL_FIELDS, bulk_keys, args.no_download)
    sch_i = load_schedule(y, "sch_i", SCH_I_FIELDS, bulk_keys, args.no_download)
    print(f"\n=== {y} field verification (schedules: H={len(sch_h)} I={len(sch_i)}) ===")
    mismatches = []
    for key, rec in bulk.items():
        if key not in db_year:
            continue
        row = db_year[key]
        label = f"{row['sponsor_name']} (EIN {row['ein']})"
        for fld, bulk_val in (("total_participants", rec["total_participants"]),
                              ("active_participants", rec["active_participants"])):
            dbv = row[fld]
            if bulk_val is not None and dbv is not None and int(bulk_val) != int(dbv):
                mismatches.append((label, fld, dbv, int(bulk_val), "F_5500/SF"))
        if rec["sponsor_city"] and row["sponsor_city"] and \
                rec["sponsor_city"].strip().lower() != str(row["sponsor_city"]).strip().lower():
            mismatches.append((label, "sponsor_city", row["sponsor_city"], rec["sponsor_city"], "F_5500/SF"))
        if rec["naics_code"] and row["naics_code"] and rec["naics_code"] != str(row["naics_code"]):
            mismatches.append((label, "naics_code", row["naics_code"], rec["naics_code"], "F_5500/SF"))
        money, src = authoritative_money(key, sch_h, sch_i, rec)
        for fld in MONEY_FIELDS:
            av = money.get(fld)
            dbv = row[fld]
            if av is None or dbv is None:
                continue
            if abs(av - dbv) > 1.0:
                mismatches.append((label, fld, dbv, av, src))
    if mismatches:
        print(f"  {len(mismatches)} field mismatches:")
        for label, fld, dbv, av, src in mismatches:
            print(f"    {label}: {fld} DB={dbv} DOL={av} [{src}]")
    else:
        print("  all common rows match DOL source values (within $1) ✓")

    # ── report CSV ──
    report = os.path.join(os.path.dirname(__file__) or ".", f"dol_scan_{y}_report.csv")
    with open(report, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "sponsor_name", "city", "ein", "plan_num", "source",
                    "participants", "detail"])
        for key, rec in new_rows:
            w.writerow(["NEW", rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                        rec["pn_raw"], rec["source"], rec["total_participants"], ""])
        for key, rec in returning:
            w.writerow(["RETURNING", rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                        rec["pn_raw"], rec["source"], rec["total_participants"], ""])
        for key, rec, other in dupes:
            w.writerow(["POSSIBLE_DUPE", rec["sponsor_name"], rec["sponsor_city"],
                        rec["ein_raw"], rec["pn_raw"], rec["source"],
                        rec["total_participants"], f"name-matches existing EIN {other}"])
        for key, rec, why in excluded:
            w.writerow(["EXCLUDED", rec["sponsor_name"], rec["sponsor_city"],
                        rec["ein_raw"], rec["pn_raw"], rec["source"],
                        rec["total_participants"], why])
        for k in gone:
            r = db_year[k]
            w.writerow(["DB_ONLY", r["sponsor_name"], r["sponsor_city"], r["ein"],
                        r["plan_num"], "db", r["total_participants"], "not in fresh bulk"])
        for label, fld, dbv, av, src in mismatches:
            w.writerow(["FIELD_MISMATCH", label, "", "", "", src, "", f"{fld}: DB={dbv} DOL={av}"])
    print(f"\nReport: {report}")

    # ── optional import ──
    if args.import_new and (new_rows or returning):
        bak = DB_PATH + f".pre-{y}import.bak"
        if not os.path.exists(bak):
            shutil.copy2(DB_PATH, bak)
            print(f"DB backed up -> {bak}")
        now = datetime.now(timezone.utc).isoformat()
        records = []
        for key, rec in new_rows + returning:
            codes = rec["codes"]
            money, src = authoritative_money(key, sch_h, sch_i, rec)
            sector = fa.NAICS_SECTORS.get(rec["naics_code"][:2], "") if rec["naics_code"] else ""
            records.append({
                "filing_year": y,
                "ein": key[0], "plan_num": rec["pn_raw"] or "001",
                "plan_name": rec["plan_name"], "sponsor_name": rec["sponsor_name"],
                "sponsor_city": rec["sponsor_city"], "sponsor_state": "MA",
                "sponsor_zip": rec["sponsor_zip"], "type_plan_entity": rec["type_plan_entity"],
                "type_pension_bnft": codes,
                "is_esop": 1,
                "is_ksop": 1 if ("2J" in codes and "2K" in codes) else 0,
                "total_participants": int(rec["total_participants"]) if rec["total_participants"] is not None else None,
                "active_participants": int(rec["active_participants"]) if rec["active_participants"] is not None else None,
                "total_assets": money.get("total_assets"),
                "total_liabilities": money.get("total_liabilities"),
                "employer_contributions": money.get("employer_contributions"),
                "participant_contributions": money.get("participant_contributions"),
                "benefits_paid": money.get("benefits_paid"),
                "net_income": money.get("net_income"),
                "naics_code": rec["naics_code"], "industry_sector": sector,
                "plan_eff_date": rec["plan_eff_date"], "fetched": now,
                "employer_securities": (sch_h.get(key) or {}).get("employer_securities"),
            })
        cur = conn.executemany("""
            INSERT OR IGNORE INTO form5500_filings
            (filing_year, ein, plan_num, plan_name, sponsor_name, sponsor_city,
             sponsor_state, sponsor_zip, type_plan_entity, type_pension_bnft,
             is_esop, is_ksop, total_participants, active_participants,
             total_assets, total_liabilities, employer_contributions,
             participant_contributions, benefits_paid, net_income,
             naics_code, industry_sector, plan_eff_date, fetched, employer_securities)
            VALUES
            (:filing_year, :ein, :plan_num, :plan_name, :sponsor_name, :sponsor_city,
             :sponsor_state, :sponsor_zip, :type_plan_entity, :type_pension_bnft,
             :is_esop, :is_ksop, :total_participants, :active_participants,
             :total_assets, :total_liabilities, :employer_contributions,
             :participant_contributions, :benefits_paid, :net_income,
             :naics_code, :industry_sector, :plan_eff_date, :fetched, :employer_securities)
        """, records)
        conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                     (f"filers_{y}_imported", now))
        conn.commit()
        print(f"Imported {cur.rowcount} new {y} filings "
              f"(annual_summary untouched; possible dupes excluded).")
    conn.close()


if __name__ == "__main__":
    main()
