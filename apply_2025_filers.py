#!/usr/bin/env python3
"""
Bring form year 2025 up to date with DOL, and fix two stale hardcoded lists that
the review surfaced.

Reads the 2025 bulk files at run time rather than carrying copied figures, so
re-running against a newer DOL release picks up what has arrived since.

WHAT GETS IMPORTED
------------------
Every MA filing for plan year 2025 carrying an ESOP code (2O / 2P / 2Q) that the
database does not already hold — 22 plans as of the 2026-07 release, taking 2025
from 9 tracked filings to 31. Four are companies new to the dataset entirely:
BostonBean Coffee, Helical Drilling, Wildlife Acoustics, and FE Holdings (whose
plan is named for Intransit Container).

Financials come from the 2025 Schedule H / Schedule I files, Schedule H winning
where a plan filed both, matching the precedence used everywhere else.

MATCHING RULES THIS SCRIPT RELIES ON
------------------------------------
  - Schedule rows are filtered on SCH_*_PLAN_YEAR_BEGIN_DATE starting with 2025.
    The 2025 bulk files also contain rows for other plan years, and joining on
    (EIN, plan number) alone silently pairs a plan with the wrong year.
  - Main-form rows are likewise restricted to plan years beginning in 2025. This
    is what keeps Colonial Federal's plan-year-2026 final return out: it shares
    an EIN and plan number with the 2025 return already in the database, so
    without the filter it would overwrite a live plan with a wind-up row.

EASTERN BANK IS NOT A DUPLICATE THIS TIME
-----------------------------------------
Eastern Bank's ESOP is imported, reversing the treatment it got for 2024. The
two years are genuinely different filings. For 2024, EIN 043067724 filed a
return literally named "CAMBRIDGE BANCORP EMPLOYEE STOCK OWNERSHIP PLAN" whose
LAST_RPT_SPONS_EIN pointed at Cambridge's own EIN — the same plan filed under
the acquirer's number, correctly excluded as a cross-EIN duplicate. The 2025
return under that EIN is named "EASTERN BANK EMPLOYEE STOCK OWNERSHIP PLAN",
carries no prior-EIN link, and reports 2,202 participants and $273.2M. That is
Eastern's own plan, which this dataset tracked from 2020 to 2023 (1,786 ->
1,904 participants) and which filed nothing for 2024. Cambridge's own plan files
separately for 2025 as a final amended return (485 participants) and is already
in the database.

TWO STALE LISTS CORRECTED
-------------------------
1. Process Cooling Systems (EIN 042760904) is removed from ZOMBIE_PLAN_EINS.
   The list describes it as "$0 assets, 59 active", but its 2024 return reports
   67 participants, 59 active and $10,174,947 in assets — up from $1.3M — and it
   is not a final filing. It filed again for 2025 with 53 active and $8.9M. The
   "$0" almost certainly dates from before refetch_dol_financials.py corrected
   small-plan Schedule I assets, which that script documents as having left
   "~640 plan-years materially understated"; the exclusion list was never
   revisited afterwards. Because the list excludes by EIN across all years, this
   was dropping a live ESOP from every "active" figure. Removing it raises 2024
   from 114 active plans to 115.

   The other five entries stay. All report zero active participants, so the
   `COALESCE(active_participants, 0) > 0` clause already excludes them; the list
   is a documented safety net should a wind-down plan report stray actives.

2. Eastern Bank (EIN 043067724) is added to Tier 3 on the Year-over-Year page.
   It filed in 2023 but not 2024 and had never been researched, so it showed as
   the single "unverified" late filer. Its 2025 return is positive evidence the
   plan is active, so the classification is no longer unknown.

PLAN NUMBERS ARE STORED UNPADDED
--------------------------------
The nine 2025 rows already present were written with zero-padded plan numbers
("003") by import_2025_filers.py, while every year from 2014 to 2024 stores them
unpadded ("3"). The Year-over-Year page joins on (EIN, plan_num) exactly, so
leaving both conventions in place would break every 2024 -> 2025 comparison the
moment 2025 becomes a complete year. This normalizes the existing rows and
writes new ones unpadded.

Idempotent.
"""
import csv
import io
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import datetime, timezone

import config
import form5500_analysis as fa
import safe_console

safe_console.enable_utf8_stdout()

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")
CACHE = "C:/tmp/dol"
YEAR = 2025

# Companies new to the dataset have no prior row to inherit casing from, and
# .title() mangles initialisms ("FE" -> "Fe") and camel-cased brands.
NEW_COMPANY_NAMES = {
    "43444078": ("BostonBean Coffee Company, Inc.", "Woburn"),
    "364615460": ("Helical Drilling, Inc.", "Braintree"),
    "200508239": ("Wildlife Acoustics, Inc.", "Maynard"),
    "921304786": ("FE Holdings, Inc.", "Mansfield"),
}

H_FIELDS = {"total_assets": "TOT_ASSETS_EOY_AMT",
            "total_liabilities": "TOT_LIABILITIES_EOY_AMT",
            "employer_contributions": "EMPLR_CONTRIB_INCOME_AMT",
            "participant_contributions": "PARTICIPANT_CONTRIB_AMT",
            "benefits_paid": "TOT_DISTRIB_BNFT_AMT",
            "net_income": "NET_INCOME_AMT",
            "employer_securities": "EMPLR_SEC_EOY_AMT"}
I_FIELDS = {"total_assets": "SMALL_TOT_ASSETS_EOY_AMT",
            "total_liabilities": "SMALL_TOT_LIABILITIES_EOY_AMT",
            "employer_contributions": "SMALL_EMPLR_CONTRIB_INCOME_AMT",
            "participant_contributions": "SMALL_PARTICIPANT_CONTRIB_AMT",
            "benefits_paid": "SMALL_TOT_DISTRIB_BNFT_AMT",
            "net_income": "SMALL_NET_INCOME_AMT"}


def norm_ein(v):
    return re.sub(r"\D", "", str(v or "")).lstrip("0")


def norm_pn(v):
    return re.sub(r"\D", "", str(v or "")).lstrip("0") or "1"


def num(v):
    s = str(v).strip().replace(",", "") if v is not None else ""
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def rows_of(path):
    zf = zipfile.ZipFile(path)
    inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig",
                                           errors="replace"))


def load_schedules():
    """(ein, pn) -> financials, Schedule H preferred, plan year 2025 only."""
    out = {}
    for kind, einc, pnc, dcol, fields in (
            ("SCH_I", "SCH_I_EIN", "SCH_I_PLAN_NUM", "SCH_I_PLAN_YEAR_BEGIN_DATE", I_FIELDS),
            ("SCH_H", "SCH_H_EIN", "SCH_H_PN", "SCH_H_PLAN_YEAR_BEGIN_DATE", H_FIELDS)):
        path = os.path.join(CACHE, f"{YEAR}_Latest_F_{kind}_{YEAR}_Latest.zip")
        if not os.path.exists(path):
            print(f"  ! missing {path} — financials from {kind} unavailable")
            continue
        for r in rows_of(path):
            if str(r.get(dcol) or "")[:4] != str(YEAR):
                continue
            vals = {f: num(r.get(c)) for f, c in fields.items()}
            if vals.get("total_assets") is None:
                continue
            out[(norm_ein(r.get(einc)), norm_pn(r.get(pnc)))] = vals  # H loaded last, so H wins
    return out


def main():
    bak = DB + f".pre-{YEAR}import.bak"
    if not os.path.exists(bak):
        shutil.copy2(DB, bak)
        print(f"backup -> {bak}")

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # ── normalize the padded plan numbers already stored for 2025 ──
    fixed = 0
    for r in conn.execute(f"SELECT id, plan_num FROM form5500_filings "
                          f"WHERE filing_year={YEAR}").fetchall():
        want = norm_pn(r["plan_num"])
        if want != r["plan_num"]:
            conn.execute("UPDATE form5500_filings SET plan_num=? WHERE id=?", (want, r["id"]))
            fixed += 1
    print(f"plan numbers unpadded on existing {YEAR} rows: {fixed}")

    existing = {(norm_ein(r["ein"]), norm_pn(r["plan_num"]))
                for r in conn.execute(f"SELECT ein, plan_num FROM form5500_filings "
                                      f"WHERE filing_year={YEAR}")}
    sched = load_schedules()

    main_path = os.path.join(CACHE, f"{YEAR}_Latest_F_5500_{YEAR}_Latest.zip")
    now = datetime.now(timezone.utc).isoformat()
    imported = skipped_year = 0

    for r in rows_of(main_path):
        if str(r.get("SPONS_DFE_MAIL_US_STATE", "")).strip().upper() != "MA":
            continue
        codes = str(r.get("TYPE_PENSION_BNFT_CODE") or "").upper()
        if not any(c in codes for c in ("2O", "2P", "2Q")):
            continue
        if str(r.get("FORM_PLAN_YEAR_BEGIN_DATE") or "")[:4] != str(YEAR):
            skipped_year += 1
            continue
        ein, pn = norm_ein(r.get("SPONS_DFE_EIN")), norm_pn(r.get("SPONS_DFE_PN"))
        if (ein, pn) in existing:
            continue

        money = sched.get((ein, pn), {})
        es, ta = money.get("employer_securities"), money.get("total_assets")
        if es is not None and (es <= 0 or (ta is not None and es > ta)):
            es = None  # same integrity guard the rest of the pipeline applies

        prior = conn.execute(
            "SELECT sponsor_name, sponsor_city, naics_code, industry_sector "
            "FROM form5500_filings WHERE ein=? AND filing_year<? "
            "ORDER BY filing_year DESC LIMIT 1", (ein, YEAR)).fetchone()
        if prior:
            sponsor, city = prior["sponsor_name"], prior["sponsor_city"]
        elif ein in NEW_COMPANY_NAMES:
            sponsor, city = NEW_COMPANY_NAMES[ein]
        else:
            sponsor = str(r.get("SPONSOR_DFE_NAME") or "").strip().title()
            city = str(r.get("SPONS_DFE_MAIL_US_CITY") or "").strip().title()

        naics = str(r.get("BUSINESS_CODE") or "").strip() or (prior["naics_code"] if prior else "")
        sector = fa.NAICS_SECTORS.get(naics[:2], "") if naics else ""
        if not sector and prior:
            sector = prior["industry_sector"]

        rec = {
            "filing_year": YEAR, "ein": ein, "plan_num": pn,
            "plan_name": str(r.get("PLAN_NAME") or "").strip(),
            "sponsor_name": sponsor, "sponsor_city": city, "sponsor_state": "MA",
            "sponsor_zip": str(r.get("SPONS_DFE_MAIL_US_ZIP") or "").strip(),
            "type_plan_entity": str(r.get("TYPE_PLAN_ENTITY_CD") or "").strip(),
            "type_pension_bnft": codes,
            "is_esop": 1,
            "is_ksop": 1 if ("2J" in codes and "2K" in codes) else 0,
            "total_participants": int(num(r.get("TOT_PARTCP_BOY_CNT")) or 0),
            "active_participants": int(num(r.get("TOT_ACTIVE_PARTCP_CNT")) or 0),
            "total_assets": money.get("total_assets"),
            "total_liabilities": money.get("total_liabilities"),
            "employer_contributions": money.get("employer_contributions"),
            "participant_contributions": money.get("participant_contributions"),
            "benefits_paid": money.get("benefits_paid"),
            "net_income": money.get("net_income"),
            "naics_code": naics, "industry_sector": sector,
            "plan_eff_date": str(r.get("PLAN_EFF_DATE") or "").strip(),
            "fetched": now, "employer_securities": es,
        }
        cols = ", ".join(rec)
        ph = ", ".join(":" + c for c in rec)
        cur = conn.execute(
            f"INSERT OR IGNORE INTO form5500_filings ({cols}) VALUES ({ph})", rec)
        if cur.rowcount:
            imported += 1
            existing.add((ein, pn))
            a = rec["total_assets"]
            print(f"  + {sponsor[:34]:34} PN={pn:3} tot={rec['total_participants']:>5} "
                  f"act={rec['active_participants']:>5} "
                  f"assets={('$%s' % format(a, ',.0f')) if a is not None else '-'}")

    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 (f"filers_{YEAR}_imported", now))
    conn.commit()
    n = conn.execute(f"SELECT COUNT(*) FROM form5500_filings "
                     f"WHERE filing_year={YEAR}").fetchone()[0]
    conn.close()
    print(f"\nimported {imported}; skipped {skipped_year} row(s) for other plan years; "
          f"{YEAR} now holds {n} filings")

    # Summaries deliberately NOT recomputed for the open year — see
    # form5500_analysis.recompute_annual_summaries(), which excludes it by design.
    fa.recompute_annual_summaries()
    print("annual summaries recomputed (open year still excluded)")


if __name__ == "__main__":
    main()
