#!/usr/bin/env python3
"""
Curated import of form-year-2025 MA ESOP filers from F_5500_2025_Latest.zip.

The 2025 file holds 9 MA ESOP-coded rows (June 2026 refresh). Two are excluded:
  - EIN 043067724 "EASTERN BANK" PN 003: cross-EIN duplicate. The identical
    filing (485 participants, plan year ending 2025-11-30) is also submitted
    under EIN 042777442 "CAMBRIDGE BANCORP" — the EIN that carries this plan's
    2014-2024 history in our DB. The Cambridge-EIN copy is imported; the
    Eastern-EIN copy is skipped (same dedup rule used across the dataset).
  - EIN 041983105 Colonial Federal PN 004 with plan year 2026-01-01→2026-04-30:
    a plan-year-2026 termination filing — belongs to filing_year 2026, not 2025.
    (Colonial's full-year 2025 row IS imported.)

Financials come from F_SCH_H_2025 / F_SCH_I_2025 (cached in /tmp/dol), with the
same precedence and employer_securities integrity guards as the main pipeline.
Idempotent; form5500_annual_summary is left untouched.
"""
import csv
import io
import os
import sqlite3
import zipfile
from datetime import datetime, timezone

import form5500_analysis as fa
from scan_dol_filers import SCH_I_FIELDS, norm_key, pick_col, to_num

import safe_console

safe_console.enable_utf8_stdout()

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")
CACHE = "/tmp/dol"

EXCLUDE_EINS = {"43067724"}          # Eastern-EIN copy of the Cambridge filing


def rows_from_zip(path):
    zf = zipfile.ZipFile(path)
    inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig",
                                           errors="replace"))


def main():
    rdr = rows_from_zip(os.path.join(CACHE, "2025_Latest_F_5500_2025_Latest.zip"))
    picked, skipped = {}, []
    for row in rdr:
        if str(row.get("SPONS_DFE_MAIL_US_STATE", "")).strip().upper() != "MA":
            continue
        codes = str(row.get("TYPE_PENSION_BNFT_CODE") or "").upper()
        if "2O" not in codes and "2P" not in codes:
            continue
        key = norm_key(row.get("SPONS_DFE_EIN"), row.get("SPONS_DFE_PN"))
        yb = str(row.get("FORM_PLAN_YEAR_BEGIN_DATE") or "")
        if key[0] in EXCLUDE_EINS:
            skipped.append((row.get("SPONSOR_DFE_NAME"), "cross-EIN dupe of Cambridge Bancorp"))
            continue
        if yb >= "2026":
            skipped.append((row.get("SPONSOR_DFE_NAME"), f"plan year {yb} -> filing_year 2026"))
            continue
        picked[key] = row
    print(f"2025 rows picked: {len(picked)}, skipped: {len(skipped)}")
    for nm, why in skipped:
        print(f"  skipped {str(nm).strip()!r}: {why}")

    # financials from the 2025 schedule files
    fins = {}
    for kind, fields, datecol in (("F_SCH_H", fa.SCHEDULE_FINANCIAL_FIELDS,
                                   "SCH_H_PLAN_YEAR_BEGIN_DATE"),
                                  ("F_SCH_I", SCH_I_FIELDS,
                                   "SCH_I_PLAN_YEAR_BEGIN_DATE")):
        path = os.path.join(CACHE, f"2025_Latest_{kind}_2025_Latest.zip")
        if not os.path.exists(path):
            continue
        srdr = rows_from_zip(path)
        headers = srdr.fieldnames or []
        ein_c = pick_col(headers, ["SCH_H_EIN", "SCH_I_EIN", "EIN"])
        pn_c = pick_col(headers, ["SCH_H_PN", "SCH_I_PLAN_NUM", "SCH_I_PN", "PN"])
        fmap = {db: pick_col(headers, cands) for db, cands in fields.items()}
        for row in srdr:
            key = norm_key(row.get(ein_c), row.get(pn_c) if pn_c else "1")
            if key in picked and key not in fins:
                fins[key] = {db: to_num(row.get(c)) for db, c in fmap.items() if c}
                print(f"  financials [{kind}] {key[0]}: "
                      f"{ {k: v for k, v in fins[key].items() if v is not None} }")

    conn = sqlite3.connect(DB)
    now = datetime.now(timezone.utc).isoformat()
    n = 0
    for key, row in picked.items():
        codes = str(row.get("TYPE_PENSION_BNFT_CODE") or "").upper()
        money = fins.get(key, {})
        es, ta = money.get("employer_securities"), money.get("total_assets")
        if es is not None and (es <= 0 or (ta is not None and es > ta)):
            es = None
        naics = str(row.get("BUSINESS_CD") or "").strip()
        rec = {
            "filing_year": 2025, "ein": key[0],
            "plan_num": str(row.get("SPONS_DFE_PN") or "").strip() or "001",
            "plan_name": str(row.get("PLAN_NAME") or "").strip(),
            "sponsor_name": str(row.get("SPONSOR_DFE_NAME") or "").strip(),
            "sponsor_city": str(row.get("SPONS_DFE_MAIL_US_CITY") or "").strip(),
            "sponsor_state": "MA",
            "sponsor_zip": str(row.get("SPONS_DFE_MAIL_US_ZIP") or "").strip(),
            "type_plan_entity": str(row.get("TYPE_PLAN_ENTITY_CD") or "").strip(),
            "type_pension_bnft": codes,
            "is_esop": 1,
            "is_ksop": 1 if ("2J" in codes and "2K" in codes) else 0,
            "total_participants": to_num(row.get("TOT_PARTCP_BOY_CNT")),
            "active_participants": to_num(row.get("TOT_ACTIVE_PARTCP_CNT")),
            "total_assets": ta,
            "total_liabilities": money.get("total_liabilities"),
            "employer_contributions": money.get("employer_contributions"),
            "participant_contributions": money.get("participant_contributions"),
            "benefits_paid": money.get("benefits_paid"),
            "net_income": money.get("net_income"),
            "naics_code": naics,
            "industry_sector": fa.NAICS_SECTORS.get(naics[:2], "") if naics else "",
            "plan_eff_date": str(row.get("PLAN_EFF_DATE") or "").strip(),
            "fetched": now, "employer_securities": es,
        }
        for k in ("total_participants", "active_participants"):
            if rec[k] is not None:
                rec[k] = int(rec[k])
        cols = ", ".join(rec)
        ph = ", ".join(":" + c for c in rec)
        cur = conn.execute(
            f"INSERT OR IGNORE INTO form5500_filings ({cols}) VALUES ({ph})", rec)
        n += cur.rowcount
        print(f"  {'imported' if cur.rowcount else 'already present'}: "
              f"{rec['sponsor_name']} ({rec['sponsor_city']}) tot={rec['total_participants']} "
              f"act={rec['active_participants']} assets={rec['total_assets']}")
    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 ("filers_2025_imported", now))
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM form5500_filings "
                         "WHERE filing_year=2025").fetchone()[0]
    print(f"inserted {n}; DB now has {total} filing_year=2025 rows")
    conn.close()


if __name__ == "__main__":
    main()
