#!/usr/bin/env python3
"""
Import plan-year-2025 MA ESOP filings that live in the 2024-form DOL bulk files.

Short/final plan years are often filed on the prior year's form version, so they
appear in F_5500_2024_Latest.zip with FORM_PLAN_YEAR_BEGIN_DATE in 2025. These
belong to filing_year=2025 in our DB (plan-year basis, same convention as
refetch_dol_financials). Found by scan: Harpoon Brewery (terminated 2025-03-08)
and B&B Engineering / L.W. Bills (terminated 2025-08-07) — both final filings.

Financials come from the 2024-form Schedule H/I bulk files, matching rows whose
SCH_*_PLAN_YEAR_BEGIN_DATE is also in 2025. Idempotent: INSERT OR IGNORE on
(filing_year, ein, plan_num); annual_summary is left untouched.
"""
import csv
import io
import os
import re
import sqlite3
import zipfile
from datetime import datetime, timezone

import form5500_analysis as fa
from scan_dol_filers import SCH_I_FIELDS, norm_key, pick_col, to_num

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")
CACHE = "/tmp/dol"


def rows_from_zip(path):
    zf = zipfile.ZipFile(path)
    inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig",
                                           errors="replace"))


def main():
    # 1. plan-year-2025 MA ESOP rows in the 2024 main form file
    rdr = rows_from_zip(os.path.join(CACHE, "2024_Latest_F_5500_2024_Latest.zip"))
    strays = {}
    for row in rdr:
        if str(row.get("SPONS_DFE_MAIL_US_STATE", "")).strip().upper() != "MA":
            continue
        codes = str(row.get("TYPE_PENSION_BNFT_CODE") or "").upper()
        if "2O" not in codes and "2P" not in codes:
            continue
        if str(row.get("FORM_PLAN_YEAR_BEGIN_DATE") or "") < "2025":
            continue
        key = norm_key(row.get("SPONS_DFE_EIN"), row.get("SPONS_DFE_PN"))
        strays[key] = row
    print(f"plan-year-2025 MA ESOP rows in 2024-form file: {len(strays)}")

    # 2. their Schedule H/I financials (2024-form schedule files, 2025 plan years)
    fins = {}
    for kind, fields, datecol in (("F_SCH_H", fa.SCHEDULE_FINANCIAL_FIELDS,
                                   "SCH_H_PLAN_YEAR_BEGIN_DATE"),
                                  ("F_SCH_I", SCH_I_FIELDS,
                                   "SCH_I_PLAN_YEAR_BEGIN_DATE")):
        path = os.path.join(CACHE, f"2024_Latest_{kind}_2024_Latest.zip")
        if not os.path.exists(path):
            continue
        rdr = rows_from_zip(path)
        headers = rdr.fieldnames or []
        ein_c = pick_col(headers, ["SCH_H_EIN", "SCH_I_EIN", "EIN"])
        pn_c = pick_col(headers, ["SCH_H_PN", "SCH_I_PN", "PN"])
        fmap = {db: pick_col(headers, cands) for db, cands in fields.items()}
        for row in rdr:
            if str(row.get(datecol) or "") < "2025":
                continue
            key = norm_key(row.get(ein_c), row.get(pn_c) if pn_c else "1")
            if key in strays and key not in fins:   # H scanned first -> H wins
                fins[key] = {db: to_num(row.get(c)) for db, c in fmap.items() if c}
                print(f"  financials [{kind}] for EIN {key[0]}: "
                      f"{ {k: v for k, v in fins[key].items() if v is not None} }")

    # 3. insert
    conn = sqlite3.connect(DB)
    now = datetime.now(timezone.utc).isoformat()
    n = 0
    for key, row in strays.items():
        codes = str(row.get("TYPE_PENSION_BNFT_CODE") or "").upper()
        money = fins.get(key, {})
        es = money.get("employer_securities")
        ta = money.get("total_assets")
        if es is not None and (es <= 0 or (ta is not None and es > ta)):
            es = None   # same integrity guard as the main pipeline
        naics = str(row.get("BUSINESS_CD") or "").strip()
        rec = {
            "filing_year": 2025, "ein": key[0],
            "plan_num": str(row.get("SPONS_DFE_PN") or "").strip() or "001",
            "plan_name": str(row.get("PLAN_NAME") or "").strip(),
            "sponsor_name": str(row.get("SPONSOR_DFE_NAME") or
                                row.get("SPONS_DFE_NAME") or "").strip(),
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
              f"{rec['sponsor_name'] or rec['plan_name']} "
              f"({rec['sponsor_city']}) PN={rec['plan_num']} "
              f"plan year ended {row.get('FORM_TAX_PRD')}")
    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 ("filers_2025_strays_imported", now))
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM form5500_filings "
                         "WHERE filing_year=2025").fetchone()[0]
    print(f"inserted {n}; DB now has {total} filing_year=2025 rows")
    conn.close()


if __name__ == "__main__":
    main()
