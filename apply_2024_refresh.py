#!/usr/bin/env python3
"""
One-off maintenance: apply the verified 2024 corrections found by scan_dol_filers.py
against the June 2026 DOL F_5500_2024_Latest refresh.

Actions (all verified against raw DOL filings — see dol_scan_2024_report.csv):
  1. DELETE Green Tech Enterprises 2024 PN=1 row — superseded original filing
     (received 2025-10-15) with erroneous financials ($54.1M liabilities on an
     $8.7M plan, -$45.5M net income). The company refiled the same plan year as
     PN=002 on 2026-04-21 with corrected values; that row stays. Same pattern as
     prior phantom/dupe removals.
  2. FIX Web Industries 2024 employer_contributions 0 -> 4,482,639
     (Schedule H, EMPLR_CONTRIB_INCOME_AMT, plan year beginning 2024-10-01).
  3. IMPORT Brockway-Smith Company 2024 (EIN 041123740, PN 003) — private MA
     company whose plan carries ESOP code 2O; filed 2025-07-07; known EIN that
     lapsed and returned. Financials pulled from the 2024 Schedule I/H bulk data.
  4. Recompute annual summaries so 2024 counts/totals reflect 1-3.

Deliberately NOT done here (user decisions, see report):
  - Cabot / Waters / Crane NXT / Eastern Bank (Cambridge Bancorp EIN): publicly
    traded sponsors -> investor-owned exclusion question.
  - Janis Research & OEM Connect 2024 rows: latest DOL filings dropped the
    ESOP pension codes (now 2Q / 2Q3D); rows kept for history.
  - Harpoon 2024 stays 346 participants: the 11-participant filing is the
    plan-year-2025 short-year termination filing (imported as 2025 instead).
"""
import csv
import io
import os
import re
import shutil
import sqlite3
import zipfile
from datetime import datetime, timezone

import form5500_analysis as fa
from scan_dol_filers import (CACHE_DIR, SCH_VARIANTS, SCH_I_FIELDS, fetch_zip_rows,
                             index_schedule, norm_key)

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

BROCKWAY = {
    "filing_year": 2024, "ein": "41123740", "plan_num": "003",
    "plan_name": "BROCKWAY-SMITH COMPANY PROFIT SHARING PLAN & TRUST",
    "sponsor_name": "BROCKWAY-SMITH COMPANY", "sponsor_city": "ANDOVER",
    "sponsor_state": "MA", "sponsor_zip": "", "type_plan_entity": "2",
    "type_pension_bnft": "2I2O", "is_esop": 1, "is_ksop": 0,
    "total_participants": 66, "active_participants": 57,
    "total_assets": None, "total_liabilities": None,
    "employer_contributions": None, "participant_contributions": None,
    "benefits_paid": None, "net_income": None,
    "naics_code": "", "industry_sector": "",
    "plan_eff_date": "", "employer_securities": None,
}


def main():
    bak = DB + ".pre-2024refresh.bak"
    if not os.path.exists(bak):
        shutil.copy2(DB, bak)
        print(f"backup -> {bak}")

    # Brockway financials from the full 2024 schedule bulk files (the filtered
    # CSVs in data/form5500 predate this EIN's return, so go to the source).
    keys = {norm_key("41123740", "003"),
            norm_key("43298009", "002"), norm_key("43298009", "003")}
    sch_h = index_schedule(fetch_zip_rows(2024, SCH_VARIANTS["sch_h"], False),
                           fa.SCHEDULE_FINANCIAL_FIELDS, keys) or {}
    sch_i = index_schedule(fetch_zip_rows(2024, SCH_VARIANTS["sch_i"], False),
                           SCH_I_FIELDS, keys) or {}
    key = norm_key("41123740", "003")
    money = sch_h.get(key) or sch_i.get(key) or {}
    src = "SCH_H" if key in sch_h else ("SCH_I" if key in sch_i else "none")
    print(f"Brockway-Smith schedule financials [{src}]: "
          f"{ {k: v for k, v in money.items() if v is not None} }")
    for fld in ("total_assets", "total_liabilities", "employer_contributions",
                "participant_contributions", "benefits_paid", "net_income"):
        if money.get(fld) is not None:
            BROCKWAY[fld] = money[fld]
    if money.get("employer_securities") and BROCKWAY["total_assets"] and \
            money["employer_securities"] <= BROCKWAY["total_assets"]:
        BROCKWAY["employer_securities"] = money["employer_securities"]

    # informational: did Northern Construction's Dec PN=003 refile change values?
    for pn in ("002", "003"):
        k = norm_key("43298009", pn)
        vals = sch_h.get(k) or sch_i.get(k)
        if vals:
            print(f"Northern Construction PN={pn} schedule values: "
                  f"{ {f: v for f, v in vals.items() if v is not None} }")

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    # 1. delete superseded Green Tech PN=1 2024 row
    cur = conn.execute(
        "DELETE FROM form5500_filings WHERE filing_year=2024 AND ein='332432954' "
        "AND plan_num='1'")
    print(f"Green Tech PN=1 2024 superseded row deleted: {cur.rowcount}")

    # 2. Web Industries employer_contributions
    cur = conn.execute(
        "UPDATE form5500_filings SET employer_contributions=4482639 "
        "WHERE filing_year=2024 AND ein='42880295' AND employer_contributions=0")
    print(f"Web Industries employer_contributions fixed: {cur.rowcount}")

    # 3. import Brockway-Smith
    BROCKWAY["fetched"] = datetime.now(timezone.utc).isoformat()
    cols = ", ".join(BROCKWAY)
    ph = ", ".join(":" + c for c in BROCKWAY)
    cur = conn.execute(
        f"INSERT OR IGNORE INTO form5500_filings ({cols}) VALUES ({ph})", BROCKWAY)
    print(f"Brockway-Smith 2024 imported: {cur.rowcount}")

    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 ("refresh_2024_applied", datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

    # 4. recompute summaries (uses its own connection)
    fa.recompute_annual_summaries()
    print("annual summaries recomputed")

    conn = sqlite3.connect(DB)
    n, p, a = conn.execute(
        "SELECT COUNT(*), SUM(total_participants), SUM(total_assets) "
        "FROM form5500_filings WHERE filing_year=2024 AND is_esop=1").fetchone()
    print(f"2024 now: {n} plans, {p:,} participants, ${a:,.0f} assets")
    conn.close()


if __name__ == "__main__":
    main()
