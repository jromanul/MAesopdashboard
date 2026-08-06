#!/usr/bin/env python3
"""
Import genuine FY2024 late filers found by a fresh scan_dol_filers.py --year 2024
run (2026-08-06) against DOL's F_5500_2024_Latest.zip, which now includes several
plans that hadn't been published yet as of the May 2026 refresh.

All seven are fiscal-year filers (plan year Oct 2024 - Sep 2025, DOL bucket
"2024") whose extended filing deadline (~9.5 months after PY end = ~mid-2026)
only just passed; DATE_RECEIVED for each is 2026-06-03 through 2026-07-19.
Each continues an unbroken filing history already in the DB (verified by EIN),
so financials come straight from Schedule H (large plans) / Schedule I (small
plans) in the fresh 2024 bulk data, matching the repo's existing precedence.

Two of these seven directly correct standing errors in the Year-over-Year
"Terminated ESOPs" (Tier 1) classification, which was researched as of the May
2026 vintage and is now factually out of date:
  - International Data Group (EIN 042597651): the Tier-1 note claims Blackstone's
    2021 acquisition ended the ESOP ("filed 2024 401(k)+welfare, no ESOP"). The
    FY2024 filing that has now arrived shows the ESOP (PN=001) is still active:
    $3.17M -> $3.00M assets, 530 active participants, not a final return. The
    prior "no ESOP filed" conclusion was a filing-lag false negative, not
    evidence of termination.
  - Barclay Water Treatment (EIN 042772059): the Tier-1 note ("Acquired by
    Ecolab Nov 2024, cashed out at close") is directionally correct - the
    FY2024 filing shows assets fell from $121.0M (BOY) to $37.3M (EOY),
    $227.3M in benefits paid out, and active participants dropped to 0 - but
    the plan is NOT fully wound down (FINAL_FILING_IND=0, $37.3M still held).
    Removing it from the hardcoded terminated list and importing its real
    numbers is a strictly more accurate representation than the prior
    hand-written note using stale 2023 figures.
Once imported, both EINs' 2024 rows satisfy the YoY page's own NOT EXISTS
matching, so they fall out of the terminated/late-filer tables automatically.

Excluded from this import (see chat/session notes, 2026-08-06):
  - Cabot Corp, Waters Technologies, Crane NXT: all three "NEW" hits from the
    scan are ordinary large-public-company 401(k)/savings plans that merely
    offer employer stock as one investment option (codes alongside 2J/2K),
    not closely-held ESOPs - confirmed by pulling their raw F_5500 plan names
    ("CABOT 401(K) PLAN", "THE WATERS EMPLOYEE INVESTMENT PLAN", "CRANE NXT,
    CO. SAVINGS AND INVESTMENT PLAN"). Inconsistent with what this dashboard
    tracks; a scope decision, not a data gap.
  - Northern Construction Holding PN=003, Green Tech PN=001: administrative
    refiles/stale duplicates of rows already correctly held under PN=002 -
    same participant/financial data, no new information.
  - EIN 043067724 ("Eastern Bank") PN=003 copy of the Cambridge Bancorp ESOP:
    known cross-EIN duplicate per import_2025_filers.py's precedent - the
    canonical copy already lives under EIN 042777442 and already has a
    complete, correct 2024 row.

Also fixes two data-entry inconsistencies:

  1. Brockway-Smith's 2024 row (added in apply_2024_refresh.py) was inserted
     with plan_num='003' (zero-padded) while its 2014-2022 history uses
     plan_num='3', breaking the exact-match join the Year-over-Year page
     relies on. Normalized to '3' here.

  2. DOL bulk data carries sponsor names and cities in ALL CAPS, but this
     dataset stores them in Title Case. Inserting raw DOL strings produces
     case-duplicate cities ("NEW BEDFORD" alongside "New Bedford"), which
     split into separate bars on the Geography tab's Top-20 Cities chart and
     separate rows in the city aggregation feeding the choropleth. Rather
     than .title() (which mangles "Landry'S" and drops legitimate historical
     name changes), each row inherits the sponsor_name/sponsor_city casing
     already used by that same plan's most recent prior-year row, so a plan
     reads identically across every year on every page. Also repairs the
     pre-existing "ANDOVER"/"BROCKWAY-SMITH COMPANY" instance left by
     apply_2024_refresh.py.

Idempotent: re-running only re-applies the normalizations (inserts are
INSERT OR IGNORE).
"""
import csv
import io
import os
import shutil
import sqlite3
import zipfile
from datetime import datetime, timezone

import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")
CACHE = "C:/tmp/dol"

RECORDS = [
    {
        "ein": "42699206", "plan_num": "3",
        "plan_name": "PACKAGING CONSULTANTS, INC. EMPLOYEE STOCK OWNERSHIP PLAN & TRUST",
        "sponsor_name": "PACKAGING CONSULTANTS, INC.", "sponsor_city": "NEW BEDFORD",
        "sponsor_zip": "02745", "type_plan_entity": "2", "type_pension_bnft": "2E2O",
        "total_participants": 12, "active_participants": 10,
        "naics_code": "424990", "plan_eff_date": "1990-02-01",
        "total_assets": 114538.0, "total_liabilities": None,
        "employer_contributions": None, "participant_contributions": None,
        "benefits_paid": 43425.0, "net_income": -56441.0, "employer_securities": None,
    },
    {
        "ein": "42932946", "plan_num": "2",
        "plan_name": "DARMANN ABRASIVE PRODUCTS, INC. EMPLOYEE STOCK OWNERSHIP PLAN",
        "sponsor_name": "DARMANN ABRASIVE PRODUCTS, INC.", "sponsor_city": "CLINTON",
        "sponsor_zip": "01510", "type_plan_entity": "2", "type_pension_bnft": "2P2I",
        "total_participants": 81, "active_participants": 38,
        "naics_code": "327900", "plan_eff_date": "1998-10-01",
        "total_assets": 4078604.0, "total_liabilities": 636341.0,
        "employer_contributions": 104655.0, "participant_contributions": None,
        "benefits_paid": 518397.0, "net_income": -1357099.0, "employer_securities": None,
    },
    {
        "ein": "42556035", "plan_num": "2",
        "plan_name": "LANDRY'S BICYCLES EMPLOYEE STOCK OWNERSHIP PLAN",
        "sponsor_name": "LANDRY'S, INC.", "sponsor_city": "NATICK",
        "sponsor_zip": "01760", "type_plan_entity": "2", "type_pension_bnft": "2F2I2P2Q3H3I",
        "total_participants": 112, "active_participants": 68,
        "naics_code": "451110", "plan_eff_date": "2009-10-01",
        "total_assets": 3688643.0, "total_liabilities": 2474209.0,
        "employer_contributions": 307720.0, "participant_contributions": 0.0,
        "benefits_paid": 126934.0, "net_income": 741794.0, "employer_securities": None,
    },
    {
        "ein": "42631963", "plan_num": "3",
        "plan_name": "NEW ENGLAND BIOLABS, INC. NON-VOTING STOCK OWNERSHIP PLAN",
        "sponsor_name": "NEW ENGLAND BIOLABS, INC.", "sponsor_city": "IPSWICH",
        "sponsor_zip": "01938", "type_plan_entity": "2", "type_pension_bnft": "2E2I2O3I",
        "total_participants": 443, "active_participants": 482,
        "naics_code": "325900", "plan_eff_date": "1985-10-01",
        "total_assets": 162974294.0, "total_liabilities": 141745.0,
        "employer_contributions": 7889804.0, "participant_contributions": None,
        "benefits_paid": 16613397.0, "net_income": -37876863.0, "employer_securities": None,
    },
    {
        "ein": "42631963", "plan_num": "5",
        "plan_name": "NEW ENGLAND BIOLABS, INC. VOTING STOCK OWNERSHIP PLAN",
        "sponsor_name": "NEW ENGLAND BIOLABS, INC.", "sponsor_city": "IPSWICH",
        "sponsor_zip": "01938", "type_plan_entity": "2", "type_pension_bnft": "2I2O3I",
        "total_participants": 436, "active_participants": 482,
        "naics_code": "325900", "plan_eff_date": "2021-06-28",
        "total_assets": 78571398.0, "total_liabilities": 121071.0,
        "employer_contributions": 1100000.0, "participant_contributions": None,
        "benefits_paid": 7251995.0, "net_income": -20666659.0, "employer_securities": None,
    },
    {
        "ein": "42597651", "plan_num": "1",
        "plan_name": "INTERNATIONAL DATA GROUP, INC. EMPLOYEE STOCK OWNERSHIP PLAN",
        "sponsor_name": "INTERNATIONAL DATA GROUP, INC.", "sponsor_city": "NEEDHAM",
        "sponsor_zip": "02494", "type_plan_entity": "2", "type_pension_bnft": "2I2O3I",
        "total_participants": 1106, "active_participants": 543,
        "naics_code": "511120", "plan_eff_date": "1976-01-01",
        "total_assets": 3004672.0, "total_liabilities": 70858.0,
        "employer_contributions": None, "participant_contributions": None,
        "benefits_paid": 540436.0, "net_income": -166028.0, "employer_securities": None,
    },
    {
        "ein": "42772059", "plan_num": "2",
        "plan_name": "BARCLAY WATER TREATMENT CO., INC. EMPLOYEE STOCK OWNERSHIP PLAN",
        "sponsor_name": "BARCLAY WATER TREATMENT CO., INC.", "sponsor_city": "NEWTON",
        "sponsor_zip": "02458", "type_plan_entity": "2", "type_pension_bnft": "2F2I2P2Q3I",
        "total_participants": 260, "active_participants": 0,
        "naics_code": "325900", "plan_eff_date": "2009-10-01",
        "total_assets": 37315395.0, "total_liabilities": 0.0,
        "employer_contributions": 5137200.0, "participant_contributions": None,
        "benefits_paid": 227308202.0, "net_income": -80511156.0, "employer_securities": None,
    },
]


def main():
    bak = DB + ".pre-2024latefilers.bak"
    if not os.path.exists(bak):
        shutil.copy2(DB, bak)
        print(f"backup -> {bak}")

    conn = sqlite3.connect(DB)
    now = datetime.now(timezone.utc).isoformat()

    # Fix Brockway-Smith plan_num padding inconsistency ('003' -> '3')
    cur = conn.execute(
        "UPDATE form5500_filings SET plan_num='3' "
        "WHERE filing_year=2024 AND ein='41123740' AND plan_num='003'")
    print(f"Brockway-Smith plan_num normalized: {cur.rowcount}")

    n = 0
    for rec in RECORDS:
        codes = rec["type_pension_bnft"]
        row = dict(rec)
        row["filing_year"] = 2024
        row["sponsor_state"] = "MA"
        row["is_esop"] = 1
        row["is_ksop"] = 1 if ("2J" in codes and "2K" in codes) else 0
        row["industry_sector"] = fa.NAICS_SECTORS.get(row["naics_code"][:2], "") if row["naics_code"] else ""
        row["fetched"] = now
        cols = ", ".join(row)
        ph = ", ".join(":" + c for c in row)
        cur = conn.execute(
            f"INSERT OR IGNORE INTO form5500_filings ({cols}) VALUES ({ph})", row)
        n += cur.rowcount
        print(f"  {'imported' if cur.rowcount else 'already present'}: "
              f"{row['sponsor_name']} PN={row['plan_num']} "
              f"tot={row['total_participants']} act={row['active_participants']} "
              f"assets={row['total_assets']}")

    # Inherit sponsor_name / sponsor_city casing from each plan's most recent
    # prior-year row so a plan reads identically across every year and page.
    fixed = 0
    for ein, plan_num in conn.execute(
            "SELECT DISTINCT ein, plan_num FROM form5500_filings "
            "WHERE filing_year = 2024 AND is_esop = 1 "
            "AND (sponsor_city = UPPER(sponsor_city) "
            "     OR sponsor_name = UPPER(sponsor_name))").fetchall():
        prior = conn.execute(
            "SELECT sponsor_name, sponsor_city FROM form5500_filings "
            "WHERE ein = ? AND plan_num = ? AND filing_year < 2024 "
            "AND sponsor_city <> UPPER(sponsor_city) "
            "ORDER BY filing_year DESC LIMIT 1", (ein, plan_num)).fetchone()
        if not prior:
            continue
        cur = conn.execute(
            "UPDATE form5500_filings SET sponsor_name = ?, sponsor_city = ? "
            "WHERE filing_year = 2024 AND ein = ? AND plan_num = ?",
            (prior[0], prior[1], ein, plan_num))
        if cur.rowcount:
            fixed += cur.rowcount
            print(f"  casing normalized: EIN {ein} PN={plan_num} -> "
                  f"{prior[0]!r} / {prior[1]!r}")
    print(f"sponsor name/city casing rows normalized: {fixed}")

    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 ("late_filers_2024_2026_08_06_imported", now))
    conn.commit()

    n2024, p2024, a2024 = conn.execute(
        "SELECT COUNT(*), SUM(total_participants), SUM(total_assets) "
        "FROM form5500_filings WHERE filing_year=2024 AND is_esop=1").fetchone()
    print(f"\n2024 now: {n2024} plans, {p2024:,} participants, ${a2024:,.0f} assets")
    print(f"Inserted {n} new rows.")
    conn.close()

    fa.recompute_annual_summaries()
    print("annual summaries recomputed")


if __name__ == "__main__":
    main()
