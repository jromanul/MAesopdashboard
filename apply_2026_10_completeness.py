#!/usr/bin/env python3
"""
October 2026 completeness sweep (third round): look for any Massachusetts ESOP
the dataset is missing, by routes the earlier sweeps did not take, and add
what it finds. Reads DOL's bulk files at run time (DOL_CACHE_DIR).
Idempotent; backs up the database first.

WHAT WAS SEARCHED
-----------------
Five independent searches, each checked by an adversarial reviewer, then a
completeness review:
   - The Massachusetts Center for Employee Ownership's own inventories (its
     2023, 2024 and 2025 reports to the Legislature) and symposium and
     success-story material: 182 companies listed as ESOPs. 153 are in the
     dataset; the other 29 are out under the owner's rules - based in another
     state (ReVision Energy, ME; Hallam-ICS, VT; North Shore Mechanical, whose
     ESOP is its Pennsylvania parent's), or employee-owned without an ESOP
     (Meketa, Boston Trust Walden, Charlesbridge, Hill-Engineers and others).
   - Other directories: the Certified Employee-Owned directory (157
     Massachusetts companies), NCEO, the ESOP Association's New England
     chapter, advisors' and lenders' deal announcements and press. Nothing
     missing. (NCEO's count of 137 Massachusetts ESOPs matches DOL's 2023
     data once the owner's exclusions are applied.)
   - The Form 5500-SF for 2014-2022, never read before. ESOPs may not file it,
     and it cannot carry an ESOP code, but three did.
   - Plans holding employer stock, every year: no untracked Massachusetts plan
     with 20% or more of its assets in company stock is an ESOP.
   - Filing linkage: returns pointing at a tracked plan as their predecessor,
     and administrator addresses.
   - The completeness review added Schedule R's ESOP questions (no untracked
     Massachusetts plan answers yes), leveraged-ESOP loans on Schedule H,
     orphan schedules, address anomalies and looser plan-name patterns.

WHAT IS ADDED (scan_dol_filers.ESOP_WITHOUT_CODE)
-------------------------------------------------
   - BBN Technologies, 2019: the residual ESOP's final return (January-April
     2019), filed on the 2018 5500-SF. Tracked 2014-2018.
   - New England Discount Retailers, 2019: the ESOP's first plan year (from
     September 30, 2019), filed on the 5500-SF. Tracked from 2020.
   - Picis, Inc. (Wakefield), 2014: its ESOP's final return (January-April
     2014), filed by Optum Clinical Solutions on the 2013 5500-SF. Picis is on
     MassCEO's inventory.
   - MIS Training Institute, 2015: the ESOP held company stock into 2015 and
     repaid its loan before the plan was renamed a profit-sharing plan (the
     test that counts Haberman 2017 and Brockway-Smith 2023). Tracked 2014.
The pipeline now reads the 5500-SF returns of reviewed plans (dol_bulk's
SF_PLAN_KEYS) and the 2013 form, for plan-year-2014 short final returns filed
on it. The one other such return with a Massachusetts address, Beacon
Federal's ESOP (a New York bank's plan wound down by Berkshire Bank), stays
out (MA_BASED_OVERRIDES). Rows already tracked without an ESOP code on their
return - BBN 2014-2018, RCM Services 2015, TRO 2017, Needham Bank 2023 - are
now on the same reviewed list, so a rebuild keeps them.

NOT ADDED
---------
   - LeBaron Foundry's plan (2014): named an ESOP but never coded one, and as
     a 5500-SF filer it certified it held no company stock.
   - Surety Bond Professionals: its ESOP was announced in 2026; no return yet.

STILL TO COME
-------------
DOL's September 30, 2026 release is the newest (its files are unchanged as of
October 29). Two fiscal-year 2024 plans have not yet filed (Aerodyne Research,
James Monroe Wire & Cable), and most 2025 returns are due in later releases.

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_completeness.py [--dry-run]
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import config
import form5500_analysis as fa
import refetch_dol_financials as refetch
from apply_2026_10_field_audit import fill_industry, return_fields
from apply_2026_10_owner_rules import participants_to_year_end
from apply_2026_10_sweep import import_missing_returns

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-completeness.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== returns the dataset is missing (all years) ===")
    n_new = import_missing_returns(conn, dry_run)

    print("\n=== locating each row's return (2014-2025) ===")
    located = refetch.locate_returns(conn)
    print(f"  {len(located)} rows located")

    print("\n=== participants, financials and fields from each row's return ===")
    n_part = participants_to_year_end(conn, located, dry_run)
    stats = refetch.rederive(conn, located, dry_run)
    fields = return_fields(conn, located, dry_run)
    n_ind = fill_industry(conn, located, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (("last_import", now), ("completeness_2026_10_applied", now)):
            conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)", (k, v))
        conn.commit()
    conn.close()

    if not dry_run:
        refetch.save_filtered_schedules(located)
        fa.nullify_unreported_employer_securities()
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nimported {n_new}; participant rows {n_part}; financial rows changed "
          f"{stats['changed']}; return fields {sum(fields.values())}; industry {n_ind}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
