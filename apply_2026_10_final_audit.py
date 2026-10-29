#!/usr/bin/env python3
"""
October 2026 final completeness audit: every Massachusetts ESOP plan-year
2014-2025 that DOL holds is in the dataset, exactly once. Reads DOL's bulk
files at run time (DOL_CACHE_DIR). Idempotent; backs up the database first.

WHAT WAS CHECKED
----------------
   - Every return on every form 2013-2025, Latest and All files, Form 5500 and
     5500-SF, reconciled against the database. An ESOP-coded return with a
     Massachusetts mailing OR location address is either tracked or on a
     reviewed list (scan_dol_filers); every stored row has its return. The same
     pass looked at ESOP returns of tracked companies filed from other states,
     plans named like ESOPs without the code, and gaps and edges in each
     plan's run of years.
   - scan_dol_filers.py for each form year 2014-2025: no new or returning
     filer outside the two below, and every stored field matches its return.
   - Independent reviews of outside sources (news, ESOP advisors and lenders,
     directories, past ESOPs) and of every exclusion and special case.

WHAT IS FIXED
-------------
A plan year that begins in the last week of December now counts in the
following year, the year it covers (dol_bulk rule 8):
   - New England Discount Retailers runs a 52/53-week fiscal year. Its plan
     year of December 26, 2022 to December 31, 2023 is its 2023 year, and the
     ESOP was missing from 2023. Its 2022 row now describes the year from
     December 27, 2021, and its 2021 row the short year from September 27, 2021
     (filed from Mashpee, before the move to Holliston).
   - Savogran's final return, from December 31, 2024 to April 23, 2025, is a
     2025 filing (0 participants; the plan was wound down).

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_final_audit.py [--dry-run]
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import config
import dol_bulk
import form5500_analysis as fa
import refetch_dol_financials as refetch
import scan_dol_filers as scan
from apply_2026_10_field_audit import fill_industry, return_fields
from apply_2026_10_owner_rules import participants_to_year_end
from apply_2026_10_page_audit import restore_verified_naics
from apply_2026_10_sweep import import_missing_returns

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH


def cities_from_returns(conn, located: dict, dry_run: bool) -> int:
    """Each row's city as filed on its own return (an address change is real
    information), with the spelling the plan uses in its other years."""
    n = 0
    for row_id, (y, ret, _sched) in located.items():
        if not dol_bulk.is_ma_address(ret["state"], ret["sponsor_zip"]) or not ret["sponsor_city"]:
            continue  # filed from elsewhere: the row carries the company's home city
        r = conn.execute("SELECT sponsor_name, sponsor_city FROM form5500_filings WHERE id=?",
                         (row_id,)).fetchone()
        if r is None:
            continue
        want = scan.inherit_city(r["sponsor_city"], ret["sponsor_city"])
        if (r["sponsor_city"] or "") == want:
            continue
        n += 1
        print(f"  {y} {r['sponsor_name'][:40]:40} {r['sponsor_city']} -> {want}")
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET sponsor_city=? WHERE id=?", (want, row_id))
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-final-audit.bak"
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
    n_city = cities_from_returns(conn, located, dry_run)
    n_ind = fill_industry(conn, located, dry_run)
    print("\n=== reviewed industry codes ===")
    n_naics = restore_verified_naics(conn, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (("last_import", now), ("final_audit_2026_10_applied", now)):
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
          f"{stats['changed']}; return fields {sum(fields.values())}; cities {n_city}; "
          f"industry {n_ind}; reviewed industry codes {n_naics}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
