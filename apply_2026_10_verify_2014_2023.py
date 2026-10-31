#!/usr/bin/env python3
"""
October 2026 verification of form years 2014-2023. Needs no DOL files.
Idempotent; backs up the database first.

WHAT WAS CHECKED
----------------
Independent reviewers rebuilt every 2014-2023 row (1,299) from DOL's raw bulk
files (September 30, 2026 release), one year each, with their own readers
rather than the dashboard's pipeline. They compared participants (line 6f),
active participants (line 6a(2)), all seven dollar fields, plan name, ZIP and
pension codes. Other reviewers re-derived the complete set of Massachusetts
ESOP plan-years for each year, and a last one checked every Trends figure and
export. Two skeptics checked each reported difference. With 2024-2025
verified the same way (apply_2026_10_verify_2024_2025.py), every row of
2014-2025 matches its return. No Massachusetts ESOP plan-year is missing, and
no row is extra.

WHAT IS FIXED
-------------
   - Fred C. Church, Inc., 2022: line 6a(2) is blank on its return (the plan
     year of its January 2022 acquisition), but the row stored 0 active
     participants. A blank is "not reported", so the field is NULL.

Usage:
    python3 apply_2026_10_verify_2014_2023.py [--dry-run]
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import config
import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH

# (EIN, plan number, year, field): fields the return leaves blank.
NOT_REPORTED = [
    ("42445292", "2", 2022, "active_participants"),
]


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-verify-2014-2023.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    n = 0
    for ein, pn, y, field in NOT_REPORTED:
        for r in conn.execute(f"SELECT id, sponsor_name, {field} AS v FROM form5500_filings "
                              f"WHERE ltrim(ein,'0')=? AND ltrim(plan_num,'0')=? AND filing_year=? "
                              f"AND {field} IS NOT NULL", (ein, pn, y)).fetchall():
            n += 1
            print(f"  {y} {r['sponsor_name']}: {field} {r['v']} -> NULL (blank on the return)")
            if not dry_run:
                conn.execute(f"UPDATE form5500_filings SET {field}=NULL WHERE id=?", (r["id"],))
    if not dry_run:
        conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                     ("verify_2014_2023_applied", datetime.now(timezone.utc).isoformat()))
        conn.commit()
    conn.close()
    if not dry_run:
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    print(f"\nrows changed {n}" + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
