#!/usr/bin/env python3
"""
October 2026 verification of form years 2024 and 2025. Needs no DOL files.
Idempotent; backs up the database first.

WHAT WAS CHECKED
----------------
Independent reviewers rebuilt every 2024 row (133) and every 2025 row (57) from
DOL's raw bulk files (the September 30, 2026 release, still the newest), with
their own readers rather than the dashboard's pipeline. In every row, all
participant and dollar figures match the return, and every Massachusetts ESOP
plan-year for both years is listed. A second pair of reviewers checked every
figure on the dashboard's pages and in its downloads against the database.
Each reported difference was checked by two skeptics.

WHAT IS FIXED
-------------
   - Gemini Industries' 2024 plan name carried an annotation, "(Gemini
     Industries ESOP)", that is not on the return; it now reads as filed,
     "GEMINI STOCK PROGRAM", like its 2019-2023 rows.
   - Cambridge Bancorp's 2024-2025 rows carried Eastern Bank's Boston office,
     the acquirer that files the return. They now carry Cambridge, the bank's
     home, as other acquired banks' rows do (scan_dol_filers.MA_BASED_OVERRIDES).
   - config.DATA_AS_OF moves to the date of this reconciliation.

Usage:
    python3 apply_2026_10_verify_2024_2025.py [--dry-run]
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

import config
import form5500_analysis as fa
import scan_dol_filers as scan

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH

PLAN_NAMES = [  # (EIN, plan number, year, plan name as filed)
    ("42917465", "2", 2024, "GEMINI STOCK PROGRAM"),
]


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-verify-2024-2025.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    n = 0
    for ein, pn, y, name in PLAN_NAMES:
        for r in conn.execute("SELECT id, plan_name FROM form5500_filings WHERE ltrim(ein,'0')=? "
                              "AND ltrim(plan_num,'0')=? AND filing_year=? AND plan_name<>?",
                              (ein, pn, y, name)).fetchall():
            n += 1
            print(f"  {y} plan name {r['plan_name']!r} -> {name!r}")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET plan_name=? WHERE id=?", (name, r["id"]))
    # Rows filed by a parent or acquirer carry the company's home (city, ZIP).
    for r in conn.execute("SELECT id, filing_year, ein, plan_num, sponsor_name, sponsor_city, sponsor_zip "
                          "FROM form5500_filings WHERE filing_year IN (2024, 2025)").fetchall():
        hit = scan._ma_override(scan.dol_bulk.norm_ein(r["ein"]), scan.dol_bulk.norm_pn(r["plan_num"]),
                                r["filing_year"])
        if not (hit and hit[1] and hit[2]):
            continue
        city, zip_code = hit[2]
        if (r["sponsor_city"], r["sponsor_zip"]) == (city, zip_code):
            continue
        n += 1
        print(f"  {r['filing_year']} {r['sponsor_name'][:40]:40} {r['sponsor_city']} {r['sponsor_zip']} "
              f"-> {city} {zip_code}")
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET sponsor_city=?, sponsor_zip=? WHERE id=?",
                         (city, zip_code, r["id"]))
    if not dry_run:
        conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                     ("verify_2024_2025_applied", datetime.now(timezone.utc).isoformat()))
        conn.commit()
    conn.close()
    if not dry_run:
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    print(f"\nrows changed {n}" + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
