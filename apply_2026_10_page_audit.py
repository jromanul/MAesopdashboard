#!/usr/bin/env python3
"""
October 2026 page audit: data fixes found while checking every page of the
dashboard against the database and the returns. Needs no DOL files.
Idempotent; backs up the database first.

WHAT IS FIXED
-------------
1. INDUSTRY CODES. The owner's May 31, 2026 review set a website-checked NAICS
   code for every active company's 2024 row (commit 2d5ab22). The June 2026
   refresh wrote the codes on the returns back over 62 of them, and the 2025
   returns imported since inherited those. Each row from 2024 on takes the
   reviewed code again (scan_dol_filers.VERIFIED_NAICS, which new imports now
   apply as well); the sector follows the code. Among the sector changes:
   Windover Construction and G&C Concrete to Construction, Williams Stone and
   Stonetek to Manufacturing, CHMWarnick to Real Estate.

2. SPONSOR NAME. Paragus Strategic IT's 2023 and 2024 rows read "Paragus";
   its returns for those years name the sponsor PARAGUS STRATEGIC IT, INC., as
   in 2015-2022. (The company audit later gave every year the ", Inc.".)

Usage:
    python3 apply_2026_10_page_audit.py [--dry-run]
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

NAME_FIXES = {  # EIN: sponsor name as on the returns
    "204270900": "Paragus Strategic IT, Inc.",
}


def restore_verified_naics(conn, dry_run: bool) -> int:
    n = 0
    for r in conn.execute("SELECT id, filing_year, ein, sponsor_name, naics_code, industry_sector "
                          "FROM form5500_filings WHERE filing_year>=? ORDER BY filing_year, sponsor_name",
                          (scan.VERIFIED_NAICS_FROM,)).fetchall():
        naics = scan.VERIFIED_NAICS.get(scan.dol_bulk.norm_ein(r["ein"]))
        if not naics:
            continue
        sector = fa.NAICS_SECTORS.get(naics[:2], "")
        if (naics, sector) == (r["naics_code"] or "", r["industry_sector"] or ""):
            continue
        n += 1
        print(f"  {r['filing_year']} {r['sponsor_name'][:40]:40} {r['naics_code']} -> {naics}"
              + (f"  ({r['industry_sector']} -> {sector})" if sector != r["industry_sector"] else ""))
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET naics_code=?, industry_sector=? WHERE id=?",
                         (naics, sector, r["id"]))
    if not dry_run:
        conn.commit()
    return n


def fix_names(conn, dry_run: bool) -> int:
    n = 0
    for ein, name in NAME_FIXES.items():
        rows = conn.execute("SELECT id, filing_year, sponsor_name FROM form5500_filings "
                            "WHERE ltrim(ein,'0')=? AND sponsor_name<>?", (ein, name)).fetchall()
        for r in rows:
            n += 1
            print(f"  {r['filing_year']} {r['sponsor_name']!r} -> {name!r}")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_name=? WHERE id=?", (name, r["id"]))
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-page-audit.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== reviewed industry codes ===")
    n_naics = restore_verified_naics(conn, dry_run)
    print("\n=== sponsor names ===")
    n_names = fix_names(conn, dry_run)

    if not dry_run:
        conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                     ("page_audit_2026_10_applied", datetime.now(timezone.utc).isoformat()))
        conn.commit()
    conn.close()

    if not dry_run:
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nindustry rows {n_naics}; names {n_names}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
