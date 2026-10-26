#!/usr/bin/env python3
"""
October 2026 sweep: a multi-angle search of DOL's EFAST2 bulk data (September
30, 2026 release) for Massachusetts ESOP returns the dataset does not hold —
new 2025 filings, late 2024 filings, and anything else it had missed. Reads
the bulk files at run time (DOL_CACHE_DIR), so it carries no copied figures.
Idempotent; backs up the database first.

HOW THE SEARCH WAS RUN
----------------------
Every MA-addressed return on the 2014-2025 forms was searched seven ways:
ESOP pension codes in the Latest files; the same in the All files (every
version DOL holds, including superseded and FILING_ERROR ones); the Form
5500-SF; plan names that read like an ESOP but carry no ESOP code; sponsor
and plan-administrator addresses; plans holding employer stock; continuity of
every tracked plan into the next year; and late returns filed on later forms.
Each candidate was then checked against the return itself, its schedules, and
the reviewed exclusion lists. A completeness review of that search pointed out
that the Latest-file loss described in 2 below can hide a year in any form
year, so the All files for 2014-2022 were fetched too, and every tracked plan
was checked for missing years inside, before and after its tracked history.

WHAT IT FOUND
-------------
1. FORM YEAR 2025: nothing new. Every 2025 ESOP return in the release is
   already in the database (58 filings). The only other one is Avidia Bank's
   plan number 002, a FILING_ERROR copy of its plan 004 with identical figures.
   With the 2024 returns below added, 51 of the 134 plans that filed for 2024
   have a 2025 return on file; most of the rest file on extension or fiscal
   years and are due in later DOL releases.

2. RETURNS DOL'S "LATEST" FILE DROPS (2023-2024). The Latest file keeps one
   return per EIN, plan number and plan year, across the Form 5500 and the
   5500-SF. When a sponsor files a different plan under the same number, or
   its newest version of a return is flagged FILING_ERROR, the ESOP return
   disappears from Latest but stays in the All file. Every earlier scan read
   Latest only. dol_bulk.py now reads both (see its rules 4 and 5):
   - Eastern Bank, 2024 (2,041 participants, $256.5M). Its own ESOP did file
     for 2024; the acquired Cambridge Bancorp plan, filed later under Eastern's
     EIN and plan number 003, displaced it. The Year-over-Year page listed
     Eastern as a 2024 gap; it no longer does. (The Cambridge copy stays
     excluded: scan_dol_filers.KNOWN_NOT_TRACKED now scopes it by plan name.)
   - Enfield Enterprises, 2023 and 2024. Its 401(k), filed on the 5500-SF
     under the same plan number 001, displaced both ESOP returns. The plan is
     tracked 2020-2022, so this fills its history through 2024.
   - Atlantic Sintered ESOP (sponsor Atlantic Holdings, Inc., Wrentham), 2023
     and 2024. New to the dataset: a leveraged ESOP effective January 1, 2023.
     Its 2024 return's only version is FILING_ERROR, and its 2023 return's
     newest amendment is, so neither is in Latest. A FILING_ERROR return is used
     only when no accepted version exists (dol_bulk.STATUS_RANK).

3. LATE RETURNS ON LATER FORMS (2016-2019). A plan catching up on missed years
   (for example through DOL's Delinquent Filer Voluntary Compliance program)
   files each overdue year on the form current when it files, so a return
   for plan year 2017 can sit in a later year's bulk file. Scans looked only
   at each year's own form. Now imported:
   - Econocorp, Inc., 2016-2019, all four filed on the 2022 form (tracked
     2014-2015 and 2020-2022; the four-year gap closes).
   - Strem Chemicals, 2018 (filed on the 2020 form) and New England Natural
     Bakers, 2018 (on the 2021 form) — each the one missing year in an
     otherwise continuous history.

4. THE SAME LOSS IN EARLIER YEARS (2014-2022 All files). Three more:
   - Halloran Consulting Group, 2022 (136 participants, $8.3M). Its 401(k)
     was filed under the same plan number 001 later the same day and displaced
     the ESOP's accepted, amended return. The plan is tracked 2020-2021 (plan
     001) and 2023-2024 (plan 002); this closes its one-year gap.
   - Washburn-Garfield Corporation, 2019 (fiscal year from November 1, 2019;
     22 participants, $1.9M). Its only version is FILING_ERROR, as with
     Atlantic Sintered 2024. Tracked 2014-2018 and 2020-2024.
   - Northern Construction Holding, 2022 (51 participants, $1.2M): the
     plan's first return (it is effective January 1, 2022; tracked from 2023).
     The sponsor's 401(k), filed on the 5500-SF under the same plan number 002
     seven weeks later, displaced it; the sponsor has since renumbered the ESOP
     to 003. Its name and industry come from the plan's next year
     (scan_dol_filers._prior_row now falls back to the nearest later row when
     there is no earlier one).
   Each was checked by two independent reviewers; in all three the return's
   opening balances equal the prior tracked year's closing balances, or its
   closing balances the next year's opening ones, to the dollar.

5. A ROW NOW DESCRIBES A FULL YEAR: New England Biolabs' Voting Stock
   Ownership Plan (plan 005), 2021. Two plan years of that plan begin in 2021:
   a short first year (June 28 - September 30, 2021) and the full fiscal year
   from October 1, 2021. The full year's return was accepted, but a later
   amendment was rejected, so only the short year was in Latest and the row
   was built from it ($891.8M of assets, 389 participants). The dataset's rule
   prefers the full year (dol_bulk.canonical_return; the same rule as Erland
   2023), which now reads $656.1M of assets, $342.1M of liabilities, $396.1M
   paid out and 407 participants (390 active). Its opening assets equal the
   short year's closing assets, and the next row (2022) begins October 1,
   2022, so the plan's October 2021 - September 2022 year, previously absent,
   is now the one counted - on the same fiscal year as the sponsor's
   Non-Voting plan (003), whose 2021 row already covered it.

6. REMOVED: The Severn Group, 2023. Its return reads "Upper Marlboro, MA
   20774" — the ZIP and city are Maryland, as is the state on its other
   returns. dol_bulk.is_ma_address now requires a Massachusetts ZIP as well as
   the MA state code, so a state typo can no longer bring a plan in. It was
   the only tracked row affected. Its Tier 1 "relocated to MD" note on the
   Year-over-Year page goes with it.

7. CITY SPELLINGS: three filed abbreviations or typos are given the spelling
   the same sponsor uses in its other years (scan_dol_filers.CITY_SPELLINGS,
   which new imports and verification now apply too).

NOT IMPORTED, AND WHY
---------------------
   - About 100 ordinary 401(k) and savings plans that hold some company stock
     but carry no ESOP code (Alnylam, Kadant, Needham Bank's 401(k), and so on).
   - Meditech's profit-sharing plan and CDM Smith's plan: the companies are
     employee-owned, but neither plan is coded or structured as an ESOP.
   - 1A Auto, 2024: its return is addressed to New Hampshire.
   - Short final returns filed on a closed year's form for the next plan year
     (for example Georgetown Savings Bank's plan year 2018, filed on the 2017
     form). Only the open form year also reads the previous form; closed years
     read their own form and later ones. Whether closed years should pick
     these up is left to the owner.
   - The Gillette Company, 2016-2020. Its ESOP returns for those years are
     mailed from Procter & Gamble in Cincinnati, so the mailing-state rule
     leaves them out; from 2021 it files from Boston under a new EIN and is
     tracked. Changing that is a rule decision left to the owner.

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_sweep.py [--dry-run]
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
from apply_2026_10_refresh import fix_participant_pairs

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH
IMPORT_YEARS = tuple(range(2014, config.FORM5500_OPEN_FORM_YEAR + 1))


def import_missing_returns(conn, dry_run: bool) -> int:
    total = 0
    for y in IMPORT_YEARS:
        db_keys = {scan.norm_key(e, p) for e, p in conn.execute(
            "SELECT ein, plan_num FROM form5500_filings WHERE filing_year=? AND is_esop=1", (y,))}
        returns, sched = scan.gather(y, db_keys)
        _db, new_rows, returning, dupes, _excluded, _gone = scan.classify(conn, y, returns)
        rows = new_rows + returning
        for _k, rec, why in dupes:
            print(f"  {y} ? not imported: {rec['sponsor_name']} - {why}")
        if not rows:
            continue
        print(f"\n{y}: {len(rows)} to import ({len(new_rows)} new sponsors)")
        for _k, rec in rows:
            print(f"    {rec['sponsor_name']} PY {rec['plan_year_begin']} "
                  f"(filed on the {rec['form_year']} form, {rec['variant']} file, "
                  f"{rec['filing_status'] or 'FILING_RECEIVED'})")
        if not dry_run:
            total += scan.import_rows(conn, y, rows, sched)
    return total


def remove_non_ma_rows(conn, located: dict, dry_run: bool) -> int:
    """Drop rows whose own return is not addressed to Massachusetts."""
    n = 0
    for row_id, (y, ret, _sched) in located.items():
        if dol_bulk.is_ma_address(ret["state"], ret["sponsor_zip"]):
            continue
        r = conn.execute("SELECT sponsor_name FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        if r is None:
            continue
        n += 1
        print(f"  {y} {r['sponsor_name']}: return addressed to {ret['sponsor_city']}, "
              f"{ret['state']} {ret['sponsor_zip']} - not a Massachusetts filing; row removed")
        if not dry_run:
            conn.execute("DELETE FROM form5500_filings WHERE id=?", (row_id,))
    if not dry_run:
        conn.commit()
    return n


def fix_city_spellings(conn, dry_run: bool) -> int:
    """Stored cities take scan_dol_filers.CITY_SPELLINGS, as new imports do."""
    n = 0
    for filed, good in scan.CITY_SPELLINGS.items():
        cnt = conn.execute("SELECT COUNT(*) FROM form5500_filings WHERE upper(sponsor_city)=?",
                           (filed,)).fetchone()[0]
        if cnt:
            n += cnt
            print(f"  {filed.title()!r} -> {good!r} ({cnt} rows)")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_city=? WHERE upper(sponsor_city)=?",
                             (good, filed))
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-sweep.bak"
        if not os.path.exists(bak):
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== returns the database does not hold ===")
    n_new = import_missing_returns(conn, dry_run)

    print("\n=== locating each row's return (2014-2025) ===")
    located = refetch.locate_returns(conn)
    print(f"  {len(located)} rows located")

    print("\n=== rows not addressed to Massachusetts ===")
    n_removed = remove_non_ma_rows(conn, located, dry_run)
    located = {k: v for k, v in located.items()
               if dol_bulk.is_ma_address(v[1]["state"], v[1]["sponsor_zip"])}

    print("\n=== financials re-derived for every row (new rows included) ===")
    stats = refetch.rederive(conn, located, dry_run)
    print("\n=== participant counts paired with the same return ===")
    n_pairs = fix_participant_pairs(conn, located, dry_run)

    print("\n=== city spellings ===")
    n_cities = fix_city_spellings(conn, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (("last_import", now), ("sweep_2026_10_applied", now),
                     ("dol_release", config.DOL_RELEASE_DATE)):
            conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)", (k, v))
        conn.commit()
    conn.close()

    if not dry_run:
        refetch.save_filtered_schedules(located)
        fa.nullify_unreported_employer_securities()
        fa.recompute_annual_summaries()
        # The app opens the database in WAL mode. Fold the log into the main
        # file, which is what gets committed (the -wal file is gitignored).
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nimported {n_new}; non-MA rows removed {n_removed}; financial rows changed "
          f"{stats['changed']}; participant pairs fixed {n_pairs}; city spellings fixed {n_cities}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
