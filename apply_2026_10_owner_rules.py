#!/usr/bin/env python3
"""
October 2026: apply the dashboard owner's eligibility decisions (2026-10-28) to
every form year, 2014-2025. Reads DOL's bulk files at run time (DOL_CACHE_DIR),
so it carries no copied figures. Idempotent; backs up the database first.

THE DECISIONS
-------------
The owner's instruction: only Massachusetts-based ESOPs are counted, and every
ESOP is eligible - a company either has an ESOP or it does not, and the
dashboard draws no line between, say, a 30%-owned and a 100%-owned ESOP
company. On the open questions raised in the October refresh, the owner
authorized the analyst's best-practice recommendation. Those are:

1. EVERY ESOP COUNTS. The "investor-owned" exclusion of three bank ESOPs is
   lifted, so bank ESOPs are treated alike: Wellesley Bank, Peoples Federal
   Savings Bank and Chicopee Savings Bank's 2017 final return join Brookline,
   Eastern, HarborOne, Western New England and the rest. Two kinds of plan
   stay out because they are not ESOPs: public companies' ordinary 401(k) and
   savings plans that merely offer a company-stock fund (EXCLUDED_EINS: Cabot,
   Waters, Crane NXT, State Street, Raytheon, RTX, GE), and plans at
   employee-owned companies that are not ESOPs (Meditech's profit-sharing
   plan, CDM Smith's plans).

2. MASSACHUSETTS-BASED means the employer the ESOP covers is headquartered in
   Massachusetts that year. The Form 5500 mailing address decides unless it is
   not the company's home: a Massachusetts company whose parent or
   administrator files from another state is counted, and a company based
   elsewhere whose return carries a Massachusetts address (an acquirer's,
   lawyer's or administrator's) is not. The reviewed cases are listed in
   scan_dol_filers.MA_BASED_OVERRIDES. (The return's separate "location
   address" field cannot decide this: only 104 of 1,691 ESOP returns with a
   Massachusetts address fill it in, and none disagrees with the mailing
   address.)

3. EVERY RETURN COUNTS IN THE YEAR ITS PLAN YEAR BEGINS, in every year. A
   plan that ends early in a year files its short final return on the form
   then current, which is the previous year's; until now only the open form
   year read the previous form for these.

4. PARTICIPANTS ARE THE YEAR-END TOTAL (Form 5500 line 6f), on the same date
   as the active count (line 6a) and every dollar figure. The start-of-year
   total (line 5) ran below the active count in 101 plan-years, because a plan
   that grows during the year adds participants after line 5 is measured,
   and it needed hand overrides for three plans whose first year began before
   they had members (Shawmut Group 2017, Forge Group 2022, Sullivan Investment
   2023); line 6f is filled on every tracked return and is never below the
   active count. This is also the count DOL's own Form 5500 statistics use.

5. The header keeps the Massachusetts Center for Employee Ownership as the
   publishing organization: the project describes itself as part of the
   MassCEO Intelligence Dashboard and feeds the MassCEO Policy Tracker.

WHAT CHANGED
------------
Every case below was researched (the bulk data for 2014-2025, Latest and All,
plus public records for where each company was based) and then checked by a
separate adversarial reviewer; a completeness review looked for anything
missed. scan_dol_filers.MA_BASED_OVERRIDES records each address decision.

Added - Massachusetts companies whose ESOP returns are filed from another state:
   - The Gillette Company, 2016-2020 (about 1,500 participants, $290-386M).
     The returns are mailed from Procter & Gamble's Cincinnati benefits office;
     Gillette stayed at 1 Gillette Park, Boston, where the same EIN's other plan
     files every year. Its 2021 original under the old EIN duplicates the
     tracked 2021 re-filing under the new EIN and stays out.
   - Belmont Savings Bank's ESOP, 2018-2020, wound down by People's United
     from Connecticut after it bought BSB Bancorp (tracked 2014-2017).
   - Litecontrol Corporation (Plympton), 2014 and its 2015 final return, filed
     by Hubbell from Connecticut after the 2014 sale.
   - Environmental Compliance Services (Agawam), 2017-2018 wind-down, filed by
     ECS Holdco from Connecticut (tracked 2014-2016).
   - United Bank's own ESOP (West Springfield), 2014, filed from Glastonbury,
     CT after the bank's April 2014 merger with Rockville Financial.
   - LSB Corporation / RiverBank (North Andover), 2014 final return, filed by
     People's United.
   - Essex County Gas Company's legacy ESOP (Amesbury), 2014-2024, filed by
     KeySpan and then National Grid. A small residual plan (6 participants or
     fewer); it is counted because it is an ESOP.

Added - bank ESOPs no longer excluded as "investor-owned":
   - Wellesley Bank, 2014-2021 (2021 is its final return, filed on the 2020
     form, whose only version DOL flagged with errors).
   - Peoples Federal Savings Bank, 2014-2015, filed by Rockland Trust after the
     2015 acquisition (a different plan from the Central Co-operative Bank
     ESOP Rockland also files: scan_dol_filers.DISTINCT_PLANS).
   - Chicopee Savings Bank, 2017 final return (2014-2016 already tracked).

Added - short final returns filed on the previous year's form, now counted
in the year they cover: World Publications 2015; Stevenson & Associates and
Infoscitex 2017; Shift Communications, Meetinghouse Bank and Georgetown
Savings Bank 2018; Interaction Associates and Atlantic Plywood 2019; Expert
Laser Services 2020; Channing Bete, Nye Lubricants and Applied Marketing
Science 2021. Each reports no active participants, so it counts among the
plans that filed, not the active plans.

Added - Haberman Insurance Group, 2017: its 401(k) and ESOP held company stock
until it was sold during that year (the same test that counts Brockway-Smith
2023); from 2018 it is an ordinary 401(k).

Removed - companies based outside Massachusetts:
   - Forge Group, 2021-2025: a Maryland/DC insurance holding company whose
     returns use a Worcester P.O. box.
   - Manganaro North America, 2024-2025: run from Beltsville, MD (its
     executives and back office are there, and the same EIN's 401(k) files
     from there); the ESOP's Woburn address is its Manganaro Northeast
     subsidiary's office. The closest call in this review: Manganaro's roots
     and northeast operations are in Massachusetts, and the group's
     registered principal office was not checked in the state's records.
   - Coastway Community Bank, 2018-2019: a Warwick, RI bank whose ESOP
     HarborOne wound down from Brockton.
   - New England Bank, 2014: the ESOP of an Enfield, CT bank, wound down by
     United Bank from West Springfield.
   - Dynamic Air Corporation, 2020: a West Kennebunk, ME company whose 2020
     return used a Boston mail-forwarding box.

Reviewed and unchanged:
   - Abt Associates stays: every Abt return, its federal (SAM) registration
     and the industry rankings give Cambridge as headquarters, although its
     largest office has been in Rockville, MD since 2018.
   - 1A Auto stays out: it has been headquartered in Nashua, NH since 2020,
     and its ESOP files from there.
   - Danafilms 2014-2016 stays: the wind-down of a Westborough company's ESOP,
     treated like the other post-sale wind-downs above, although the company
     itself had moved to Kentucky as RKW North America.
   - Brockway-Smith 2024-2025 stays: the plan was renamed a profit-sharing
     plan when the company was sold in 2023, but these are the wind-down and
     final returns of its ESOP.

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_owner_rules.py [--dry-run]
"""
from __future__ import annotations

import collections
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
from apply_2026_10_sweep import fix_city_spellings, import_missing_returns

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH


def remove_ineligible_rows(conn, located: dict, dry_run: bool) -> int:
    """Drop tracked rows that are not a Massachusetts-based company's ESOP, or
    that a reviewed exclusion (a duplicate, a non-ESOP) now covers."""
    n = 0
    for row_id, (y, ret, _sched) in sorted(located.items(), key=lambda kv: kv[1][0]):
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        if r is None:
            continue
        key = scan.norm_key(r["ein"], r["plan_num"])
        why = None
        if not scan.is_ma_based(key[0], key[1], y, ret):
            hit = scan.MA_BASED_OVERRIDES.get(key)
            why = hit[3] if hit else f"return addressed to {ret['sponsor_city']}, {ret['state']}"
        else:
            why = scan.not_tracked_reason(key[0], key[1], y, ret["plan_name"])
        if not why:
            continue
        n += 1
        print(f"  {y} {r['sponsor_name']} (EIN {r['ein']}, plan {r['plan_num']}): removed - {why}")
        if not dry_run:
            conn.execute("DELETE FROM form5500_filings WHERE id=?", (row_id,))
    if not dry_run:
        conn.commit()
    return n


def participants_to_year_end(conn, located: dict, dry_run: bool) -> int:
    """Every row's participant counts from its own return: line 6f and 6a(2)."""
    changed = 0
    before, after = collections.Counter(), collections.Counter()
    for row_id, (y, ret, _sched) in located.items():
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        if r is None:
            continue
        key = (y, dol_bulk.norm_ein(r["ein"]), dol_bulk.norm_pn(r["plan_num"]))
        override = scan.PARTICIPANT_OVERRIDES.get(key, {})
        upd = {}
        for f in ("total_participants", "active_participants"):
            v = override.get(f, ret[f])
            if v is not None and (r[f] is None or int(v) != int(r[f])):
                upd[f] = int(v)
        before[y] += r["total_participants"] or 0
        after[y] += upd.get("total_participants", r["total_participants"]) or 0
        if not upd:
            continue
        changed += 1
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET " + ", ".join(f"{f}=?" for f in upd) +
                         " WHERE id=?", list(upd.values()) + [row_id])
    for y in sorted(before):
        print(f"  {y}: total participants, all filed plans {before[y]:,} -> {after[y]:,}")
    if not dry_run:
        conn.commit()
    return changed


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-owner-rules.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== returns the rules now count (all years) ===")
    n_new = import_missing_returns(conn, dry_run)

    print("\n=== locating each row's return (2014-2025) ===")
    located = refetch.locate_returns(conn)
    print(f"  {len(located)} rows located")

    print("\n=== rows the rules no longer count ===")
    n_removed = remove_ineligible_rows(conn, located, dry_run)
    if not dry_run:
        located = {k: v for k, v in located.items()
                   if conn.execute("SELECT 1 FROM form5500_filings WHERE id=?", (k,)).fetchone()}

    print("\n=== participants: year-end total (line 6f) ===")
    n_part = participants_to_year_end(conn, located, dry_run)

    print("\n=== financials re-derived for every row ===")
    stats = refetch.rederive(conn, located, dry_run)

    print("\n=== city spellings ===")
    n_cities = fix_city_spellings(conn, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (("last_import", now), ("owner_rules_2026_10_applied", now),
                     ("participant_basis", "line 6f (year-end total)"),
                     ("dol_release", config.DOL_RELEASE_DATE)):
            conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)", (k, v))
        conn.commit()
    conn.close()

    if not dry_run:
        refetch.save_filtered_schedules(located)
        fa.nullify_unreported_employer_securities()
        fa.recompute_annual_summaries()
        # The app opens the database in WAL mode; fold the log into the file
        # that is committed (the -wal file is gitignored).
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nimported {n_new}; rows removed {n_removed}; participant counts moved to "
          f"line 6f on {n_part} rows; financial rows changed {stats['changed']}; "
          f"city spellings fixed {n_cities}" + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
