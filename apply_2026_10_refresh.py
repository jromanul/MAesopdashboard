#!/usr/bin/env python3
"""
October 2026 refresh: bring every form year up to DOL's September 30, 2026
EFAST2 release, and correct the row-level defects a full 2014-2025 re-verification
found. Reads the bulk files at run time (DOL_CACHE_DIR), so it carries no copied
figures. Idempotent; backs up the database first.

WHAT THE RE-VERIFICATION COVERED
--------------------------------
Every stored ESOP plan-year, 2014-2025 (1,402 rows before this refresh), was matched to the return it
represents in DOL's current bulk data, and that return's own Schedule H / I,
joined by ACK_ID. Every row has a return in DOL's data. Every MA ESOP-coded
return DOL holds that the database does not was reviewed by hand; those that are
deliberately not tracked are now listed in scan_dol_filers.KNOWN_NOT_TRACKED.

1. NEW FILINGS (imported through scan_dol_filers, same rules as a scan)
   - 2024: three fiscal-year plans whose 2024 returns arrived after the August
     refresh — Shawmut Group (received 2026-09-14; 1,218 participants), Erland
     Construction (2026-08-17) and Paul K. Guillow (2026-08-17). All three were
     on the Year-over-Year research list as "still employee-owned, late filer";
     the filings confirm it.
   - 2023: Brockway-Smith Company. Its 2014-2022 and 2024 returns are tracked,
     but 2023 was never imported, leaving a one-year hole in its history.
   - 2025: 27 plans filed since the August refresh, taking form year 2025 from
     31 tracked filings to 58. Four sponsors are new to the dataset: Avidia
     Bank, Synapse Energy Economics, At Work Collaborative, and RCM Services
     under a new EIN (its return names the old one as the prior sponsor, so it
     inherits that history's name and industry).

2. PARTICIPANTS AND DOLLARS FROM TWO DIFFERENT RETURNS (12 rows)
   When a plan terminates early in a year, its short final return is often
   filed on the previous year's form, so that form year holds two returns under
   one EIN and plan number. The old import kept the final return's participant
   counts while refetch_dol_financials.py kept the regular return's dollars, so
   these rows paired, e.g., Atlantic Plywood 2018's full-year $12.0M with the
   final return's 0 active participants. Participant counts are restored to the
   regular return: World Publications 2014, Stevenson & Associates 2016, Shift
   Communications, Meetinghouse Bank and Georgetown Savings Bank 2017,
   Interaction Associates and Atlantic Plywood 2018, Expert Laser Services 2019,
   Channing Bete, Nye Lubricants and Applied Marketing Science 2020, Dynamic Air
   2022.

3. FINANCIALS RE-DERIVED STRICTLY (refetch_dol_financials.py, rewritten)
   - Two rows took their dollars from the wrong return of a pair: Erland 2023
     (its Jan-Oct 2023 transition year instead of its Nov 2023-Oct 2024 year,
     which is where its participant counts came from) and Savogran 2024 (its
     one-day-later short final year, $0, instead of the full 2024 year,
     $504,065 — the record the August audit identified as correct).
   - Beginning-of-year and receivable fallbacks removed: where the end-of-year
     figure was blank, the old field lists used the beginning-of-year column,
     and for employer contributions the contributions-receivable balance. New
     England Biolabs 2020 carried $345M of employer securities that its return
     does not report; Centra Technology carried the same $9,235 "contribution"
     three years running.
   - Employer securities filled from Schedule I line 3d for small plans, which
     report it there; the dataset had treated it as unavailable.

4. ADDRESS FROM THE ROW'S OWN RETURN
   - The same pairing problem left two rows with the final return's city
     (Interaction Associates 2018, Atlantic Plywood 2018 - the latter a
     "Wodburn" typo on that return).
   - Dynamic Air 2022 is removed: its plan-year-2022 return is addressed to
     West Kennebunk, Maine, as was its 2021 return (correctly absent). It was
     counted only because its plan-year-2023 final return, filed on the same
     form, gave a Boston address.

5. CONSISTENCY
   - Form year 2025 rows written by the earliest imports kept DOL's ALL-CAPS
     sponsor names; they now inherit the casing, and the curated industry, of
     the plan's own earlier rows, as every other year does.
   - Sponsor names that read badly in the tables are corrected (see
     SPONSOR_NAME_FIXES): casing artifacts, one company spelled two ways in
     consecutive years, and bare abbreviations.

KEPT AS THEY ARE
----------------
   - Three participant counts that differ from the filed line-5 figure
     (scan_dol_filers.PARTICIPANT_OVERRIDES).
   - Curated NAICS codes (checked against company websites, commit 2d5ab22).

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_refresh.py [--dry-run]
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

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH
IMPORT_YEARS = (2023, 2024, 2025)

# Sponsor names as they should read in the dashboard's tables. Most are casing
# artifacts of title-casing DOL's ALL-CAPS strings ("Landry'S", "Rcm", "Btse");
# a few reconcile one company's name across years ("Acentech Inc." beside
# "Acentech Incorporated"), and a few replace a bare abbreviation with the
# sponsor's filed name so a reader can tell who it is. Curated brand names
# chosen in the EO-inventory audit ("Janis Research", "Harpoon Brewery") stay.
SPONSOR_NAME_FIXES = {
    "Landry'S Bicycles": "Landry's Bicycles",
    "Btse": "Barrett Tree Service East, Inc.",
    "BARRETT TREE SERVICE EAST, INC.": "Barrett Tree Service East, Inc.",
    "Spec": "SPEC Process Engineering & Construction, Inc.",
    "Rcm Services, Inc.": "RCM Services, Inc.",
    "Oem Connect, Ltd.": "OEM Connect, Ltd.",
    "Nmr Group": "NMR Group",
    "Lda Architecture & Interiors": "LDA Architecture & Interiors",
    "Swissturn/Usa, Inc.": "Swissturn/USA, Inc.",
    "Chmwarnick, Inc.": "CHMWarnick, Inc.",
    "Asc Management Holdings, Inc": "ASC Management Holdings, Inc.",
    "Aha Consulting Engineers Inc": "AHA Consulting Engineers, Inc.",
    "Aha Consulting Engineers , Inc": "AHA Consulting Engineers, Inc.",
    "Dmh Electric, Inc.": "DMH Electric, Inc.",
    "Sps New England, Inc.": "SPS New England, Inc.",
    "Symmes Maini & Mckee Associates, Inc.": "Symmes Maini & McKee Associates, Inc.",
    "Harborone Bank": "HarborOne Bank",
    "Bankprov": "BankProv",
    "Courtsmart Digital Systems": "CourtSmart Digital Systems",
    "Acentech Incorporated Employee Stock": "Acentech Incorporated",
    "Acentech Inc.": "Acentech Incorporated",
    "Prellwitz / Chilinski Associates, Inc.": "Prellwitz/Chilinski Associates, Inc.",
    "The Comrex Corporation": "Comrex Corporation",
    "Brookline Bancorp, Inc": "Brookline Bancorp, Inc.",
    "Nevo Technologies, Inc": "Nevo Technologies, Inc.",
    "Dietz & Company Architects": "Dietz & Company Architects, Inc.",
    "ABT GLOBAL INC.": "Abt Global, Inc. (Abt Associates)",
    "ABT ASSOCIATES INC.": "Abt Associates Inc.",
    "ABT ASSOCIATES INC": "Abt Associates Inc.",
    "CAPE AIR/NANTUCKET AIRLINES": "Cape Air / Nantucket Airlines (Hyannis Air Service)",
    "FRED C. CHURCH,INC.": "Fred C. Church, Inc.",
    "NEW ENGLAND BIOLABS, INC.": "New England Biolabs, Inc.",
    "New England Biolabs, Inc": "New England Biolabs, Inc.",
    "GEMINI INDUSTRIES INC.": "Gemini Industries Inc.",
    "The L.S.Starrett Company": "The L.S. Starrett Company",
    "Tri-Wire": "Tri-Wire Engineering Solutions, Inc.",
    "Tech-Etch": "Tech-Etch, Inc.",
    "David Clark": "David Clark Holdings, Inc.",
    "Manganaro": "Manganaro North America, Inc.",
    "Consigli Building Group": "Consigli Building Group, Inc.",
    "Sullivan Investment Co., Inc.": "Sullivan Investment Co., Inc. (Sullivan Tire)",
}

# Westfield Financial renamed itself Western New England Bancorp in October 2016;
# its later returns still carried the old name through casing inheritance.
RENAMED_FROM_YEAR = {"731627673": (2017, "Western New England Bancorp, Inc.")}


def import_new_filings(conn, dry_run: bool) -> int:
    total = 0
    for y in IMPORT_YEARS:
        db_keys = {scan.norm_key(e, p) for e, p in conn.execute(
            "SELECT ein, plan_num FROM form5500_filings WHERE filing_year=? AND is_esop=1", (y,))}
        returns, sched = scan.gather(y, db_keys)
        _db, new_rows, returning, dupes, excluded, _gone = scan.classify(conn, y, returns)
        rows = new_rows + returning
        print(f"\n{y}: {len(rows)} to import ({len(new_rows)} new sponsors), "
              f"{len(dupes)} possible dupes, {len(excluded)} excluded")
        for _k, rec, why in dupes:
            print(f"  ? dupe, not imported: {rec['sponsor_name']} - {why}")
        if dry_run:
            for _k, rec in rows:
                print(f"  would import {rec['sponsor_name']} PY {rec['plan_year_begin']}")
            continue
        total += scan.import_rows(conn, y, rows, sched)
    return total


def fix_participant_pairs(conn, located: dict, dry_run: bool) -> int:
    """Restore participant counts to the return the row's financials come from."""
    fixed = 0
    for row_id, (y, ret, _sched) in located.items():
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        key = (y, dol_bulk.norm_ein(r["ein"]), dol_bulk.norm_pn(r["plan_num"]))
        override = scan.PARTICIPANT_OVERRIDES.get(key, {})
        upd = {}
        for f in ("total_participants", "active_participants"):
            v = ret[f]
            if f in override or v is None or r[f] is None or int(v) == int(r[f]):
                continue
            upd[f] = int(v)
        if not upd:
            continue
        fixed += 1
        print(f"  {y} {r['sponsor_name'][:34]:34} " + ", ".join(
            f"{f} {r[f]} -> {v}" for f, v in upd.items()) +
            f"  (return for plan year {ret['plan_year_begin']})")
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET " + ", ".join(f"{f}=?" for f in upd) +
                         " WHERE id=?", list(upd.values()) + [row_id])
    if not dry_run:
        conn.commit()
    return fixed


def align_open_year_identity(conn, located: dict, dry_run: bool) -> int:
    """Open-year rows take the sponsor casing and curated industry of the plan's
    own most recent earlier row, so a plan reads the same in every year — but
    the city on the return wins when the plan has moved."""
    y = config.FORM5500_OPEN_FORM_YEAR
    filed_city = {row_id: ret["sponsor_city"] for row_id, (yy, ret, _s) in located.items()
                  if yy == y}
    n = 0
    for r in conn.execute("SELECT * FROM form5500_filings WHERE filing_year=? AND is_esop=1",
                          (y,)).fetchall():
        prior = conn.execute(
            "SELECT sponsor_name, sponsor_city, naics_code, industry_sector "
            "FROM form5500_filings WHERE ltrim(ein,'0')=? AND filing_year<? "
            "ORDER BY filing_year DESC LIMIT 1",
            (dol_bulk.norm_ein(r["ein"]), y)).fetchone()
        if not prior:
            continue
        want = {k: prior[k] for k in ("sponsor_name", "naics_code", "industry_sector")
                if prior[k]}
        want["sponsor_city"] = scan.inherit_city(prior["sponsor_city"],
                                                 filed_city.get(r["id"], r["sponsor_city"]))
        diff = {k: v for k, v in want.items() if r[k] != v}
        if not diff:
            continue
        n += 1
        print(f"  {r['sponsor_name'][:34]:34} -> " + ", ".join(f"{k}={v!r}" for k, v in diff.items()))
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET " + ", ".join(f"{k}=?" for k in diff) +
                         " WHERE id=?", list(diff.values()) + [r["id"]])
    if not dry_run:
        conn.commit()
    return n


def fix_address_from_own_return(conn, located: dict, dry_run: bool) -> tuple[int, int]:
    """City (and state) come from the return the row represents.

    The same two-returns-on-one-form problem as fix_participant_pairs left a
    few rows carrying the final return's address. Where that return was the
    only Massachusetts-addressed one, the row should never have been counted:
    Dynamic Air's plan-year-2022 return is mailed from West Kennebunk, Maine
    (as was its 2021 return, which is correctly absent); only its plan-year-2023
    final return, filed on the same form, gives a Boston address.
    """
    moved = removed = 0
    for row_id, (y, ret, _sched) in located.items():
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        if r is None:
            continue
        if ret["state"] != "MA":
            removed += 1
            print(f"  {y} {r['sponsor_name']}: its return is addressed to {ret['sponsor_city']}, "
                  f"{ret['state']} - not a Massachusetts filing; row removed")
            if not dry_run:
                conn.execute("DELETE FROM form5500_filings WHERE id=?", (row_id,))
            continue
        filed = ret["sponsor_city"].strip()
        if filed and filed.lower() != (r["sponsor_city"] or "").strip().lower():
            moved += 1
            print(f"  {y} {r['sponsor_name']}: city {r['sponsor_city']!r} -> {filed.title()!r}")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_city=? WHERE id=?",
                             (filed.title(), row_id))
    if not dry_run:
        conn.commit()
    return moved, removed


def fix_sponsor_names(conn, dry_run: bool) -> int:
    n = 0
    for bad, good in SPONSOR_NAME_FIXES.items():
        cnt = conn.execute("SELECT COUNT(*) FROM form5500_filings WHERE sponsor_name=?",
                           (bad,)).fetchone()[0]
        if cnt:
            n += cnt
            print(f"  {bad!r} -> {good!r} ({cnt} rows)")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_name=? WHERE sponsor_name=?",
                             (good, bad))
    for ein, (from_year, name) in RENAMED_FROM_YEAR.items():
        cnt = conn.execute("SELECT COUNT(*) FROM form5500_filings WHERE ltrim(ein,'0')=? "
                           "AND filing_year>=? AND sponsor_name<>?", (ein, from_year, name)).fetchone()[0]
        if cnt:
            n += cnt
            print(f"  EIN {ein} from {from_year} -> {name!r} ({cnt} rows)")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_name=? WHERE ltrim(ein,'0')=? "
                             "AND filing_year>=?", (name, ein, from_year))
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-refresh.bak"
        if not os.path.exists(bak):
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== 1. new filings ===")
    n_new = import_new_filings(conn, dry_run)

    print("\n=== locating each row's return (2014-2025) ===")
    located = refetch.locate_returns(conn)
    print(f"  {len(located)} rows located")

    print("\n=== 2. participant counts paired with the right return ===")
    n_pairs = fix_participant_pairs(conn, located, dry_run)

    print("\n=== 3. financials re-derived strictly ===")
    stats = refetch.rederive(conn, located, dry_run)

    print("\n=== 4. address from each row's own return ===")
    n_moved, n_removed = fix_address_from_own_return(conn, located, dry_run)

    print("\n=== 5. open-year identity aligned to earlier rows; display names ===")
    n_ident = align_open_year_identity(conn, located, dry_run)
    n_names = fix_sponsor_names(conn, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        for k, v in (("last_import", now), ("refresh_2026_10_applied", now),
                     ("dol_release", config.DOL_RELEASE_DATE)):
            conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)", (k, v))
        conn.commit()
    conn.close()

    if not dry_run:
        refetch.save_filtered_schedules(located)
        fa.nullify_unreported_employer_securities()
        fa.recompute_annual_summaries()

    print(f"\nimported {n_new}; participant pairs fixed {n_pairs}; financial rows changed "
          f"{stats['changed']}; cities corrected {n_moved}; non-MA rows removed {n_removed}; "
          f"open-year identity aligned {n_ident}; names fixed {n_names}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
