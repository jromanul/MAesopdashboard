#!/usr/bin/env python3
"""
October 2026 field audit: every stored field of every row, 2014-2025, checked
against the row's own Form 5500 return, not only the participant counts,
city and dollar figures that scan_dol_filers.py verifies. Reads DOL's bulk
files at run time (DOL_CACHE_DIR). Idempotent; backs up the database first.

WHAT THE AUDIT FOUND, AND WHAT IS FIXED
---------------------------------------
1. ESOP CODES. 1,191 rows written by the earliest imports (2014-2023) had no
   type-of-pension-benefit codes stored. Every row now carries its return's
   codes.

2. KSOP FLAG. A KSOP is an ESOP with a 401(k) feature: code 2J alongside the
   ESOP code (scan_dol_filers.is_ksop_codes). The written rule also required
   2K (a 401(m) arrangement), which twelve stored flags contradicted: Strem
   Chemicals' and Haberman Insurance's 401(k)-and-ESOP plans carry 2J
   without 2K and were flagged as KSOPs in every year but Strem 2018 and
   Haberman 2017. The test is now 2J, applied to every row from its return's
   codes; those two rows join the other 31.

3. ZIP CODES. 1,190 rows had lost a leading zero ("2114" for 02114) when an
   early import read the seed CSV with ZIPs as numbers; the CSV loader now
   reads them as text. Each row's ZIP is its return's, as filed (Interaction
   Associates 2018 carried the ZIP of its 2019 final return). Rows of
   Massachusetts companies whose returns are filed from another state
   (scan_dol_filers.MA_BASED_OVERRIDES) carried that state's ZIP - Gillette's
   Boston rows showed Cincinnati's 45202 - and the 25 such rows now carry the
   company's home ZIP.

4. INDUSTRY. 92 rows (2014-2023) had no NAICS code or industry sector. Each
   takes the plan's own curated code from its other years, or failing that
   the code on its return; the sector follows the code. One row's sector
   contradicted its code (Danafilms 2014, a film maker filed under
   Information & Technology). The 197 curated codes that differ from the code
   on the return are kept: they were checked against company websites.

5. SMALLER FIELDS. Two 2024 rows had no plan-entity type; one plan name
   differed from its return in punctuation (Applied Marketing Science 2020).
   (Gemini Industries' 2024 plan name, once annotated "(Gemini Industries
   ESOP)", is as filed since the 2024-2025 verification.)

6. SPONSOR NAMES. Formatting slips that made one company read differently
   from one year to the next, and casing left by title-casing DOL's capitals
   ("Bbn", "Scc And Integrated It") - 92 rows (SPONSOR_NAME_FIXES). Real renames - La Capra
   to Daymark, Westfield Financial to Western New England Bancorp, Provident
   to BankProv - are kept.

Usage:
    DOL_CACHE_DIR=/path/to/zips python3 apply_2026_10_field_audit.py [--dry-run]
"""
from __future__ import annotations

import collections
import os
import shutil
import sqlite3
import sys

import config
import dol_bulk
import form5500_analysis as fa
import refetch_dol_financials as refetch
import scan_dol_filers as scan

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH

SPONSOR_NAME_FIXES = {
    "Williams Stone Co. , Inc.": "Williams Stone Co., Inc.",
    "Applied Marketing Science, Inc": "Applied Marketing Science, Inc.",
    "Oem Connect, Ltd": "OEM Connect, Ltd.",
    "The L. S. Starrett Company": "The L.S. Starrett Company",
    "Goody, Clancy And Associates": "Goody, Clancy & Associates",
    "Tro Jung/Brannen": "TRO Jung/Brannen",
    "Tro Corporation": "TRO Corporation",
    "Danafilms, Inc..": "Danafilms, Inc.",
    "Danafilms": "Danafilms, Inc.",
    "Paragus Strategic It": "Paragus Strategic IT",
    "Bbn Technologies": "BBN Technologies",
    "Scc And Integrated It Holding Corp": "SCC and Integrated IT Holding Corp.",
    "The Bsc Companies, Inc.": "The BSC Companies, Inc.",
    "The Mmr Group, Inc.": "The MMR Group, Inc.",
    "Mis Training Institute Holdings, Inc.": "MIS Training Institute Holdings, Inc.",
    "Arc/Architectural Resources Cambridge": "ARC/Architectural Resources Cambridge",
    "Add Inc.": "ADD Inc.",
    "Diamond Antenna And Microwave Corporation": "Diamond Antenna and Microwave Corporation",
    "Shawmut Design And Construction": "Shawmut Design and Construction",
}


def return_fields(conn, located: dict, dry_run: bool) -> collections.Counter:
    """Codes, KSOP flag, plan entity type, plan name and ZIP from each row's return."""
    n = collections.Counter()
    for row_id, (y, ret, _sched) in located.items():
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        if r is None:
            continue
        key = scan.norm_key(r["ein"], r["plan_num"])
        upd = {}
        codes = ret["codes"]
        if (r["type_pension_bnft"] or "") != codes:
            upd["type_pension_bnft"] = codes
        ksop = 1 if scan.is_ksop_codes(codes) else 0
        if int(r["is_ksop"] or 0) != ksop:
            upd["is_ksop"] = ksop
        if not r["type_plan_entity"] and ret["type_plan_entity"]:
            upd["type_plan_entity"] = ret["type_plan_entity"]
        if not r["plan_eff_date"] and ret["plan_eff_date"]:
            upd["plan_eff_date"] = ret["plan_eff_date"]
        stored = (r["plan_name"] or "").strip()
        if stored.upper() != ret["plan_name"].strip().upper() and "(" not in stored:
            upd["plan_name"] = ret["plan_name"].strip()
        hit = scan._ma_override(key[0], key[1], y)
        if hit and hit[1] and hit[2]:
            want_zip = hit[2][1]
        elif dol_bulk.is_ma_address(ret["state"], ret["sponsor_zip"]):
            want_zip = ret["sponsor_zip"].strip()
        else:
            want_zip = r["sponsor_zip"]
        if want_zip and (r["sponsor_zip"] or "") != want_zip:
            upd["sponsor_zip"] = want_zip
        for f in upd:
            n[f] += 1
        if upd and not dry_run:
            conn.execute("UPDATE form5500_filings SET " + ", ".join(f"{f}=?" for f in upd) +
                         " WHERE id=?", list(upd.values()) + [row_id])
    if not dry_run:
        conn.commit()
    return n


def fill_industry(conn, located: dict, dry_run: bool) -> int:
    """A blank NAICS takes the plan's curated code from its other years, or the
    return's; the sector always follows the code."""
    ret_naics = {row_id: ret["naics_code"] for row_id, (_y, ret, _s) in located.items()}
    n = 0
    for r in conn.execute("SELECT * FROM form5500_filings").fetchall():
        naics = r["naics_code"] or ""
        if not naics:
            other = conn.execute(
                "SELECT naics_code FROM form5500_filings WHERE ltrim(ein,'0')=? AND naics_code<>'' "
                "ORDER BY abs(filing_year-?) LIMIT 1", (dol_bulk.norm_ein(r["ein"]), r["filing_year"])).fetchone()
            naics = (other[0] if other else "") or ret_naics.get(r["id"], "")
        sector = fa.NAICS_SECTORS.get(naics[:2], "") if naics else ""
        if not sector:
            sector = r["industry_sector"] or ""
        if (naics, sector) == ((r["naics_code"] or ""), (r["industry_sector"] or "")):
            continue
        n += 1
        print(f"  {r['filing_year']} {r['sponsor_name'][:40]:40} NAICS {r['naics_code']!r} -> {naics!r}, "
              f"sector {r['industry_sector']!r} -> {sector!r}")
        if not dry_run:
            conn.execute("UPDATE form5500_filings SET naics_code=?, industry_sector=? WHERE id=?",
                         (naics, sector, r["id"]))
    if not dry_run:
        conn.commit()
    return n


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
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-field-audit.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== locating each row's return (2014-2025) ===")
    located = refetch.locate_returns(conn)
    print(f"  {len(located)} rows located")

    print("\n=== fields from each row's own return ===")
    counts = return_fields(conn, located, dry_run)
    for f, c in sorted(counts.items()):
        print(f"  {f}: {c} rows")

    print("\n=== industry ===")
    n_ind = fill_industry(conn, located, dry_run)

    print("\n=== sponsor names ===")
    n_names = fix_sponsor_names(conn, dry_run)
    conn.close()

    if not dry_run:
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nreturn fields corrected {sum(counts.values())} (on {len(counts)} fields); industry "
          f"rows {n_ind}; names {n_names}" + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
