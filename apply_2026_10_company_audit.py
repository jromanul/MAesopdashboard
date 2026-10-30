#!/usr/bin/env python3
"""
October 2026 company-information audit: every company's name, industry and
town, in every year 2014-2025, checked against its returns and public sources.
Needs no DOL files. Idempotent; backs up the database first.

WHAT WAS CHECKED
----------------
   - Every stored field against the row's own return. Plan names, plan
     effective dates and plan types match; the scans already verify
     participants, money, codes, city and ZIP.
   - Completeness, again: a reconciliation of every 2013-2025 filing finds no
     Massachusetts ESOP return unaccounted for.
   - Industry: each company's code in every year, by reviewers working from
     company sources, with a second reviewer confirming each change.
   - Names: every sponsor name against the returns, under one rule. A company
     reads the same in every year unless it was genuinely renamed. The name is
     the one filed, properly cased, with a brand or operating name in
     parentheses where the filer is a holding company.

WHAT IS FIXED
-------------
1. INDUSTRY. One code per company in every year (scan_dol_filers.VERIFIED_NAICS,
   now applied from 2014):
     - the owner's May 2026 website-verified codes for the 2024 companies;
     - reviewed codes for the rest.
   Padded codes such as 339900 or 541600 were not real 6-digit codes and became
   the specific code. Bank codes were left as filed. Among the sector changes
   in 2014-2023: Windover Construction to Construction, Williams Stone to
   Manufacturing, Visidyne to Professional & Technical Services, Channing Bete
   (a publisher) to Information. No 2024 or 2025 sector changes.

2. NAMES. Names taken from a plan's title rather than the company's are
   replaced by the filed name. For example:
     - "Notch" becomes Notch Welding & Mechanical Contractors, Inc.;
     - "Vertex" becomes The Vertex Companies, Inc.;
     - "USI" becomes USI Holdings, Inc. (Ultra Services).
   Brand names keep the filed name in front:
     - Mass. Bay Brewing Company, Inc. (Harpoon Brewery);
     - The Fitzpatrick Companies, Inc. (Country Curtains).
   Suffixes are restored as filed, and four renames now start in the year the
   returns show them. Picis keeps its name; wound-down shells keep the
   company's name (D.N. Lukens, Channing Bete, Washburn-Garfield, Janis
   Research).

3. TOWNS. Short forms (Marlboro, Middleboro, Southboro, Westboro) become the
   municipality's official name (scan_dol_filers.CITY_SPELLINGS).

Usage:
    python3 apply_2026_10_company_audit.py [--dry-run]
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
from apply_2026_10_page_audit import restore_verified_naics

import safe_console

safe_console.enable_utf8_stdout()

DB = config.DB_PATH

ALL = range(2014, 2026)

# (EIN, years, name[, plan number]): each company's name as filed, reviewed
# against its returns. A plan number scopes an EIN that files several
# companies' plans.
SPONSOR_NAMES = [
    ("42444641", {2017}, "Sallop Insurance"),
    ("731627673", {2016}, "Western New England Bancorp, Inc."),
    ("844259761", {2020}, "David Clark Holdings, Inc."),
    ("851665706", {2024}, "Northern Tree Holdco, Inc."),
    ("42318619", {2014}, "TRO Jung/Brannen Corporation"),
    ("42472856", ALL, "The James Monroe Wire & Cable Corp."),
    ("770370729", ALL, "Win-It-Too, Inc. (Global Beer Network)"),
    ("42883090", ALL, "World Publications Group, Inc."),
    ("203724403", ALL, "The Vertex Companies, Inc."),
    ("824155907", ALL, "USI Holdings, Inc. (Ultra Services)"),
    ("42587894", ALL, "Notch Welding & Mechanical Contractors, Inc."),
    ("42831820", ALL, "Burkart-Phelan, Inc."),
    ("41967105", ALL, "J.F. White Contracting Company"),
    ("42925204", ALL, "Mass. Bay Brewing Company, Inc. (Harpoon Brewery)"),
    ("42259905", ALL, "The Fitzpatrick Companies, Inc. (Country Curtains)"),
    ("832236376", ALL, "Munro Associates, LLC (The Vault)"),
    ("42373147", ALL, "Albert F. Fitzgerald, Inc. (Fitzgerald Tile)"),
    ("42556035", ALL, "Landry's, Inc. (Landry's Bicycles)"),
    ("921304786", ALL, "FE Holdings, Inc. (InTransit Container)"),
    ("42453189", ALL, "Architectural Resources Cambridge, Inc."),
    ("412126830", ALL, "BBN Technologies Holding Corp."),
    ("43579044", ALL, "Erland Construction Holding Company, Inc."),
    ("208818800", ALL, "Raycon Construction Holding Company"),
    ("822323992", ALL, "Cadmus Holding Company, Inc."),
    ("42793755", ALL, "The Cadmus Group, Inc."),
    ("42348065", ALL, "Janis Research Company Inc."),
    ("42717456", ALL, "Atlantic Fasteners Co., Inc."),
    ("43026692", ALL, "Baxter, Inc."),
    ("43521482", ALL, "Blue Q Corporation"),
    ("43401502", ALL, "Carney, Sandoe & Associates, Inc."),
    ("42301242", ALL, "Goody, Clancy & Associates, Inc."),
    ("41203530", ALL, "Spaulding Brick Co., Inc."),
    ("42086300", ALL, "Fraser Engineering Co., Inc."),
    ("42550058", ALL, "International Cars, Ltd., Inc."),
    ("43358086", ALL, "CourtSmart Digital Systems Inc."),
    ("42300185", ALL, "Dale Medical Products, Inc."),
    ("43553283", ALL, "NMR Group, Inc."),
    ("204270900", ALL, "Paragus Strategic IT, Inc."),
    ("813645861", ALL, "Shawmut Group Inc."),
    ("311060347", ALL, "Stevenson & Associates, Inc."),
    ("42630303", ALL, "Titan Roofing, Inc."),
    ("43041248", ALL, "G&C Concrete Construction, Inc."),
    ("920450207", ALL, "Rick Roy Construction, LLC"),
    ("43340225", ALL, "LDA Architecture & Interiors, Inc."),
    ("41810570", ALL, "The Savogran Company"),
    ("42079638", ALL, "The Haberman Insurance Group, Inc."),
    ("42274728", ALL, "Feingold & Feingold Insurance Agency, Inc."),
    ("42273500", ALL, "L.P.M. Holding Company, Inc."),
    ("42745661", ALL, "Lessard Taylor Communications Inc."),
    ("41782600", ALL, "Central Co-operative Bank", "4"),
    ("41294820", ALL, "Everett Co-operative Bank"),
]


def fix_names(conn, dry_run: bool) -> int:
    n = 0
    for entry in SPONSOR_NAMES:
        ein, years, name = entry[:3]
        pn = entry[3] if len(entry) > 3 else None
        sql = ("SELECT id, filing_year, sponsor_name FROM form5500_filings "
               "WHERE ltrim(ein,'0')=? AND sponsor_name<>?")
        args = [ein, name]
        if pn:
            sql += " AND ltrim(plan_num,'0')=?"
            args.append(pn)
        for r in conn.execute(sql, args).fetchall():
            if r["filing_year"] not in years:
                continue
            n += 1
            print(f"  {r['filing_year']} {r['sponsor_name']!r} -> {name!r}")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_name=? WHERE id=?", (name, r["id"]))
    if not dry_run:
        conn.commit()
    return n


def fix_town_spellings(conn, dry_run: bool) -> int:
    n = 0
    for r in conn.execute("SELECT id, filing_year, sponsor_name, sponsor_city FROM form5500_filings").fetchall():
        want = scan.city_spelling(r["sponsor_city"] or "")
        if want and want != r["sponsor_city"] and want.upper() != (r["sponsor_city"] or "").upper():
            n += 1
            print(f"  {r['filing_year']} {r['sponsor_name'][:40]:40} {r['sponsor_city']} -> {want}")
            if not dry_run:
                conn.execute("UPDATE form5500_filings SET sponsor_city=? WHERE id=?", (want, r["id"]))
    if not dry_run:
        conn.commit()
    return n


def main():
    dry_run = "--dry-run" in sys.argv
    if not dry_run:
        bak = DB + ".pre-2026-10-company-audit.bak"
        if not os.path.exists(bak):
            with sqlite3.connect(DB) as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            shutil.copy2(DB, bak)
            print(f"backup -> {bak}")
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    print("=== industry codes (one per company, every year) ===")
    n_naics = restore_verified_naics(conn, dry_run)
    print("\n=== sponsor names ===")
    n_names = fix_names(conn, dry_run)
    print("\n=== town spellings ===")
    n_towns = fix_town_spellings(conn, dry_run)

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("UPDATE form5500_filings SET fetched=? WHERE fetched IS NULL OR fetched=''", (now,))
        conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                     ("company_audit_2026_10_applied", now))
        conn.commit()
    conn.close()

    if not dry_run:
        fa.recompute_annual_summaries()
        with sqlite3.connect(DB) as c:
            c.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print(f"\nindustry rows {n_naics}; names {n_names}; towns {n_towns}"
          + (" (dry run, nothing written)" if dry_run else ""))


if __name__ == "__main__":
    main()
