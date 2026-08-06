#!/usr/bin/env python3
"""
Export a deduplicated DOL ESOP snapshot for the MassCEO master list.

This is the "handshake" file between this dashboard and the master spreadsheet:

  form5500-dashboard  --> DOL ESOP snapshot (CSV) -->  MA Employee Owned Businesses.xlsx
  (authoritative for         (this script's output)     (authoritative for contacts,
   ESOP existence /                                       outreach, co-ops, EOTs)
   participants / assets)

It reads form5500_filings, keeps ONE row per company (by EIN, using that
company's most recent ESOP filing), and writes a clean CSV that Claude Cowork
consumes during reconciliation (see the eo-business-dedup-reconcile skill).
Cowork must NEVER read this SQLite database directly -- only this snapshot.

Run this whenever the database changes, then run the reconciliation skill.

Output columns:
  ein, company_name, city, naics_code, industry, last_filing_year,
  total_participants, total_assets

Usage:
  python export_esop_snapshot.py                      # default DB + default output path
  python export_esop_snapshot.py --out /some/where.csv
  python export_esop_snapshot.py --db data/other.db
"""

import argparse
import csv
import os
import sqlite3
from datetime import datetime, timezone

import safe_console

safe_console.enable_utf8_stdout()

DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

# Default destination: the Business Tracking & Outreach folder, alongside the
# master "MA Employee Owned Businesses" workbook the snapshot is reconciled against.
DEFAULT_OUT = os.path.expanduser(
    "~/MassCEO/Business Tracking & Outreach/DOL ESOP Snapshot (from form5500-dashboard).csv"
)

OUTPUT_COLUMNS = [
    "ein", "company_name", "city", "naics_code", "industry",
    "last_filing_year", "total_participants", "total_assets",
]


def _num(value):
    """Sort-key helper: treat NULLs as -1 so any real value wins a tie-break."""
    return value if isinstance(value, (int, float)) else -1


def build_snapshot(db_path):
    """Return (one row per company at its latest ESOP filing, db meta dict)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT ein, sponsor_name, sponsor_city, naics_code, industry_sector,
                   filing_year, total_participants, total_assets
            FROM form5500_filings
            WHERE is_esop = 1 AND ein IS NOT NULL AND TRIM(ein) <> ''
            """
        ).fetchall()
        meta = dict(conn.execute("SELECT key, value FROM form5500_meta").fetchall())
    finally:
        conn.close()

    # Keep, per EIN, the most recent filing (break ties by participants then assets).
    best = {}
    for r in rows:
        ein = r["ein"].strip()
        candidate = (r["filing_year"] or 0, _num(r["total_participants"]), _num(r["total_assets"]))
        if ein not in best or candidate > best[ein][0]:
            best[ein] = (candidate, r)

    snapshot = []
    for ein, (_, r) in best.items():
        snapshot.append({
            "ein": ein,
            "company_name": (r["sponsor_name"] or "").strip(),
            "city": (r["sponsor_city"] or "").strip(),
            "naics_code": r["naics_code"] or "",
            "industry": r["industry_sector"] or "",
            "last_filing_year": r["filing_year"],
            "total_participants": r["total_participants"],
            "total_assets": r["total_assets"],
        })
    snapshot.sort(key=lambda d: d["company_name"].lower())
    return snapshot, meta


def write_csv(snapshot, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(snapshot)


def write_about(snapshot, meta, out_path, db_path):
    """Companion freshness file so Cowork can tell whether the snapshot is stale."""
    about_path = os.path.splitext(out_path)[0] + ".about.txt"
    generated = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    lines = [
        "DOL ESOP snapshot -- provenance",
        f"generated_at:      {generated}",
        f"source_db:         {os.path.abspath(db_path)}",
        f"db_last_import:    {meta.get('last_import', 'unknown')}",
        f"db_last_recompute: {meta.get('last_recompute', 'unknown')}",
        f"company_count:     {len(snapshot)}",
        "",
        "Authoritative for ESOP existence / participants / assets only.",
        "Reconcile via the eo-business-dedup-reconcile skill; do not edit by hand.",
    ]
    with open(about_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return about_path, generated


def main():
    parser = argparse.ArgumentParser(
        description="Export a deduped DOL ESOP snapshot for the MassCEO master list."
    )
    parser.add_argument("--db", default=DB_PATH, help="Path to form5500_dashboard.db")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output CSV path")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        raise SystemExit(f"ERROR: database not found: {args.db}")

    snapshot, meta = build_snapshot(args.db)
    write_csv(snapshot, args.out)
    about_path, generated = write_about(snapshot, meta, args.out, args.db)

    print(f"Wrote {len(snapshot)} unique MA ESOP companies")
    print(f"  snapshot:  {args.out}")
    print(f"  about:     {about_path}")
    print(f"  generated: {generated}")
    print(f"  db last_import: {meta.get('last_import', 'unknown')}")


if __name__ == "__main__":
    main()
