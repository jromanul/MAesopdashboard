#!/usr/bin/env python3
"""
Regenerate the repo-root seed CSVs (form5500_ma_esops.csv,
form5500_annual_summary.csv) from the current database.

These CSVs exist only as an auto-seed path: app.py imports them at startup
IF AND ONLY IF the database has no filings yet (see form5500_analysis.has_data()
/ has_financial_data() and the "Data Loading" block in app.py). Since
data/form5500_dashboard.db is committed to git and already populated, they are
not read in normal operation -- but they should still reflect the database, in
case the DB is ever reset and the app falls back to reseeding from them.

filing_year=2025 (config.FORM5500_OPEN_FORM_YEAR) is excluded from both files,
matching form5500_analysis.recompute_annual_summaries(): that year is tracked
separately on the "2025 Filers" page and must not seed a complete-year row.

Column set is a superset of what import_from_csv()/import_from_summary_csv()
read (both use dict-style .get() lookups, so extra columns are ignored) --
notably this now includes employer_securities, ma_total_benefits_paid, and
ma_total_employer_securities, which the previous seed files predated and
would have silently lost on a reseed.

Usage:
    python export_seed_csvs.py
"""
import csv
import os
import sqlite3

import config

import safe_console

safe_console.enable_utf8_stdout()

DB_PATH = config.DB_PATH
OPEN_YEAR = config.FORM5500_OPEN_FORM_YEAR

FILINGS_COLS = [
    "filing_year", "ein", "plan_num", "plan_name", "sponsor_name",
    "sponsor_city", "sponsor_state", "sponsor_zip", "type_plan_entity",
    "type_pension_bnft", "is_esop", "is_ksop", "total_participants",
    "active_participants", "total_assets", "total_liabilities",
    "employer_contributions", "participant_contributions", "benefits_paid",
    "net_income", "employer_securities", "naics_code", "industry_sector",
    "plan_eff_date",
]

SUMMARY_COLS = [
    "filing_year", "ma_plan_count", "ma_esop_count", "ma_ksop_count",
    "ma_total_participants", "ma_active_participants", "ma_total_assets",
    "ma_avg_plan_assets", "ma_total_contributions", "ma_avg_participants",
    "ma_total_benefits_paid", "ma_total_employer_securities",
    "us_total_esop_count", "us_total_participants", "us_total_assets",
]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    filings_path = os.path.join(os.path.dirname(__file__) or ".", config.FORM5500_RECORDS_CSV)
    rows = conn.execute(
        f"SELECT {', '.join(FILINGS_COLS)} FROM form5500_filings "
        f"WHERE is_esop = 1 AND filing_year < ? ORDER BY filing_year, ein, plan_num",
        (OPEN_YEAR,),
    ).fetchall()
    with open(filings_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(FILINGS_COLS)
        for r in rows:
            w.writerow([r[c] if r[c] is not None else "" for c in FILINGS_COLS])
    print(f"{config.FORM5500_RECORDS_CSV}: {len(rows)} rows (filing_year < {OPEN_YEAR})")

    summary_path = os.path.join(os.path.dirname(__file__) or ".", config.FORM5500_SUMMARY_CSV)
    srows = conn.execute(
        f"SELECT {', '.join(SUMMARY_COLS)} FROM form5500_annual_summary "
        f"WHERE filing_year < ? ORDER BY filing_year",
        (OPEN_YEAR,),
    ).fetchall()
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SUMMARY_COLS)
        for r in srows:
            w.writerow([r[c] if r[c] is not None else "" for c in SUMMARY_COLS])
    print(f"{config.FORM5500_SUMMARY_CSV}: {len(srows)} rows (filing_year < {OPEN_YEAR})")

    conn.close()


if __name__ == "__main__":
    main()
