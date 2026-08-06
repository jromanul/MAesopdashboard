#!/usr/bin/env python3
"""
Corrections from the full 2024 row-level audit (2026-08-06).

Every 2024 ESOP row was re-verified field-by-field against DOL's current bulk
data — the main F_5500 form for identity/participants and Schedule H/I for
financials. Four plans needed correction; the other 124 matched exactly.

Two matching rules matter here, and getting either wrong produces convincing
but wrong "discrepancies" (both bit during this audit):

  1. The 2024 bulk files contain schedule rows for MULTIPLE plan years — a
     plan-year-2025 short/final filing ships inside the 2024 dataset. Schedule
     rows must therefore be filtered on SCH_*_PLAN_YEAR_BEGIN_DATE starting
     with 2024, not merely joined on (EIN, plan number). Without that filter
     B&B Engineering matches its PY2025 row (assets $0) instead of its real
     PY2024 row ($2,368,209).
  2. A sponsor can file more than one return for the same form year — an
     amended full-year return plus a short final-year return beginning
     2024-12-31. Savogran files both; the full-year amended return (5
     participants / 3 active) is the 2024 record, while the short one (3 / 0)
     belongs to the following period. Comparisons must accept a value that
     matches ANY candidate filing rather than picking one and trusting it.

Corrections applied:

  Brockway-Smith Company (EIN 041123740, PN 003) — imported by
    apply_2024_refresh.py with financials and several identity fields left
    empty. Cause: that script looked up Schedule I's plan number as
    "SCH_I_PN", which is not a DOL column (the real name is SCH_I_PLAN_NUM),
    so every Schedule I plan number collapsed to a default and this plan's
    row could not be matched. The column-name bug is fixed; this backfills the
    data it cost — assets, benefits paid and net income from Schedule I, plus
    NAICS, plan effective date and ZIP from the main form. Note the plan is
    winding down hard: $40.6M -> $2.0M with $24.4M distributed.

  Diamond Antenna and Microwave Corporation (EIN 043247749, PN 003) and
  Web Industries, Inc. (EIN 042880295, PN 002) — benefits paid and net income
    were never populated though both report them. Web Industries also reports
    a $9.4M distribution against $90.4M in assets, so the omission was visible
    on the dashboard as an em-dash in a row that otherwise had full financials.

  Seal-Ryt Corp. (EIN 651174540, PN 002) — filed an AMENDED return received
    2026-07-25, after the May 2026 data vintage. Assets $145,269 -> $145,245
    and net income -$527,177 -> -$527,201 per the amended Schedule I.

Explicitly NOT changed: participant counts anywhere. `total_participants` is
DOL's beginning-of-year total (TOT_PARTCP_BOY_CNT) while `active_participants`
is the end-of-year active count (TOT_ACTIVE_PARTCP_CNT) — DOL publishes no
end-of-year total in this layout, so the two sit on different bases and a
growing plan can legitimately show more active than total. That pairing is the
convention across all eleven years in this dataset; changing it would alter
every headline participant figure and break year-over-year comparability, so it
is documented rather than silently "fixed".

Idempotent.
"""
import os
import shutil
import sqlite3
from datetime import datetime, timezone

import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

DB = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

# (ein, plan_num) -> {column: value}
FIXES = {
    ("41123740", "3"): {
        "total_assets": 1987631.0,
        "benefits_paid": 24374746.0,
        "net_income": -38636031.0,
        "naics_code": "423300",
        "industry_sector": fa.NAICS_SECTORS.get("42", ""),
        "plan_eff_date": "1976-01-01",
        "sponsor_zip": "018101492",
    },
    ("43247749", "3"): {
        "benefits_paid": 22406206.0,
        "net_income": -13359328.0,
        "employer_contributions": 0.0,
        "participant_contributions": 0.0,
    },
    ("42880295", "2"): {
        "benefits_paid": 9429370.0,
        "net_income": -5461115.0,
    },
    ("651174540", "2"): {
        "total_assets": 145245.0,
        "net_income": -527201.0,
    },
}


def main():
    bak = DB + ".pre-2024auditfixes.bak"
    if not os.path.exists(bak):
        shutil.copy2(DB, bak)
        print(f"backup -> {bak}")

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    total = 0
    for (ein, pn), cols in FIXES.items():
        row = conn.execute(
            "SELECT sponsor_name FROM form5500_filings "
            "WHERE filing_year=2024 AND ein=? AND plan_num=?", (ein, pn)).fetchone()
        if not row:
            print(f"  !! no 2024 row for EIN {ein} PN {pn} — skipped")
            continue
        sets = ", ".join(f"{c}=?" for c in cols)
        cur = conn.execute(
            f"UPDATE form5500_filings SET {sets} "
            f"WHERE filing_year=2024 AND ein=? AND plan_num=?",
            list(cols.values()) + [ein, pn])
        total += cur.rowcount
        print(f"  {row['sponsor_name']}: set {', '.join(cols)}")

    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 ("audit_fixes_2024_applied", datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

    fa.nullify_unreported_employer_securities()
    fa.recompute_annual_summaries()
    print(f"\nrows updated: {total}; employer-securities guards + summaries re-run")

    conn = sqlite3.connect(DB)
    n, p, a = conn.execute(
        "SELECT COUNT(*), SUM(total_participants), SUM(total_assets) "
        "FROM form5500_filings WHERE filing_year=2024 AND is_esop=1").fetchone()
    print(f"2024 now: {n} plans, {p:,} participants, ${a:,.0f} assets")
    conn.close()


if __name__ == "__main__":
    main()
