"""
Shared helper functions for the Form 5500 ESOP Dashboard.
"""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


def format_last_updated(iso_str: str | None) -> str:
    """Format a UTC ISO timestamp to a readable local time string."""
    if not iso_str:
        return "Never"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%b %d, %Y at %I:%M %p UTC")
    except (ValueError, TypeError):
        return iso_str


def to_csv_bytes(data: list[dict]) -> bytes:
    """Convert a list of dicts to CSV bytes for download."""
    if not data:
        return b""
    df = pd.DataFrame(data)
    return df.to_csv(index=False).encode("utf-8")


# ── Shareable data package ──────────────────────

# (database column, column heading in the exported CSV). Ordered for a reader,
# not for the schema: who the plan is, then where, then how big.
_EXPORT_COLUMNS = [
    ("plan_name", "Plan Name"),
    ("sponsor_name", "Sponsor"),
    ("sponsor_city", "City"),
    ("sponsor_state", "State"),
    ("sponsor_zip", "ZIP"),
    ("industry_sector", "Industry"),
    ("naics_code", "NAICS Code"),
    ("plan_eff_date", "Plan Effective Date"),
    ("is_ksop", "KSOP (401k+ESOP)"),
    ("total_participants", "Total Participants (beginning of year)"),
    ("active_participants", "Active Participants (end of year)"),
    ("total_assets", "Total Assets"),
    ("total_liabilities", "Total Liabilities"),
    ("employer_securities", "Employer Securities (company stock)"),
    ("employer_contributions", "Employer Contributions"),
    ("participant_contributions", "Participant Contributions"),
    ("benefits_paid", "Benefits Paid"),
    ("net_income", "Net Income"),
    ("ein", "EIN"),
    ("plan_num", "Plan Number"),
    ("filing_year", "Form Year"),
]


# Stored as floats, but DOL reports whole dollars and whole people. Exported as
# nullable integers so a recipient opening the CSV sees 729347060, not
# 729347060.0, and a value the plan never reported stays an empty cell.
_WHOLE_NUMBER_COLUMNS = {
    "total_participants", "active_participants", "total_assets",
    "total_liabilities", "employer_securities", "employer_contributions",
    "participant_contributions", "benefits_paid", "net_income",
}


def _export_frame(rows: list[dict]) -> pd.DataFrame:
    """Project rows onto the reader-facing export columns, in order."""
    df = pd.DataFrame(rows)
    out = pd.DataFrame()
    for src, label in _EXPORT_COLUMNS:
        if src not in df.columns:
            continue
        col = df[src]
        if src == "is_ksop":
            col = col.map(lambda v: "Yes" if v else "No")
        elif src in _WHOLE_NUMBER_COLUMNS:
            col = pd.to_numeric(col, errors="coerce").round().astype("Int64")
        out[label] = col
    return out


def build_share_package(year: int, all_rows: list[dict], active_rows: list[dict],
                        summaries: list[dict], data_as_of: str,
                        dashboard_url: str = "https://maesopdashboard.streamlit.app") -> bytes:
    """Build the distributable ZIP of a single form year's MA ESOP data.

    Produced in memory so it works on a read-only host. Ships both the complete
    filed set and the active subset, because the two answer different questions
    and quoting the wrong one is the easiest way for a recipient to get a figure
    wrong. The README carries the counts and the definitions so the numbers can
    be checked against the dashboard without asking anyone.
    """
    import zipfile

    all_df = _export_frame(all_rows)
    act_df = _export_frame(active_rows)

    sum_rows = [s for s in summaries if s.get("ma_plan_count")]
    sum_df = pd.DataFrame([{
        "Form Year": s["filing_year"],
        "Plans Filed": s["ma_plan_count"],
        "ESOPs": s.get("ma_esop_count"),
        "KSOPs": s.get("ma_ksop_count"),
        "Total Participants": s.get("ma_total_participants"),
        "Active Participants": s.get("ma_active_participants"),
        "Total Assets": s.get("ma_total_assets"),
        "Employer Contributions": s.get("ma_total_contributions"),
        "Benefits Paid": s.get("ma_total_benefits_paid"),
        "Employer Securities": s.get("ma_total_employer_securities"),
    } for s in sum_rows])

    n_all, n_act = len(all_rows), len(active_rows)
    n_excl = n_all - n_act
    assets_all = sum(r.get("total_assets") or 0 for r in all_rows)
    assets_act = sum(r.get("total_assets") or 0 for r in active_rows)
    part_all = sum(r.get("total_participants") or 0 for r in all_rows)
    part_act = sum(r.get("total_participants") or 0 for r in active_rows)
    ksops = sum(1 for r in all_rows if r.get("is_ksop"))
    stock_rep = sum(1 for r in active_rows if (r.get("employer_securities") or 0) > 0)
    span = (f"{min(s['filing_year'] for s in sum_rows)}-{max(s['filing_year'] for s in sum_rows)}"
            if sum_rows else str(year))

    readme = f"""MASSACHUSETTS EMPLOYEE STOCK OWNERSHIP PLANS (ESOPs)
Form 5500 data for form year {year}

Source:  U.S. Department of Labor, EFAST2 Form 5500 bulk datasets
         (annual returns filed by employee benefit plans)
Data as of: {data_as_of}
Dashboard:  {dashboard_url}


WHAT IS IN THIS PACKAGE
-----------------------
1. MA_ESOP_{year}_all_filed_plans.csv   ({n_all} plans)
   Every Massachusetts ESOP that filed a Form 5500 for form year {year},
   including plans that are winding down. Use this for "how many ESOPs
   filed in Massachusetts".

2. MA_ESOP_{year}_active_plans.csv      ({n_act} plans)
   The subset still operating: plans reporting at least one active
   participant, excluding a short list of known defunct/wind-down plans.
   Use this for "how many working Massachusetts ESOPs are there" and for
   any per-plan average. This is the set the dashboard's headline
   figures describe.

3. MA_ESOP_annual_summary_{span}.csv
   One row per form year, for trends over time.

4. README.txt (this file)


HEADLINE FIGURES FOR {year}
{'-' * (len('HEADLINE FIGURES FOR ') + len(str(year)))}
  Plans filed                {n_all:>18,}
  Active plans               {n_act:>18,}
    (excluded as inactive)   {n_excl:>18,}
  KSOPs (401(k)+ESOP)        {ksops:>18,}
  Participants, all filed    {part_all:>18,}
  Participants, active plans {part_act:>18,}
  Assets, all filed          {'$' + format(assets_all, ',.0f'):>18}
  Assets, active plans       {'$' + format(assets_act, ',.0f'):>18}


HOW TO READ THE COLUMNS
-----------------------
Total Participants is the plan's total at the BEGINNING of the plan year
(Form 5500, line 5). Active Participants is the count of currently employed
participants at the END of the year (line 6a). DOL does not publish an
end-of-year total in this dataset, so the two figures are measured on
different dates -- a plan that grew during the year can show more active
participants than the beginning-of-year total. Both are reproduced exactly
as filed; neither has been adjusted.

"Total Participants" counts everyone owed a balance, including retirees and
former employees who have not yet been paid out. It is not a headcount of
current employees; "Active Participants" is closer to that.

Employer Securities -- the company stock held in the ESOP trust -- is
reported only on Schedule H, which large plans file. Plans filing Schedule I
or Form 5500-SF do not itemize it, so the field is blank for them rather
than zero. {stock_rep} of the {n_act} active plans report it. Do not sum this
column and compare it to total assets across all plans; the denominators
differ.

Dollar figures are end-of-year values as filed with DOL. Blank means the
plan did not report that item, which is not the same as zero.


HOW PLANS WERE IDENTIFIED
-------------------------
A filing is included when the sponsor's mailing state is MA and the Form
5500 "type of pension benefit" codes include 2O (ESOP), 2P (leveraged ESOP)
or 2Q. No keyword or plan-name matching is used, so plans are counted on
what the sponsor certified to DOL rather than on how the plan is named.

Deliberately excluded: ordinary 401(k)/savings plans at large publicly
traded employers that carry an ESOP code only because company stock is one
investment option among many. These are not closely held employee-owned
companies, and including them would overstate Massachusetts employee
ownership by thousands of participants each.

Because plans may file up to roughly 18 months after a plan year begins,
a recent form year keeps growing as late filers arrive. Figures for {year}
reflect what DOL had published as of {data_as_of}.


QUESTIONS
---------
Massachusetts Center for Employee Ownership -- {dashboard_url}
Underlying filings are public: https://www.efast.dol.gov/5500Search/
"""

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"MA_ESOP_{year}_all_filed_plans.csv", all_df.to_csv(index=False))
        zf.writestr(f"MA_ESOP_{year}_active_plans.csv", act_df.to_csv(index=False))
        if not sum_df.empty:
            zf.writestr(f"MA_ESOP_annual_summary_{span}.csv", sum_df.to_csv(index=False))
        zf.writestr("README.txt", readme)
    return buf.getvalue()
