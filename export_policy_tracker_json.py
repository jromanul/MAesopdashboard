#!/usr/bin/env python3
"""
Build a self-contained JSON export of the ESOP trends + industry views, for the
MassCEO Policy Tracker to render as its own "ESOPs" tab.

The consuming project does not have this database, so everything it needs is
pre-aggregated here: no joins, no filtering rules to re-implement, no chance of
the two dashboards disagreeing because one of them re-derived a figure slightly
differently. It reads the same functions this dashboard renders from, so the
numbers are identical by construction.

Two population bases appear in the output, and mixing them up is the single
easiest way to publish a wrong number, so each block states which one it uses:

  active (115 for 2024) - plans reporting at least one active participant, minus
      a short list of known defunct/wind-down plans. Answers "how many working
      Massachusetts ESOPs are there". Every per-plan average and the whole
      industry section uses this.
  filed (128 for 2024) - every ESOP that filed a Form 5500 for the year,
      including plans winding down. Answers "how many ESOPs filed". The
      year-over-year trend series uses this, because a trend line has to count
      the same way in every year and prior years cannot be re-adjudicated.

Usage:
    venv\\Scripts\\python export_policy_tracker_json.py
Writes: exports/ma_esop_export.json
"""
import json
import os
import statistics as stats
from datetime import datetime, timezone

import config
import form5500_analysis as fa
import safe_console

safe_console.enable_utf8_stdout()

OUT_DIR = os.path.join(os.path.dirname(__file__) or ".", "exports")
OUT_PATH = os.path.join(OUT_DIR, "ma_esop_export.json")


def _eff_year(row):
    d = str(row.get("plan_eff_date") or "")
    if len(d) >= 4 and d[:4].isdigit() and d[:4] != "1900":
        return int(d[:4])
    return None


def _num(v):
    return v if isinstance(v, (int, float)) else 0


def build():
    year = fa.get_latest_year()
    filed = fa.get_ma_filings(year)
    active = fa.get_ma_filings(year, exclude_zombie=True)
    fin = fa.get_financial_summary(year, exclude_zombie=True)
    summaries = [s for s in fa.get_annual_summaries() if s.get("ma_plan_count")]

    assets = [_num(r["total_assets"]) for r in active]
    parts = [_num(r["total_participants"]) for r in active]
    act_parts = [_num(r["active_participants"]) for r in active if _num(r["active_participants"]) > 0]
    per_plan = [_num(r["total_assets"]) / _num(r["active_participants"])
                for r in active
                if _num(r["active_participants"]) > 0 and _num(r["total_assets"]) > 0]
    ages = [year - _eff_year(r) for r in active if _eff_year(r)]

    total_assets = sum(assets)
    total_parts = sum(parts)
    total_active = sum(_num(r["active_participants"]) for r in active)
    employer_sec = sum(_num(r["employer_securities"]) for r in active)
    plans_with_assets = sum(1 for a in assets if a > 0)

    # ── headline ──
    headline = {
        "basis": "active",
        "active_plans": len(active),
        "filed_plans": len(filed),
        "excluded_inactive": len(filed) - len(active),
        "ksop_plans": sum(1 for r in filed if r.get("is_ksop")),
        "total_participants": total_parts,
        "total_active_participants": total_active,
        "total_assets": round(total_assets),
        "mean_assets_per_plan": round(total_assets / plans_with_assets) if plans_with_assets else 0,
        "median_assets_per_plan": round(stats.median(assets)) if assets else 0,
        "mean_participants_per_plan": round(total_parts / len(active), 1) if active else 0,
        "median_participants_per_plan": round(stats.median(parts)) if parts else 0,
        "aggregate_assets_per_participant": round(total_assets / total_parts) if total_parts else 0,
        "aggregate_assets_per_active_participant": round(total_assets / total_active) if total_active else 0,
        "median_assets_per_active_participant_per_plan": round(stats.median(per_plan)) if per_plan else 0,
        "mean_assets_per_active_participant_per_plan": round(stats.mean(per_plan)) if per_plan else 0,
        "employer_securities": round(employer_sec),
        "employer_securities_pct_of_assets": round(employer_sec / total_assets * 100, 1) if total_assets else 0,
        "plans_reporting_employer_securities": sum(
            1 for r in active if _num(r["employer_securities"]) > 0),
    }

    # ── typical plan (median vs mean) ──
    typical = {
        "basis": "active",
        "plans_described": len(active),
        "rows": [
            {"metric": "ESOP age (years since plan start)",
             "median": round(stats.median(ages), 1) if ages else None,
             "mean": round(stats.mean(ages), 1) if ages else None, "unit": "years"},
            {"metric": "Total participants (incl. retirees/separated)",
             "median": round(stats.median(parts)) if parts else None,
             "mean": round(stats.mean(parts)) if parts else None, "unit": "people"},
            {"metric": "Active participants (current employees)",
             "median": round(stats.median(act_parts)) if act_parts else None,
             "mean": round(stats.mean(act_parts)) if act_parts else None, "unit": "people"},
            {"metric": "Plan assets",
             "median": round(stats.median(assets)) if assets else None,
             "mean": round(stats.mean(assets)) if assets else None, "unit": "usd"},
            {"metric": "Assets per active participant (per plan)",
             "median": round(stats.median(per_plan)) if per_plan else None,
             "mean": round(stats.mean(per_plan)) if per_plan else None, "unit": "usd"},
        ],
    }

    # ── trend series (filed basis) ──
    trends = {
        "basis": "filed",
        "years": [s["filing_year"] for s in summaries],
        "series": {
            "plan_count": [s["ma_plan_count"] for s in summaries],
            "total_participants": [s["ma_total_participants"] for s in summaries],
            "active_participants": [s["ma_active_participants"] for s in summaries],
            "total_assets": [round(_num(s["ma_total_assets"])) for s in summaries],
            "mean_plan_assets": [round(_num(s["ma_avg_plan_assets"])) for s in summaries],
            "employer_contributions": [round(_num(s["ma_total_contributions"])) for s in summaries],
            "benefits_paid": [round(_num(s.get("ma_total_benefits_paid"))) for s in summaries],
            "employer_securities": [round(_num(s.get("ma_total_employer_securities"))) for s in summaries],
        },
    }

    # ── distributions (active basis) ──
    def _bins(values, spec):
        out = []
        for lo, hi, label in spec:
            out.append({"label": label,
                        "count": sum(1 for v in values if lo <= v < hi)})
        return out

    distributions = {
        "basis": "active",
        "plan_assets": _bins([a for a in assets], config.FORM5500_ASSET_BINS),
        "participant_counts": _bins(parts, config.FORM5500_PARTICIPANT_BINS),
    }

    # ── formation cohorts + decade shares (active basis) ──
    cohort_spec = [("Before 1990", 0, 1990), ("1990-1999", 1990, 2000),
                   ("2000-2009", 2000, 2010), ("2010-2019", 2010, 2020),
                   ("2020-present", 2020, 9999)]
    cohorts = []
    for label, lo, hi in cohort_spec:
        grp = [r for r in active if _eff_year(r) and lo <= _eff_year(r) < hi]
        cohorts.append({"era": label, "plans": len(grp),
                        "participants": sum(_num(r["total_participants"]) for r in grp),
                        "assets": round(sum(_num(r["total_assets"]) for r in grp))})
    decades = []
    for d0 in (1970, 1980, 1990, 2000, 2010, 2020):
        grp = [r for r in active if _eff_year(r) and d0 <= _eff_year(r) < d0 + 10]
        decades.append({"decade": f"{d0}s", "plans": len(grp),
                        "pct_of_active": round(len(grp) / len(active) * 100, 1) if active else 0})

    # ── industry (active basis) ──
    by_sector = {}
    for r in active:
        s = r.get("industry_sector") or "(Unclassified)"
        e = by_sector.setdefault(s, {"sector": s, "plans": 0, "participants": 0, "assets": 0.0})
        e["plans"] += 1
        e["participants"] += _num(r["total_participants"])
        e["assets"] += _num(r["total_assets"])
    sectors = sorted(by_sector.values(), key=lambda d: (-d["plans"], d["sector"]))
    for e in sectors:
        e["assets"] = round(e["assets"])
        e["avg_account_balance"] = (round(e["assets"] / e["participants"])
                                    if e["participants"] else None)

    industry = {
        "basis": "active",
        "sectors": sectors,
        "avg_account_balance_min_plans": 2,
        "totals": {"plans": len(active), "participants": total_parts,
                   "assets": round(total_assets)},
    }

    # ── compact plan roster, so the consumer can build its own tables ──
    roster = [{
        "plan_name": r.get("plan_name"),
        "sponsor": r.get("sponsor_name"),
        "city": r.get("sponsor_city"),
        "industry": r.get("industry_sector"),
        "plan_year_started": _eff_year(r),
        "is_ksop": bool(r.get("is_ksop")),
        "total_participants": r.get("total_participants"),
        "active_participants": r.get("active_participants"),
        "total_assets": r.get("total_assets"),
        "employer_securities": r.get("employer_securities"),
    } for r in sorted(active, key=lambda x: -_num(x["total_assets"]))]

    return {
        "meta": {
            "title": "Massachusetts ESOPs — Form 5500",
            "form_year": year,
            "data_as_of": config.DATA_AS_OF,
            "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source": "U.S. Department of Labor, EFAST2 Form 5500 bulk datasets",
            "source_url": "https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets",
            "dashboard_url": "https://maesopdashboard.streamlit.app",
            "open_form_year": config.FORM5500_OPEN_FORM_YEAR,
            "bases": {
                "active": ("Plans reporting at least one active participant, minus a short "
                           "list of known defunct/wind-down plans. Use for 'how many working "
                           "MA ESOPs' and for every per-plan average."),
                "filed": ("Every ESOP that filed a Form 5500 for the year, including plans "
                          "winding down. Use for year-over-year trend lines."),
            },
            "caveats": [
                ("total_participants is the plan total at the BEGINNING of the plan year "
                 "(Form 5500 line 5); active_participants is current employees at the END "
                 "of the year (line 6a). DOL publishes no end-of-year total, so the two sit "
                 "on different dates and a growing plan can show more active than total. "
                 "Reproduced exactly as filed."),
                ("employer_securities (company stock) is reported only on Schedule H, which "
                 "large plans file. It is null for plans filing Schedule I or 5500-SF — null "
                 "means not reported, which is not the same as zero. Do not divide it by "
                 "total assets across all plans; the denominators differ."),
                ("A form year keeps growing for roughly 18 months as fiscal-year and extended "
                 "filers arrive, so the most recent year is not final."),
                ("Ordinary 401(k)/savings plans at large public companies that merely offer "
                 "employer stock are deliberately excluded; they are not employee-owned firms."),
            ],
        },
        "headline": headline,
        "typical_plan": typical,
        "trends": trends,
        "distributions": distributions,
        "formation_cohorts": {"basis": "active", "cohorts": cohorts},
        "decade_shares": {"basis": "active", "decades": decades},
        "industry": industry,
        "plans": roster,
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    payload = build()
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    h = payload["headline"]
    print(f"wrote {OUT_PATH}")
    print(f"  form year {payload['meta']['form_year']}  as of {payload['meta']['data_as_of']}")
    print(f"  active {h['active_plans']} / filed {h['filed_plans']}")
    print(f"  participants {h['total_participants']:,}  assets ${h['total_assets']:,}")
    print(f"  median plan assets ${h['median_assets_per_plan']:,}")
    print(f"  median assets per active participant (per plan) "
          f"${h['median_assets_per_active_participant_per_plan']:,}")
    print(f"  trend years {payload['trends']['years'][0]}-{payload['trends']['years'][-1]}, "
          f"{len(payload['industry']['sectors'])} sectors, {len(payload['plans'])} plans in roster")


if __name__ == "__main__":
    main()
