"""
Plotly chart builders for the Form 5500 ESOP Dashboard.
Each function takes prepared data and returns a plotly.graph_objects.Figure.
"""
from __future__ import annotations

import plotly.graph_objects as go
import pandas as pd
from collections import Counter

import config

# ── Shared Helpers ──────────────────────────────

PLOTLY_CONFIG = {
    "displayModeBar": True,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
    "toImageButtonOptions": {"format": "png", "filename": "form5500_chart"},
    "displaylogo": False,
}

_NAVY = config.CHART_COLORS["navy"]
_GOLD = config.CHART_COLORS["gold"]
_RED = config.CHART_COLORS["red"]
_GREEN = config.CHART_COLORS["green"]
_GRAY = config.CHART_COLORS["gray"]
_PURPLE = config.CHART_COLORS["purple"]
_CRANBERRY = config.CHART_COLORS["cranberry"]
_FONT = config.CHART_FONT_FAMILY
_TEXT_BLACK = "#111111"  # Black for all chart labels / axis text
_TEXT_DARK = "#333333"   # Slightly lighter for secondary labels (source annotations)


def _apply_layout(fig: go.Figure, title: str = "", height: int | None = None,
                  source: str = "", y_title: str = "", x_title: str = "") -> go.Figure:
    fig.update_layout(
        title=dict(text=title, font=dict(family=_FONT, size=16, color=_TEXT_BLACK), x=0, y=0.98),
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=height or config.CHART_HEIGHT_MD,
        margin=dict(l=60, r=30, t=50, b=70 if source else 40),
        legend=dict(orientation="h", yanchor="bottom", y=-0.22, xanchor="center", x=0.5,
                    font=dict(size=11, color=_TEXT_BLACK)),
        hovermode="x unified",
    )
    fig.update_xaxes(
        showgrid=False, linecolor="#E0E0E0", linewidth=1,
        title=dict(text=x_title, font=dict(size=11, color=_TEXT_BLACK)),
        tickfont=dict(color=_TEXT_BLACK),
    )
    fig.update_yaxes(
        showgrid=True, gridcolor="#F0F0F0", gridwidth=1, linecolor="#E0E0E0", linewidth=1,
        title=dict(text=y_title, font=dict(size=11, color=_TEXT_BLACK)),
        tickfont=dict(color=_TEXT_BLACK),
    )
    if source:
        fig.add_annotation(
            text=f"Source: {source}", xref="paper", yref="paper",
            x=0, y=-0.18, showarrow=False,
            font=dict(size=9, color=_TEXT_DARK),
        )
    return fig


def _truncate(s: str, max_len: int = 40) -> str:
    if not s:
        return "Unknown"
    return s[:max_len] + "..." if len(s) > max_len else s


# ══════════════════════════════════════════════════
# FORM 5500 MA ESOP ANALYSIS CHARTS
# ══════════════════════════════════════════════════

def build_f5500_plan_count_trend(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    counts = [s["ma_plan_count"] for s in summaries]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=years, y=counts, mode="lines+markers",
        name="MA ESOP Plans",
        line=dict(color=_NAVY, width=3),
        marker=dict(size=8, color=_NAVY),
        hovertemplate="Year %{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="MA ESOP Plan Count Over Time",
                  y_title="Number of Plans", source="DOL Form 5500 Bulk Data",
                  height=config.CHART_HEIGHT_MD)
    fig.update_xaxes(dtick=1)
    return fig


def build_f5500_participants_trend(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    total = [s.get("ma_total_participants") or 0 for s in summaries]
    active = [s.get("ma_active_participants") or 0 for s in summaries]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=years, y=total, mode="lines+markers",
        name="Total Participants",
        line=dict(color=_NAVY, width=3),
        marker=dict(size=8, color=_NAVY),
        hovertemplate="Year %{x}: %{y:,.0f} total<extra></extra>",
    ))
    if any(a > 0 for a in active):
        fig.add_trace(go.Scatter(
            x=years, y=active, mode="lines+markers",
            name="Active Participants",
            line=dict(color=_GOLD, width=2, dash="dash"),
            marker=dict(size=6, color=_GOLD),
            hovertemplate="Year %{x}: %{y:,.0f} active<extra></extra>",
        ))
    _apply_layout(fig, title="MA ESOP Participants Over Time",
                  y_title="Participants", source="DOL Form 5500 Bulk Data",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickformat=",")
    fig.update_xaxes(dtick=1)
    return fig


def build_f5500_assets_trend(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    assets_b = [(s.get("ma_total_assets") or 0) / 1e9 for s in summaries]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=years, y=assets_b, mode="lines+markers",
        name="Total Assets",
        line=dict(color=_NAVY, width=3),
        marker=dict(size=8, color=_NAVY),
        fill="tozeroy", fillcolor="rgba(27,58,92,0.1)",
        hovertemplate="Year %{x}: $%{y:.2f}B<extra></extra>",
    ))
    _apply_layout(fig, title="MA ESOP Total Assets Over Time",
                  y_title="Assets ($ Billions)", source="DOL Form 5500 (Schedule H/I)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="B")
    fig.update_xaxes(dtick=1)
    return fig


def build_f5500_avg_plan_assets_trend(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    avg_m = [(s.get("ma_avg_plan_assets") or 0) / 1e6 for s in summaries]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=years, y=avg_m, mode="lines+markers",
        name="Avg Plan Assets",
        line=dict(color=_GOLD, width=3),
        marker=dict(size=8, color=_GOLD),
        hovertemplate="Year %{x}: $%{y:.1f}M<extra></extra>",
    ))
    _apply_layout(fig, title="Average MA ESOP Plan Assets Over Time",
                  y_title="Average Assets ($ Millions)", source="DOL Form 5500 (Schedule H/I)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M")
    fig.update_xaxes(dtick=1)
    return fig


def build_f5500_contributions_bar(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    contribs_m = [(s.get("ma_total_contributions") or 0) / 1e6 for s in summaries]
    texts = [f"${c:.1f}M" for c in contribs_m]

    fig = go.Figure(go.Bar(
        x=years, y=contribs_m,
        marker_color=_NAVY,
        text=texts, textposition="outside",
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="Year %{x}: $%{y:.1f}M<extra></extra>",
    ))
    _apply_layout(fig, title="Annual Employer Contributions to MA ESOPs",
                  y_title="Contributions ($ Millions)", source="DOL Form 5500 (Schedule H/I)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M")
    fig.update_xaxes(dtick=1)
    fig.update_layout(bargap=0.35)
    return fig


def build_f5500_city_map(city_data: list[dict]) -> go.Figure:
    coords = config.MA_CITY_COORDS
    import random

    lats, lons, texts, sizes = [], [], [], []
    for entry in city_data:
        city = entry.get("sponsor_city", "")
        count = entry.get("plan_count", 1)
        c = coords.get(city) or coords.get(city.title())
        if c:
            random.seed(hash(city + "f5500"))
            lats.append(c[0] + random.uniform(-0.005, 0.005))
            lons.append(c[1] + random.uniform(-0.005, 0.005))
        else:
            random.seed(hash(city + "f5500"))
            lats.append(42.3 + random.uniform(-0.1, 0.1))
            lons.append(-71.5 + random.uniform(-0.3, 0.3))
        raw_assets = entry.get("total_assets")
        if raw_assets and raw_assets > 0:
            assets_m = raw_assets / 1e6
            texts.append(f"<b>{city}</b><br>{count} ESOPs<br>${assets_m:.0f}M assets")
        else:
            partcp = entry.get("total_partcp") or entry.get("total_participants") or 0
            texts.append(f"<b>{city}</b><br>{count} ESOPs<br>{partcp:,} participants")
        sizes.append(max(6, min(25, count * 4)))

    fig = go.Figure(go.Scattergeo(
        lat=lats, lon=lons, text=texts,
        hovertemplate="%{text}<extra></extra>",
        mode="markers",
        marker=dict(
            size=sizes, color=_NAVY,
            line=dict(width=1, color="white"),
            opacity=0.75, sizemode="diameter",
        ),
    ))
    fig.update_geos(
        scope="usa",
        center=dict(lat=42.25, lon=-71.8),
        projection_scale=22,
        showland=True, landcolor="#F0F2F6",
        showlakes=True, lakecolor="white",
        showocean=True, oceancolor="#E8F0FE",
        showcountries=False,
        showsubunits=True, subunitcolor="#CCCCCC",
    )
    fig.update_layout(
        height=500,
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor="white",
        font=dict(family=_FONT, color=_TEXT_BLACK),
        title=dict(text="MA ESOPs by City (Form 5500 Data)",
                  font=dict(size=16, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


def build_f5500_top_cities_bar(city_data: list[dict], top_n: int = 20) -> go.Figure:
    top = city_data[:top_n]
    cities = [d.get("sponsor_city", "Unknown") for d in reversed(top)]
    counts = [d.get("plan_count", 0) for d in reversed(top)]

    fig = go.Figure(go.Bar(
        y=cities, x=counts, orientation="h",
        marker_color=_NAVY,
        hovertemplate="%{y}: %{x} ESOPs<extra></extra>",
    ))
    fig.update_layout(
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        plot_bgcolor="white", paper_bgcolor="white",
        height=max(350, top_n * 25 + 80),
        margin=dict(l=150, r=30, t=40, b=50),
        xaxis=dict(title="Number of ESOPs", showgrid=True, gridcolor="#F0F0F0",
                   tickfont=dict(color=_TEXT_BLACK), title_font=dict(color=_TEXT_BLACK)),
        yaxis=dict(showgrid=False, tickfont=dict(color=_TEXT_BLACK)),
        title=dict(text=f"Top {top_n} MA Cities by ESOP Count",
                  font=dict(size=15, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


def build_f5500_industry_bar(industry_data: list[dict], top_n: int = 15) -> go.Figure:
    top = [d for d in industry_data if d.get("industry_sector")][:top_n]
    if not top:
        fig = go.Figure()
        fig.add_annotation(text="No industry data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    sectors = [d["industry_sector"] for d in reversed(top)]
    counts = [d["plan_count"] for d in reversed(top)]

    fig = go.Figure(go.Bar(
        y=sectors, x=counts, orientation="h",
        marker_color=_GOLD,
        hovertemplate="%{y}: %{x} ESOPs<extra></extra>",
    ))
    fig.update_layout(
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        plot_bgcolor="white", paper_bgcolor="white",
        height=max(300, len(top) * 28 + 80),
        margin=dict(l=200, r=30, t=40, b=50),
        xaxis=dict(title="Number of ESOPs", showgrid=True, gridcolor="#F0F0F0",
                   tickfont=dict(color=_TEXT_BLACK), title_font=dict(color=_TEXT_BLACK)),
        yaxis=dict(showgrid=False, tickfont=dict(color=_TEXT_BLACK)),
        title=dict(text="MA ESOPs by Industry Sector",
                  font=dict(size=15, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


def build_f5500_asset_histogram(asset_values: list[float]) -> go.Figure:
    if not asset_values:
        fig = go.Figure()
        fig.add_annotation(text="No asset data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    bins = config.FORM5500_ASSET_BINS
    labels = [b[2] for b in bins]
    counts = []
    for low, high, _ in bins:
        counts.append(sum(1 for v in asset_values if low <= v < high))

    fig = go.Figure(go.Bar(
        x=labels, y=counts,
        marker_color=_NAVY,
        text=counts, textposition="outside",
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="Distribution of MA ESOP Plan Assets",
                  y_title="Number of Plans", source="DOL Form 5500 (Schedule H/I)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_xaxes(tickangle=-45)
    fig.update_layout(bargap=0.25)
    return fig


def build_f5500_participant_histogram(partcp_values: list[int]) -> go.Figure:
    if not partcp_values:
        fig = go.Figure()
        fig.add_annotation(text="No participant data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    bins = config.FORM5500_PARTICIPANT_BINS
    labels = [b[2] for b in bins]
    counts = []
    for low, high, _ in bins:
        counts.append(sum(1 for v in partcp_values if low <= v < high))

    fig = go.Figure(go.Bar(
        x=labels, y=counts,
        marker_color=_GOLD,
        text=counts, textposition="outside",
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="Distribution of MA ESOP Participant Counts",
                  y_title="Number of Plans", source="DOL Form 5500 Bulk Data",
                  height=config.CHART_HEIGHT_MD)
    fig.update_xaxes(tickangle=-45)
    fig.update_layout(bargap=0.25)
    return fig


def build_f5500_ma_share_bars(summaries: list[dict]) -> go.Figure:
    if not summaries:
        fig = go.Figure()
        fig.add_annotation(text="No data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    latest = next((s for s in reversed(summaries) if (s.get("ma_plan_count") or 0) > 0), summaries[-1])
    categories = ["Plans", "Participants", "Assets"]

    us_est = config.NATIONAL_ESOP_ESTIMATES
    ma_vals = [
        latest.get("ma_plan_count") or 0,
        latest.get("ma_total_participants") or 0,
        (latest.get("ma_total_assets") or 0) / 1e9,
    ]
    us_vals = [
        latest.get("us_total_esop_count") or us_est["total_plans"],
        latest.get("us_total_participants") or us_est["total_participants"],
        (latest.get("us_total_assets") or us_est["total_assets"]) / 1e9,
    ]

    pcts = []
    for m, u in zip(ma_vals, us_vals):
        pcts.append(m / u * 100 if u > 0 else 0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=categories, y=pcts,
        marker_color=[_GOLD, _GOLD, _GOLD],
        text=[f"{p:.1f}%" for p in pcts],
        textposition="outside",
        textfont=dict(size=13, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: MA = %{text} of US total<extra></extra>",
    ))
    _nat_year = config.NATIONAL_ESOP_DATA["as_of_year"]
    _apply_layout(fig, title=f"MA Share of National ESOP Totals ({latest['filing_year']})",
                  y_title="MA as % of US Total",
                  source=f"MA: DOL Form 5500 ({latest['filing_year']}); US: NCEO ({_nat_year})",
                  height=config.CHART_HEIGHT_SM + 50)
    fig.update_yaxes(ticksuffix="%")
    fig.update_layout(bargap=0.5)
    return fig


def build_f5500_regional_breakdown(city_data: list[dict]) -> go.Figure:
    regions = config.MA_REGIONS
    region_counts = Counter()
    for entry in city_data:
        city = entry.get("sponsor_city", "")
        count = entry.get("plan_count", 1)
        placed = False
        for region, region_cities in regions.items():
            if city in region_cities or city.title() in region_cities:
                region_counts[region] += count
                placed = True
                break
        if not placed:
            region_counts["Other"] += count

    if not region_counts:
        fig = go.Figure()
        fig.add_annotation(text="No regional data", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    labels = list(region_counts.keys())
    values = list(region_counts.values())
    colors = [_NAVY, _GOLD, _GREEN, _PURPLE, _RED, _GRAY, "#D4A827"][:len(labels)]

    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.55,
        marker=dict(colors=colors),
        textinfo="label+percent",
        textfont=dict(size=11, family=_FONT, color=_TEXT_BLACK),
        hovertemplate="%{label}: %{value} ESOPs<br>(%{percent})<extra></extra>",
    ))
    fig.update_layout(
        showlegend=False,
        height=config.CHART_HEIGHT_SM + 40,
        margin=dict(l=10, r=10, t=30, b=20),
        paper_bgcolor="white",
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        title=dict(text="MA ESOPs by Region (Form 5500)",
                  font=dict(size=14, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


# ══════════════════════════════════════════════════
# FORM 5500 — FINANCIAL ANALYSIS CHARTS (Schedule H/I)
# ══════════════════════════════════════════════════

def build_f5500_assets_per_participant_bar(plans_data: list, top_n: int = 20) -> go.Figure:
    if not plans_data:
        fig = go.Figure()
        fig.add_annotation(text="No financial data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    top = plans_data[:top_n]
    names = [_truncate(d.get("plan_name", "Unknown"), 40) for d in reversed(top)]
    values = [(d.get("assets_per_participant") or 0) / 1000 for d in reversed(top)]

    fig = go.Figure(go.Bar(
        y=names, x=values, orientation="h",
        marker_color=_NAVY,
        text=[f"${v:.0f}K" for v in values],
        textposition="outside",
        textfont=dict(size=10, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{y}<br>$%{x:.0f}K per participant<extra></extra>",
    ))
    fig.update_layout(
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        plot_bgcolor="white", paper_bgcolor="white",
        height=max(400, min(top_n, len(plans_data)) * 25 + 80),
        margin=dict(l=260, r=80, t=40, b=50),
        xaxis=dict(title="Assets Per Participant ($K)", showgrid=True, gridcolor="#F0F0F0",
                   tickprefix="$", ticksuffix="K",
                   tickfont=dict(color=_TEXT_BLACK), title_font=dict(color=_TEXT_BLACK)),
        yaxis=dict(showgrid=False, tickfont=dict(color=_TEXT_BLACK)),
        title=dict(text=f"Top {min(top_n, len(plans_data))} MA ESOPs by Assets Per Participant",
                  font=dict(size=15, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


def build_f5500_employer_securities_donut(financial_summary: dict) -> go.Figure:
    total = (financial_summary.get("total_assets") or 0)
    securities = (financial_summary.get("total_employer_securities") or 0)

    if total <= 0:
        fig = go.Figure()
        fig.add_annotation(text="No asset data available", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    other = max(0, total - securities)
    labels = ["Employer Securities", "Other Assets"]
    values = [securities, other]
    colors = [_GOLD, _NAVY]

    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.6,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="label+percent",
        textfont=dict(size=12, family=_FONT, color=_TEXT_BLACK),
        hovertemplate="%{label}: $%{value:,.0f}<br>(%{percent})<extra></extra>",
    ))
    if total >= 1e9:
        total_str = f"${total / 1e9:.1f}B"
    elif total >= 1e6:
        total_str = f"${total / 1e6:.0f}M"
    else:
        total_str = f"${total:,.0f}"
    fig.add_annotation(
        text=f"{total_str}<br>Total",
        x=0.5, y=0.5, font=dict(size=16, color=_TEXT_BLACK, family=_FONT),
        showarrow=False,
    )
    fig.update_layout(
        showlegend=True,
        height=config.CHART_HEIGHT_MD,
        margin=dict(l=10, r=10, t=40, b=30),
        paper_bgcolor="white",
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5,
                    font=dict(color=_TEXT_BLACK)),
        title=dict(text="ESOP Asset Composition: Company Stock vs Other",
                  font=dict(size=14, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


def build_f5500_contributions_vs_distributions(summaries: list) -> go.Figure:
    valid = [s for s in summaries
             if (s.get("ma_total_contributions") or 0) > 0
             or (s.get("ma_total_benefits_paid") or 0) > 0]
    if not valid:
        fig = go.Figure()
        fig.add_annotation(text="No contribution/distribution data available",
                          x=0.5, y=0.5, showarrow=False,
                          font=dict(size=14, color=_TEXT_DARK))
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    years = [s["filing_year"] for s in valid]
    contribs_m = [(s.get("ma_total_contributions") or 0) / 1e6 for s in valid]
    benefits_m = [(s.get("ma_total_benefits_paid") or 0) / 1e6 for s in valid]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=years, y=contribs_m, name="Contributions In",
        marker_color=_NAVY,
        hovertemplate="Year %{x}: $%{y:.1f}M contributed<extra></extra>",
    ))
    fig.add_trace(go.Bar(
        x=years, y=benefits_m, name="Benefits Paid Out",
        marker_color=_GOLD,
        hovertemplate="Year %{x}: $%{y:.1f}M distributed<extra></extra>",
    ))
    _apply_layout(fig, title="Contributions vs Distributions \u2014 MA ESOPs",
                  y_title="Amount ($ Millions)", source="DOL Form 5500 (Schedule H/I)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M")
    fig.update_xaxes(dtick=1)
    fig.update_layout(barmode="group", bargap=0.25, bargroupgap=0.1)
    return fig
