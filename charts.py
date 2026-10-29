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
    # Show the toolbar only on hover so it never overlaps in-plot chart titles
    "displayModeBar": "hover",
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
_TEXT_BLACK = "#0F1E2E"  # Primary ink for titles / axis labels
_TEXT_DARK = "#64758A"   # Muted ink for secondary labels (source annotations)
_GRID = "#EEF2F7"        # Hairline gridlines — present but never competing with the data
_AXIS = "#E4EAF1"


def _apply_layout(fig: go.Figure, title: str = "", height: int | None = None,
                  source: str = "", y_title: str = "", x_title: str = "",
                  bottom: int | None = None) -> go.Figure:
    """Shared chart chrome.

    Presentation only. Gridlines are hairline-weight and horizontal-only so the
    marks carry the chart; the plot area is transparent because each chart sits
    on its own white card (see theme.CSS), which avoids a white-on-white seam.
    """
    fig.update_layout(
        title=dict(text=title,
                   font=dict(family=_FONT, size=14, color=_TEXT_BLACK, weight=700),
                   x=0, y=0.97, xanchor="left"),
        font=dict(family=_FONT, color=_TEXT_BLACK, size=11.5),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=height or config.CHART_HEIGHT_MD,
        margin=dict(l=58, r=34, t=46, b=bottom or (92 if source else 38)),
        legend=dict(orientation="h", yanchor="bottom", y=-0.20, xanchor="center", x=0.5,
                    font=dict(size=10.5, color=_TEXT_DARK),
                    bgcolor="rgba(0,0,0,0)", borderwidth=0),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#0A2E52", bordercolor="#0A2E52",
                        font=dict(family=_FONT, color="#FFFFFF", size=11.5)),
        separators=".,",
    )
    if len(fig.data) > 1:
        # A legend under the plot collides with year labels that Plotly turns on a
        # narrow (phone) screen, so a multi-series chart puts it under the title.
        fig.update_layout(legend=dict(yanchor="bottom", y=1.0, xanchor="left", x=0),
                          margin=dict(t=72))
    fig.update_xaxes(
        showgrid=False, showline=True, linecolor=_AXIS, linewidth=1, ticks="",
        title=dict(text=x_title, font=dict(size=10.5, color=_TEXT_DARK)),
        tickfont=dict(color=_TEXT_DARK, size=10.5),
        zeroline=False,
    )
    fig.update_yaxes(
        showgrid=True, gridcolor=_GRID, gridwidth=1, showline=False, ticks="",
        title=dict(text=y_title, font=dict(size=10.5, color=_TEXT_DARK)),
        tickfont=dict(color=_TEXT_DARK, size=10.5),
        zeroline=True, zerolinecolor=_AXIS, zerolinewidth=1,
    )
    if source:
        # Pinned to the bottom edge of the figure (not a fraction of the plot
        # height), so it clears rotated tick labels at any chart height.
        b = bottom or (92 if source else 38)
        fig.add_annotation(
            text=f"Source: {source}", xref="paper", yref="paper",
            x=0, y=0, xanchor="left", yanchor="top", yshift=-(b - 24),
            showarrow=False, font=dict(size=10, color=_TEXT_DARK),
        )
    return fig


def _year_axis(fig: go.Figure, years: list) -> go.Figure:
    """Whole-year ticks with half a year of padding, so neither end is clipped.
    Labels turn at most 45 degrees when crowded (a phone), never 90."""
    if years:
        fig.update_xaxes(dtick=1, range=[min(years) - 0.5, max(years) + 0.5],
                         autotickangles=[0, -45])
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
        x=years, y=counts, mode="lines+markers+text",
        name="MA ESOP Plans",
        line=dict(color=_NAVY, width=3),
        marker=dict(size=8, color=_NAVY),
        text=counts, textposition="top center", cliponaxis=False,
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="Year %{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="MA ESOP Plans Filed, by Form Year",
                  y_title="Plans filed", source="U.S. DOL Form 5500 (EFAST2 bulk data)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(rangemode="tozero")
    _year_axis(fig, years)
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
    _apply_layout(fig, title="Participants in MA ESOPs",
                  y_title="Participants", source="U.S. DOL Form 5500 (EFAST2 bulk data)",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickformat=",", rangemode="tozero")
    _year_axis(fig, years)
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
        fill="tozeroy", fillcolor="rgba(20,85,143,0.10)",
        hovertemplate="Year %{x}: $%{y:.2f}B<extra></extra>",
    ))
    _apply_layout(fig, title="Total Assets Held by MA ESOPs",
                  y_title="Assets ($ billions)", source="U.S. DOL Form 5500, Schedules H and I",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="B", rangemode="tozero")
    _year_axis(fig, years)
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
    _apply_layout(fig, title="Average Assets per Filed Plan",
                  y_title="Average assets ($ millions)", source="U.S. DOL Form 5500, Schedules H and I",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M", rangemode="tozero")
    _year_axis(fig, years)
    return fig


def build_f5500_contributions_bar(summaries: list[dict]) -> go.Figure:
    years = [s["filing_year"] for s in summaries]
    contribs_m = [(s.get("ma_total_contributions") or 0) / 1e6 for s in summaries]
    texts = [f"${c:.0f}M" for c in contribs_m]

    fig = go.Figure(go.Bar(
        x=years, y=contribs_m,
        marker_color=_NAVY,
        text=texts, textposition="outside", cliponaxis=False,
        textfont=dict(size=10, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="Year %{x}: $%{y:.1f}M<extra></extra>",
    ))
    _apply_layout(fig, title="Employer Contributions to MA ESOPs",
                  y_title="Contributions ($ millions)", source="U.S. DOL Form 5500, Schedules H and I",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M",
                     range=[0, max(contribs_m or [0]) * 1.15 or 1])
    _year_axis(fig, years)
    # On a narrow screen a label wider than its bar is hidden rather than
    # overprinting its neighbour; hover still gives every value.
    fig.update_layout(bargap=0.35, uniformtext=dict(minsize=8, mode="hide"))
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
    total = sum(d["plan_count"] for d in top)

    fig = go.Figure(go.Bar(
        y=sectors, x=counts, orientation="h",
        marker_color=[sector_color(s) for s in sectors],
        text=[f"{c}" for c in counts], textposition="outside", cliponaxis=False,
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        customdata=[[c / total * 100 if total else 0, "plan" if c == 1 else "plans"]
                    for c in counts],
        hovertemplate="%{y}: %{x} %{customdata[1]} (%{customdata[0]:.1f}%)<extra></extra>",
    ))
    _apply_layout(fig, title="Active MA ESOPs by Industry Sector",
                  x_title="Active plans", source="U.S. DOL Form 5500; sector from NAICS business code",
                  height=max(320, len(top) * 30 + 140), bottom=84)
    # A fixed left margin wide enough for the longest sector name, so the source
    # line can start at the figure's edge rather than the plot's.
    fig.update_layout(hovermode="closest", showlegend=False, margin=dict(l=244, r=40))
    fig.update_annotations(xshift=-232)
    fig.update_xaxes(showgrid=True, gridcolor=_GRID, range=[0, max(counts) * 1.12])
    fig.update_yaxes(showgrid=False, zeroline=False, automargin=False,
                     tickfont=dict(color=_TEXT_BLACK, size=11.5))
    return fig


def sector_color(sector: str) -> str:
    """Fixed colour per industry sector, so a sector reads the same in every chart."""
    return config.SECTOR_COLORS.get(sector, config.SECTOR_COLORS["Other"])


def build_f5500_industry_share(labels: list[str], values: list[float], title: str,
                               value_fmt: str = ",.0f") -> go.Figure:
    """Donut of one measure's split by sector, coloured with sector_color()."""
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.5, sort=False, direction="clockwise",
        marker=dict(colors=[sector_color(l) for l in labels],
                    line=dict(color="white", width=1.5)),
        texttemplate="%{percent:.0%}", textposition="inside",
        insidetextorientation="horizontal", textfont=dict(size=11, family=_FONT),
        hovertemplate="%{label}<br>%{value:" + value_fmt + "} (%{percent:.1%})<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor="center",
                   font=dict(size=14, family=_FONT, color=_TEXT_BLACK, weight=700)),
        height=config.CHART_HEIGHT_MD + 40,
        margin=dict(t=50, b=10, l=10, r=10),
        showlegend=True,
        legend=dict(orientation="h", y=-0.02, yanchor="top", x=0.5, xanchor="center",
                    font=dict(size=10.5, color=_TEXT_DARK)),
        uniformtext=dict(minsize=10, mode="hide"),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_FONT, color=_TEXT_BLACK),
        hoverlabel=dict(bgcolor="#0A2E52", font=dict(family=_FONT, color="#FFFFFF", size=11.5)),
    )
    return fig


def build_f5500_category_bar(categories: list[str], values: list[float], title: str,
                             y_title: str, text: list[str] | None = None,
                             source: str = "U.S. DOL Form 5500 (plan effective date)") -> go.Figure:
    """Single-series vertical bar for an ordered category axis (eras, decades)."""
    fig = go.Figure(go.Bar(
        x=categories, y=values, marker_color=_NAVY,
        text=text if text is not None else values, textposition="outside", cliponaxis=False,
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: %{text}<extra></extra>",
    ))
    _apply_layout(fig, title=title, y_title=y_title, source=source,
                  height=config.CHART_HEIGHT_SM + 90, bottom=110)
    fig.update_layout(hovermode="closest", bargap=0.3, showlegend=False)
    fig.update_xaxes(automargin=True, autotickangles=[0, -45])
    fig.update_yaxes(range=[0, (max(values) if values else 1) * 1.18])
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
        text=counts, textposition="outside", cliponaxis=False,
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="Active Plans by Total Assets",
                  y_title="Active plans", x_title="Plan assets (end of plan year)",
                  source="U.S. DOL Form 5500, Schedules H and I",
                  height=config.CHART_HEIGHT_MD + 30, bottom=128)
    fig.update_xaxes(tickangle=-35)
    if counts:
        fig.update_yaxes(range=[0, max(counts) * 1.18])  # headroom for outside labels
    fig.update_layout(bargap=0.25, hovermode="closest")
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
        text=counts, textposition="outside", cliponaxis=False,
        textfont=dict(size=11, color=_TEXT_BLACK, family=_FONT),
        hovertemplate="%{x}: %{y} plans<extra></extra>",
    ))
    _apply_layout(fig, title="Active Plans by Number of Participants",
                  y_title="Active plans", x_title="Participants (end of plan year)",
                  source="U.S. DOL Form 5500 (EFAST2 bulk data)",
                  height=config.CHART_HEIGHT_MD + 30, bottom=128)
    fig.update_xaxes(tickangle=-35)
    if counts:
        fig.update_yaxes(range=[0, max(counts) * 1.18])  # headroom for outside labels
    fig.update_layout(bargap=0.25, hovermode="closest")
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
        labels=labels, values=values, hole=0.55, sort=False,
        marker=dict(colors=colors, line=dict(color="white", width=1.5)),
        textinfo="percent",
        textposition="inside",
        insidetextorientation="horizontal",
        textfont=dict(size=12, family=_FONT),
        hovertemplate="%{label}: %{value} ESOPs<br>(%{percent})<extra></extra>",
    ))
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="h", y=-0.05, x=0.5, xanchor="center",
                    font=dict(size=10, color=_TEXT_BLACK)),
        height=config.CHART_HEIGHT_SM + 80,
        margin=dict(l=10, r=10, t=40, b=70),
        paper_bgcolor="white",
        font=dict(family=_FONT, color=_TEXT_BLACK, size=12),
        title=dict(text="MA ESOPs by Region (Form 5500)", x=0.5, xanchor="center",
                  font=dict(size=14, color=_TEXT_BLACK, family=_FONT)),
    )
    return fig


# ══════════════════════════════════════════════════
# FORM 5500 — FINANCIAL ANALYSIS CHARTS (Schedule H/I)
# ══════════════════════════════════════════════════

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
    _apply_layout(fig, title="Employer Contributions In vs. Benefits Paid Out",
                  y_title="Amount ($ millions)", source="U.S. DOL Form 5500, Schedules H and I",
                  height=config.CHART_HEIGHT_MD)
    fig.update_yaxes(tickprefix="$", ticksuffix="M", tickformat=",", rangemode="tozero")
    _year_axis(fig, years)
    fig.update_layout(barmode="group", bargap=0.25, bargroupgap=0.1)
    return fig
