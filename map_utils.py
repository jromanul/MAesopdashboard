"""
Form 5500 ESOP Dashboard — Municipal Choropleth Map Utilities

Provides reusable choropleth map functions using Plotly's px.choropleth_map
(MapLibre-based, no API key required). Requires a MassGIS town boundary
GeoJSON file at data/geo/ma_towns.geojson.

All functions return None when the GeoJSON file is not present, enabling
graceful degradation to existing scatter maps.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

import config

_log = logging.getLogger(__name__)

_GEOJSON_PATH = Path(__file__).parent / "data" / "geo" / "ma_towns.geojson"

_NAVY = config.CHART_COLORS["navy"]
_GOLD = config.CHART_COLORS["gold"]
_FONT = config.CHART_FONT_FAMILY

_MA_CENTER = {"lat": 42.10, "lon": -71.80}
_DEFAULT_ZOOM = 7.3

# Light-but-visible blue at the low end so towns with only 1–2 ESOPs are not
# washed out against the basemap, deepening to navy at the high end.
_DEFAULT_SCALE = [
    [0.0, "#9DC3E6"],
    [0.25, "#5B9BD5"],
    [0.5, "#2E75B6"],
    [0.75, "#1A5490"],
    [1.0, "#0A2E52"],
]

_NAME_OVERRIDES = {
    "Manchester": "MANCHESTER-BY-THE-SEA",
}

# Villages, neighborhoods, abbreviations, and common misspellings that are NOT
# MassGIS towns, mapped to their parent municipality (keyed by UPPER-cased input).
# Without this, these filings silently drop off the choropleth (e.g. Hyannis —
# the 6th-largest ESOP city — and every Boston neighborhood). Values are matched
# against GeoJSON properties.TOWN (all confirmed present in data/geo/ma_towns.geojson).
_VILLAGE_TO_TOWN = {
    # Boston neighborhoods
    "ALLSTON": "BOSTON", "BRIGHTON": "BOSTON", "CHARLESTOWN": "BOSTON",
    "DORCHESTER": "BOSTON", "HYDE PARK": "BOSTON", "JAMAICA PLAIN": "BOSTON",
    "MATTAPAN": "BOSTON", "ROSLINDALE": "BOSTON", "ROXBURY": "BOSTON",
    "WEST ROXBURY": "BOSTON", "EAST BOSTON": "BOSTON",
    # Villages → parent town
    "HYANNIS": "BARNSTABLE", "WEST YARMOUTH": "YARMOUTH", "SOUTH YARMOUTH": "YARMOUTH",
    "EAST OTIS": "OTIS", "NORTH CHELMSFORD": "CHELMSFORD", "N. CHELMSFORD": "CHELMSFORD",
    "DEVENS": "AYER", "SOUTH DEERFIELD": "DEERFIELD",
    "SOUTH LANCASTER": "LANCASTER", "SO. LANCASTER": "LANCASTER",
    "INDIAN ORCHARD": "SPRINGFIELD", "HAYDENVILLE": "WILLIAMSBURG",
    "WEST HATFIELD": "HATFIELD",
    # Abbreviations / misspellings → correct town
    "MARLBORO": "MARLBOROUGH", "MIDDLEBORO": "MIDDLEBOROUGH",
    "SOUTHBORO": "SOUTHBOROUGH", "WESTBORO": "WESTBOROUGH",
    "W. SPRINGFIELD": "WEST SPRINGFIELD", "RANDDOLPH": "RANDOLPH",
    "WODBURN": "WOBURN",
}


def _normalize_town_name(name: str) -> str:
    if not name:
        return ""
    if name in _NAME_OVERRIDES:
        return _NAME_OVERRIDES[name]
    key = name.strip().upper()
    return _VILLAGE_TO_TOWN.get(key, key)


def has_geojson() -> bool:
    return _GEOJSON_PATH.exists()


@st.cache_data(ttl=None)
def _load_geojson_cached(_mtime: float) -> dict | None:
    try:
        with open(_GEOJSON_PATH, "r") as f:
            data = json.load(f)
        return data
    except Exception as e:
        _log.error("Failed to load GeoJSON: %s", e)
        return None


def load_geojson() -> dict | None:
    if not _GEOJSON_PATH.exists():
        return None
    mtime = _GEOJSON_PATH.stat().st_mtime
    return _load_geojson_cached(mtime)


def create_choropleth_map(
    df: pd.DataFrame,
    value_col: str,
    title: str = "",
    color_scale=None,
    hover_cols: list[str] | None = None,
    legend_title: str = "",
    height: int = 600,
    source: str = "",
    range_color: tuple | None = None,
    municipality_col: str = "municipality",
    hover_fields: list[tuple[str, str, str]] | None = None,
) -> go.Figure | None:
    """Municipal choropleth. ``hover_fields`` is [(label, column, d3-format)]
    shown under the town name, e.g. [("Plans", "plan_count", ",d")]."""
    geojson = load_geojson()
    if geojson is None:
        return None

    if df.empty or value_col not in df.columns:
        return None

    plot_df = df.copy()
    plot_df["_town_match"] = plot_df[municipality_col].apply(_normalize_town_name)

    # Roll villages/neighborhoods up into their parent town: sum all numeric
    # columns per town so a town's shade reflects every filing within it (e.g.
    # Boston = Boston + Allston + Dorchester) and duplicate town rows merge into
    # one polygon rather than overwriting each other.
    _num_cols = [c for c in plot_df.select_dtypes(include="number").columns]
    if _num_cols:
        plot_df = plot_df.groupby("_town_match", as_index=False)[_num_cols].sum()
    else:
        plot_df = plot_df.drop_duplicates(subset="_town_match")
    plot_df[municipality_col] = plot_df["_town_match"].str.title()

    if color_scale is None:
        color_scale = _DEFAULT_SCALE

    hover_data_dict = {}
    if hover_cols:
        for col in hover_cols:
            if col in plot_df.columns:
                hover_data_dict[col] = True
    hover_data_dict["_town_match"] = False

    fig = px.choropleth_map(
        plot_df,
        geojson=geojson,
        locations="_town_match",
        featureidkey="properties.TOWN",
        color=value_col,
        hover_name=municipality_col,
        hover_data=hover_data_dict if hover_data_dict else None,
        color_continuous_scale=color_scale,
        range_color=range_color,
        opacity=1.0,
        zoom=_DEFAULT_ZOOM,
        center=_MA_CENTER,
        map_style="carto-positron",
        height=height,
        title=title,
    )

    fig.update_traces(marker_line_width=0.4, marker_line_color="rgba(255,255,255,.7)")
    if hover_fields:
        cols = [c for _l, c, _f in hover_fields if c in plot_df.columns]
        fig.update_traces(
            customdata=plot_df[cols].fillna(0).to_numpy(),
            hovertemplate="<b>%{hovertext}</b><br>" + "<br>".join(
                f"{label}: %{{customdata[{i}]:{fmt}}}"
                for i, (label, _c, fmt) in enumerate(
                    [h for h in hover_fields if h[1] in plot_df.columns]))
            + "<extra></extra>")

    fig.update_layout(
        font=dict(family=_FONT, color="#0F1E2E", size=12),
        title=dict(font=dict(size=14, family=_FONT, color="#0F1E2E", weight=700), x=0.01),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=50 if title else 10, b=34 if source else 0),
        coloraxis_colorbar=dict(
            title=dict(text=legend_title or value_col.replace("_", " ").title(),
                       font=dict(size=11, color="#3D4E60")),
            thickness=14,
            len=0.7,
            tickfont=dict(size=10.5, color="#55667A"),
        ),
        hoverlabel=dict(bgcolor="#0A2E52", bordercolor="#0A2E52",
                        font=dict(family=_FONT, color="#FFFFFF", size=12)),
    )

    if source:
        fig.add_annotation(
            text=f"Source: {source}",
            xref="paper", yref="paper",
            x=0, y=0, yanchor="top", yshift=-8, showarrow=False,
            font=dict(size=10, color="#55667A", family=_FONT),
        )

    return fig
