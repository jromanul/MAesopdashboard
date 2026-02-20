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

_DEFAULT_SCALE = [
    [0.0, "#EDF4FB"],
    [0.25, "#1A6BB5"],
    [0.5, "#14558F"],
    [0.75, "#F6C51B"],
    [1.0, "#B8930E"],
]

_NAME_OVERRIDES = {
    "Manchester": "MANCHESTER-BY-THE-SEA",
}


def _normalize_town_name(name: str) -> str:
    if not name:
        return ""
    if name in _NAME_OVERRIDES:
        return _NAME_OVERRIDES[name]
    return name.strip().upper()


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
) -> go.Figure | None:
    geojson = load_geojson()
    if geojson is None:
        return None

    if df.empty or value_col not in df.columns:
        return None

    plot_df = df.copy()
    plot_df["_town_match"] = plot_df[municipality_col].apply(_normalize_town_name)

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

    fig.update_traces(marker_line_width=0)

    fig.update_layout(
        font=dict(family=_FONT, color=_NAVY, size=12),
        paper_bgcolor="white",
        margin=dict(l=0, r=0, t=50 if title else 10, b=30 if source else 0),
        coloraxis_colorbar=dict(
            title=legend_title or value_col.replace("_", " ").title(),
            thickness=15,
            len=0.7,
        ),
    )

    if source:
        fig.add_annotation(
            text=f"Source: {source}",
            xref="paper", yref="paper",
            x=0, y=-0.03, showarrow=False,
            font=dict(size=9, color="#95A5A6", family=_FONT),
        )

    return fig
