"""
Form 5500 ESOP Dashboard — Municipal Choropleth Map Utilities

Provides a municipal choropleth drawn from a MassGIS town boundary GeoJSON
file at data/geo/ma_towns.geojson (Plotly geo, no basemap tiles or API key).

All functions return None when the GeoJSON file is not present, enabling
graceful degradation to existing scatter maps.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

import config

_log = logging.getLogger(__name__)

_GEOJSON_PATH = Path(__file__).parent / "data" / "geo" / "ma_towns.geojson"

_NAVY = config.CHART_COLORS["navy"]
_GOLD = config.CHART_COLORS["gold"]
_FONT = config.CHART_FONT_FAMILY

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
# Without this, these filings silently drop off the choropleth (e.g. Hyannis
# and every Boston neighborhood). Values are matched
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


def _ring_area(ring: list) -> float:
    """Signed planar area of a ring: positive when it runs counterclockwise."""
    return sum(x1 * y2 - x2 * y1 for (x1, y1, *_), (x2, y2, *_) in zip(ring, ring[1:] + ring[:1])) / 2


def _rewind_for_d3(data: dict) -> dict:
    """Wind outer rings clockwise and holes counterclockwise, as Plotly's geo
    (d3) renderer expects. MassGIS publishes the opposite (the GeoJSON
    standard's winding), which d3 reads as "everything but the town" and
    paints over the whole map."""
    for feat in data.get("features", []):
        geom = feat.get("geometry") or {}
        polys = ([geom["coordinates"]] if geom.get("type") == "Polygon"
                 else geom.get("coordinates", []) if geom.get("type") == "MultiPolygon" else [])
        for poly in polys:
            for i, ring in enumerate(poly):
                if (_ring_area(ring) > 0) == (i == 0):
                    ring.reverse()
    return data


@st.cache_data(ttl=None)
def _load_geojson_cached(_mtime: float) -> dict | None:
    try:
        with open(_GEOJSON_PATH, "r") as f:
            data = json.load(f)
        if "bbox" not in data:
            xs, ys = [], []
            for feat in data.get("features", []):
                geom = feat.get("geometry") or {}
                polys = ([geom["coordinates"]] if geom.get("type") == "Polygon"
                         else geom.get("coordinates", []))
                for poly in polys:
                    for x, y, *_ in poly[0]:
                        xs.append(x)
                        ys.append(y)
            data["bbox"] = [min(xs), min(ys), max(xs), max(ys)]
        return _rewind_for_d3(data)
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

    # Every town is drawn, those without a sponsor in a neutral grey, so the
    # state's outline reads at any width. A flat outline map (no basemap tiles)
    # framed on the state shows all of Massachusetts on first load, phone
    # included; a tiled map opens at one zoom and crops the state on a phone.
    towns = sorted({f["properties"]["TOWN"] for f in geojson["features"]})
    full = pd.DataFrame({"_town_match": towns}).merge(plot_df, on="_town_match", how="left")
    full[municipality_col] = full["_town_match"].str.title()
    z = full[value_col].fillna(0)
    zmax = float(range_color[1]) if range_color else float(z.max() or 1)
    zmin = float(range_color[0]) if range_color else 0.0
    # Zero gets its own grey step below the scale's lightest blue.
    cut = min(0.5 / zmax, 0.5) if zmax > 0 else 0.5
    scale = [[0.0, "#EEF2F7"], [cut, "#EEF2F7"]] + [
        [cut + (1 - cut) * stop, color] for stop, color in color_scale]

    hover = hover_fields or [(legend_title or value_col, value_col, ",d")]
    hover = [h for h in hover if h[1] in full.columns]
    fig = go.Figure(go.Choropleth(
        geojson=geojson, featureidkey="properties.TOWN",
        locations=full["_town_match"], z=z, zmin=zmin, zmax=zmax,
        colorscale=scale, marker_line_width=0.4, marker_line_color="#FFFFFF",
        text=full[municipality_col],
        customdata=full[[c for _l, c, _f in hover]].fillna(0).to_numpy(),
        hovertemplate="<b>%{text}</b><br>" + "<br>".join(
            f"{label}: %{{customdata[{i}]:{fmt}}}" for i, (label, _c, fmt) in enumerate(hover))
        + "<extra></extra>",
        colorbar=dict(
            title=dict(text=legend_title or value_col.replace("_", " ").title(),
                       side="top", font=dict(size=11, color="#3D4E60")),
            orientation="h", x=0.5, xanchor="center", y=0, yanchor="top",
            len=0.6, thickness=12, tickfont=dict(size=10.5, color="#55667A"),
            tick0=0, dtick=max(1, round(zmax / 5)) if zmax >= 5 else 1),
    ))
    # The state's own extent (not fitbounds, which leaves the map at a fraction
    # of the card on a wide screen).
    x0, y0, x1, y1 = geojson["bbox"]
    fig.update_geos(lonaxis_range=[x0 - 0.04, x1 + 0.04], lataxis_range=[y0 - 0.04, y1 + 0.04],
                    visible=False, projection_type="mercator", bgcolor="rgba(0,0,0,0)")
    fig.update_layout(
        height=height,
        font=dict(family=_FONT, color="#0F1E2E", size=12),
        # The title sits just above the map, below the band the toolbar takes on a
        # touch screen, so the toolbar never covers it.
        title=dict(text=title, font=dict(size=14, family=_FONT, color="#0F1E2E", weight=700),
                   x=0.01, xanchor="left", yref="paper", y=1, yanchor="bottom",
                   pad=dict(b=6)),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=72 if title else 10, b=(104 if "<br>" in source else 90)
                    if source else 70),
        hoverlabel=dict(bgcolor="#0A2E52", bordercolor="#0A2E52",
                        font=dict(family=_FONT, color="#FFFFFF", size=12)),
    )

    if source:
        fig.add_annotation(
            text=f"Source: {source}",
            xref="paper", yref="paper", x=0, xanchor="left",
            y=0, yanchor="top", yshift=-62, showarrow=False, align="left",
            font=dict(size=10, color="#55667A", family=_FONT),
        )

    return fig
