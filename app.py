"""
Form 5500 ESOP Dashboard — Standalone Streamlit Application
Duplicate of the Form 5500 ESOP section from the MassCEO Intelligence Dashboard.
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

import config
import utils
import charts
import form5500_analysis
import map_utils

# ── Page config ─────────────────────────────────

st.set_page_config(
    page_title=config.APP_TITLE,
    page_icon=config.APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────

st.markdown(
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Source+Sans+Pro:wght@400;600;700&display=swap">'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200">',
    unsafe_allow_html=True,
)
st.markdown("""
<style>

    /* ── Global font — exclude icon elements ── */
    html, body, [class*="st-"] { font-family: 'Source Sans Pro', sans-serif; }

    /* ── Fix: Ensure Material Symbols render as icons, not text ── */
    .material-symbols-rounded,
    [data-testid="stExpanderToggleIcon"],
    [data-testid="stExpanderToggleIcon"] *,
    span[data-testid="stIconMaterial"],
    [class*="Icon"] span,
    .e1nzilvr5 span,
    span.material-icons {
        font-family: 'Material Symbols Rounded', 'Material Icons', sans-serif !important;
        font-feature-settings: 'liga' !important;
        -webkit-font-feature-settings: 'liga' !important;
        font-size: 24px !important;
        direction: ltr;
        display: inline-block;
        letter-spacing: normal;
        line-height: 1;
        text-transform: none;
        white-space: nowrap;
        word-wrap: normal;
        -webkit-font-smoothing: antialiased;
        text-rendering: optimizeLegibility;
    }

    /* Force Streamlit's internal icon spans to use icon font, not body font */
    [data-testid="stSidebar"] span[class*="Icon"],
    [data-testid="stExpander"] span[class*="Icon"],
    button span[class*="Icon"],
    summary span[class*="Icon"],
    [data-testid="stMarkdownContainer"] span.material-symbols-rounded {
        font-family: 'Material Symbols Rounded' !important;
        font-feature-settings: 'liga' !important;
        -webkit-font-feature-settings: 'liga' !important;
    }

    /* ── Global background: white, all text black ── */
    .stApp, [data-testid="stAppViewContainer"] {
        background-color: #FFFFFF;
    }
    /* Force all main-area headings and text to black */
    [data-testid="stAppViewContainer"] h1,
    [data-testid="stAppViewContainer"] h2,
    [data-testid="stAppViewContainer"] h3,
    [data-testid="stAppViewContainer"] h4,
    [data-testid="stAppViewContainer"] h5,
    [data-testid="stAppViewContainer"] h6 {
        color: #111111 !important;
    }
    [data-testid="stAppViewContainer"] p,
    [data-testid="stAppViewContainer"] li,
    [data-testid="stAppViewContainer"] span,
    [data-testid="stAppViewContainer"] label,
    [data-testid="stAppViewContainer"] .stMarkdown,
    [data-testid="stAppViewContainer"] .stCaption,
    [data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] {
        color: #222222 !important;
    }

    /* ── Button fixes: ensure text is visible ── */
    button[kind="secondary"],
    button[data-testid="stBaseButton-secondary"],
    .stButton button {
        color: #14558F !important;
        border: 2px solid #14558F !important;
        background-color: #FFFFFF !important;
        font-weight: 600 !important;
    }
    button[kind="secondary"]:hover,
    button[data-testid="stBaseButton-secondary"]:hover,
    .stButton button:hover {
        background-color: #EDF4FB !important;
        color: #14558F !important;
    }
    button[kind="secondary"] p,
    button[data-testid="stBaseButton-secondary"] p,
    .stButton button p,
    button[kind="secondary"] span,
    button[data-testid="stBaseButton-secondary"] span,
    .stButton button span {
        color: #14558F !important;
    }
    /* Download button */
    .stDownloadButton button {
        background-color: #14558F !important;
        color: #FFFFFF !important;
        border: none !important;
    }
    .stDownloadButton button p,
    .stDownloadButton button span {
        color: #FFFFFF !important;
    }
    .stDownloadButton button:hover {
        background-color: #1A6BB5 !important;
    }

    /* ── Sidebar: always visible, not collapsible ── */
    [data-testid="stSidebar"] {
        background-color: #14558F;
        min-width: 280px !important;
        max-width: 280px !important;
        transform: none !important;
        position: relative !important;
        transition: none !important;
    }
    [data-testid="stSidebar"][aria-expanded="false"] {
        min-width: 280px !important;
        max-width: 280px !important;
        margin-left: 0 !important;
        transform: none !important;
        display: block !important;
    }
    /* Hide the collapse/close button */
    [data-testid="stSidebar"] button[kind="header"],
    [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
    [data-testid="collapsedControl"],
    [data-testid="stSidebarCollapsedControl"],
    button[data-testid="stSidebarCollapseButton"] {
        display: none !important;
        visibility: hidden !important;
    }
    /* Hide the expand arrow when collapsed */
    [data-testid="stSidebarNav"],
    [data-testid="collapsedControl"] {
        display: none !important;
    }
    [data-testid="stSidebar"] *,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] div {
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stMetricValue,
    [data-testid="stSidebar"] [data-testid="stMetricValue"],
    [data-testid="stSidebar"] [data-testid="stMetricValue"] div {
        color: #F6C51B !important;
    }
    [data-testid="stSidebar"] .stMetricLabel,
    [data-testid="stSidebar"] [data-testid="stMetricLabel"],
    [data-testid="stSidebar"] [data-testid="stMetricLabel"] p,
    [data-testid="stSidebar"] [data-testid="stMetricLabel"] div {
        color: rgba(255,255,255,0.7) !important;
    }
    [data-testid="stSidebar"] a {
        color: #F6C51B !important;
    }
    [data-testid="stSidebar"] .stMarkdown p,
    [data-testid="stSidebar"] .stMarkdown li {
        color: rgba(255,255,255,0.9) !important;
    }
    [data-testid="stSidebar"] h3 {
        color: #F6C51B !important;
    }
    [data-testid="stSidebar"] hr {
        border-color: rgba(255,255,255,0.2) !important;
    }

    /* ── Dataframe / Table styling: white bg, black text ── */
    [data-testid="stDataFrame"],
    [data-testid="stDataFrame"] > div,
    [data-testid="stDataFrame"] iframe,
    .stDataFrame,
    .stDataFrame > div {
        background-color: #FFFFFF !important;
    }
    /* Glide data grid header and cells */
    [data-testid="stDataFrame"] [data-testid="glideDataEditor"],
    [data-testid="stDataFrame"] .dvn-scroller,
    [data-testid="stDataFrame"] canvas {
        background-color: #FFFFFF !important;
    }
    /* Table wrapper and search input */
    [data-testid="stDataFrame"] .stDataFrameResizable {
        background-color: #FFFFFF !important;
        border: 1px solid #E0E0E0 !important;
        border-radius: 8px !important;
    }
    /* Column header text */
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrame"] [role="columnheader"],
    [data-testid="stDataFrame"] .header-cell {
        color: #111111 !important;
        background-color: #F5F5F5 !important;
        font-weight: 600 !important;
    }
    /* Table cell text */
    [data-testid="stDataFrame"] td,
    [data-testid="stDataFrame"] [role="gridcell"],
    [data-testid="stDataFrame"] .data-cell {
        color: #111111 !important;
        background-color: #FFFFFF !important;
    }
    /* Force glide-data-grid text color via CSS custom properties */
    [data-testid="stDataFrame"],
    [data-testid="stDataFrame"] * {
        --gdg-text-dark: #111111 !important;
        --gdg-text-medium: #333333 !important;
        --gdg-text-light: #555555 !important;
        --gdg-text-bubble: #111111 !important;
        --gdg-bg-cell: #FFFFFF !important;
        --gdg-bg-header: #F5F7FA !important;
        --gdg-text-header: #111111 !important;
        --gdg-text-group-header: #111111 !important;
        --gdg-border-color: #E0E0E0 !important;
        --gdg-header-font-style: 600 13px !important;
        --gdg-base-font-style: 13px !important;
        color: #111111 !important;
    }
    /* Search/text inputs in main area */
    [data-testid="stAppViewContainer"] [data-testid="stTextInput"] input {
        background-color: #FFFFFF !important;
        color: #111111 !important;
        border: 1px solid #CCCCCC !important;
    }
    [data-testid="stAppViewContainer"] [data-testid="stTextInput"] label {
        color: #222222 !important;
    }

    /* ── Vertical nav in left column ── */
    div[data-testid="stRadio"] > label {
        display: none;
    }
    div[data-testid="stRadio"] > div {
        gap: 2px !important;
    }
    div[data-testid="stRadio"] > div > label {
        background-color: #EDF4FB !important;
        border-radius: 6px !important;
        padding: 0.55rem 0.9rem !important;
        margin: 0 !important;
        cursor: pointer !important;
        transition: all 0.15s ease !important;
        border-left: 3px solid transparent !important;
    }
    div[data-testid="stRadio"] > div > label:hover {
        background-color: #D9E8F7 !important;
        border-left-color: #F6C51B !important;
    }
    div[data-testid="stRadio"] > div > label[data-checked="true"],
    div[data-testid="stRadio"] > div > label:has(input:checked) {
        background-color: #14558F !important;
        border-left-color: #F6C51B !important;
    }
    div[data-testid="stRadio"] > div > label[data-checked="true"] p,
    div[data-testid="stRadio"] > div > label[data-checked="true"] span,
    div[data-testid="stRadio"] > div > label[data-checked="true"] div,
    div[data-testid="stRadio"] > div > label:has(input:checked) p,
    div[data-testid="stRadio"] > div > label:has(input:checked) span,
    div[data-testid="stRadio"] > div > label:has(input:checked) div {
        color: #FFFFFF !important;
    }
    div[data-testid="stRadio"] > div > label p,
    div[data-testid="stRadio"] > div > label span {
        color: #14558F !important;
        font-weight: 600 !important;
        font-size: 0.9rem !important;
    }

    .main-header {
        background: linear-gradient(135deg, #14558F 0%, #1A6BB5 50%, #14558F 100%);
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: white !important;
        border-bottom: 4px solid #F6C51B;
    }
    .main-header h1,
    .main-header h1 *,
    [data-testid="stAppViewContainer"] .main-header h1 { color: #FFFFFF !important; margin: 0; font-size: 2rem; }
    .main-header p,
    [data-testid="stAppViewContainer"] .main-header p { color: #F6C51B !important; margin: 0.3rem 0 0 0; font-size: 1rem; }
    .main-header .header-date,
    [data-testid="stAppViewContainer"] .main-header .header-date { color: rgba(255,255,255,0.8) !important; font-size: 0.85rem; text-align: right; }

    .section-header {
        border-bottom: 3px solid #F6C51B;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
        color: #14558F;
    }

    /* ── Metric Cards ── */
    .metric-card {
        background: #FFFFFF;
        border-left: 4px solid #14558F;
        padding: 1rem 1.2rem;
        border-radius: 0 8px 8px 0;
        margin-bottom: 0.5rem;
        transition: box-shadow 0.2s ease;
        box-shadow: 0 1px 4px rgba(20,85,143,0.08);
    }
    .metric-card:hover { box-shadow: 0 3px 12px rgba(20,85,143,0.18); }
    .metric-card h3 { margin: 0; font-size: 1.6rem; color: #14558F; }
    .metric-card p { margin: 0.2rem 0 0 0; color: #535353; font-size: 0.85rem; }
    .metric-card .delta-up { color: #27AE60; font-size: 0.85rem; }
    .metric-card .delta-down { color: #E74C3C; font-size: 0.85rem; }
    .metric-card-ma { border-left-color: #F6C51B; background: #FFFDF5; }
    .metric-card-ma h3 { color: #B8930E; }

    /* ── Callout Boxes ── */
    .callout-eo {
        background: linear-gradient(135deg, #EDF4FB 0%, #D9E8F7 100%);
        border: 2px solid #14558F;
        border-radius: 10px;
        padding: 1.2rem;
        margin: 1rem 0;
    }
    .callout-eo h4 { color: #14558F; margin-top: 0; }

    .callout-warning {
        background: linear-gradient(135deg, #FFFCEB 0%, #FFF8D6 100%);
        border: 2px solid #B8930E;
        border-radius: 10px;
        padding: 1.2rem;
        margin: 1rem 0;
    }
    .callout-warning h4 { color: #B8930E; margin-top: 0; }

    /* ── HTML data tables ── */
    .data-table-wrapper {
        max-height: 620px;
        overflow-y: auto;
        overflow-x: auto;
        border: 1px solid #E0E0E0;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    .data-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.85rem;
        font-family: 'Source Sans Pro', sans-serif;
        background: #FFFFFF;
    }
    .data-table thead {
        position: sticky;
        top: 0;
        z-index: 2;
    }
    .data-table th {
        background-color: #14558F !important;
        color: #FFFFFF !important;
        padding: 10px 12px;
        text-align: left;
        font-weight: 600;
        white-space: nowrap;
        border-bottom: 2px solid #0E3D6B;
    }
    .data-table td {
        padding: 8px 12px;
        color: #111111 !important;
        background-color: #FFFFFF !important;
        border-bottom: 1px solid #EEEEEE;
        white-space: nowrap;
    }
    .data-table tbody tr:nth-child(even) td {
        background-color: #F9FAFB !important;
    }
    .data-table tbody tr:hover td {
        background-color: #EDF4FB !important;
    }
    .data-table td.num {
        text-align: right;
        font-variant-numeric: tabular-nums;
    }
    .data-table td.row-num {
        text-align: center;
        color: #999999 !important;
        font-weight: 600;
        min-width: 35px;
    }

    /* ── Totals footer row ── */
    .data-table tfoot {
        position: sticky;
        bottom: 0;
        z-index: 2;
    }
    .data-table tfoot td {
        background-color: #0E3D6B !important;
        color: #FFFFFF !important;
        font-weight: 700 !important;
        border-top: 2px solid #F6C51B !important;
        border-bottom: none !important;
        padding: 10px 12px;
        white-space: nowrap;
        font-variant-numeric: tabular-nums;
    }
    .data-table tfoot td.num {
        text-align: right;
    }

    /* ── Sortable column headers ── */
    .data-table th.sortable {
        cursor: pointer;
        user-select: none;
        position: relative;
        padding-right: 22px;
    }
    .data-table th.sortable:hover {
        background-color: #1A6BB5 !important;
    }
    .data-table th.sortable::after {
        content: '\u2195';
        position: absolute;
        right: 6px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 0.75rem;
        opacity: 0.5;
    }
    .data-table th.sortable.sort-asc::after {
        content: '\u25B2';
        opacity: 1;
    }
    .data-table th.sortable.sort-desc::after {
        content: '\u25BC';
        opacity: 1;
    }

</style>
""", unsafe_allow_html=True)


# ── Helper: Render DataFrame as HTML table ──────

def _render_html_table(df, money_cols=None, number_cols=None, height=600,
                       numbered=False, sortable_cols=None, show_totals=False):
    """Render a DataFrame as a styled HTML table with guaranteed black text.

    - Money columns: show '$X' for values, '$0' for zero, '\u2014' for NULL/missing.
    - Number columns: show 'X' for values, '0' for zero, '\u2014' for NULL/missing.
    - Text columns: show value or '\u2014' for empty/NULL.
    - numbered: if True, adds a '#' column on the left with row numbers (1, 2, 3...).
    - sortable_cols: list of column names that can be sorted by clicking the header.
    - show_totals: if True, adds a Total row at the bottom summing numeric columns.
    """
    import hashlib
    money_cols = money_cols or []
    number_cols = number_cols or []
    sortable_cols = sortable_cols or []

    table_id = "tbl_" + hashlib.md5(str(df.columns.tolist()).encode()).hexdigest()[:8]

    html = f'<div class="data-table-wrapper" style="max-height:{height}px;">'
    html += f'<table class="data-table" id="{table_id}"><thead><tr>'
    if numbered:
        html += '<th style="text-align:center;min-width:35px;">#</th>'
    for col_idx, col in enumerate(df.columns):
        if col in sortable_cols:
            html += f'<th class="sortable" data-col="{col_idx}">{col}</th>'
        else:
            html += f'<th>{col}</th>'
    html += '</tr></thead><tbody>'

    na_cell = '<td style="color:#999999;text-align:center;">\u2014</td>'

    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        html += '<tr>'
        if numbered:
            html += f'<td class="row-num">{row_idx}</td>'
        for col in df.columns:
            val = row[col]
            is_missing = pd.isna(val) or val is None or str(val).strip() in ("", "None", "nan")

            # Embed raw numeric value as data-val for sorting
            sort_attr = ""
            if col in sortable_cols:
                if is_missing:
                    sort_attr = ' data-val="-999999999999"'
                elif col in money_cols or col in number_cols:
                    try:
                        sort_attr = f' data-val="{float(val)}"'
                    except (ValueError, TypeError):
                        sort_attr = ' data-val="-999999999999"'
                else:
                    # Text-based sortable columns (e.g. year strings)
                    try:
                        sort_attr = f' data-val="{float(val)}"'
                    except (ValueError, TypeError):
                        sort_attr = ' data-val="-999999999999"'

            if col in money_cols:
                if is_missing:
                    html += f'<td style="color:#999999;text-align:center;"{sort_attr}>\u2014</td>'
                else:
                    try:
                        num = float(val)
                        if num == 0:
                            html += f'<td class="num" style="color:#999999;"{sort_attr}>$0</td>'
                        elif num < 0:
                            html += f'<td class="num" style="color:#C0392B;"{sort_attr}>-${abs(num):,.0f}</td>'
                        else:
                            html += f'<td class="num"{sort_attr}>${num:,.0f}</td>'
                    except (ValueError, TypeError):
                        html += f'<td{sort_attr}>{val}</td>'
            elif col in number_cols:
                if is_missing:
                    html += f'<td style="color:#999999;text-align:center;"{sort_attr}>\u2014</td>'
                else:
                    try:
                        num = float(val)
                        if num == 0:
                            html += f'<td class="num" style="color:#999999;"{sort_attr}>0</td>'
                        else:
                            html += f'<td class="num"{sort_attr}>{num:,.0f}</td>'
                    except (ValueError, TypeError):
                        html += f'<td{sort_attr}>{val}</td>'
            else:
                if is_missing:
                    if sort_attr:
                        html += f'<td style="color:#999999;text-align:center;"{sort_attr}>\u2014</td>'
                    else:
                        html += na_cell
                else:
                    html += f'<td{sort_attr}>{val}</td>'
        html += '</tr>'
    html += '</tbody>'

    # ── Totals footer row ──
    if show_totals:
        _tfoot_style = ('style="background-color:#0E3D6B !important;color:#FFFFFF !important;'
                        'font-weight:700;border-top:2px solid #F6C51B;"')
        html += f'<tfoot><tr {_tfoot_style}>'
        if numbered:
            html += f'<td {_tfoot_style}></td>'

        # Pre-compute totals for ratio columns (e.g., Assets/Participant)
        _ratio_cols = {}
        if "Assets/Participant" in df.columns:
            _tot_assets = pd.to_numeric(df.get("Total Assets"), errors="coerce").sum()
            _tot_ptcp = pd.to_numeric(df.get("Participants"), errors="coerce").sum()
            if _tot_ptcp > 0:
                _ratio_cols["Assets/Participant"] = _tot_assets / _tot_ptcp

        first_col = True
        for col in df.columns:
            if col in _ratio_cols:
                # Use weighted average instead of sum
                avg = _ratio_cols[col]
                if col in money_cols:
                    cell_val = f'${avg:,.0f}'
                else:
                    cell_val = f'{avg:,.0f}'
                html += (f'<td class="num" {_tfoot_style}>{cell_val}</td>')
                first_col = False
            elif col in money_cols or col in number_cols:
                # Sum non-null values
                total = pd.to_numeric(df[col], errors="coerce").sum()
                if col in money_cols:
                    if total < 0:
                        cell_val = f'-${abs(total):,.0f}'
                    else:
                        cell_val = f'${total:,.0f}'
                else:
                    cell_val = f'{total:,.0f}'
                html += (f'<td class="num" {_tfoot_style}>{cell_val}</td>')
                first_col = False
            else:
                if first_col:
                    html += f'<td {_tfoot_style}>TOTAL</td>'
                    first_col = False
                else:
                    html += f'<td {_tfoot_style}></td>'
        html += '</tr></tfoot>'

    html += '</table></div>'

    st.markdown(html, unsafe_allow_html=True)

    # Inject JavaScript for sorting via st.components.v1.html (Streamlit strips <script> from st.markdown)
    if sortable_cols:
        import streamlit.components.v1 as components
        sort_js = f"""
<script>
(function() {{
  // Access the parent Streamlit document
  var doc = window.parent.document;
  var table = doc.getElementById('{table_id}');
  if (!table) return;
  var headers = table.querySelectorAll('th.sortable');
  var hasNumberCol = {'true' if numbered else 'false'};

  headers.forEach(function(th) {{
    // Remove any existing listeners by cloning
    var newTh = th.cloneNode(true);
    th.parentNode.replaceChild(newTh, th);

    newTh.addEventListener('click', function() {{
      var colIdx = parseInt(newTh.getAttribute('data-col'));
      var tbody = table.querySelector('tbody');
      var rows = Array.from(tbody.querySelectorAll('tr'));
      var tdIndex = hasNumberCol ? colIdx + 1 : colIdx;

      // Determine sort direction (default first click = descending / highest first)
      var isDesc = newTh.classList.contains('sort-desc');
      // Clear all sort indicators
      var allHeaders = table.querySelectorAll('th.sortable');
      allHeaders.forEach(function(h) {{ h.classList.remove('sort-asc', 'sort-desc'); }});

      if (isDesc) {{
        newTh.classList.add('sort-asc');
      }} else {{
        newTh.classList.add('sort-desc');
      }}
      var ascending = isDesc;

      rows.sort(function(a, b) {{
        var aCell = a.children[tdIndex];
        var bCell = b.children[tdIndex];
        var aVal = aCell ? parseFloat(aCell.getAttribute('data-val')) : -999999999999;
        var bVal = bCell ? parseFloat(bCell.getAttribute('data-val')) : -999999999999;
        if (isNaN(aVal)) aVal = -999999999999;
        if (isNaN(bVal)) bVal = -999999999999;
        return ascending ? aVal - bVal : bVal - aVal;
      }});

      // Re-append sorted rows and renumber
      rows.forEach(function(row, idx) {{
        tbody.appendChild(row);
        if (hasNumberCol) {{
          row.children[0].textContent = idx + 1;
        }}
      }});
    }});
  }});
}})();
</script>"""
        components.html(sort_js, height=0, scrolling=False)


# ── Helper: Metric Card ─────────────────────────

def _render_metric(col, value, label, source="", ma=False, delta=None, delta_fmt="", scope=""):
    card_class = "metric-card metric-card-ma" if ma else "metric-card"
    delta_html = ""
    if delta is not None:
        arrow = "\u25b2" if delta >= 0 else "\u25bc"
        cls = "delta-up" if delta >= 0 else "delta-down"
        delta_html = f'<span class="{cls}">{arrow} {delta_fmt}</span>'
    source_html = f'<p style="font-size:0.7rem;color:#8899AA;">{source}</p>' if source else ""
    _scope = scope if scope else ("MA" if ma else "")
    if _scope == "MA":
        badge_html = '<span style="display:inline-block;font-size:0.6rem;font-weight:700;color:#B8930E;background:#F6C51B22;padding:1px 7px;border-radius:8px;margin-bottom:4px;">MA</span>'
    elif _scope == "US":
        badge_html = '<span style="display:inline-block;font-size:0.6rem;font-weight:700;color:#14558F;background:#14558F18;padding:1px 7px;border-radius:8px;margin-bottom:4px;">US</span>'
    elif _scope == "Federal":
        badge_html = '<span style="display:inline-block;font-size:0.6rem;font-weight:700;color:#6C3483;background:#6C348318;padding:1px 7px;border-radius:8px;margin-bottom:4px;">Federal</span>'
    else:
        badge_html = ""
    with col:
        st.markdown(
            f'<div class="{card_class}">'
            f'{badge_html}'
            f'<h3>{value}</h3>'
            f'<p>{label}</p>'
            f'{delta_html}'
            f'{source_html}'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── Header ──────────────────────────────────────

st.markdown(
    f'<div class="main-header">'
    f'<div style="display:flex;justify-content:space-between;align-items:center;">'
    f'<div>'
    f'<h1>{config.APP_ICON} {config.APP_TITLE}</h1>'
    f'<p>Original analysis of DOL Form 5500 bulk filings.</p>'
    f'</div>'
    f'<div class="header-date">{datetime.now().strftime("%B %d, %Y")}</div>'
    f'</div>'
    f'</div>',
    unsafe_allow_html=True,
)

# ── Data Loading ────────────────────────────────

_f5500_records_path = os.path.join(os.path.dirname(__file__) or ".", config.FORM5500_RECORDS_CSV)
_f5500_summary_path = os.path.join(os.path.dirname(__file__) or ".", config.FORM5500_SUMMARY_CSV)

if not form5500_analysis.has_data():
    if os.path.exists(_f5500_records_path):
        with st.spinner("Importing Form 5500 data from pre-processed CSV..."):
            form5500_analysis.import_from_csv(_f5500_records_path)
            st.rerun()
    elif os.path.exists(_f5500_summary_path):
        with st.spinner("Importing Form 5500 summary data..."):
            form5500_analysis.import_from_summary_csv(_f5500_summary_path)
            st.rerun()

# Auto-import Schedule H/I financial data if available but not yet loaded
if form5500_analysis.has_data() and not form5500_analysis.has_financial_data():
    _schedule_dir = config.FORM5500_SCHEDULE_DIR
    if os.path.isdir(_schedule_dir):
        _sch_csv_files = [f for f in os.listdir(_schedule_dir) if f.lower().endswith(".csv")]
        if _sch_csv_files:
            with st.spinner(f"Importing Schedule H/I financial data from {len(_sch_csv_files)} file(s)..."):
                _sch_results = form5500_analysis.import_all_schedule_csvs()
                if any(r.get("updated", 0) > 0 for r in _sch_results):
                    form5500_analysis.recompute_annual_summaries()
                    st.rerun()

# Controls
f5500_c1, f5500_c2, f5500_c3 = st.columns([2, 2, 6])
with f5500_c1:
    if st.button("Rebuild Form 5500 Data", type="secondary"):
        st.warning("This downloads ~2-5 GB of DOL bulk data and takes 15-45 minutes. "
                  "For best results, run the standalone script instead:\n\n"
                  "```\npython3 process_form5500.py --import-to-db\n```")
with f5500_c2:
    _f5500_meta_import = form5500_analysis.get_meta("last_import")
    if _f5500_meta_import:
        st.caption(f"Last import: {utils.format_last_updated(_f5500_meta_import)}")
    else:
        st.caption("No data imported yet")

# ── Main Content ────────────────────────────────

f5500_summaries = form5500_analysis.get_annual_summaries()

if f5500_summaries:
    _f5500_valid = [s for s in f5500_summaries if s.get("ma_plan_count", 0) > 0]
    if _f5500_valid:
        latest_year = _f5500_valid[-1]["filing_year"]
    else:
        latest_year = f5500_summaries[-1]["filing_year"]

    # ── Sidebar Navigation (placed first in sidebar) ──
    _nav_options = [
        "\U0001f4ca Overview",
        "\U0001f4c8 Trends",
        "\U0001f5fa\ufe0f Geography",
        "\U0001f504 Year-over-Year",
        "\U0001f1fa\U0001f1f8 National Comparison",
        "\U0001f4d6 Methodology",
    ]

    with st.sidebar:
        _selected_page = st.radio(
            "Navigation",
            _nav_options,
            key="f5500_nav",
            label_visibility="collapsed",
        )

    # ────────────────────────────────────
    # PAGE: Overview (Summary Cards + Full Data Table + Distribution)
    # ────────────────────────────────────
    if _selected_page == "\U0001f4ca Overview":
        latest = next((s for s in reversed(f5500_summaries) if s.get("ma_plan_count", 0) > 0), f5500_summaries[-1])
        st.markdown(f"#### Most Recent Filing Year: {latest_year}")

        # Compute Overview metrics from *active* filings only (excluding zombie plans)
        _ov_fin = form5500_analysis.get_financial_summary(
            latest_year, exclude_zombie=True)
        _ov_plan_count = _ov_fin.get("plans_total", 0) or 0
        _ov_total_part = _ov_fin.get("total_participants", 0) or 0
        _ov_total_assets = _ov_fin.get("total_assets", 0) or 0
        _ov_avg_assets = _ov_fin.get("avg_assets_per_plan", 0) or 0
        _ov_avg_part = (_ov_total_part / _ov_plan_count) if _ov_plan_count > 0 else 0
        _ov_unique_cos = len({f["ein"] for f in
                             form5500_analysis.get_ma_filings(latest_year,
                                                              exclude_zombie=True)})

        # Row 1: Core counts
        oc1, oc2, oc3 = st.columns(3)
        _render_metric(oc1, f"{_ov_plan_count:,}", "Active MA ESOP Plans",
                      f"DOL Form 5500 ({_ov_unique_cos} unique companies)", ma=True)
        _render_metric(oc2, f"{_ov_total_part:,}", "Total MA ESOP Participants",
                      "DOL Form 5500", ma=True)
        if _ov_total_assets > 0:
            assets_label = f"${_ov_total_assets / 1e9:.1f}B" if _ov_total_assets >= 1e9 else f"${_ov_total_assets / 1e6:.0f}M"
        else:
            assets_label = "N/A"
        _render_metric(oc3, assets_label, "Total MA ESOP Assets",
                      "DOL Form 5500 (Schedule H/I)" if _ov_total_assets > 0 else "Financial data not in main filing", ma=True)

        # Row 2: Averages
        oc4, oc5, oc6 = st.columns(3)
        if _ov_avg_assets > 0:
            avg_label = f"${_ov_avg_assets / 1e6:.1f}M" if _ov_avg_assets >= 1e6 else f"${_ov_avg_assets:,.0f}"
        else:
            avg_label = "N/A"
        _render_metric(oc4, avg_label, "Average Plan Size (Assets)",
                      "DOL Form 5500 (Schedule H/I)" if _ov_avg_assets > 0 else "Requires Schedule H/I data")
        _render_metric(oc5, f"{_ov_avg_part:,.0f}", "Avg Participants Per Plan", "DOL Form 5500")
        if _ov_total_assets > 0 and _ov_total_part > 0:
            _ov_avg_assets_per_part = _ov_total_assets / _ov_total_part
            avg_per_part_label = f"${_ov_avg_assets_per_part:,.0f}"
        else:
            avg_per_part_label = "N/A"
        _render_metric(oc6, avg_per_part_label, "Avg Assets Per Participant",
                      "DOL Form 5500 (Schedule H/I)" if _ov_total_assets > 0 else "Requires Schedule H/I data")

        # KSOP note
        ksop_count = latest.get("ma_ksop_count") or 0
        if ksop_count > 0:
            st.caption(f"_Note: {ksop_count} of the {latest['ma_plan_count']} total filed plans are KSOPs (combined 401(k)/ESOP plans)._")

        # Zombie / exclusion note
        _zombie_count = len(form5500_analysis.ZOMBIE_PLAN_EINS)
        st.caption(
            f"_Note: {_zombie_count} plans with $0 assets or 0 active participants "
            f"(appearing defunct / winding down) are excluded from this page. "
            f"All {latest['ma_plan_count']} filed plans are included in other tabs. "
            f"See the Year-over-Year tab for details._"
        )

        # 2024 data disclaimer
        if latest_year == 2024:
            st.info(
                "**2024 Data Disclaimer:** The data shown reflects filings available through the "
                "DOL EFAST2 bulk data releases and individual filing searches as of April 16, "
                "2026. Some plans file on fiscal-year schedules or request extensions, so their "
                "2024 filings may not yet be published by DOL.\n\n"
                "13 ESOPs that filed in 2023 have been confirmed as terminated (acquired, merged, "
                "or wound down), and 10 additional plans have no 2024 ESOP filing yet on DOL "
                "and are presumed late filers. See the Year-over-Year tab for details."
            )

        # Financial data: show employer securities section if available
        _has_fin = form5500_analysis.has_financial_data()
        if _has_fin:
            _fin_summary = form5500_analysis.get_financial_summary(
                latest_year, exclude_zombie=True)
            _es = _fin_summary.get("total_employer_securities", 0) or 0
            if _es > 0:
                st.markdown("---")
                st.markdown("##### ESOP-Specific Financial Data")
                _sec_c1, _sec_c2, _sec_c3 = st.columns(3)
                _es_label = f"${_es / 1e9:.1f}B" if _es >= 1e9 else f"${_es / 1e6:.0f}M"
                _render_metric(_sec_c1, _es_label, "Employer Securities (Company Stock)",
                              "Schedule H, Part I Line 1c", ma=True)
                _es_pct = _es / _fin_summary["total_assets"] * 100 if _fin_summary.get("total_assets", 0) > 0 else 0
                _render_metric(_sec_c2, f"{_es_pct:.0f}%", "Stock as % of Total Assets",
                              "Schedule H, Part I Line 1c / Total Assets", ma=True)
                _avg_pp = _fin_summary.get("avg_assets_per_participant", 0)
                _avg_pp_label = f"${_avg_pp:,.0f}" if _avg_pp > 0 else "N/A"
                _render_metric(_sec_c3, _avg_pp_label, "Avg Assets Per Participant",
                              "Schedule H/I", ma=True)

        elif not _has_fin:
            st.markdown(
                '<div class="callout-eo" style="border-left-color: #C5960C;">'
                '<h4>Financial Data Note</h4>'
                '<p>Asset and contribution data require Schedule H/I filings, which are separate from the main '
                'Form 5500. The current dataset includes plan counts, participants, and geographic data from the '
                'main filing. To add financial data, place Schedule H/I CSV files in <code>data/form5500/</code> '
                'and refresh this page, or re-run the processor with Schedule H/I downloads enabled.</p>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Full Data Table (active MA ESOP filings) ──
        st.markdown("---")
        st.markdown(f"#### Active MA ESOP Filings ({latest_year})")

        f5500_search = st.text_input("Search by plan name, sponsor, or city", key="f5500_search")

        filings = form5500_analysis.get_ma_filings(latest_year,
                                                     exclude_zombie=True)
        if filings:
            f_df = pd.DataFrame(filings)
            if f5500_search:
                sl = f5500_search.lower()
                mask = f_df.apply(lambda r: sl in (
                    str(r.get("plan_name", "")) + str(r.get("sponsor_name", "")) +
                    str(r.get("sponsor_city", ""))
                ).lower(), axis=1)
                f_df = f_df[mask]

            display_cols = {
                "plan_name": "Plan Name",
                "sponsor_name": "Sponsor",
                "sponsor_city": "City",
                "industry_sector": "Industry",
                "plan_eff_date": "Plan Year Started",
                "is_ksop": "KSOP?",
                "total_participants": "Participants",
                "active_participants": "Active Participants",
                "total_assets": "Total Assets",
                "total_liabilities": "Total Liabilities",
                "employer_securities": "Employer Securities",
                "employer_contributions": "Employer Contributions",
                "benefits_paid": "Benefits Paid",
                "net_income": "Net Income",
            }
            avail = [c for c in display_cols if c in f_df.columns]
            show_df = f_df[avail].copy()
            show_df.columns = [display_cols[c] for c in avail]

            if "Plan Year Started" in show_df.columns:
                show_df["Plan Year Started"] = show_df["Plan Year Started"].apply(
                    lambda x: str(x)[:4] if pd.notna(x) and x else "")
            if "KSOP?" in show_df.columns:
                show_df["KSOP?"] = show_df["KSOP?"].apply(lambda x: "Yes" if x else "No")

            # Compute Assets per Participant column
            if "Total Assets" in show_df.columns and "Participants" in show_df.columns:
                def _calc_app(row):
                    assets = row.get("Total Assets")
                    ptcp = row.get("Participants")
                    try:
                        a = float(assets)
                        p = float(ptcp)
                        if pd.notna(a) and pd.notna(p) and p > 0 and a > 0:
                            return a / p
                    except (ValueError, TypeError):
                        pass
                    return None
                show_df["Assets/Participant"] = show_df.apply(_calc_app, axis=1)

            _sortable = ["Plan Year Started", "Participants", "Active Participants",
                         "Total Assets", "Total Liabilities", "Employer Securities",
                         "Employer Contributions", "Benefits Paid", "Net Income",
                         "Assets/Participant"]
            _render_html_table(show_df,
                               money_cols=["Total Assets", "Total Liabilities",
                                           "Employer Securities", "Employer Contributions",
                                           "Benefits Paid", "Net Income",
                                           "Assets/Participant"],
                               number_cols=["Participants", "Active Participants"],
                               height=600,
                               numbered=True,
                               sortable_cols=_sortable,
                               show_totals=True)
            _zombie_n = len(form5500_analysis.ZOMBIE_PLAN_EINS)
            _total_filed = latest['ma_plan_count']
            st.caption(
                f"Showing {len(show_df)} of {len(filings)} active MA ESOP filings "
                f"for {latest_year} ({_zombie_n} defunct/winding-down plans excluded "
                f"from {_total_filed} total filed)")

            csv_data = utils.to_csv_bytes(filings)
            st.download_button("Download Active MA ESOP Data as CSV", csv_data,
                              f"ma_esops_form5500_{latest_year}.csv", "text/csv")
        else:
            st.info("No filing-level data available. Run the Form 5500 processor with "
                   "`--import-to-db` to load individual filing records.")

        # ── Plan Size Distribution (moved from Distribution tab) ──
        st.markdown("---")
        st.markdown(f"#### Plan Size Distribution ({latest_year})")

        dcol1, dcol2 = st.columns(2)
        with dcol1:
            asset_vals = form5500_analysis.get_asset_distribution(
                latest_year, exclude_zombie=True)
            fig_ahist = charts.build_f5500_asset_histogram(asset_vals)
            st.plotly_chart(fig_ahist, use_container_width=True, config=charts.PLOTLY_CONFIG)
        with dcol2:
            partcp_vals = form5500_analysis.get_participant_distribution(
                latest_year, exclude_zombie=True)
            fig_phist = charts.build_f5500_participant_histogram(partcp_vals)
            st.plotly_chart(fig_phist, use_container_width=True, config=charts.PLOTLY_CONFIG)

        if partcp_vals and not asset_vals:
            st.markdown(
                '<div class="callout-eo" style="border-left-color: #C5960C;">'
                '<h4>Data Note</h4>'
                '<p>Participant count distribution is available from the main Form 5500 filing. '
                'Asset distribution requires Schedule H/I data which is not yet included in this dataset.</p>'
                '</div>',
                unsafe_allow_html=True,
            )

        # ── Industry Breakdown (moved from Industry tab) ──
        st.markdown("---")
        st.markdown(f"#### MA ESOPs by Industry ({latest_year})")

        industry_data = form5500_analysis.get_ma_filings_by_industry(
            latest_year, exclude_zombie=True)
        if industry_data:
            fig_ind = charts.build_f5500_industry_bar(industry_data)
            st.plotly_chart(fig_ind, use_container_width=True, config=charts.PLOTLY_CONFIG)
        else:
            st.markdown(
                '<div class="callout-eo" style="border-left-color: #C5960C; border-color: #C5960C;">'
                '<h4>Industry Classification Not Available</h4>'
                '<p>NAICS (industry) codes are not included in the main Form 5500 filing. '
                'Industry classification requires cross-referencing with Schedule C data, '
                'the IRS Business Master File, or external business registries (e.g., D&B, SBA). '
                'This analysis is planned for a future update.</p>'
                '</div>',
                unsafe_allow_html=True,
            )

    # ────────────────────────────────────
    # PAGE: Trends (5-10 year time series)
    # ────────────────────────────────────
    elif _selected_page == "\U0001f4c8 Trends":
        st.markdown("#### MA ESOP Trends Over Time")
        st.caption("The most recent filing year may show incomplete data due to DOL filing lag. "
                   "Form 5500 filings are due 7 months after plan year end (extensions allow up to 9.5 months), "
                   "so some plans may not yet appear in the most recent year's data.")

        fig_count = charts.build_f5500_plan_count_trend(f5500_summaries)
        st.plotly_chart(fig_count, use_container_width=True, config=charts.PLOTLY_CONFIG)

        tcol1, tcol2 = st.columns(2)
        with tcol1:
            fig_partcp = charts.build_f5500_participants_trend(f5500_summaries)
            st.plotly_chart(fig_partcp, use_container_width=True, config=charts.PLOTLY_CONFIG)
        with tcol2:
            fig_assets = charts.build_f5500_assets_trend(f5500_summaries)
            st.plotly_chart(fig_assets, use_container_width=True, config=charts.PLOTLY_CONFIG)

        tcol3, tcol4 = st.columns(2)
        with tcol3:
            fig_avg = charts.build_f5500_avg_plan_assets_trend(f5500_summaries)
            st.plotly_chart(fig_avg, use_container_width=True, config=charts.PLOTLY_CONFIG)
        with tcol4:
            fig_contrib = charts.build_f5500_contributions_bar(f5500_summaries)
            st.plotly_chart(fig_contrib, use_container_width=True, config=charts.PLOTLY_CONFIG)

    # ────────────────────────────────────
    # PAGE: Geography
    # ────────────────────────────────────
    elif _selected_page == "\U0001f5fa\ufe0f Geography":
        st.markdown(f"#### Geographic Distribution of MA ESOPs ({latest_year})")

        city_data = form5500_analysis.get_ma_filings_by_city(latest_year)
        if city_data:
            _city_df = pd.DataFrame(city_data).rename(columns={"sponsor_city": "municipality"})
            _esop_choropleth = map_utils.create_choropleth_map(
                _city_df, value_col="plan_count",
                title=f"MA ESOPs by Municipality ({latest_year})",
                color_scale=[[0, "#E8F0FE"], [0.5, "#1B3A5C"], [1, "#0A1628"]],
                legend_title="ESOP Count",
                source="DOL Form 5500 Filings",
            )
            if _esop_choropleth is not None:
                st.plotly_chart(_esop_choropleth, use_container_width=True, config=charts.PLOTLY_CONFIG)
            else:
                fig_map = charts.build_f5500_city_map(city_data)
                st.plotly_chart(fig_map, use_container_width=True, config=charts.PLOTLY_CONFIG)

            gcol1, gcol2 = st.columns([3, 2])
            with gcol1:
                fig_cities = charts.build_f5500_top_cities_bar(city_data)
                st.plotly_chart(fig_cities, use_container_width=True, config=charts.PLOTLY_CONFIG)
            with gcol2:
                fig_region = charts.build_f5500_regional_breakdown(city_data)
                st.plotly_chart(fig_region, use_container_width=True, config=charts.PLOTLY_CONFIG)
        else:
            st.info("No geographic data available. Run the Form 5500 processor to load filing data.")

    # ────────────────────────────────────
    # PAGE: Year-over-Year Changes
    # ────────────────────────────────────
    elif _selected_page == "\U0001f504 Year-over-Year":
        _yoy_year = 2024
        st.markdown(f"#### Year-over-Year Changes ({_yoy_year - 1} \u2192 {_yoy_year})")

        new_plans, terminated, late_filers = \
            form5500_analysis.get_new_and_terminated(_yoy_year)

        _no_file_total = len(terminated) + len(late_filers)
        net_change = len(new_plans) - _no_file_total
        yoy_c1, yoy_c2, yoy_c3, yoy_c4 = st.columns(4)
        _render_metric(yoy_c1, str(len(new_plans)), "New ESOPs", f"New in {_yoy_year}")
        _render_metric(yoy_c2, str(len(terminated)), "Confirmed Terminated",
                      "Acquired / ESOP closed")
        _render_metric(yoy_c3, str(len(late_filers)), "Late Filers",
                      "No 2024 filing yet on DOL")
        net_label = f"+{net_change}" if net_change > 0 else str(net_change)
        _render_metric(yoy_c4, net_label, "Net Change",
                      f"{_yoy_year - 1} \u2192 {_yoy_year}")

        st.caption(f"_Plans that filed Form 5500 in {_yoy_year - 1} but are **absent** "
                   f"from the {_yoy_year} dataset are classified based on DOL EFAST2 review "
                   f"and public records research (as of Apr 16, 2026). "
                   f"**Confirmed Terminated** = the sponsor was acquired, merged, or the ESOP "
                   f"was otherwise closed (see Reason column for details). "
                   f"**Late Filer** = no 2024 Form 5500 filing of any kind appears on DOL yet; "
                   f"these plans are believed still active. "
                   f"Financial data shown is from their last filing ({_yoy_year - 1})._")

        _yoy_all_cols = ["yoy_status", "yoy_note", "plan_name", "sponsor_name",
                         "sponsor_city",
                         "industry_sector", "plan_eff_date", "total_participants",
                         "active_participants", "total_assets", "total_liabilities",
                         "employer_securities", "employer_contributions",
                         "benefits_paid", "net_income"]
        _yoy_col_map = {"yoy_status": "Status", "yoy_note": "Reason",
                         "plan_name": "Plan Name", "sponsor_name": "Sponsor",
                         "sponsor_city": "City", "industry_sector": "Industry",
                         "plan_eff_date": "Plan Year Started",
                         "total_participants": "Participants",
                         "active_participants": "Active Participants",
                         "total_assets": "Total Assets",
                         "total_liabilities": "Total Liabilities",
                         "employer_securities": "Employer Securities",
                         "employer_contributions": "Employer Contributions",
                         "benefits_paid": "Benefits Paid",
                         "net_income": "Net Income"}
        _yoy_money_cols = ["Total Assets", "Total Liabilities", "Employer Securities",
                           "Employer Contributions", "Benefits Paid", "Net Income"]

        def _render_yoy_table(plans, height=400):
            """Helper to render a YoY category table."""
            df = pd.DataFrame(plans)
            cols = [c for c in _yoy_all_cols if c in df.columns]
            if not cols:
                return
            disp = df[cols].copy()
            disp.columns = [_yoy_col_map.get(c, c.replace("_", " ").title()) for c in cols]
            if "Plan Year Started" in disp.columns:
                disp["Plan Year Started"] = disp["Plan Year Started"].apply(
                    lambda x: str(x)[:4] if pd.notna(x) and x else "N/A")
            _render_html_table(disp,
                               money_cols=_yoy_money_cols,
                               number_cols=["Participants", "Active Participants"],
                               height=height)

        if new_plans:
            st.markdown(f"##### New ESOP Filings in {_yoy_year}")
            st.caption(f"Plans that filed in {_yoy_year} but did **not** file in {_yoy_year - 1}. "
                       f"**New ESOP (Started {_yoy_year})** = plan established in {_yoy_year}. "
                       f"**New ESOP (Started {_yoy_year - 1})** = plan established in {_yoy_year - 1}, first full-year filing in {_yoy_year}. "
                       f"**New Filing** = first appearance in DOL data but plan started earlier. "
                       f"**Returning** = previously filed, skipped {_yoy_year - 1}, reappeared in {_yoy_year}.")
            _render_yoy_table(new_plans)

        if terminated:
            st.markdown(f"##### Confirmed Terminated ESOPs ({len(terminated)})")
            st.caption(f"These ESOPs have been confirmed as terminated — typically due to "
                       f"acquisition, merger, or plan wind-down. "
                       f"Verified via DOL EFAST2 and public records research as of Apr 16, 2026. "
                       f"Financial data shown is from their last ESOP filing ({_yoy_year - 1}).")
            _render_yoy_table(terminated)

        if late_filers:
            st.markdown(f"##### Late Filers ({len(late_filers)})")
            st.caption(f"No 2024 Form 5500 ESOP filing appears on the DOL EFAST2 system yet "
                       f"for these sponsors (as of Apr 16, 2026). Plans can file on extension up to "
                       f"9.5 months after their plan year ends, and DOL bulk data releases may lag "
                       f"further. Plans marked **Late Filer (Active ESOP)** have been confirmed "
                       f"as still employee-owned via public records. "
                       f"Financial data shown is from their last filing ({_yoy_year - 1}).")
            _render_yoy_table(late_filers)

        if not new_plans and not terminated and not late_filers:
            st.info("Year-over-year comparison requires filing-level data for two consecutive years. "
                   "Run the Form 5500 processor with `--import-to-db` to enable this analysis.")

        # Contributions vs Distributions chart (moved from Financial Analysis)
        st.markdown("---")
        st.markdown("##### Contributions vs Distributions Over Time")
        st.caption("Money flowing into ESOPs (employer contributions) vs money flowing "
                   "out (benefits paid to departing participants).")

        _fig_flow = charts.build_f5500_contributions_vs_distributions(f5500_summaries)
        st.plotly_chart(_fig_flow, use_container_width=True, config=charts.PLOTLY_CONFIG)

    # ────────────────────────────────────
    # PAGE: National Comparison
    # ────────────────────────────────────
    elif _selected_page == "\U0001f1fa\U0001f1f8 National Comparison":
        st.markdown(f"#### MA vs National ESOP Comparison ({latest_year})")

        # Determine national data source year
        _nat_data_year = config.NATIONAL_ESOP_DATA["as_of_year"]
        st.caption(f"_National ESOP statistics are from the most recent NCEO analysis "
                   f"({_nat_data_year} filings). MA data is from {latest_year} DOL Form 5500 filings._")

        fig_share = charts.build_f5500_ma_share_bars(f5500_summaries)
        st.plotly_chart(fig_share, use_container_width=True, config=charts.PLOTLY_CONFIG)

        latest = next((s for s in reversed(f5500_summaries) if s.get("ma_plan_count", 0) > 0), f5500_summaries[-1])
        ma_plans = latest.get("ma_plan_count", 0)
        us_plans = latest.get("us_total_esop_count", 0) or config.NATIONAL_ESOP_ESTIMATES["total_plans"]
        ma_workforce = config.MA_WORKFORCE_SIZE
        us_workforce = config.US_WORKFORCE_SIZE

        if ma_workforce and us_workforce and ma_plans and us_plans:
            st.markdown("---")
            st.markdown("##### ESOPs Per 100,000 Workers")
            ma_per_100k = ma_plans / ma_workforce * 100_000
            us_per_100k = us_plans / us_workforce * 100_000

            pc1, pc2, pc3 = st.columns(3)
            _render_metric(pc1, f"{ma_per_100k:.1f}", "MA ESOPs per 100K Workers",
                          f"DOL {latest_year} + BLS", ma=True)
            _render_metric(pc2, f"{us_per_100k:.1f}", "US ESOPs per 100K Workers",
                          f"NCEO {_nat_data_year} + BLS")
            ratio = ma_per_100k / us_per_100k if us_per_100k > 0 else 0
            _render_metric(pc3, f"{ratio:.2f}x", "MA vs National Rate",
                          f"{'Above' if ratio > 1 else 'Below'} national average")

        # Share cards
        st.markdown("---")
        st.markdown("##### MA Share of National Totals")
        sc1, sc2, sc3 = st.columns(3)
        us_est = config.NATIONAL_ESOP_ESTIMATES
        ma_plan_pct = ma_plans / us_plans * 100 if us_plans > 0 else 0
        _render_metric(sc1, f"{ma_plan_pct:.1f}%", "MA Share of US ESOP Plans",
                      f"{ma_plans:,} of {us_plans:,} (NCEO {_nat_data_year})", ma=True)
        ma_part = latest.get("ma_total_participants") or 0
        us_part = latest.get("us_total_participants") or us_est["total_participants"]
        ma_part_pct = ma_part / us_part * 100 if us_part > 0 else 0
        _render_metric(sc2, f"{ma_part_pct:.1f}%", "MA Share of US ESOP Participants",
                      f"{ma_part:,} of {us_part:,} (NCEO {_nat_data_year})", ma=True)
        ma_assets = latest.get("ma_total_assets", 0) or 0
        us_assets = latest.get("us_total_assets", 0) or us_est["total_assets"]
        if ma_assets > 0 and us_assets > 0:
            ma_assets_pct = ma_assets / us_assets * 100
            _render_metric(sc3, f"{ma_assets_pct:.1f}%", "MA Share of US ESOP Assets",
                          f"${ma_assets / 1e9:.1f}B of ${us_assets / 1e9:.1f}B (NCEO {_nat_data_year})", ma=True)
        else:
            _render_metric(sc3, "N/A", "MA Share of US ESOP Assets",
                          "Financial data requires Schedule H/I", ma=True)

    # ────────────────────────────────────
    # PAGE: Methodology
    # ────────────────────────────────────
    elif _selected_page == "\U0001f4d6 Methodology":
        st.markdown(config.FORM5500_METHODOLOGY)

else:
    # No Form 5500 data — show setup instructions
    st.markdown(
        '<div class="callout-eo">'
        '<h4>Form 5500 Data Not Yet Loaded</h4>'
        '<p>This dashboard analyzes DOL Form 5500 bulk filings to build a unique Massachusetts-specific '
        'ESOP dataset. Nobody else produces this data at the state level.</p>'
        '<p><b>To load the data:</b></p>'
        '<ol>'
        '<li>Open a terminal in the dashboard directory</li>'
        '<li>Run: <code>python3 process_form5500.py --import-to-db</code></li>'
        '<li>This downloads DOL bulk data (~2-5 GB) and processes it (15-45 min)</li>'
        '<li>Refresh this page when complete</li>'
        '</ol>'
        '<p>Alternatively, place pre-processed CSV files (<code>form5500_ma_esops.csv</code> and/or '
        '<code>form5500_annual_summary.csv</code>) in the dashboard directory and they will be '
        'auto-imported on next page load.</p>'
        '</div>',
        unsafe_allow_html=True,
    )

# ── Sidebar: About / Data Sources / Quick Stats (below navigation) ──

with st.sidebar:
    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "This dashboard analyzes **DOL Form 5500** bulk filings to provide "
        "the most detailed Massachusetts-specific ESOP dataset available."
    )
    st.markdown("---")
    st.markdown("### Data Sources")
    st.markdown(
        f"- [DOL Form 5500 Search]({config.DOL_FORM_5500_URL})\n"
        f"- [DOL Form 5500 Bulk Downloads]({config.DOL_FORM_5500_BULK})"
    )
    st.markdown("---")
    st.markdown("### Quick Stats")
    if f5500_summaries:
        _sidebar_latest = next((s for s in reversed(f5500_summaries) if s.get("ma_plan_count", 0) > 0), None)
        if _sidebar_latest:
            _sb_yr = _sidebar_latest["filing_year"]
            _sb_fin = form5500_analysis.get_financial_summary(
                _sb_yr, exclude_zombie=True)
            _sb_plans = _sb_fin.get("plans_total", 0) or 0
            _sb_part = _sb_fin.get("total_participants", 0) or 0
            _sb_assets = _sb_fin.get("total_assets", 0) or 0
            st.metric("Active MA ESOP Plans", f"{_sb_plans:,}")
            st.metric("Total Participants", f"{_sb_part:,}")
            if _sb_assets > 0:
                st.metric("Total Assets", f"${_sb_assets / 1e9:.1f}B" if _sb_assets >= 1e9 else f"${_sb_assets / 1e6:.0f}M")
            st.metric("Filing Year", str(_sb_yr))
    else:
        st.info("No data loaded yet")
