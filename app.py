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
import theme

# ── Page config ─────────────────────────────────

st.set_page_config(
    page_title=config.APP_TITLE,
    page_icon=config.APP_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ──────────────────────────────────

st.markdown(theme.FONT_LINKS, unsafe_allow_html=True)
st.markdown(theme.CSS, unsafe_allow_html=True)


# ── Helper: shareable data package ──────────────

@st.cache_data(show_spinner=False)
def _build_share_package(year: int) -> bytes:
    """ZIP of a form year's MA ESOP data, built in memory and cached.

    Cached on `year` so the archive is assembled once per session rather than on
    every rerun; it is rebuilt whenever the app restarts, which is also when the
    underlying database can have changed.
    """
    return utils.build_share_package(
        year=year,
        all_rows=form5500_analysis.get_ma_filings(year),
        active_rows=form5500_analysis.get_ma_filings(year, exclude_zombie=True),
        summaries=form5500_analysis.get_annual_summaries(),
        data_as_of=config.DATA_AS_OF,
    )


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

    _table_cls = "data-table numbered" if numbered else "data-table"
    html = f'<div class="data-table-wrapper" style="max-height:{height}px;">'
    html += f'<table class="{_table_cls}" id="{table_id}"><thead><tr>'
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
                    # Unreported employer-securities → NULL (renders "—", not "$0")
                    form5500_analysis.nullify_unreported_employer_securities()
                    form5500_analysis.recompute_annual_summaries()
                    st.rerun()

# Controls
f5500_c0, f5500_c2, f5500_c3 = st.columns([3, 3, 6])
with f5500_c0:
    # The dataset download leads, since sharing the data is the common errand.
    # Uses get_latest_year() rather than the `latest_year` computed further down
    # so the button can sit above the page's main content.
    _dl_year = form5500_analysis.get_latest_year()
    if _dl_year:
        st.download_button(
            f"⬇  Download {_dl_year} dataset (ZIP)",
            _build_share_package(_dl_year),
            f"MA_ESOP_Form5500_{_dl_year}.zip", "application/zip",
            help=(f"Everything for form year {_dl_year}: all filed plans and the "
                  "active subset as CSVs, the year-by-year summary, and a README "
                  "documenting sources, definitions and caveats."))
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
        "\U0001f3ed Industry",
        "\U0001f5fa\ufe0f Geography",
        "\U0001f195 2025 Filers",
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
        _render_metric(oc1, f"{_ov_plan_count:,}", "Active MA ESOP Plans", ma=True)
        _render_metric(oc2, f"{_ov_total_part:,}", "Total MA ESOP Participants", ma=True)
        if _ov_total_assets > 0:
            assets_label = f"${_ov_total_assets / 1e9:.1f}B" if _ov_total_assets >= 1e9 else f"${_ov_total_assets / 1e6:.0f}M"
        else:
            assets_label = "N/A"
        _render_metric(oc3, assets_label, "Total MA ESOP Assets", ma=True)

        # Row 2: Averages
        oc4, oc5, oc6 = st.columns(3)
        if _ov_avg_assets > 0:
            avg_label = f"${_ov_avg_assets / 1e6:.1f}M" if _ov_avg_assets >= 1e6 else f"${_ov_avg_assets:,.0f}"
        else:
            avg_label = "N/A"
        _render_metric(oc4, avg_label, "Average Plan Size (Assets)")
        _render_metric(oc5, f"{_ov_avg_part:,.0f}", "Avg Participants Per Plan")
        if _ov_total_assets > 0 and _ov_total_part > 0:
            _ov_avg_assets_per_part = _ov_total_assets / _ov_total_part
            avg_per_part_label = f"${_ov_avg_assets_per_part:,.0f}"
        else:
            avg_per_part_label = "N/A"
        _render_metric(oc6, avg_per_part_label, "Avg Assets Per Participant")

        # 2024 data disclaimer
        if latest_year == 2024:
            st.info(
                "**2024 Data Disclaimer:** The data shown reflects filings available through the "
                f"DOL EFAST2 bulk data releases and individual filing searches as of {config.DATA_AS_OF}. "
                "Some plans file on fiscal-year schedules or request extensions, so their "
                "2024 filings may not yet be published by DOL."
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
                              "Line 1c ÷ all active-plan assets (see below for "
                              "the ratio among only stock-reporting plans)", ma=True)
                # Count of active plans actually reporting employer securities
                # (distinct from the "Avg Assets Per Participant" card above, which
                # would otherwise be duplicated here).
                _stock_holders = sum(
                    1 for _f in form5500_analysis.get_ma_filings(
                        latest_year, exclude_zombie=True)
                    if float(_f.get("employer_securities") or 0) > 0)
                _render_metric(_sec_c3, f"{_stock_holders} of {_ov_plan_count}",
                              "Plans Reporting Company Stock",
                              "Schedule H, Part I Line 1c", ma=True)

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
            _total_filed = latest['ma_plan_count']
            _excluded_n = _total_filed - len(filings)
            st.caption(
                f"Showing {len(show_df)} of {len(filings)} active MA ESOP filings "
                f"for {latest_year} ({_excluded_n} plans with 0 active participants or "
                f"on the known wind-down list excluded from {_total_filed} total filed)")
            st.caption(
                "**Participants** is the plan's total at the *beginning* of the plan "
                "year (Form 5500 line 5); **Active Participants** is the count of "
                "currently employed participants at the *end* of the year (line 6a). "
                "DOL publishes no end-of-year total in this dataset, so the two "
                "figures sit on different dates — a plan that grew during the year can "
                "therefore show more active participants than the beginning-of-year "
                "total. Both are reproduced exactly as filed.")

        else:
            st.info("No filing-level data available. Run the Form 5500 processor with "
                   "`--import-to-db` to load individual filing records.")




        # ── Supplementary Analysis (moved from Additional Data Analysis tab) ──
        st.markdown("---")
        _ada = [r for r in form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)]
        if _ada:
            _adf = pd.DataFrame(_ada)
            for _c in ["total_assets", "total_participants", "active_participants",
                       "employer_securities", "employer_contributions",
                       "benefits_paid", "is_ksop"]:
                if _c in _adf.columns:
                    _adf[_c] = pd.to_numeric(_adf[_c], errors="coerce")

            # Per-plan assets per active participant, used by the largest-plans
            # table below. The typical-plan profile that also derived this moved
            # to the Trends page, so it is computed here independently.
            _appp = _adf[(_adf["active_participants"].fillna(0) > 0) &
                         (_adf["total_assets"].fillna(0) > 0)].copy()
            _appp["_per"] = _appp["total_assets"] / _appp["active_participants"]


            # ===== Largest plans (replaces Wealth Concentration) =====
            st.markdown("##### Largest MA ESOP Plans")
            st.caption("The biggest active MA ESOPs two ways: by total plan assets, "
                       "and by assets per active participant (a per-worker wealth "
                       "measure). Different plans top each list.")
            _lc1, _lc2 = st.columns(2)
            with _lc1:
                st.caption("**Top 5 by total assets**")
                _top_assets = _adf.sort_values("total_assets", ascending=False).head(5)[
                    ["sponsor_name", "total_assets"]].copy()
                _top_assets.columns = ["Company", "Total Assets"]
                _render_html_table(_top_assets, money_cols=["Total Assets"], height=240)
            with _lc2:
                st.caption("**Top 5 by assets per active participant**")
                _byper = _appp.sort_values("_per", ascending=False).head(5)[
                    ["sponsor_name", "_per", "active_participants"]].copy()
                _byper.columns = ["Company", "Assets / Active Participant", "Active Participants"]
                _render_html_table(_byper,
                                   money_cols=["Assets / Active Participant"],
                                   number_cols=["Active Participants"], height=240)
            st.caption("_Assets-per-participant uses each plan's total assets divided "
                       "by its active participants; smaller plans with concentrated "
                       "stock often rank highest._")


    # ────────────────────────────────────
    # PAGE: Trends (5-10 year time series)
    # ────────────────────────────────────
    elif _selected_page == "\U0001f4c8 Trends":
        st.markdown("#### MA ESOP Trends Over Time")
        st.caption("The most recent filing year may show incomplete data due to DOL filing lag. "
                   "Form 5500 filings are due 7 months after plan year end (extensions allow up to 9.5 months), "
                   "so some plans may not yet appear in the most recent year's data.")

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
        st.markdown("---")

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

        st.markdown("---")
        st.markdown("##### Contributions vs Distributions Over Time")
        st.caption("Money flowing into ESOPs (employer contributions) vs money flowing "
                   "out (benefits paid to departing participants).")

        _fig_flow = charts.build_f5500_contributions_vs_distributions(f5500_summaries)
        st.plotly_chart(_fig_flow, use_container_width=True, config=charts.PLOTLY_CONFIG)

        # ── Plan-profile and maturity views (moved here from the Overview) ──
        st.markdown("---")
        _ada = form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)
        if _ada:
            _adf = pd.DataFrame(_ada)
            for _c in ["total_assets", "total_participants", "active_participants",
                       "employer_securities", "employer_contributions",
                       "benefits_paid", "is_ksop"]:
                if _c in _adf.columns:
                    _adf[_c] = pd.to_numeric(_adf[_c], errors="coerce")
            # ===== Typical Plan Profile (median + mean) =====
            st.markdown("##### Typical Plan Profile")
            st.caption("The median describes the *typical* plan; the mean is shown "
                       "alongside it because a few very large ESOPs (Consigli, "
                       "Gillette, Abt) pull averages well above the typical plan. "
                       f"Based on {len(_adf)} active MA ESOPs in {latest_year}.")
            _m_eff = pd.to_datetime(_adf.get("plan_eff_date"), errors="coerce").dt.year
            _m_age = (latest_year - _m_eff).dropna()
            _ap = pd.to_numeric(_adf["active_participants"], errors="coerce")
            _ap_pos = _ap[_ap > 0]
            _appp = _adf[(_adf["active_participants"].fillna(0) > 0) &
                         (_adf["total_assets"].fillna(0) > 0)].copy()
            _appp["_per"] = _appp["total_assets"] / _appp["active_participants"]
            _agg_assets = _adf["total_assets"].fillna(0).sum()
            _agg_active = _adf["active_participants"].fillna(0).sum()
            _agg_per = (_agg_assets / _agg_active) if _agg_active else 0

            def _fmt_money(v):
                return f"${v/1e6:.1f}M" if v >= 1e6 else f"${v:,.0f}"

            _profile_rows = [
                {"Metric": "ESOP age (years since plan start)",
                 "Median": f"{_m_age.median():.0f} yrs",
                 "Mean": f"{_m_age.mean():.1f} yrs"},
                {"Metric": "Total participants (incl. retirees/separated)",
                 "Median": f"{_adf['total_participants'].median():,.0f}",
                 "Mean": f"{_adf['total_participants'].mean():,.0f}"},
                {"Metric": "Active participants (current employees)",
                 "Median": f"{_ap_pos.median():,.0f}",
                 "Mean": f"{_ap_pos.mean():,.0f}"},
                {"Metric": "Plan assets",
                 "Median": _fmt_money(_adf['total_assets'].median()),
                 "Mean": _fmt_money(_adf['total_assets'].mean())},
                {"Metric": "Assets per active participant (per plan)",
                 "Median": f"${_appp['_per'].median():,.0f}",
                 "Mean": f"${_appp['_per'].mean():,.0f}"},
            ]
            _render_html_table(pd.DataFrame(_profile_rows), height=260)
            _agg_assets_disp = _fmt_money(_agg_assets).replace("$", "\\$")
            _agg_per_disp = ("$" + format(_agg_per, ",.0f")).replace("$", "\\$")
            st.caption(
                f"_Aggregate assets per active participant (total plan assets "
                f"{_agg_assets_disp} / {_agg_active:,.0f} active "
                f"participants across all active MA ESOPs) = "
                f"**{_agg_per_disp}**  -  this weights "
                "every participant equally, unlike the per-plan median/mean above. "
                "'Active' participants are current employees still accruing shares; "
                "'total' also includes retirees and separated participants owed a "
                "balance._")

            st.markdown("---")

            # ===== ESOP maturity / formation cohorts =====
            st.markdown("##### ESOP Maturity  -  Formation Cohorts")
            st.caption("When today's active MA ESOPs first established their plans "
                       "(by ESOP plan effective date). Shows the age profile and "
                       "succession-pipeline maturity of the sector.")
            _adf["_eff_year"] = pd.to_datetime(
                _adf.get("plan_eff_date"), errors="coerce").dt.year
            _cohorts = [
                ("Before 1990", 0, 1989),
                ("1990-1999", 1990, 1999),
                ("2000-2009", 2000, 2009),
                ("2010-2019", 2010, 2019),
                ("2020-present", 2020, 9999),
            ]
            _crows = []
            for _lbl, _lo, _hi in _cohorts:
                _m = _adf[(_adf["_eff_year"] >= _lo) & (_adf["_eff_year"] <= _hi)]
                _crows.append({"Era": _lbl, "Plans": int(len(_m)),
                               "Participants": int(_m["total_participants"].fillna(0).sum()),
                               "Assets": float(_m["total_assets"].fillna(0).sum())})
            _unknown = int(_adf["_eff_year"].isna().sum())
            if _unknown:
                _crows.append({"Era": "Unknown", "Plans": _unknown, "Participants": 0, "Assets": 0.0})
            _cdf = pd.DataFrame(_crows)
            _cc1, _cc2 = st.columns([3, 2])
            with _cc1:
                _fig_c = go.Figure(go.Bar(
                    x=_cdf["Era"], y=_cdf["Plans"],
                    marker_color=config.CHART_COLORS["navy"],
                    text=_cdf["Plans"], textposition="outside"))
                _fig_c.update_layout(
                    height=config.CHART_HEIGHT_SM, margin=dict(t=10, b=10, l=10, r=10),
                    yaxis_title="Active plans", plot_bgcolor="white",
                    font=dict(family=config.CHART_FONT_FAMILY))
                st.plotly_chart(_fig_c, use_container_width=True, key="ov_cohort")
            with _cc2:
                _render_html_table(_cdf, money_cols=["Assets"],
                                   number_cols=["Plans", "Participants"], height=260)

            st.markdown("---")

            # ===== % of ESOPs started by decade =====
            st.markdown("##### % of ESOPs Started, by Decade")
            st.caption("Share of today's active MA ESOPs by the decade their plan "
                       "was established. (DOL placeholder dates of 1900-01-01 are "
                       "counted as 'Unknown').")

            def _decade_label(y):
                if pd.isna(y) or int(y) <= 1900:
                    return "Unknown"
                d = int(y) // 10 * 10
                return "Before 1970" if d < 1970 else f"{d}s"

            _adf["_dec"] = _adf["_eff_year"].apply(_decade_label)
            _dec_order = ["Before 1970", "1970s", "1980s", "1990s",
                          "2000s", "2010s", "2020s", "Unknown"]
            _dtot = len(_adf)
            _drows = []
            for _d in _dec_order:
                _n = int((_adf["_dec"] == _d).sum())
                if _n:
                    _drows.append({"Decade": _d, "Plans": _n,
                                   "% of Active ESOPs": _n / _dtot * 100})
            _ddf = pd.DataFrame(_drows)
            _dc1, _dc2 = st.columns([3, 2])
            with _dc1:
                _fig_d = go.Figure(go.Bar(
                    x=_ddf["Decade"], y=_ddf["% of Active ESOPs"],
                    marker_color=config.CHART_COLORS["navy"],
                    text=[f"{v:.1f}%" for v in _ddf["% of Active ESOPs"]],
                    textposition="outside"))
                _fig_d.update_layout(
                    height=config.CHART_HEIGHT_SM,
                    margin=dict(t=10, b=10, l=10, r=10),
                    yaxis_title="% of active ESOPs", plot_bgcolor="white",
                    font=dict(family=config.CHART_FONT_FAMILY))
                st.plotly_chart(_fig_d, use_container_width=True, key="ov_decade")
            with _dc2:
                _ddf_disp = _ddf.copy()
                if "% of Active ESOPs" in _ddf_disp.columns:
                    _ddf_disp["% of Active ESOPs"] = _ddf_disp["% of Active ESOPs"].apply(
                        lambda x: f"{x:.1f}%" if pd.notna(x) else "")
                _render_html_table(_ddf_disp, number_cols=["Plans"], height=320)

    # ────────────────────────────────────
    # PAGE: Industry
    # ────────────────────────────────────
    elif _selected_page == "🏭 Industry":
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
        st.markdown("---")
        _ada = form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)
        if _ada:
            _adf = pd.DataFrame(_ada)
            for _c in ["total_assets", "total_participants", "active_participants",
                       "employer_securities", "employer_contributions",
                       "benefits_paid", "is_ksop"]:
                if _c in _adf.columns:
                    _adf[_c] = pd.to_numeric(_adf[_c], errors="coerce")

            # ===== Average account balance by industry =====
            st.markdown("##### Average Account Balance by Industry")
            st.caption("Assets per participant by industry  -  a proxy for "
                       "per-worker wealth accumulation. Weighted (sector assets / "
                       "sector participants).")
            _ind = _adf[_adf["total_participants"].fillna(0) > 0].copy()
            _grp = _ind.groupby(_ind["industry_sector"].fillna("(Unclassified)")).agg(
                Plans=("ein", "count"),
                _a=("total_assets", "sum"),
                _p=("total_participants", "sum")).reset_index()
            _grp["Avg Account Balance"] = (_grp["_a"] / _grp["_p"]).round(0)
            _grp = _grp.rename(columns={"industry_sector": "Industry"})
            # Ordered by plan count so the rows line up with the sector chart above,
            # which is what a reader compares this against. Balance is broken out per
            # row, so it does not also need to drive the ordering.
            _grp = _grp[_grp["Plans"] >= 2].sort_values(
                ["Plans", "Avg Account Balance"], ascending=[False, False])
            _gdisp = _grp[["Industry", "Plans", "Avg Account Balance"]].copy()
            _render_html_table(_gdisp, money_cols=["Avg Account Balance"],
                               number_cols=["Plans"], height=440)
            st.caption("_Industries with at least 2 active plans shown._")

            st.markdown("---")

            # ===== Top industries (pie charts) =====
            st.markdown("##### Top Industries for MA ESOPs")
            st.caption("Industry mix of active MA ESOPs three ways  -  by number of "
                       "plans, by participants (people), and by assets (dollars). "
                       "Smaller sectors are grouped into 'Other' for legibility.")

            _adf["_one"] = 1

            def _industry_pie(metric_col, title, key, top_n=7):
                _g = _adf.copy()
                _g["industry_sector"] = _g["industry_sector"].fillna("(Unclassified)")
                _agg = _g.groupby("industry_sector")[metric_col].sum().sort_values(ascending=False)
                _agg = _agg[_agg > 0]
                if len(_agg) > top_n:
                    _top = _agg.head(top_n)
                    _labels = list(_top.index) + ["Other"]
                    _values = list(_top.values) + [float(_agg.iloc[top_n:].sum())]
                else:
                    _labels = list(_agg.index)
                    _values = [float(v) for v in _agg.values]
                _fig = go.Figure(go.Pie(
                    labels=_labels, values=_values, hole=0.45, sort=False,
                    marker=dict(colors=config.CHART_PALETTE,
                                line=dict(color="white", width=1.5)),
                    textinfo="percent", textfont_size=12,
                    hovertemplate="%{label}<br>%{value:,.0f} (%{percent})<extra></extra>"))
                _fig.update_layout(
                    title=dict(text=title, x=0.5, xanchor="center",
                               font=dict(size=14, family=config.CHART_FONT_FAMILY)),
                    height=config.CHART_HEIGHT_MD,
                    margin=dict(t=50, b=70, l=10, r=10),
                    showlegend=True,
                    legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center",
                                font=dict(size=10)),
                    font=dict(family=config.CHART_FONT_FAMILY))
                st.plotly_chart(_fig, use_container_width=True, key=key)

            _pc1, _pc2, _pc3 = st.columns(3)
            with _pc1:
                _industry_pie("_one", "By Number of Plans", "ov_pie_plans")
            with _pc2:
                _industry_pie("total_participants", "By Participants", "ov_pie_part")
            with _pc3:
                _industry_pie("total_assets", "By Assets ($)", "ov_pie_assets")

            st.caption("_Percentages are shares of the active-MA-ESOP total for each "
                       "measure. 'By Assets' is dominated by a few large plans; "
                       "'By Number of Plans' best reflects how common each "
                       "industry is._")

    # ────────────────────────────────────
    # PAGE: Geography
    # ────────────────────────────────────
    elif _selected_page == "\U0001f5fa\ufe0f Geography":
        st.markdown(f"#### Geographic Distribution of MA ESOPs ({latest_year})")
        st.caption(f"_Shows all filed {latest_year} plans by sponsor city (not the "
                   f"zombie-excluded 'active' subset on the Overview). Villages and "
                   f"neighborhoods are rolled into their parent municipality on the map "
                   f"(e.g. Hyannis \u2192 Barnstable, Allston \u2192 Boston)._")

        city_data = form5500_analysis.get_ma_filings_by_city(latest_year)
        if city_data:
            _city_df = pd.DataFrame(city_data).rename(columns={"sponsor_city": "municipality"})
            _esop_choropleth = map_utils.create_choropleth_map(
                _city_df, value_col="plan_count",
                title=f"MA ESOPs by Municipality ({latest_year})",
                color_scale=[[0, "#9DC3E6"], [0.25, "#5B9BD5"], [0.5, "#2E75B6"],
                             [0.75, "#1A5490"], [1, "#0A2E52"]],
                legend_title="ESOP Count",
                source="DOL Form 5500 Filings",
            )
            if _esop_choropleth is not None:
                st.plotly_chart(_esop_choropleth, use_container_width=True, config=charts.PLOTLY_CONFIG)
            else:
                fig_map = charts.build_f5500_city_map(city_data)
                st.plotly_chart(fig_map, use_container_width=True, config=charts.PLOTLY_CONFIG)
        else:
            st.info("No geographic data available. Run the Form 5500 processor to load filing data.")

    # ────────────────────────────────────
    # PAGE: 2025 Filers (early tracking — form year still open)
    # ────────────────────────────────────
    elif _selected_page == "\U0001f195 2025 Filers":
        st.markdown("#### 2025 Filers — Early Tracking")
        _f25 = form5500_analysis.get_ma_filings(2025)
        st.markdown(
            '<div class="callout-eo">'
            '<p><b>Form year 2025 is still open.</b> Most calendar-year 2025 plans '
            'file between July and October 2026, so this page shows <b>early filers '
            'only</b> — typically fiscal-year plans whose 2025 plan year has already '
            'ended, short plan years, and final (termination) filings. Counts and '
            'totals here will grow all year and are <b>not comparable</b> to the '
            'complete 2014&ndash;2024 years.</p>'
            '<p>These filings are kept out of the Overview, Trends, Industry and '
            'Geography pages until the filing year is substantially complete.</p>'
            '</div>',
            unsafe_allow_html=True,
        )
        if _f25:
            _prior_eins = form5500_analysis.get_prior_eins(2025)
            _n_new = sum(1 for _f in _f25 if _f.get("ein") not in _prior_eins)
            _tot_p = sum(_f.get("total_participants") or 0 for _f in _f25)
            # `is not None`, not truthiness: a plan that reported exactly $0 did
            # report. Four of the 2025 filings are final/wind-up returns showing
            # $0, and treating them as "not reported" understated the count.
            _assets_known = [_f["total_assets"] for _f in _f25
                             if _f.get("total_assets") is not None]

            def _fmt25(v):
                if v >= 1e9:
                    return f"${v / 1e9:.2f}B"
                if v >= 1e6:
                    return f"${v / 1e6:.1f}M"
                return f"${v:,.0f}"

            mcol1, mcol2, mcol3, mcol4 = st.columns(4)
            _render_metric(mcol1, f"{len(_f25):,}", "2025 Filings Received So Far")
            _render_metric(mcol2, f"{_n_new:,}", "First-Time Filers (new EIN)")
            _render_metric(mcol3, f"{_tot_p:,}", "Participants (filed so far)")
            _render_metric(mcol4,
                           _fmt25(sum(_assets_known)) if _assets_known else "N/A",
                           f"Assets ({len(_assets_known)} plans reporting)")

            f25_search = st.text_input("Search by plan name, sponsor, or city",
                                       key="f5500_2025_search")
            f25_df = pd.DataFrame(_f25)
            f25_df["new_to_dataset"] = f25_df["ein"].apply(
                lambda e: "Yes" if e not in _prior_eins else "No")
            if f25_search:
                _sl = f25_search.lower()
                _mask = f25_df.apply(lambda r: _sl in (
                    str(r.get("plan_name", "")) + str(r.get("sponsor_name", "")) +
                    str(r.get("sponsor_city", ""))
                ).lower(), axis=1)
                f25_df = f25_df[_mask]

            _f25_cols = {
                "sponsor_name": "Sponsor",
                "plan_name": "Plan Name",
                "sponsor_city": "City",
                "industry_sector": "Industry",
                "new_to_dataset": "New to Dataset?",
                "is_ksop": "KSOP?",
                "total_participants": "Participants",
                "active_participants": "Active Participants",
                "total_assets": "Total Assets",
                "employer_contributions": "Employer Contributions",
                "benefits_paid": "Benefits Paid",
            }
            _avail25 = [c for c in _f25_cols if c in f25_df.columns]
            _show25 = f25_df[_avail25].copy()
            _show25.columns = [_f25_cols[c] for c in _avail25]
            if "KSOP?" in _show25.columns:
                _show25["KSOP?"] = _show25["KSOP?"].apply(lambda x: "Yes" if x else "No")
            _render_html_table(_show25,
                               money_cols=["Total Assets", "Employer Contributions",
                                           "Benefits Paid"],
                               number_cols=["Participants", "Active Participants"],
                               height=500,
                               numbered=True,
                               sortable_cols=["Participants", "Active Participants",
                                              "Total Assets", "Employer Contributions",
                                              "Benefits Paid"],
                               show_totals=True)
            st.caption(
                f"Showing {len(_show25)} of {len(_f25)} MA ESOP filings received for "
                f"form year 2025. Financial fields appear once the plan's "
                f"Schedule H/I or 5500-SF data is published by DOL.")
            st.download_button("Download 2025 Early Filers as CSV",
                               utils.to_csv_bytes(_f25),
                               "ma_esops_form5500_2025_early.csv", "text/csv")
        else:
            st.info("No 2025 filings imported yet. Run "
                    "`python3 scan_dol_filers.py --year 2025 --import-new` "
                    "to scan DOL bulk data and import early 2025 filers.")


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
        '<li>Run <code>python3 scan_dol_filers.py --year 2024 --import-new</code> (repeat per filing year)</li>'
        '<li>Re-derive financials: <code>python3 refetch_dol_financials.py</code></li>'
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
