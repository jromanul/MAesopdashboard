"""
Massachusetts ESOP Dashboard — Streamlit application.

Analysis of U.S. Department of Labor Form 5500 filings by Massachusetts
employee stock ownership plans. Originally a duplicate of the Form 5500 ESOP
section of the MassCEO Intelligence Dashboard.
"""

import os
import re
import html as _html
import streamlit as st
import pandas as pd

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
    # "auto": open on desktop (where theme.CSS also pins it), collapsed on
    # phones so the page opens on its content rather than behind the menu.
    initial_sidebar_state="auto",
)

# ── Custom CSS ──────────────────────────────────

st.markdown(theme.FONT_LINKS, unsafe_allow_html=True)
st.markdown(theme.CSS, unsafe_allow_html=True)


# ── Helper: shareable data package ──────────────

@st.cache_data(show_spinner=False)
def _build_share_package(year: int, data_as_of: str) -> bytes:
    """ZIP of a form year's MA ESOP data, built in memory and cached.

    Cached on `year` and the data vintage so the archive is assembled once per
    session rather than on every rerun, and is rebuilt when the data refreshes.
    """
    return utils.build_share_package(
        year=year,
        all_rows=form5500_analysis.get_ma_filings(year),
        active_rows=form5500_analysis.get_ma_filings(year, exclude_zombie=True),
        summaries=form5500_analysis.get_annual_summaries(),
        data_as_of=data_as_of,
        dol_release=config.DOL_RELEASE_DATE,
        org_name=config.ORG_NAME,
        dashboard_url=config.DASHBOARD_URL,
    )


# ── Helpers: Streamlit version compatibility ────
# Streamlit Cloud installs the newest release on every rebuild. Newer releases
# deprecate `use_container_width` (for `width="stretch"`) and
# `st.components.v1.html` (for `st.iframe`); older ones lack the replacements.
# Pick whichever the installed version supports so a rebuild cannot break us.

import inspect as _inspect

_PLOTLY_PARAMS = set(_inspect.signature(st.plotly_chart).parameters)
_STRETCH = ({"width": "stretch"} if "width" in _PLOTLY_PARAMS
            else {"use_container_width": True})
_BUTTON_STRETCH = ({"width": "stretch"}
                   if "width" in _inspect.signature(st.download_button).parameters
                   else {"use_container_width": True})


def _chart(fig, key=None, alt=None):
    """st.plotly_chart at full width, with alt text where supported."""
    kw = dict(_STRETCH, config=charts.PLOTLY_CONFIG)
    if key:
        kw["key"] = key
    if alt and "alt" in _PLOTLY_PARAMS:
        kw["alt"] = alt
    st.plotly_chart(fig, **kw)


def _inject_script(html_doc: str):
    """Run a script that needs the parent document (st.iframe, else components)."""
    if hasattr(st, "iframe"):
        st.iframe(html_doc, height=1)
    else:
        import streamlit.components.v1 as components
        components.html(html_doc, height=0, scrolling=False)


# ── Helpers: formatting ─────────────────────────

def _money(v, digits: int = 1) -> str:
    """$3.77B / $31.9M / $504,065 — one money style across every page."""
    if v is None or pd.isna(v):
        return "N/A"
    a = abs(v)
    sign = "-" if v < 0 else ""
    if a >= 1e9:
        return f"{sign}${a / 1e9:.{digits}f}B"
    if a >= 1e6:
        return f"{sign}${a / 1e6:.{digits}f}M"
    return f"{sign}${a:,.0f}"


def _md_safe(text: str) -> str:
    """Escape '$' so Streamlit markdown does not read a pair of them as LaTeX."""
    return text.replace("$", "\\$")


# ── Helper: Render DataFrame as HTML table ──────

def _render_html_table(df, money_cols=None, number_cols=None, height=600,
                       numbered=False, sortable_cols=None, show_totals=False,
                       caption=None):
    """Render a DataFrame as a styled HTML table.

    - Money columns: show '$X' for values, '$0' for zero, '—' for NULL/missing.
    - Number columns: show 'X' for values, '0' for zero, '—' for NULL/missing.
    - Text columns: show value or '—' for empty/NULL.
    - numbered: if True, adds a '#' column on the left with row numbers (1, 2, 3...).
    - sortable_cols: list of column names that can be sorted by clicking the header.
    - show_totals: if True, adds a Total row at the bottom summing numeric columns.
    - caption: visually hidden table caption for screen readers.
    """
    import hashlib
    money_cols = money_cols or []
    number_cols = number_cols or []
    sortable_cols = sortable_cols or []

    table_id = "tbl_" + hashlib.md5(
        (str(df.columns.tolist()) + str(caption)).encode()).hexdigest()[:8]

    _table_cls = "data-table numbered" if numbered else "data-table"
    html = f'<div class="data-table-wrapper" style="max-height:{height}px;" tabindex="0">'
    html += f'<table class="{_table_cls}" id="{table_id}">'
    if caption:
        html += (f'<caption style="position:absolute;left:-9999px;">'
                 f'{_html.escape(caption)}</caption>')
    html += '<thead><tr>'
    if numbered:
        html += '<th scope="col" style="text-align:center;min-width:35px;">#</th>'
    for col_idx, col in enumerate(df.columns):
        align = ' style="text-align:right;"' if (col in money_cols or col in number_cols) else ""
        if col in sortable_cols:
            html += (f'<th scope="col" class="sortable" data-col="{col_idx}" '
                     f'title="Sort by {_html.escape(col)}"{align}>{col}</th>')
        else:
            html += f'<th scope="col"{align}>{col}</th>'
    html += '</tr></thead><tbody>'

    na_cell = '<td style="color:#55667A;text-align:center;">—</td>'

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
                else:
                    try:
                        sort_attr = f' data-val="{float(val)}"'
                    except (ValueError, TypeError):
                        sort_attr = ' data-val="-999999999999"'

            if col in money_cols:
                if is_missing:
                    html += f'<td style="color:#55667A;text-align:center;"{sort_attr}>—</td>'
                else:
                    try:
                        num = float(val)
                        if num == 0:
                            html += f'<td class="num" style="color:#55667A;"{sort_attr}>$0</td>'
                        elif num < 0:
                            html += f'<td class="num" style="color:#B03A2E;"{sort_attr}>-${abs(num):,.0f}</td>'
                        else:
                            html += f'<td class="num"{sort_attr}>${num:,.0f}</td>'
                    except (ValueError, TypeError):
                        html += f'<td{sort_attr}>{val}</td>'
            elif col in number_cols:
                if is_missing:
                    html += f'<td style="color:#55667A;text-align:center;"{sort_attr}>—</td>'
                else:
                    try:
                        num = float(val)
                        if num == 0:
                            html += f'<td class="num" style="color:#55667A;"{sort_attr}>0</td>'
                        else:
                            html += f'<td class="num"{sort_attr}>{num:,.0f}</td>'
                    except (ValueError, TypeError):
                        html += f'<td{sort_attr}>{val}</td>'
            else:
                if is_missing:
                    if sort_attr:
                        html += f'<td style="color:#55667A;text-align:center;"{sort_attr}>—</td>'
                    else:
                        html += na_cell
                else:
                    html += f'<td{sort_attr}>{_html.escape(str(val))}</td>'
        html += '</tr>'
    html += '</tbody>'

    # ── Totals footer row ──
    if show_totals:
        _tfoot_style = ('style="background-color:#0A2E52 !important;color:#FFFFFF !important;'
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
                cell_val = f'${avg:,.0f}' if col in money_cols else f'{avg:,.0f}'
                html += f'<td class="num" {_tfoot_style}>{cell_val}</td>'
                first_col = False
            elif col in money_cols or col in number_cols:
                total = pd.to_numeric(df[col], errors="coerce").sum()
                if col in money_cols:
                    cell_val = f'-${abs(total):,.0f}' if total < 0 else f'${total:,.0f}'
                else:
                    cell_val = f'{total:,.0f}'
                html += f'<td class="num" {_tfoot_style}>{cell_val}</td>'
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
    newTh.setAttribute('tabindex', '0');
    newTh.setAttribute('aria-sort', 'none');

    function sortBy() {{
      var colIdx = parseInt(newTh.getAttribute('data-col'));
      var tbody = table.querySelector('tbody');
      var rows = Array.from(tbody.querySelectorAll('tr'));
      var tdIndex = hasNumberCol ? colIdx + 1 : colIdx;

      // Determine sort direction (default first click = descending / highest first)
      var isDesc = newTh.classList.contains('sort-desc');
      var allHeaders = table.querySelectorAll('th.sortable');
      allHeaders.forEach(function(h) {{
        h.classList.remove('sort-asc', 'sort-desc');
        h.setAttribute('aria-sort', 'none');
      }});

      if (isDesc) {{
        newTh.classList.add('sort-asc');
        newTh.setAttribute('aria-sort', 'ascending');
      }} else {{
        newTh.classList.add('sort-desc');
        newTh.setAttribute('aria-sort', 'descending');
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
    }}
    newTh.addEventListener('click', sortBy);
    newTh.addEventListener('keydown', function(e) {{
      if (e.key === 'Enter' || e.key === ' ') {{ e.preventDefault(); sortBy(); }}
    }});
  }});
}})();
</script>"""
        _inject_script(sort_js)


# ── Helpers: page furniture ─────────────────────

def _render_metric(col, value, label, sub="", ma=False):
    card_class = "metric-card metric-card-ma" if ma else "metric-card"
    badge_html = '<span class="metric-badge">MA</span>' if ma else ""
    sub_html = f'<p class="metric-sub">{sub}</p>' if sub else ""
    with col:
        st.markdown(
            f'<div class="{card_class}">{badge_html}<h3>{value}</h3>'
            f'<p>{label}</p>{sub_html}</div>',
            unsafe_allow_html=True,
        )


def _page_head(kicker: str, title: str, lede: str):
    st.markdown(
        f'<div class="page-head"><div class="page-kicker">{kicker}</div>'
        f'<h2>{title}</h2><p>{lede}</p></div>',
        unsafe_allow_html=True,
    )


def _note(body_html: str, title: str = "", kind: str = "info"):
    cls = "callout-warning" if kind == "warning" else "callout-eo"
    title_html = f'<div class="callout-title">{title}</div>' if title else ""
    st.markdown(f'<div class="{cls}" role="note">{title_html}{body_html}</div>',
                unsafe_allow_html=True)


def _numeric_frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in ["total_assets", "total_participants", "active_participants",
              "employer_securities", "employer_contributions",
              "benefits_paid", "is_ksop"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _norm_name(s) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


# ── Masthead ────────────────────────────────────

st.markdown(
    f'<header class="gov-header" role="banner">'
    f'<div>'
    f'<div class="gov-eyebrow">{config.ORG_NAME}</div>'
    f'<h1>{config.APP_TITLE}</h1>'
    f'<p class="gov-lede">{config.APP_SUBTITLE}</p>'
    f'</div>'
    f'<dl>'
    f'<div><dt>Data current as of</dt><dd>{config.DATA_AS_OF}</dd></div>'
    f'<div><dt>DOL data release</dt><dd>{config.DOL_RELEASE_DATE}</dd></div>'
    f'</dl>'
    f'</header>',
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

f5500_summaries = form5500_analysis.get_annual_summaries()
_open_year = config.FORM5500_OPEN_FORM_YEAR

# ── Status row: dataset download + data vintage ──

_dl_year = form5500_analysis.get_latest_year()
f5500_c0, f5500_c2 = st.columns([3, 9])
with f5500_c0:
    # The dataset download leads, since sharing the data is the common errand.
    if _dl_year:
        st.download_button(
            f"Download {_dl_year} data (ZIP)",
            _build_share_package(_dl_year, config.DATA_AS_OF),
            f"MA_ESOP_Form5500_{_dl_year}.zip", "application/zip",
            **_BUTTON_STRETCH,
            help=(f"Everything for form year {_dl_year}: all filed plans and the "
                  "active subset as CSVs, the year-by-year summary, and a README "
                  "documenting sources, definitions and caveats."))
with f5500_c2:
    if _dl_year:
        st.markdown(
            f'<div class="data-status">Most recent complete form year: <b>{_dl_year}</b>. '
            f'Reconciled filing by filing against the U.S. Department of Labor EFAST2 '
            f'bulk release of <b>{config.DOL_RELEASE_DATE}</b> on '
            f'<b>{config.DATA_AS_OF}</b>. Form year {_open_year} is still being filed and '
            f'appears only on the <b>{_open_year} Filers</b> page.</div>',
            unsafe_allow_html=True)

# ── Main Content ────────────────────────────────

if f5500_summaries:
    _f5500_valid = [s for s in f5500_summaries if s.get("ma_plan_count", 0) > 0]
    if _f5500_valid:
        latest_year = _f5500_valid[-1]["filing_year"]
    else:
        latest_year = f5500_summaries[-1]["filing_year"]
    latest = next((s for s in reversed(f5500_summaries) if s.get("ma_plan_count", 0) > 0),
                  f5500_summaries[-1])
    _first_year = _f5500_valid[0]["filing_year"] if _f5500_valid else latest_year

    # ── Sidebar Navigation (placed first in sidebar) ──
    _PAGES = {
        "overview": ":material/dashboard: Overview",
        "trends": ":material/trending_up: Trends",
        "industry": ":material/factory: Industry",
        "geography": ":material/map: Geography",
        "open_year": f":material/pending_actions: {_open_year} Filers",
    }

    with st.sidebar:
        st.markdown(
            f'<div class="side-brand"><div class="side-brand__org">{config.ORG_NAME}</div>'
            f'<div class="side-brand__title">{config.APP_TITLE}</div></div>',
            unsafe_allow_html=True)
        _selected_key = st.radio(
            "Navigation",
            list(_PAGES),
            format_func=_PAGES.get,
            key="f5500_nav_page",
            label_visibility="collapsed",
        )

    # ────────────────────────────────────
    # PAGE: Overview
    # ────────────────────────────────────
    if _selected_key == "overview":
        _page_head(
            f"Form year {latest_year}",
            "Overview",
            f"How many Massachusetts companies sponsor an employee stock ownership plan, "
            f"how many people those plans cover, and what they hold. Headline figures "
            f"describe the <b>active</b> plans: those reporting at least one current "
            f"employee as a participant.")

        _ov_rows = form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)
        _ov_df = _numeric_frame(_ov_rows)
        _ov_fin = form5500_analysis.get_financial_summary(latest_year, exclude_zombie=True)
        _ov_plan_count = _ov_fin.get("plans_total", 0) or 0
        _ov_total_part = _ov_fin.get("total_participants", 0) or 0
        _ov_total_assets = _ov_fin.get("total_assets", 0) or 0
        _ov_avg_assets = _ov_fin.get("avg_assets_per_plan", 0) or 0
        _ov_avg_part = (_ov_total_part / _ov_plan_count) if _ov_plan_count > 0 else 0
        _ov_active_part = int(_ov_df["active_participants"].fillna(0).sum()) if len(_ov_df) else 0
        _ov_filed = latest["ma_plan_count"]

        # Row 1: Core counts
        oc1, oc2, oc3 = st.columns(3)
        _render_metric(oc1, f"{_ov_plan_count:,}", "Active MA ESOP plans",
                       f"of {_ov_filed:,} plans that filed for {latest_year}", ma=True)
        _render_metric(oc2, f"{_ov_total_part:,}", "Participants in active plans",
                       f"{_ov_active_part:,} are current employees", ma=True)
        _render_metric(oc3, _money(_ov_total_assets, 2) if _ov_total_assets > 0 else "N/A",
                       "Total plan assets", "End-of-plan-year value as filed", ma=True)

        # Row 2: Averages, each with the median beside it
        oc4, oc5, oc6 = st.columns(3)
        _med_assets = _ov_df["total_assets"].median() if len(_ov_df) else 0
        _med_part = _ov_df["total_participants"].median() if len(_ov_df) else 0
        _render_metric(oc4, _money(_ov_avg_assets) if _ov_avg_assets > 0 else "N/A",
                       "Average plan size (assets)", f"Median plan: {_money(_med_assets)}")
        _render_metric(oc5, f"{_ov_avg_part:,.0f}", "Average participants per plan",
                       f"Median plan: {_med_part:,.0f}")
        if _ov_total_assets > 0 and _ov_total_part > 0:
            _per_part = f"${_ov_total_assets / _ov_total_part:,.0f}"
            _per_act = (f"${_ov_total_assets / _ov_active_part:,.0f} per current employee"
                        if _ov_active_part else "")
        else:
            _per_part, _per_act = "N/A", ""
        _render_metric(oc6, _per_part, "Assets per participant", _per_act)

        _note(
            f"<p>Figures include every {latest_year} return DOL had published in its "
            f"{config.DOL_RELEASE_DATE} release. Plans on fiscal years or extensions can "
            f"file up to 9½ months after their plan year ends, so a few {latest_year} "
            f"returns may still arrive. Each refresh re-checks every stored figure "
            f"against DOL's latest data.</p>",
            title=f"About the {latest_year} figures")

        # ESOP-specific: company stock held in the trust
        _es = float(_ov_df["employer_securities"].fillna(0).sum()) if len(_ov_df) else 0
        if _es > 0:
            st.markdown("##### Company Stock Held by Active Plans")
            _sec_c1, _sec_c2, _sec_c3 = st.columns(3)
            _render_metric(_sec_c1, _money(_es, 2), "Employer securities (company stock)",
                           "Schedule H line 1d(1) or Schedule I line 3d", ma=True)
            _es_pct = _es / _ov_total_assets * 100 if _ov_total_assets > 0 else 0
            _render_metric(_sec_c2, f"{_es_pct:.0f}%", "Share of total plan assets",
                           "Company stock ÷ assets of all active plans", ma=True)
            _stock_holders = int((_ov_df["employer_securities"].fillna(0) > 0).sum())
            _render_metric(_sec_c3, f"{_stock_holders} of {_ov_plan_count}",
                           "Plans reporting company stock",
                           "The rest report $0 or leave the line blank", ma=True)

        # ── Full Data Table (active MA ESOP filings) ──
        st.markdown("---")
        st.markdown(f"#### Active MA ESOP Plans, {latest_year}")

        f5500_search = st.text_input("Search by plan name, sponsor, or city", key="f5500_search",
                                     placeholder="e.g. Worcester, construction, Tighe")

        filings = _ov_rows
        if filings:
            f_df = pd.DataFrame(filings)
            if f5500_search:
                sl = f5500_search.lower()
                mask = f_df.apply(lambda r: sl in (
                    str(r.get("plan_name", "")) + " " + str(r.get("sponsor_name", "")) + " " +
                    str(r.get("sponsor_city", "")) + " " + str(r.get("industry_sector", ""))
                ).lower(), axis=1)
                f_df = f_df[mask]

            display_cols = {
                "sponsor_name": "Sponsor",
                "plan_name": "Plan Name",
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
                    lambda x: str(x)[:4] if pd.notna(x) and x and str(x)[:4] != "1900" else "")
            if "KSOP?" in show_df.columns:
                show_df["KSOP?"] = show_df["KSOP?"].apply(lambda x: "Yes" if x else "No")

            # Compute Assets per Participant column
            if "Total Assets" in show_df.columns and "Participants" in show_df.columns:
                def _calc_app(row):
                    try:
                        a = float(row.get("Total Assets"))
                        p = float(row.get("Participants"))
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
                               show_totals=True,
                               caption=f"Active Massachusetts ESOP plans, form year {latest_year}")
            _excluded_n = _ov_filed - len(filings)
            st.caption(
                f"Showing {len(show_df)} of {len(filings)} active plans. "
                f"{_excluded_n} of the {_ov_filed} plans that filed for {latest_year} are "
                f"excluded as inactive: they report no active participants (most are "
                f"winding down) or are on a short list of known wind-down plans. "
                f"Click a numeric column heading to sort; the table scrolls sideways.")
            st.caption(
                "**Participants** is the plan's total at the *beginning* of the plan "
                "year (Form 5500 line 5) and includes retirees and former employees still "
                "owed a balance; **Active Participants** counts current employees at the "
                "*end* of the year (line 6a). The two sit on different dates, so a plan "
                "that grew during the year can show more active participants than total. "
                "Dollar figures are end-of-year values as filed; — means the plan did "
                "not report the item.")
        else:
            st.info("No filing-level data available.")

        # ── Largest plans ──
        if len(_ov_df):
            st.markdown("---")
            _appp = _ov_df[(_ov_df["active_participants"].fillna(0) > 0) &
                           (_ov_df["total_assets"].fillna(0) > 0)].copy()
            _appp["_per"] = _appp["total_assets"] / _appp["active_participants"]

            st.markdown("##### Largest MA ESOP Plans")
            st.caption("The biggest active MA ESOPs two ways: by total plan assets, "
                       "and by assets per active participant (a per-worker wealth "
                       "measure). Different plans top each list.")
            _lc1, _lc2 = st.columns(2)
            with _lc1:
                st.markdown("**Top 5 by total assets**")
                _top_assets = _ov_df.sort_values("total_assets", ascending=False).head(5)[
                    ["sponsor_name", "sponsor_city", "total_assets"]].copy()
                _top_assets.columns = ["Company", "City", "Total Assets"]
                _render_html_table(_top_assets, money_cols=["Total Assets"], height=260,
                                   caption="Top 5 plans by total assets")
            with _lc2:
                st.markdown("**Top 5 by assets per active participant**")
                _byper = _appp.sort_values("_per", ascending=False).head(5)[
                    ["sponsor_name", "_per", "active_participants"]].copy()
                _byper.columns = ["Company", "Assets / Active", "Active"]
                _render_html_table(_byper, money_cols=["Assets / Active"],
                                   number_cols=["Active"], height=260,
                                   caption="Top 5 plans by assets per active participant")
            st.caption("Assets per active participant divides each plan's total assets by "
                       "its active participants. Plans whose balances belong mostly to "
                       "former employees, or that hold concentrated stock, rank highest.")

    # ────────────────────────────────────
    # PAGE: Trends
    # ────────────────────────────────────
    elif _selected_key == "trends":
        _page_head(
            f"Form years {_first_year}–{latest_year}",
            "Trends",
            f"How Massachusetts ESOPs have changed over {latest_year - _first_year + 1} "
            f"years of filings, followed by a profile of today's plans. Time series count "
            f"<b>every plan that filed</b> each year, so every year is counted the same "
            f"way; the {latest_year} distributions and profiles describe the "
            f"<b>active</b> plans shown on the Overview.")

        _note(
            f"<p>The most recent year can read low until late filers arrive: returns are "
            f"due 7 months after a plan year ends (9½ with an extension), and fiscal-year "
            f"plans end months after December. A drop in {latest_year} is largely plans "
            f"leaving the dataset or not yet filed, not shrinkage at continuing plans.</p>",
            title="Reading the latest year")

        st.markdown(f"#### Plan Size Distribution, {latest_year}")
        dcol1, dcol2 = st.columns(2)
        with dcol1:
            asset_vals = form5500_analysis.get_asset_distribution(
                latest_year, exclude_zombie=True)
            _chart(charts.build_f5500_asset_histogram(asset_vals),
                   alt=f"Bar chart: number of active {latest_year} plans in each total-assets range.")
        with dcol2:
            partcp_vals = form5500_analysis.get_participant_distribution(
                latest_year, exclude_zombie=True)
            _chart(charts.build_f5500_participant_histogram(partcp_vals),
                   alt=f"Bar chart: number of active {latest_year} plans in each participant-count range.")

        st.markdown("---")
        st.markdown(f"#### Change Over Time, {_first_year}–{latest_year}")
        _chart(charts.build_f5500_plan_count_trend(f5500_summaries),
               alt="Line chart: Massachusetts ESOP plans filed each form year, "
                   + ", ".join(f"{s['filing_year']}: {s['ma_plan_count']}" for s in _f5500_valid) + ".")

        tcol1, tcol2 = st.columns(2)
        with tcol1:
            _chart(charts.build_f5500_participants_trend(f5500_summaries),
                   alt="Line chart: total and active participants in Massachusetts ESOPs by form year.")
        with tcol2:
            _chart(charts.build_f5500_assets_trend(f5500_summaries),
                   alt="Area chart: total assets held by Massachusetts ESOPs by form year.")

        tcol3, tcol4 = st.columns(2)
        with tcol3:
            _chart(charts.build_f5500_avg_plan_assets_trend(f5500_summaries),
                   alt="Line chart: average assets per filed plan by form year.")
        with tcol4:
            _chart(charts.build_f5500_contributions_bar(f5500_summaries),
                   alt="Bar chart: employer contributions to Massachusetts ESOPs by form year.")

        st.markdown("---")
        st.markdown("##### Contributions vs. Distributions Over Time")
        st.caption("Money flowing into ESOPs (employer contributions) against money "
                   "flowing out (benefits paid to departing and retiring participants). "
                   "Large payouts usually mark a sale of the company or a plan winding down.")
        _chart(charts.build_f5500_contributions_vs_distributions(f5500_summaries),
               alt="Grouped bar chart: employer contributions into and benefits paid out of "
                   "Massachusetts ESOPs by form year.")

        # ── Plan-profile and maturity views ──
        st.markdown("---")
        _ada = form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)
        if _ada:
            _adf = _numeric_frame(_ada)
            # ===== Typical Plan Profile (median + mean) =====
            st.markdown("##### Typical Plan Profile")
            _biggest = ", ".join(
                _adf.sort_values("total_assets", ascending=False)["sponsor_name"].head(3)
                .str.replace(r",? (Inc|LLC|Corp)\.?$", "", regex=True))
            st.caption("The median describes the *typical* plan; the mean is shown "
                       "alongside it because a few very large ESOPs "
                       f"({_md_safe(_biggest)}) pull averages well above the typical plan. "
                       f"Based on {len(_adf)} active MA ESOPs in {latest_year}.")
            _m_eff = pd.to_datetime(_adf.get("plan_eff_date"), errors="coerce").dt.year
            _m_eff = _m_eff.where(_m_eff > 1900)
            _m_age = (latest_year - _m_eff).dropna()
            _ap = pd.to_numeric(_adf["active_participants"], errors="coerce")
            _ap_pos = _ap[_ap > 0]
            _appp = _adf[(_adf["active_participants"].fillna(0) > 0) &
                         (_adf["total_assets"].fillna(0) > 0)].copy()
            _appp["_per"] = _appp["total_assets"] / _appp["active_participants"]
            _agg_assets = _adf["total_assets"].fillna(0).sum()
            _agg_active = _adf["active_participants"].fillna(0).sum()
            _agg_per = (_agg_assets / _agg_active) if _agg_active else 0

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
                 "Median": _money(_adf['total_assets'].median()),
                 "Mean": _money(_adf['total_assets'].mean())},
                {"Metric": "Assets per active participant (per plan)",
                 "Median": f"${_appp['_per'].median():,.0f}",
                 "Mean": f"${_appp['_per'].mean():,.0f}"},
            ]
            _render_html_table(pd.DataFrame(_profile_rows), height=280,
                               caption="Typical plan profile, median and mean")
            st.caption(_md_safe(
                f"Aggregate assets per active participant: {_money(_agg_assets, 2)} in "
                f"total plan assets ÷ {_agg_active:,.0f} active participants = "
                f"${_agg_per:,.0f}. This weights every participant equally, unlike the "
                "per-plan median and mean above. 'Active' participants are current "
                "employees; 'total' also includes retirees and separated participants "
                "owed a balance."))

            st.markdown("---")

            # ===== ESOP maturity / formation cohorts =====
            st.markdown("##### ESOP Maturity: Formation Cohorts")
            st.caption("When today's active MA ESOPs first established their plans "
                       "(by ESOP plan effective date), showing the age profile and "
                       "succession-pipeline maturity of the sector.")
            _adf["_eff_year"] = _m_eff
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
                _u = _adf[_adf["_eff_year"].isna()]
                _crows.append({"Era": "Unknown", "Plans": _unknown,
                               "Participants": int(_u["total_participants"].fillna(0).sum()),
                               "Assets": float(_u["total_assets"].fillna(0).sum())})
            _cdf = pd.DataFrame(_crows)
            _cc1, _cc2 = st.columns([3, 2])
            with _cc1:
                _chart(
                    charts.build_f5500_category_bar(
                        list(_cdf["Era"]), list(_cdf["Plans"]),
                        title="Active Plans by Era Established", y_title="Active plans"),
                    key="ov_cohort",
                    alt="Bar chart: active plans by era established, "
                        + ", ".join(f"{e}: {n}" for e, n in zip(_cdf["Era"], _cdf["Plans"])) + ".")
            with _cc2:
                _cdisp = _cdf.copy()
                _cdisp["Assets"] = _cdisp["Assets"].apply(lambda v: _money(v))
                _render_html_table(_cdisp, number_cols=["Plans", "Participants"], height=320,
                                   show_totals=False,
                                   caption="Active plans, participants and assets by era established")
                st.caption(_md_safe(
                    f"Totals: {int(_cdf['Plans'].sum())} plans, "
                    f"{int(_cdf['Participants'].sum()):,} participants, "
                    f"{_money(_cdf['Assets'].sum(), 2)} in assets."))

            st.markdown("---")

            # ===== % of ESOPs started by decade =====
            st.markdown("##### Share of Active ESOPs by Decade Established")
            st.caption("Share of today's active MA ESOPs by the decade their plan "
                       "was established. DOL placeholder dates of 1900-01-01 count as "
                       "'Unknown'. Shares are rounded, so they may not sum to exactly 100%.")

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
                _chart(
                    charts.build_f5500_category_bar(
                        list(_ddf["Decade"]), list(_ddf["% of Active ESOPs"]),
                        title="Share of Active Plans by Decade Established",
                        y_title="% of active ESOPs",
                        text=[f"{v:.1f}%" for v in _ddf["% of Active ESOPs"]]),
                    key="ov_decade",
                    alt="Bar chart: share of active plans by decade established, "
                        + ", ".join(f"{d}: {v:.1f}%" for d, v in
                                    zip(_ddf["Decade"], _ddf["% of Active ESOPs"])) + ".")
            with _dc2:
                _ddf_disp = _ddf.copy()
                _ddf_disp["% of Active ESOPs"] = _ddf_disp["% of Active ESOPs"].apply(
                    lambda x: f"{x:.1f}%" if pd.notna(x) else "")
                _render_html_table(_ddf_disp, number_cols=["Plans"], height=340,
                                   caption="Share of active plans by decade established")

    # ────────────────────────────────────
    # PAGE: Industry
    # ────────────────────────────────────
    elif _selected_key == "industry":
        _page_head(
            f"Form year {latest_year}",
            "Industry",
            "Which industries Massachusetts ESOPs operate in, classified by each "
            "sponsor's NAICS business code (verified against company websites). "
            f"All figures describe the {latest_year} <b>active</b> plans.")

        industry_data = form5500_analysis.get_ma_filings_by_industry(
            latest_year, exclude_zombie=True)
        if industry_data:
            _chart(charts.build_f5500_industry_bar(industry_data),
                   alt="Horizontal bar chart: active plans by industry sector, "
                       + ", ".join(f"{d['industry_sector']}: {d['plan_count']}"
                                   for d in industry_data) + ".")
        else:
            st.info("Industry classification is not available for this year.")
        st.markdown("---")
        _ada = form5500_analysis.get_ma_filings(latest_year, exclude_zombie=True)
        if _ada:
            _adf = _numeric_frame(_ada)

            # ===== Average account balance by industry =====
            st.markdown("##### Average Account Balance by Industry")
            st.caption("Assets per participant by industry, a proxy for per-worker "
                       "wealth accumulation. Weighted: sector assets ÷ sector participants.")
            _ind = _adf[_adf["total_participants"].fillna(0) > 0].copy()
            _grp = _ind.groupby(_ind["industry_sector"].fillna("(Unclassified)")).agg(
                Plans=("ein", "count"),
                _a=("total_assets", "sum"),
                _p=("total_participants", "sum")).reset_index()
            _grp["Avg Account Balance"] = (_grp["_a"] / _grp["_p"]).round(0)
            _grp = _grp.rename(columns={"industry_sector": "Industry",
                                        "_p": "Participants", "_a": "Total Assets"})
            # Ordered by plan count so the rows line up with the sector chart above,
            # which is what a reader compares this against. Balance is broken out per
            # row, so it does not also need to drive the ordering.
            _grp = _grp[_grp["Plans"] >= 2].sort_values(
                ["Plans", "Avg Account Balance"], ascending=[False, False])
            _gdisp = _grp[["Industry", "Plans", "Participants", "Total Assets",
                           "Avg Account Balance"]].copy()
            _render_html_table(_gdisp, money_cols=["Total Assets", "Avg Account Balance"],
                               number_cols=["Plans", "Participants"], height=460,
                               caption="Average account balance by industry")
            _single = int((_ind.groupby(_ind["industry_sector"].fillna("(Unclassified)"))
                           .size() < 2).sum())
            st.caption(f"Industries with at least 2 active plans are shown. {_single} "
                       "sectors with a single plan are left out, because one plan's "
                       "balance is not a sector average.")

            st.markdown("---")

            # ===== Top industries (share charts) =====
            st.markdown("##### Industry Mix of MA ESOPs")
            st.caption("Industry mix of active MA ESOPs three ways: by number of plans, "
                       "by participants (people), and by assets (dollars). Each sector keeps "
                       "the same colour in every chart; smaller sectors are grouped into "
                       "'Other' for legibility.")

            _adf["_one"] = 1

            def _industry_share(metric_col, title, key, fmt, top_n=7):
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
                _tot = sum(_values) or 1
                _chart(charts.build_f5500_industry_share(_labels, _values, title, fmt), key=key,
                       alt=f"Donut chart, industry mix {title.lower()}: "
                           + ", ".join(f"{l} {v / _tot:.0%}" for l, v in zip(_labels, _values)) + ".")

            _pc1, _pc2, _pc3 = st.columns(3)
            with _pc1:
                _industry_share("_one", "By Number of Plans", "ov_pie_plans", ",d")
            with _pc2:
                _industry_share("total_participants", "By Participants", "ov_pie_part", ",.0f")
            with _pc3:
                _industry_share("total_assets", "By Assets", "ov_pie_assets", "$,.0f")

            st.caption("Percentages are shares of the active-plan total for each measure. "
                       "'By Assets' is dominated by a few large plans; 'By Number of "
                       "Plans' best reflects how common each industry is.")

    # ────────────────────────────────────
    # PAGE: Geography
    # ────────────────────────────────────
    elif _selected_key == "geography":
        _page_head(
            f"Form year {latest_year}",
            "Geography",
            "Where Massachusetts ESOP sponsors are based, by the city on each plan's "
            "Form 5500 mailing address. Villages and neighborhoods are rolled into their "
            "municipality (e.g. Hyannis → Barnstable, Allston → Boston).")

        _basis = st.radio(
            "Plans shown",
            ["All plans that filed", "Active plans only"],
            horizontal=True, key="geo_basis",
            help=("'All plans that filed' includes plans winding down; 'Active plans "
                  "only' matches the Overview's headline count."))
        _active_only = _basis == "Active plans only"
        city_data = form5500_analysis.get_ma_filings_by_city(
            latest_year, exclude_zombie=_active_only)
        if city_data:
            _city_df = pd.DataFrame(city_data).rename(columns={"sponsor_city": "municipality"})
            _city_df["_town"] = _city_df["municipality"].apply(map_utils._normalize_town_name)
            _by_town = (_city_df.groupby("_town")[["plan_count", "total_partcp", "total_assets"]]
                        .sum().sort_values("plan_count", ascending=False))
            _n_plans = int(_by_town["plan_count"].sum())
            _top_towns = ", ".join(f"{t.title()} ({int(n)})" for t, n in
                                   _by_town["plan_count"].head(4).items())
            gm1, gm2, gm3 = st.columns(3)
            _render_metric(gm1, f"{_n_plans:,}",
                           "Active plans" if _active_only else "Plans that filed",
                           f"Form year {latest_year}", ma=True)
            _render_metric(gm2, f"{len(_by_town):,}", "Municipalities with an ESOP sponsor",
                           "Of Massachusetts' 351 cities and towns", ma=True)
            _render_metric(gm3, _top_towns.split(" (")[0] if _top_towns else "N/A",
                           "Most ESOP sponsors", _top_towns, ma=True)

            _esop_choropleth = map_utils.create_choropleth_map(
                _city_df.drop(columns=["_town"]), value_col="plan_count",
                title=f"MA ESOPs by Municipality, {latest_year}",
                color_scale=[[0, "#9DC3E6"], [0.25, "#5B9BD5"], [0.5, "#2E75B6"],
                             [0.75, "#1A5490"], [1, "#0A2E52"]],
                legend_title="Plans",
                source="U.S. DOL Form 5500 (sponsor mailing address); MassGIS town boundaries",
                hover_fields=[("Plans", "plan_count", ",d"),
                              ("Participants", "total_partcp", ",.0f"),
                              ("Plan assets", "total_assets", "$,.0f")],
            )
            if _esop_choropleth is not None:
                _chart(_esop_choropleth,
                       alt=f"Map of Massachusetts shading each municipality by its number of "
                           f"ESOP sponsors. Most: {_top_towns}.")
            else:
                _chart(charts.build_f5500_city_map(city_data))
            st.caption("Hover over a municipality for its plans, participants and assets. "
                       "Location is the sponsor's mailing address on the return, which for "
                       "a few companies is a headquarters or administrator rather than "
                       "where most employees work.")
        else:
            st.info("No geographic data available.")

    # ────────────────────────────────────
    # PAGE: Open form year (still being filed)
    # ────────────────────────────────────
    elif _selected_key == "open_year":
        _yr = _open_year
        _page_head(
            f"Form year {_yr} · in progress",
            f"{_yr} Filers",
            f"Massachusetts ESOP returns for form year {_yr} that DOL has published so far. "
            f"The year is still being filed, so these figures will grow and are "
            f"<b>not comparable</b> to the complete {_first_year}–{latest_year} years.")
        _f25 = form5500_analysis.get_ma_filings(_yr)
        _prev = form5500_analysis.get_ma_filings(_yr - 1)
        _prior_eins = {str(e).lstrip("0") for e in form5500_analysis.get_prior_eins(_yr)}
        _prior_names = {_norm_name(n) for n in form5500_analysis.get_prior_sponsor_names(_yr)}

        _eins_now = {str(f.get("ein") or "").lstrip("0") for f in _f25}
        _names_now = {_norm_name(f.get("sponsor_name")) for f in _f25}
        _continuing = sum(1 for f in _prev
                          if str(f.get("ein") or "").lstrip("0") in _eins_now
                          or _norm_name(f.get("sponsor_name")) in _names_now)
        _cov = _continuing / len(_prev) * 100 if _prev else 0

        _note(
            f"<p>Calendar-year {_yr} plans were due by July 31, {_yr + 1}, or "
            f"October 15, {_yr + 1} with an extension; fiscal-year plans file later still. "
            f"DOL's bulk files trail receipt by several weeks, so its {config.DOL_RELEASE_DATE} "
            f"release holds only part of the year. So far <b>{_continuing} of the "
            f"{len(_prev)}</b> plans that filed for {_yr - 1} have a {_yr} return on file "
            f"({_cov:.0f}%).</p>"
            f'<div class="progress-track" role="img" aria-label="{_cov:.0f}% of {_yr - 1} '
            f'filers have filed for {_yr}"><div class="progress-fill" '
            f'style="width:{min(_cov, 100):.0f}%;"></div></div>'
            f"<p>These filings stay off the Overview, Trends, Industry and Geography pages "
            f"until the year is substantially complete.</p>",
            title=f"Form year {_yr} is still open")

        if _f25:
            def _is_new(f):
                return (str(f.get("ein") or "").lstrip("0") not in _prior_eins
                        and _norm_name(f.get("sponsor_name")) not in _prior_names)
            _n_new = sum(1 for _f in _f25 if _is_new(_f))
            _tot_p = sum(_f.get("total_participants") or 0 for _f in _f25)
            # `is not None`, not truthiness: a plan that reported exactly $0 did
            # report. Final/wind-up returns show $0, and treating them as "not
            # reported" understated the count.
            _assets_known = [_f["total_assets"] for _f in _f25
                             if _f.get("total_assets") is not None]

            mcol1, mcol2, mcol3, mcol4 = st.columns(4)
            _render_metric(mcol1, f"{len(_f25):,}", f"{_yr} returns received so far",
                           f"{_continuing} continue a {_yr - 1} plan", ma=True)
            _render_metric(mcol2, f"{_n_new:,}", "Sponsors new to the dataset",
                           "No earlier ESOP return under their EIN or name", ma=True)
            _render_metric(mcol3, f"{_tot_p:,}", "Participants (returns so far)",
                           "Beginning-of-year totals", ma=True)
            _render_metric(mcol4,
                           _money(sum(_assets_known), 2) if _assets_known else "N/A",
                           "Plan assets (returns so far)",
                           f"{len(_assets_known)} of {len(_f25)} returns report assets", ma=True)

            st.markdown(f"#### {_yr} Returns Received")
            f25_search = st.text_input("Search by plan name, sponsor, or city",
                                       key="f5500_2025_search",
                                       placeholder="e.g. Boston, Consigli")
            f25_df = pd.DataFrame(_f25)
            f25_df["new_to_dataset"] = [("Yes" if _is_new(f) else "No") for f in _f25]
            if f25_search:
                _sl = f25_search.lower()
                _mask = f25_df.apply(lambda r: _sl in (
                    str(r.get("plan_name", "")) + " " + str(r.get("sponsor_name", "")) + " " +
                    str(r.get("sponsor_city", ""))
                ).lower(), axis=1)
                f25_df = f25_df[_mask]

            _f25_cols = {
                "sponsor_name": "Sponsor",
                "plan_name": "Plan Name",
                "sponsor_city": "City",
                "industry_sector": "Industry",
                "new_to_dataset": "New Sponsor?",
                "is_ksop": "KSOP?",
                "total_participants": "Participants",
                "active_participants": "Active Participants",
                "total_assets": "Total Assets",
                "employer_securities": "Employer Securities",
                "employer_contributions": "Employer Contributions",
                "benefits_paid": "Benefits Paid",
            }
            _avail25 = [c for c in _f25_cols if c in f25_df.columns]
            _show25 = f25_df[_avail25].copy()
            _show25.columns = [_f25_cols[c] for c in _avail25]
            if "KSOP?" in _show25.columns:
                _show25["KSOP?"] = _show25["KSOP?"].apply(lambda x: "Yes" if x else "No")
            _render_html_table(_show25,
                               money_cols=["Total Assets", "Employer Securities",
                                           "Employer Contributions", "Benefits Paid"],
                               number_cols=["Participants", "Active Participants"],
                               height=520,
                               numbered=True,
                               sortable_cols=["Participants", "Active Participants",
                                              "Total Assets", "Employer Securities",
                                              "Employer Contributions", "Benefits Paid"],
                               show_totals=True,
                               caption=f"Massachusetts ESOP returns received for form year {_yr}")
            st.caption(
                f"Showing {len(_show25)} of {len(_f25)} MA ESOP returns received for form "
                f"year {_yr}, including final returns from plans that are winding down "
                f"(0 active participants). Financial figures are end-of-year values from "
                f"each return's Schedule H or I.")
            st.download_button(f"Download {_yr} returns (CSV)",
                               utils.export_csv_bytes(_f25),
                               f"MA_ESOP_Form5500_{_yr}_received_so_far.csv", "text/csv")
        else:
            st.info(f"No {_yr} filings imported yet. Run "
                    f"`python3 scan_dol_filers.py --year {_yr} --import-new` "
                    f"to scan DOL bulk data and import {_yr} filers.")

    # ── Footer (every page) ──
    st.markdown(
        f'<footer class="gov-footer" role="contentinfo">'
        f'<div class="gov-footer__grid">'
        f'<div><h6>Source</h6><p>U.S. Department of Labor, Employee Benefits Security '
        f'Administration: Form 5500 annual returns, EFAST2 bulk datasets (release of '
        f'{config.DOL_RELEASE_DATE}). Individual filings are public at '
        f'<a href="{config.DOL_FORM_5500_URL}" target="_blank" rel="noopener">efast.dol.gov</a>.'
        f'</p></div>'
        f'<div><h6>Which plans are counted</h6><p>A plan is included when its sponsor\'s '
        f'mailing address is in Massachusetts and its return carries an ESOP feature code '
        f'(2O, 2P or 2Q). 401(k) plans at large public companies that merely offer '
        f'company stock are excluded. <b>Active</b> plans report at least one current '
        f'employee as a participant.</p></div>'
        f'<div><h6>Timeliness</h6><p>Plans file up to 9½ months after their plan year '
        f'ends, so the newest form year keeps growing. This edition reflects every return '
        f'DOL had published as of {config.DOL_RELEASE_DATE}, reconciled on '
        f'{config.DATA_AS_OF}.</p></div>'
        f'</div>'
        f'<p class="gov-footer__legal">Figures are reproduced as filed with the U.S. '
        f'Department of Labor; a dash (—) means the plan did not report the item. '
        f'{config.ORG_NAME} · {config.APP_TITLE}</p>'
        f'</footer>',
        unsafe_allow_html=True,
    )

else:
    # No Form 5500 data — show setup instructions
    _note(
        '<p>This dashboard analyzes DOL Form 5500 bulk filings to build a '
        'Massachusetts-specific ESOP dataset.</p>'
        '<p><b>To load the data:</b> run <code>python3 scan_dol_filers.py --year 2024 '
        '--import-new</code> (repeat per form year), then '
        '<code>python3 refetch_dol_financials.py</code>, and refresh this page. '
        'Alternatively, place <code>form5500_ma_esops.csv</code> in the dashboard '
        'directory and it will be imported on the next page load.</p>',
        title="Form 5500 data not yet loaded")

# ── Sidebar: About / Data Sources / Quick Stats (below navigation) ──

with st.sidebar:
    st.markdown("---")
    st.markdown("### At a Glance")
    if f5500_summaries:
        _sidebar_latest = next((s for s in reversed(f5500_summaries) if s.get("ma_plan_count", 0) > 0), None)
        if _sidebar_latest:
            _sb_yr = _sidebar_latest["filing_year"]
            _sb_fin = form5500_analysis.get_financial_summary(
                _sb_yr, exclude_zombie=True)
            _sb_plans = _sb_fin.get("plans_total", 0) or 0
            _sb_part = _sb_fin.get("total_participants", 0) or 0
            _sb_assets = _sb_fin.get("total_assets", 0) or 0
            st.metric(f"Active MA ESOPs ({_sb_yr})", f"{_sb_plans:,}")
            st.metric("Participants", f"{_sb_part:,}")
            if _sb_assets > 0:
                st.metric("Plan Assets", _money(_sb_assets, 2))
    else:
        st.info("No data loaded yet")
    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        "An analysis of the annual **Form 5500** returns that employee stock ownership "
        "plans file with the U.S. Department of Labor, covering every ESOP sponsored "
        "by a Massachusetts employer since 2014."
    )
    st.markdown("### Data Sources")
    st.markdown(
        f"- [DOL Form 5500 filing search]({config.DOL_FORM_5500_URL})\n"
        f"- [DOL Form 5500 bulk datasets]({config.DOL_FORM_5500_BULK})"
    )
    st.markdown(
        f'<div class="side-asof">Data current as of {config.DATA_AS_OF}<br>'
        f'DOL release: {config.DOL_RELEASE_DATE}</div>',
        unsafe_allow_html=True)
