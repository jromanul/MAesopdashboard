"""
Visual theme for the Form 5500 ESOP Dashboard.

Presentation only — this module defines no data and changes no figures. It was
split out of app.py so the design system lives in one reviewable place.

Palette follows the Commonwealth of Massachusetts ("Mayflower") identity:
navy #14558F primary, gold #F6C51B secondary, white ground, with a supporting
neutral ramp for surfaces, hairlines and body copy.

Design notes:
  - Flat surfaces. No gradients, no bevels; depth comes from 1px hairlines and
    a single low-opacity shadow token.
  - Type: Inter, with tabular figures everywhere a number is compared down a
    column, and negative tracking on display numerals so large figures read as
    one shape rather than a row of digits.
  - Colour carries meaning, not decoration: gold marks Massachusetts-scoped
    figures and the active nav item; navy is structure; red/green appear only
    for real deltas.
  - Every functional selector from the original stylesheet is preserved —
    Material Symbols ligature fixes, the desktop sidebar lock, glide-data-grid
    custom properties, the radio-as-nav treatment, and the sticky
    header/footer/leading columns on .data-table.
"""

FONT_LINKS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    'family=Inter:wght@400;500;600;700;800&display=swap">'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    'family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200">'
)

CSS = """
<style>

/* ══════════════════════════════════════════════════════════════
   DESIGN TOKENS
   ══════════════════════════════════════════════════════════════ */
:root {
    /* Mayflower brand */
    --ma-navy:        #14558F;
    --ma-navy-900:    #0A2E52;
    --ma-navy-800:    #0E3D6B;
    --ma-navy-600:    #1A6BB5;
    --ma-navy-050:    #EDF4FB;
    --ma-gold:        #F6C51B;
    --ma-gold-deep:   #A87F08;
    --ma-gold-050:    #FFF9E6;

    /* Neutral ramp */
    --ink:            #0F1E2E;
    --ink-2:          #3D4E60;
    --ink-3:          #64758A;
    --line:           #E4EAF1;
    --line-soft:      #EEF2F7;
    --surface:        #FFFFFF;
    --surface-2:      #F7F9FC;

    /* Semantic */
    --pos:            #12805C;
    --neg:            #C0392B;

    --radius:         14px;
    --radius-sm:      10px;
    --shadow:         0 1px 2px rgba(15,30,46,.04), 0 4px 16px rgba(15,30,46,.06);
    --shadow-hover:   0 2px 4px rgba(15,30,46,.06), 0 10px 28px rgba(15,30,46,.10);
    --font:           'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
}

/* ══════════════════════════════════════════════════════════════
   BASE
   ══════════════════════════════════════════════════════════════ */
html, body, [class*="st-"] {
    font-family: var(--font);
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

.stApp, [data-testid="stAppViewContainer"] { background-color: var(--surface-2); }

/* Roomier canvas; the content column sits on white against the tinted app bg */
[data-testid="stMain"] .block-container {
    padding-top: 1.25rem;
    padding-bottom: 4rem;
    max-width: 1560px;
}

[data-testid="stAppViewContainer"] h1,
[data-testid="stAppViewContainer"] h2,
[data-testid="stAppViewContainer"] h3,
[data-testid="stAppViewContainer"] h4,
[data-testid="stAppViewContainer"] h5,
[data-testid="stAppViewContainer"] h6 {
    color: var(--ink) !important;
    letter-spacing: -0.015em;
    font-weight: 700;
}
[data-testid="stAppViewContainer"] p,
[data-testid="stAppViewContainer"] li,
[data-testid="stAppViewContainer"] span,
[data-testid="stAppViewContainer"] label,
[data-testid="stAppViewContainer"] .stMarkdown,
[data-testid="stAppViewContainer"] .stCaption,
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] {
    color: var(--ink-2) !important;
}
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"],
[data-testid="stAppViewContainer"] [data-testid="stCaptionContainer"] * {
    color: var(--ink-3) !important;
    font-size: .78rem !important;
}

/* ══════════════════════════════════════════════════════════════
   ICON FONT — Material Symbols must render as ligatures, not text
   ══════════════════════════════════════════════════════════════ */
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
    font-size: 22px !important;
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
[data-testid="stSidebar"] span[class*="Icon"],
[data-testid="stExpander"] span[class*="Icon"],
button span[class*="Icon"],
summary span[class*="Icon"],
[data-testid="stMarkdownContainer"] span.material-symbols-rounded {
    font-family: 'Material Symbols Rounded' !important;
    font-feature-settings: 'liga' !important;
    -webkit-font-feature-settings: 'liga' !important;
}

/* ══════════════════════════════════════════════════════════════
   HEADER — flat navy band, gold hairline, no gradient
   ══════════════════════════════════════════════════════════════ */
.main-header {
    background: var(--ma-navy-900);
    padding: 1.35rem 1.75rem 1.2rem;
    border-radius: var(--radius);
    margin-bottom: 1.4rem;
    position: relative;
    overflow: hidden;
}
.main-header::after {
    content: '';
    position: absolute;
    left: 0; right: 0; bottom: 0;
    height: 3px;
    background: var(--ma-gold);
}
.main-header h1,
.main-header h1 *,
[data-testid="stAppViewContainer"] .main-header h1 {
    color: #FFFFFF !important;
    margin: 0;
    font-size: 1.6rem;
    font-weight: 800;
    letter-spacing: -0.025em;
    line-height: 1.15;
}
.main-header p,
[data-testid="stAppViewContainer"] .main-header p {
    color: rgba(255,255,255,.62) !important;
    margin: .3rem 0 0 0;
    font-size: .88rem;
    font-weight: 400;
    letter-spacing: .005em;
}
.main-header .header-date,
[data-testid="stAppViewContainer"] .main-header .header-date {
    color: var(--ma-gold) !important;
    font-size: .74rem;
    font-weight: 600;
    text-align: right;
    white-space: nowrap;
    padding-left: 1.5rem;
    text-transform: uppercase;
    letter-spacing: .09em;
}

/* Section rules */
.section-header {
    border-bottom: 2px solid var(--ma-gold);
    padding-bottom: .45rem;
    margin-bottom: 1rem;
    color: var(--ma-navy);
}
[data-testid="stMain"] h4,
section.main h4 {
    font-size: 1.06rem;
    border-bottom: 1px solid var(--line);
    padding-bottom: .5rem;
    margin: .4rem 0 1rem;
    position: relative;
}
[data-testid="stMain"] h4::after,
section.main h4::after {
    content: '';
    position: absolute;
    left: 0; bottom: -1px;
    width: 40px; height: 2px;
    background: var(--ma-gold);
}
[data-testid="stMain"] .callout-eo h4,
[data-testid="stMain"] .callout-warning h4,
section.main .callout-eo h4,
section.main .callout-warning h4 {
    border-bottom: none !important;
    padding-bottom: 0 !important;
}
[data-testid="stMain"] .callout-eo h4::after,
[data-testid="stMain"] .callout-warning h4::after,
section.main .callout-eo h4::after,
section.main .callout-warning h4::after { display: none; }

/* Sentence case, not uppercase: several of these run long ("Employer-Securities
   (Company Stock) Intensity"), and tracked capitals hurt readability at length.
   Uppercase is reserved for short eyebrow labels — sidebar sections, table
   column heads, the header date. */
[data-testid="stMain"] h5,
section.main h5 {
    font-size: .98rem;
    font-weight: 700;
    color: var(--ink) !important;
    letter-spacing: -0.01em;
    border-left: 3px solid var(--ma-gold);
    padding-left: .6rem;
    margin: 1.4rem 0 .7rem;
}
[data-testid="stMain"] hr,
section.main hr {
    margin: 1.5rem 0;
    border: none;
    border-top: 1px solid var(--line);
}

/* ══════════════════════════════════════════════════════════════
   METRIC CARDS
   ══════════════════════════════════════════════════════════════ */
.metric-card {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 1.05rem 1.15rem 1rem;
    margin-bottom: .6rem;
    box-shadow: var(--shadow);
    transition: box-shadow .18s ease, transform .18s ease, border-color .18s ease;
    position: relative;
    overflow: hidden;
    height: 100%;
}
.metric-card:hover {
    box-shadow: var(--shadow-hover);
    transform: translateY(-1px);
    border-color: #D3DEEA;
}
.metric-card h3 {
    margin: .1rem 0 0;
    font-size: 1.85rem;
    font-weight: 800;
    line-height: 1.08;
    color: var(--ma-navy) !important;
    letter-spacing: -0.035em;
    font-variant-numeric: tabular-nums;
}
.metric-card p {
    margin: .35rem 0 0;
    color: var(--ink-2) !important;
    font-size: .8rem;
    font-weight: 500;
    line-height: 1.35;
}
.metric-card .delta-up   { color: var(--pos) !important; font-size: .78rem; font-weight: 600; }
.metric-card .delta-down { color: var(--neg) !important; font-size: .78rem; font-weight: 600; }

/* Equal-height cards across a row. Streamlit columns size to their content, so
   a card whose caption wraps to two lines leaves its neighbours short and the
   row reads ragged. Stretch the column chain only where a metric card actually
   lives, so chart and table columns are untouched. */
[data-testid="stHorizontalBlock"] { align-items: stretch !important; }
/* The column becomes a vertical flex context but keeps Streamlit's own
   flex-basis — overriding flex/width here would collapse the row to one
   full-width card per line. */
[data-testid="stColumn"]:has(.metric-card) {
    display: flex !important;
    flex-direction: column !important;
}
/* Every wrapper between the column and the card must grow, including the
   unnamed emotion divs Streamlit injects. */
[data-testid="stColumn"]:has(.metric-card) div:has(.metric-card) {
    display: flex !important;
    flex-direction: column !important;
    flex: 1 1 auto !important;
    width: 100%;
}
.metric-card { flex: 1 1 auto !important; }

/* Massachusetts-scoped figures carry a gold top rule */
.metric-card-ma::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    background: var(--ma-gold);
}
.metric-card-ma h3 { color: var(--ma-navy) !important; }

/* ══════════════════════════════════════════════════════════════
   CALLOUTS — flat tints
   ══════════════════════════════════════════════════════════════ */
.callout-eo {
    background: var(--ma-navy-050);
    border: 1px solid #CFE0F1;
    border-left: 3px solid var(--ma-navy);
    border-radius: var(--radius-sm);
    padding: 1rem 1.15rem;
    margin: 1rem 0;
}
.callout-eo h4 { color: var(--ma-navy) !important; margin-top: 0; }
.callout-warning {
    background: var(--ma-gold-050);
    border: 1px solid #F0DFA6;
    border-left: 3px solid var(--ma-gold);
    border-radius: var(--radius-sm);
    padding: 1rem 1.15rem;
    margin: 1rem 0;
}
.callout-warning h4 { color: var(--ma-gold-deep) !important; margin-top: 0; }

/* Streamlit's own info/warning blocks, matched to the callouts */
[data-testid="stAlert"] {
    border-radius: var(--radius-sm) !important;
    border: 1px solid var(--line) !important;
    box-shadow: none !important;
}

/* ══════════════════════════════════════════════════════════════
   BUTTONS
   ══════════════════════════════════════════════════════════════ */
button[kind="secondary"],
button[data-testid="stBaseButton-secondary"],
.stButton button {
    color: var(--ma-navy) !important;
    border: 1px solid #C8D8E8 !important;
    background-color: var(--surface) !important;
    font-weight: 600 !important;
    border-radius: 9px !important;
    padding: .42rem 1rem !important;
    font-size: .84rem !important;
    box-shadow: 0 1px 2px rgba(15,30,46,.04) !important;
    transition: all .16s ease !important;
}
button[kind="secondary"]:hover,
button[data-testid="stBaseButton-secondary"]:hover,
.stButton button:hover {
    background-color: var(--ma-navy-050) !important;
    border-color: var(--ma-navy) !important;
    color: var(--ma-navy) !important;
}
button[kind="secondary"] p,
button[data-testid="stBaseButton-secondary"] p,
.stButton button p,
button[kind="secondary"] span,
button[data-testid="stBaseButton-secondary"] span,
.stButton button span { color: var(--ma-navy) !important; }

.stDownloadButton button {
    background-color: var(--ma-navy) !important;
    color: #FFFFFF !important;
    border: 1px solid var(--ma-navy) !important;
    border-radius: 9px !important;
    font-weight: 600 !important;
    font-size: .84rem !important;
    padding: .42rem 1rem !important;
    transition: all .16s ease !important;
}
.stDownloadButton button p,
.stDownloadButton button span { color: #FFFFFF !important; }
.stDownloadButton button:hover {
    background-color: var(--ma-navy-800) !important;
    border-color: var(--ma-navy-800) !important;
    box-shadow: var(--shadow) !important;
}

/* Inputs */
[data-testid="stAppViewContainer"] [data-testid="stTextInput"] input {
    background-color: var(--surface) !important;
    color: var(--ink) !important;
    border: 1px solid #D6DFE9 !important;
    border-radius: 9px !important;
    font-size: .86rem !important;
    padding: .48rem .7rem !important;
}
[data-testid="stAppViewContainer"] [data-testid="stTextInput"] input:focus {
    border-color: var(--ma-navy) !important;
    box-shadow: 0 0 0 3px rgba(20,85,143,.12) !important;
}
[data-testid="stAppViewContainer"] [data-testid="stTextInput"] label {
    color: var(--ink-2) !important;
    font-size: .8rem !important;
    font-weight: 600 !important;
}

/* ══════════════════════════════════════════════════════════════
   SIDEBAR
   ══════════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {
    background-color: var(--ma-navy-900);
    border-right: 1px solid rgba(255,255,255,.07);
}
[data-testid="stSidebarNav"] { display: none !important; }

@media (min-width: 769px) {
    [data-testid="stSidebar"] {
        min-width: 286px !important;
        max-width: 286px !important;
        transform: none !important;
        position: relative !important;
        transition: none !important;
    }
    [data-testid="stSidebar"][aria-expanded="false"] {
        min-width: 286px !important;
        max-width: 286px !important;
        margin-left: 0 !important;
        transform: none !important;
        display: block !important;
    }
    [data-testid="stSidebar"] button[kind="header"],
    [data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
    [data-testid="collapsedControl"],
    [data-testid="stSidebarCollapsedControl"],
    [data-testid="stExpandSidebarButton"],
    button[data-testid="stSidebarCollapseButton"] {
        display: none !important;
        visibility: hidden !important;
    }
}
@media (max-width: 768px) {
    [data-testid="stSidebar"] { max-width: 85vw !important; }
}

[data-testid="stSidebar"] *,
[data-testid="stSidebar"] p,
[data-testid="stSidebar"] span,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] div { color: #FFFFFF !important; }
[data-testid="stSidebar"] .stMarkdown p,
[data-testid="stSidebar"] .stMarkdown li {
    color: rgba(255,255,255,.66) !important;
    font-size: .82rem;
    line-height: 1.5;
}
[data-testid="stSidebar"] h3 {
    color: rgba(255,255,255,.45) !important;
    font-size: .68rem !important;
    font-weight: 700 !important;
    text-transform: uppercase;
    letter-spacing: .13em;
    margin: 1.4rem 0 .55rem;
}
[data-testid="stSidebar"] a {
    color: var(--ma-gold) !important;
    text-decoration: none;
    font-weight: 500;
}
[data-testid="stSidebar"] a:hover { text-decoration: underline; }
[data-testid="stSidebar"] hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,.11) !important;
    margin: 1.1rem 0 !important;
}

/* Sidebar quick-stat metrics */
[data-testid="stSidebar"] [data-testid="stMetric"] {
    background: rgba(255,255,255,.05);
    border: 1px solid rgba(255,255,255,.09);
    border-radius: var(--radius-sm);
    padding: .6rem .75rem;
    margin-bottom: .45rem;
}
[data-testid="stSidebar"] .stMetricValue,
[data-testid="stSidebar"] [data-testid="stMetricValue"],
[data-testid="stSidebar"] [data-testid="stMetricValue"] div {
    color: var(--ma-gold) !important;
    font-weight: 800 !important;
    font-size: 1.3rem !important;
    letter-spacing: -0.03em;
    font-variant-numeric: tabular-nums;
}
[data-testid="stSidebar"] .stMetricLabel,
[data-testid="stSidebar"] [data-testid="stMetricLabel"],
[data-testid="stSidebar"] [data-testid="stMetricLabel"] p,
[data-testid="stSidebar"] [data-testid="stMetricLabel"] div {
    color: rgba(255,255,255,.55) !important;
    font-size: .68rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: .08em;
}

/* ══════════════════════════════════════════════════════════════
   NAV (radio rendered as a nav rail on the navy sidebar)
   ══════════════════════════════════════════════════════════════ */
div[data-testid="stRadio"] > label { display: none; }
div[data-testid="stRadio"] > div {
    gap: 2px !important;
    width: 100% !important;
}
div[data-testid="stRadio"] > div > label {
    display: flex !important;
    align-items: center !important;
    width: 100% !important;
    box-sizing: border-box !important;
    background-color: transparent !important;
    border-radius: 9px !important;
    padding: .58rem .8rem !important;
    margin: 0 !important;
    cursor: pointer !important;
    transition: background-color .15s ease, box-shadow .15s ease !important;
    border-left: 3px solid transparent !important;
    overflow: hidden !important;
}
/* Hide the leftover baseweb radio dot so only the icon + label show */
div[data-testid="stRadio"] > div > label > div:first-child { display: none !important; }

div[data-testid="stRadio"] > div > label p,
div[data-testid="stRadio"] > div > label span {
    color: rgba(255,255,255,.74) !important;
    font-weight: 500 !important;
    font-size: .875rem !important;
    letter-spacing: -0.005em;
}
div[data-testid="stRadio"] > div > label:hover {
    background-color: rgba(255,255,255,.08) !important;
}
div[data-testid="stRadio"] > div > label:hover p,
div[data-testid="stRadio"] > div > label:hover span { color: #FFFFFF !important; }

div[data-testid="stRadio"] > div > label[data-checked="true"],
div[data-testid="stRadio"] > div > label:has(input:checked) {
    background-color: rgba(246,197,27,.14) !important;
    border-left-color: var(--ma-gold) !important;
}
div[data-testid="stRadio"] > div > label[data-checked="true"] p,
div[data-testid="stRadio"] > div > label[data-checked="true"] span,
div[data-testid="stRadio"] > div > label[data-checked="true"] div,
div[data-testid="stRadio"] > div > label:has(input:checked) p,
div[data-testid="stRadio"] > div > label:has(input:checked) span,
div[data-testid="stRadio"] > div > label:has(input:checked) div {
    color: #FFFFFF !important;
    font-weight: 600 !important;
}

/* ══════════════════════════════════════════════════════════════
   HTML DATA TABLES
   ══════════════════════════════════════════════════════════════ */
.data-table-wrapper {
    max-height: 640px;
    overflow-y: auto;
    overflow-x: auto;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    margin-bottom: 1rem;
    background: var(--surface);
    box-shadow: var(--shadow);
}
.data-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: .82rem;
    font-family: var(--font);
    background: var(--surface);
}
.data-table thead { position: sticky; top: 0; z-index: 2; }
.data-table th {
    background-color: var(--ma-navy-900) !important;
    color: #FFFFFF !important;
    padding: 11px 13px;
    text-align: left;
    font-weight: 600;
    font-size: .69rem;
    text-transform: uppercase;
    letter-spacing: .075em;
    white-space: nowrap;
    border-bottom: 2px solid var(--ma-gold);
}
.data-table td {
    padding: 9px 13px;
    color: var(--ink) !important;
    background-color: var(--surface) !important;
    border-bottom: 1px solid var(--line-soft);
    white-space: nowrap;
}
.data-table tbody tr:last-child td { border-bottom: none; }
.data-table tbody tr:nth-child(even) td { background-color: #FBFCFE !important; }
.data-table tbody tr:hover td { background-color: var(--ma-navy-050) !important; }
.data-table td.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}
.data-table td.row-num {
    text-align: center;
    color: var(--ink-3) !important;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    min-width: 35px;
}

/* Frozen leading column(s): keep row labels visible during horizontal scroll */
.data-table th:first-child,
.data-table td:first-child { position: sticky; left: 0; z-index: 1; }
.data-table thead th:first-child { z-index: 4 !important; }
.data-table tfoot td:first-child { z-index: 3 !important; }
.data-table:not(.numbered) th:first-child,
.data-table:not(.numbered) td:first-child { box-shadow: 1px 0 0 var(--line); }
.data-table.numbered th:first-child,
.data-table.numbered td:first-child {
    width: 40px !important;
    min-width: 40px !important;
    max-width: 40px !important;
}
.data-table.numbered th:nth-child(2),
.data-table.numbered td:nth-child(2) {
    position: sticky;
    left: 40px;
    z-index: 1;
    box-shadow: 1px 0 0 var(--line);
}
.data-table.numbered thead th:nth-child(2) { z-index: 4 !important; }
.data-table.numbered tfoot td:nth-child(2) { z-index: 3 !important; }

/* Totals footer */
.data-table tfoot { position: sticky; bottom: 0; z-index: 2; }
.data-table tfoot td {
    background-color: var(--ma-navy-900) !important;
    color: #FFFFFF !important;
    font-weight: 700 !important;
    border-top: 2px solid var(--ma-gold) !important;
    border-bottom: none !important;
    padding: 10px 13px;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
}
.data-table tfoot td.num { text-align: right; }

/* Sortable headers */
.data-table th.sortable {
    cursor: pointer;
    user-select: none;
    position: relative;
    padding-right: 22px;
    transition: background-color .14s ease;
}
.data-table th.sortable:hover { background-color: var(--ma-navy-800) !important; }
.data-table th.sortable::after {
    content: '\\2195';
    position: absolute;
    right: 6px;
    top: 50%;
    transform: translateY(-50%);
    font-size: .72rem;
    opacity: .38;
}
.data-table th.sortable.sort-asc::after  { content: '\\25B2'; opacity: 1; color: var(--ma-gold); }
.data-table th.sortable.sort-desc::after { content: '\\25BC'; opacity: 1; color: var(--ma-gold); }

/* ══════════════════════════════════════════════════════════════
   st.dataframe (glide-data-grid)
   ══════════════════════════════════════════════════════════════ */
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] > div,
[data-testid="stDataFrame"] iframe,
.stDataFrame,
.stDataFrame > div { background-color: var(--surface) !important; }
[data-testid="stDataFrame"] [data-testid="glideDataEditor"],
[data-testid="stDataFrame"] .dvn-scroller,
[data-testid="stDataFrame"] canvas { background-color: var(--surface) !important; }
[data-testid="stDataFrame"] .stDataFrameResizable {
    background-color: var(--surface) !important;
    border: 1px solid var(--line) !important;
    border-radius: var(--radius) !important;
}
[data-testid="stDataFrame"] th,
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] .header-cell {
    color: var(--ink) !important;
    background-color: var(--surface-2) !important;
    font-weight: 600 !important;
}
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stDataFrame"] .data-cell {
    color: var(--ink) !important;
    background-color: var(--surface) !important;
}
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] * {
    --gdg-text-dark: #0F1E2E !important;
    --gdg-text-medium: #3D4E60 !important;
    --gdg-text-light: #64758A !important;
    --gdg-text-bubble: #0F1E2E !important;
    --gdg-bg-cell: #FFFFFF !important;
    --gdg-bg-header: #F7F9FC !important;
    --gdg-text-header: #0F1E2E !important;
    --gdg-text-group-header: #0F1E2E !important;
    --gdg-border-color: #E4EAF1 !important;
    --gdg-header-font-style: 600 12px !important;
    --gdg-base-font-style: 13px !important;
    --gdg-font-family: 'Inter', sans-serif !important;
    color: var(--ink) !important;
}

/* ══════════════════════════════════════════════════════════════
   CHART CONTAINERS — sit each plot on its own white card
   ══════════════════════════════════════════════════════════════ */
[data-testid="stMain"] [data-testid="stPlotlyChart"] {
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: .55rem .65rem .2rem;
    box-shadow: var(--shadow);
    overflow: hidden;
}

/* Expanders */
[data-testid="stExpander"] {
    border: 1px solid var(--line) !important;
    border-radius: var(--radius) !important;
    background: var(--surface) !important;
    box-shadow: var(--shadow);
}
[data-testid="stExpander"] summary { font-weight: 600 !important; font-size: .88rem !important; }

</style>
"""
