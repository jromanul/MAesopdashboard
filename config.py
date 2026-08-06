"""
Form 5500 ESOP Dashboard — Configuration
"""
import os

# ──────────────────────────────────────────────
# GENERAL
# ──────────────────────────────────────────────
APP_TITLE = "Form 5500 ESOP Dashboard"
APP_ICON = "🔬"
DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

# Single source of truth for the "as of" date shown across pages (DOL EFAST2
# bulk-data vintage + public-records research date). Use everywhere instead of
# hardcoding the date in individual captions.
DATA_AS_OF = "August 6, 2026"

# ──────────────────────────────────────────────
# AUTHORITATIVE DATA SOURCES
# ──────────────────────────────────────────────
# NOTE: MA ESOP headline figures are computed live from the database
# (see form5500_analysis.get_financial_summary / get_annual_summaries), not
# hardcoded here, so they can never drift from the underlying filings.

NATIONAL_ESOP_DATA = {
    "source": "DOL Form 5500 Filings (via NCEO analysis)",
    "as_of_year": 2023,
    "esop_count": 6609,
    "total_participants_millions": 15.1,
    "active_participants_millions": 10.99,
    "total_assets_trillions": 2.1,
    "unique_companies": 6411,
    "new_esops_2023": 309,
}

# ──────────────────────────────────────────────
# CHART STYLING
# ──────────────────────────────────────────────
CHART_COLORS = {
    "navy": "#14558F",
    "gold": "#F6C51B",
    "red": "#E74C3C",
    "green": "#27AE60",
    "gray": "#95A5A6",
    "light_navy": "#1A6BB5",
    "light_gold": "#F9D44E",
    "purple": "#8E44AD",
    "bg_light": "#FFFFFF",
    "cranberry": "#680A1D",
}

CHART_PALETTE = ["#14558F", "#F6C51B", "#12805C", "#C0392B", "#6C3483", "#7C8DA1", "#4A8FD4", "#E0A93A"]
CHART_FONT_FAMILY = "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif"
CHART_HEIGHT_SM = 300
CHART_HEIGHT_MD = 400
CHART_HEIGHT_LG = 500

# ──────────────────────────────────────────────
# FORM 5500 ESOP ANALYSIS CONFIGURATION
# ──────────────────────────────────────────────

FORM5500_YEARS = list(range(2014, 2026))

# The form year DOL is still receiving filings for. Plans file 7 months after
# their plan year ends (9.5 with an extension), so a year keeps trickling in for
# roughly 18 months — see the "2025 Filers" page, which tracks this year on its
# own. It is deliberately EXCLUDED from form5500_annual_summary, which drives the
# complete-year Overview / Trends / Year-over-Year analyses; see
# form5500_analysis.recompute_annual_summaries(). Bump once a year is
# substantially filed (and the corresponding page/labels are updated).
FORM5500_OPEN_FORM_YEAR = 2025

FORM5500_RECORDS_CSV = "form5500_ma_esops.csv"
FORM5500_SUMMARY_CSV = "form5500_annual_summary.csv"

FORM5500_SCHEDULE_DIR = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500")

FORM5500_SCHEDULE_H_PATTERNS = ["sch_h_", "schedule_h_", "f_sch_h_", "SCH_H_"]
FORM5500_SCHEDULE_I_PATTERNS = ["sch_i_", "schedule_i_", "f_sch_i_", "SCH_I_"]

NATIONAL_ESOP_ESTIMATES = {
    "total_plans": 6609,
    "total_participants": 15_100_000,
    "total_assets": 2_100_000_000_000,
    "source": "National Center for Employee Ownership (NCEO), 2023 filings",
}

# Civilian labor force (BLS, 2024). Used for "ESOPs per 100K workers".
# Both on the same basis (labor force) for an apples-to-apples comparison.
MA_WORKFORCE_SIZE = 3_860_000     # MA civilian labor force, BLS 2024 (~3.86M)
US_WORKFORCE_SIZE = 168_600_000   # US civilian labor force, BLS 2024 (~168.6M)

FORM5500_ASSET_BINS = [
    (0, 1_000_000, "< $1M"),
    (1_000_000, 5_000_000, "$1M - $5M"),
    (5_000_000, 10_000_000, "$5M - $10M"),
    (10_000_000, 50_000_000, "$10M - $50M"),
    (50_000_000, 100_000_000, "$50M - $100M"),
    (100_000_000, 500_000_000, "$100M - $500M"),
    (500_000_000, 1_000_000_000, "$500M - $1B"),
    (1_000_000_000, float("inf"), "$1B+"),
]

FORM5500_PARTICIPANT_BINS = [
    (0, 50, "< 50"),
    (50, 100, "50 - 100"),
    (100, 250, "100 - 250"),
    (250, 500, "250 - 500"),
    (500, 1000, "500 - 1,000"),
    (1000, 5000, "1,000 - 5,000"),
    (5000, 10000, "5,000 - 10,000"),
    (10000, float("inf"), "10,000+"),
]

FORM5500_METHODOLOGY = """
### Data Source
All data in this section is derived from **DOL Form 5500 Annual Return/Report** filings,
downloaded as bulk datasets from the U.S. Department of Labor.

### ESOP Identification Methodology
A filing is treated as a Massachusetts ESOP when **both** of the following hold in the
DOL bulk data:

1. **Sponsor state** (SPONS_DFE_MAIL_US_STATE) = "MA" — see Geographic Filtering below.
2. **Type of Pension Benefit code** (TYPE_PENSION_BNFT_CODE) contains **2O** (ESOP),
   **2P** (leveraged ESOP — company stock acquired with borrowed funds), or **2Q**.
   2Q normally accompanies 2P on leveraged plans, but a plan winding down or restating
   can file carrying 2Q alone, so it is matched to avoid dropping those from coverage.

These codes are the operative test; no plan-name or keyword matching is used, so a plan
is counted on what its sponsor formally certified to DOL rather than on how it is named.
Most tracked plans also carry **2I** (stock bonus), which accompanies but does not by
itself establish ESOP status.

Coverage is verified each refresh by an independent check that searches every
Massachusetts filing by **plan name** (ESOP / "employee stock" / "stock ownership" /
"stock bonus") and reconciles the result against the code-based set, so a plan cannot go
missing merely because its codes changed between years.

Plans carrying both **2J** (401(k)) and **2K** (401(m)) alongside an ESOP code are flagged
as **KSOPs** (included in ESOP counts but separately identified).

**Deliberate exclusion — employer stock inside a conventional 401(k).** Some large,
publicly traded employers attach an ESOP code to an ordinary savings plan that merely
offers company stock as one investment option among many (e.g. a "401(K) PLAN" or
"SAVINGS AND INVESTMENT PLAN" carrying 2O alongside 2J/2K). These are not closely held,
employee-owned companies in the sense this dashboard tracks, and counting them would
overstate Massachusetts employee ownership by thousands of participants apiece. Such
filings are reviewed and excluded by hand; the exclusions are recorded in
`scan_dol_filers.py` so they are not silently re-imported on a later refresh.

### Geographic Filtering
Massachusetts plans are identified by the sponsor's mailing state (SPONS_DFE_MAIL_US_STATE = "MA").

### Financial Data
Asset and contribution figures are sourced from the main Form 5500 filing and supplemented
with Schedule H (for large plans, 100+ participants) and Schedule I (for small plans, <100
participants) data where available. **Employer securities** data (company stock held in the ESOP
trust) is extracted from Schedule H Part I, line 1c — this is unique to ESOPs and represents
the core ESOP asset.

**Employer securities is a Schedule H-only line item.** Plans that file Schedule I or
Form 5500-SF (small plans) do not itemize company stock, so this figure is *unavailable*
for them rather than zero. Such values are shown as "—" (not reported), never "$0", and are
excluded from employer-securities totals and stock-as-%-of-assets ratios — those figures
therefore reflect only the plans that report company stock. A "$0" appears only where a plan
explicitly reported zero employer securities on its Schedule H.

Schedule H/I data can be loaded from pre-downloaded CSV files placed in `data/form5500/`.

### Years Covered
Complete filing years **2014–2024** (11 years of DOL bulk data) drive the Overview, Trends,
and Year-over-Year pages. Early **2025** filers — fiscal-year, short, and final filings already
published by DOL — are tracked separately on the **2025 Filers** page; form year 2025 stays out
of the complete-year analyses until it is substantially filed (most calendar-year plans file
July–October of the following year).

### Caveats
- Some plans may be missed if they don't use standard ESOP type codes
- KSOP plans (combined 401(k)/ESOP) are included but flagged separately
- Participant counts may include both active and retired participants
- Asset values represent beginning-of-year or end-of-year figures depending on the filing
- Some large companies file multiple plans (counted as separate plans)
- Late filers from recent years may not be captured in the current dataset

### Reproduction
The dataset is rebuilt from DOL EFAST2 bulk filings by the scripts in the project root,
run from a checkout of the repository. Stop the app first so the SQLite database is not
locked. On Windows use the project's virtual environment — `python3` is not a Windows
command, and the system `python` does not carry this project's packages:

1. `venv\\Scripts\\python scan_dol_filers.py --year <YEAR>` — reports what DOL holds that
   the database does not, and writes `dol_scan_<YEAR>_report.csv`. Changes nothing.
2. Review that report. Adding `--import-new` imports the RETURNING bucket as well as the
   NEW one, and that bucket can contain superseded re-filings and the same plan filed
   under two EINs, which would double-count plans. Those are resolved by hand.
3. `venv\\Scripts\\python refetch_dol_financials.py` — re-derives financials for the
   complete years from the authoritative Schedule H/I data. Participant counts are never
   touched here; they come from the main form.
4. `venv\\Scripts\\python export_seed_csvs.py` — refreshes the CSV copies of the database.

A form year keeps growing for roughly 18 months after it opens, because plans may file up
to 9.5 months after a plan year ends and many run on a fiscal year, so a completed year
should be re-scanned periodically rather than assumed final. An empty database is
auto-seeded from `form5500_ma_esops.csv` if that file is present in the project root.
"""

DOL_FORM_5500_URL = "https://www.efast.dol.gov/5500Search/"
DOL_FORM_5500_BULK = "https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets"

# ──────────────────────────────────────────────
# GEOGRAPHIC DATA
# ──────────────────────────────────────────────

MA_REGIONS = {
    "Greater Boston": [
        "Boston", "Cambridge", "Somerville", "Waltham", "Newton", "Brighton",
        "Dorchester", "Jamaica Plain", "East Boston", "West Roxbury", "Revere",
        "Quincy", "Watertown", "Arlington", "Lexington", "Burlington", "Stoughton",
        "Norwell", "Norwood", "Avon", "Brockton", "Rockland", "Abington",
        "Framingham", "Natick", "Wakefield", "Woburn", "Middleton",
        "Allston", "Everett", "Medford", "Needham", "Reading", "Stoneham",
    ],
    "Northeast MA": [
        "Lowell", "Tewksbury", "N. Chelmsford", "North Chelmsford", "Andover",
        "North Reading", "Danvers", "Beverly", "Ipswich", "Newburyport",
        "Amesbury", "Lynn", "Georgetown", "Haverhill", "Salisbury",
    ],
    "MetroWest & Central MA": [
        "Worcester", "Marlborough", "Westford", "Littleton", "Devens",
        "Maynard", "Concord", "Hopkinton", "Milford", "Grafton", "Uxbridge",
        "Oxford", "S. Lancaster", "Clinton", "West Boylston",
        "Leominster", "Millbury", "West Brookfield", "Holliston", "Shirley",
    ],
    "Western MA": [
        "Springfield", "Holyoke", "Greenfield", "Northampton", "Amherst",
        "Hadley", "Easthampton", "Chicopee", "Palmer", "Orange", "Pittsfield",
        "Haydenville", "Belchertown", "West Hatfield", "Indian Orchard", "Athol",
        "Westfield", "Lee", "East Otis",
    ],
    "Southeast MA & Cape": [
        "Fall River", "New Bedford", "W. Bridgewater", "Pembroke", "Plymouth",
        "Mashpee", "West Yarmouth", "Vineyard Haven", "West Tisbury",
        "Mansfield", "Franklin", "Hyannis", "Harwich", "Foxborough",
        "Rehoboth", "Somerset", "Marshfield",
    ],
    "North Shore & Merrimack": [
        "Billerica", "Wilmington", "Spec Process Engineering",
    ],
}

MA_CITY_COORDS = {
    "Boston": (42.3601, -71.0589), "Cambridge": (42.3736, -71.1097),
    "Somerville": (42.3876, -71.0995), "Waltham": (42.3765, -71.2356),
    "Newton": (42.3370, -71.2092), "Brighton": (42.3488, -71.1572),
    "Dorchester": (42.3016, -71.0674), "Jamaica Plain": (42.3097, -71.1152),
    "East Boston": (42.3751, -71.0390), "West Roxbury": (42.2793, -71.1595),
    "Revere": (42.4084, -71.0120), "Quincy": (42.2529, -71.0023),
    "Watertown": (42.3709, -71.1828), "Arlington": (42.4153, -71.1564),
    "Lexington": (42.4473, -71.2245), "Burlington": (42.5048, -71.1956),
    "Woburn": (42.4793, -71.1523), "Billerica": (42.5584, -71.2689),
    "Springfield": (42.1015, -72.5898), "Holyoke": (42.2043, -72.6162),
    "Greenfield": (42.5876, -72.5993), "Northampton": (42.3250, -72.6412),
    "Amherst": (42.3732, -72.5199), "Worcester": (42.2626, -71.8023),
    "Lowell": (42.6334, -71.3162), "Fall River": (41.7015, -71.1550),
    "New Bedford": (41.6362, -70.9342), "Plymouth": (41.9584, -70.6673),
    "Pittsfield": (42.4501, -73.2453), "Framingham": (42.2793, -71.4162),
    "Natick": (42.2835, -71.3495), "Stoughton": (42.1243, -71.0968),
    "Brockton": (42.0834, -71.0184), "Rockland": (42.1293, -70.9078),
    "Norwell": (42.1615, -70.7930), "Norwood": (42.1945, -71.1996),
    "Avon": (42.1304, -71.0416), "Abington": (42.1048, -70.9451),
    "Wilmington": (42.5570, -71.1734), "Westford": (42.5793, -71.4376),
    "Littleton": (42.5451, -71.4862), "Devens": (42.5420, -71.6167),
    "Maynard": (42.4334, -71.4487), "Concord": (42.4604, -71.3489),
    "Milford": (42.1398, -71.5162), "Grafton": (42.2076, -71.6862),
    "Uxbridge": (42.0768, -71.6318), "Oxford": (42.1168, -71.8690),
    "Marlborough": (42.3459, -71.5523), "Hopkinton": (42.2287, -71.5223),
    "S. Lancaster": (42.4493, -71.6834), "Clinton": (42.4168, -71.6828),
    "West Boylston": (42.3668, -71.7862), "Middleton": (42.5951, -71.0162),
    "Danvers": (42.5751, -70.9301), "Beverly": (42.5584, -70.8801),
    "Ipswich": (42.6793, -70.8412), "Newburyport": (42.8126, -70.8773),
    "Amesbury": (42.8584, -70.9301), "Lynn": (42.4668, -70.9495),
    "Tewksbury": (42.6101, -71.2345), "N. Chelmsford": (42.6334, -71.3823),
    "Andover": (42.6584, -71.1370), "North Reading": (42.5751, -71.0789),
    "Mashpee": (41.6484, -70.4757), "West Yarmouth": (41.6584, -70.2334),
    "Vineyard Haven": (41.4534, -70.6034), "West Tisbury": (41.3834, -70.6734),
    "W. Bridgewater": (42.0193, -71.0078), "Pembroke": (42.0668, -70.8012),
    "Mansfield": (42.0334, -71.2190), "Franklin": (42.0837, -71.3968),
    "Hadley": (42.3584, -72.5712), "Easthampton": (42.2668, -72.6690),
    "Chicopee": (42.1487, -72.6078), "Palmer": (42.1584, -72.3287),
    "Orange": (42.5876, -72.3112), "Haydenville": (42.3834, -72.6990),
    "Belchertown": (42.2768, -72.4012), "West Hatfield": (42.3934, -72.6490),
    "Indian Orchard": (42.1534, -72.5090), "Athol": (42.5968, -72.2268),
    "Westfield": (42.1251, -72.7490), "Wakefield": (42.5068, -71.0734),
    "Middleton & Company": (42.3601, -71.0589),
}
