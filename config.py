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

# ──────────────────────────────────────────────
# AUTHORITATIVE DATA SOURCES
# ──────────────────────────────────────────────

MA_ESOP_DATA = {
    "source": "DOL Form 5500 Filings",
    "as_of_year": 2024,
    "esop_count": 110,
    "esop_plan_count": 111,
    "ksop_count": 4,
    "total_participants": 19_828,
    "active_participants": 12_724,
    "total_assets": 2_851_510_867,
    "note": "110 unique ESOP companies filed 111 plans (4 are KSOPs)",
}

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

CHART_PALETTE = ["#14558F", "#F6C51B", "#27AE60", "#E74C3C", "#8E44AD", "#95A5A6", "#1A6BB5", "#F9D44E"]
CHART_FONT_FAMILY = "Source Sans Pro, sans-serif"
CHART_HEIGHT_SM = 300
CHART_HEIGHT_MD = 400
CHART_HEIGHT_LG = 500

# ──────────────────────────────────────────────
# FORM 5500 ESOP ANALYSIS CONFIGURATION
# ──────────────────────────────────────────────

FORM5500_YEARS = list(range(2014, 2026))

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

MA_WORKFORCE_SIZE = 3_740_000
US_WORKFORCE_SIZE = 161_000_000

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
Plans are identified as ESOPs using multiple criteria applied in order:
1. **Pension Benefit Type Code** (TYPE_PENSION_BNFT_CD): Codes 2Q (leveraged ESOP),
   2R, 2S, 2T, 3H (stock bonus)
2. **ESOP Indicator Field** (p_ESOP_IND = "Y")
3. **Plan Characteristic Codes**: Matching ESOP-related codes in the plan features list
4. **Plan Name Search**: Pattern matching for "ESOP", "Employee Stock Ownership",
   "Stock Bonus Plan", and "Leveraged ESOP"

Plans containing both ESOP and 401(k) elements are flagged as **KSOPs** (included in
ESOP counts but separately identified).

### Geographic Filtering
Massachusetts plans are identified by the sponsor's mailing state (SPONS_DFE_MAIL_US_STATE = "MA").

### Financial Data
Asset and contribution figures are sourced from the main Form 5500 filing and supplemented
with Schedule H (for large plans, 100+ participants) and Schedule I (for small plans, <100
participants) data where available. **Employer securities** data (company stock held in the ESOP
trust) is extracted from Schedule H Part I, line 1c — this is unique to ESOPs and represents
the core ESOP asset.

Schedule H/I data can be loaded from pre-downloaded CSV files placed in `data/form5500/`.

### Years Covered
Data covers filing years **2014–2024** (11 filing years available as bulk downloads from DOL).

### Caveats
- Some plans may be missed if they don't use standard ESOP type codes
- KSOP plans (combined 401(k)/ESOP) are included but flagged separately
- Participant counts may include both active and retired participants
- Asset values represent beginning-of-year or end-of-year figures depending on the filing
- Some large companies file multiple plans (counted as separate plans)
- Late filers from recent years may not be captured in the current dataset

### Reproduction
To rebuild this dataset, run: `python3 process_form5500.py --import-to-db`
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
    ],
    "Northeast MA": [
        "Lowell", "Tewksbury", "N. Chelmsford", "Andover", "North Reading",
        "Danvers", "Beverly", "Ipswich", "Newburyport", "Amesbury", "Lynn",
    ],
    "MetroWest & Central MA": [
        "Worcester", "Marlborough", "Westford", "Littleton", "Devens",
        "Maynard", "Concord", "Hopkinton", "Milford", "Grafton", "Uxbridge",
        "Oxford", "S. Lancaster", "Clinton", "West Boylston",
    ],
    "Western MA": [
        "Springfield", "Holyoke", "Greenfield", "Northampton", "Amherst",
        "Hadley", "Easthampton", "Chicopee", "Palmer", "Orange", "Pittsfield",
        "Haydenville", "Belchertown", "West Hatfield", "Indian Orchard", "Athol",
        "Westfield",
    ],
    "Southeast MA & Cape": [
        "Fall River", "New Bedford", "W. Bridgewater", "Pembroke", "Plymouth",
        "Mashpee", "West Yarmouth", "Vineyard Haven", "West Tisbury",
        "Mansfield", "Franklin",
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
