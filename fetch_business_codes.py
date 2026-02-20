"""
Fetch NAICS business codes from DOL Form 5500 bulk data for existing MA ESOPs.
Downloads the main F_5500 CSV for each year, extracts BUSINESS_CD for our EINs,
and updates the database.
"""

import csv
import io
import os
import sqlite3
import sys
import zipfile
import urllib.request
import ssl

DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

# NAICS 2-digit sector mapping
NAICS_SECTORS = {
    "11": "Agriculture, Forestry, Fishing",
    "21": "Mining, Oil & Gas",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing",
    "32": "Manufacturing",
    "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade",
    "45": "Retail Trade",
    "48": "Transportation & Warehousing",
    "49": "Transportation & Warehousing",
    "51": "Information & Technology",
    "52": "Finance & Insurance",
    "53": "Real Estate",
    "54": "Professional & Technical Services",
    "55": "Management of Companies",
    "56": "Administrative & Waste Services",
    "61": "Education",
    "62": "Healthcare & Social Assistance",
    "71": "Arts, Entertainment, Recreation",
    "72": "Accommodation & Food Services",
    "81": "Other Services",
    "92": "Public Administration",
}


def naics_to_sector(code):
    """Convert a NAICS/business code to a sector name."""
    if not code or len(str(code).strip()) < 2:
        return ""
    prefix = str(code).strip()[:2]
    return NAICS_SECTORS.get(prefix, f"Other ({prefix})")


def get_ma_esop_eins():
    """Get all unique EINs and their filing years from the database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT ein, filing_year
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
        ORDER BY filing_year
    """).fetchall()
    conn.close()

    ein_years = {}
    for r in rows:
        ein = r["ein"]
        yr = r["filing_year"]
        if ein not in ein_years:
            ein_years[ein] = []
        ein_years[ein].append(yr)
    return ein_years


def download_and_extract_business_codes(year, target_eins):
    """Download DOL Form 5500 data for a year and extract business codes for target EINs."""

    # Try multiple URL patterns
    url_patterns = [
        f"https://askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_5500_{year}_Latest.zip",
        f"https://askebsa.dol.gov/FOIA%20Files/{year}/All/F_5500_{year}_All.zip",
    ]

    # Also try the short form (5500-SF) which small plans use
    sf_url_patterns = [
        f"https://askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_5500_SF_{year}_Latest.zip",
        f"https://askebsa.dol.gov/FOIA%20Files/{year}/All/F_5500_SF_{year}_All.zip",
    ]

    results = {}

    for url_set, form_type in [(url_patterns, "5500"), (sf_url_patterns, "5500-SF")]:
        for url in url_set:
            try:
                print(f"  Trying {form_type}: {url}")

                # Create SSL context that doesn't verify (DOL site sometimes has cert issues)
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MassCEO-Research/1.0"
                })

                with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
                    zip_data = resp.read()
                    print(f"  Downloaded {len(zip_data) / 1e6:.1f} MB")

                # Extract CSV from zip
                with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                    csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                    if not csv_files:
                        print(f"  No CSV found in zip")
                        continue

                    csv_name = csv_files[0]
                    print(f"  Extracting: {csv_name}")

                    with zf.open(csv_name) as f:
                        text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                        reader = csv.DictReader(text)

                        # Find the EIN and business code columns
                        headers = reader.fieldnames
                        print(f"  Columns ({len(headers)}): {headers[:10]}...")

                        ein_col = None
                        biz_col = None
                        for h in headers:
                            h_upper = h.upper().strip()
                            if h_upper in ("SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN"):
                                ein_col = h
                            if h_upper in ("BUSINESS_CD", "BUSINESS_CODE", "NAICS_CD"):
                                biz_col = h

                        if not ein_col:
                            print(f"  WARNING: No EIN column found! Headers: {headers}")
                            continue
                        if not biz_col:
                            print(f"  WARNING: No business code column found! Headers: {headers}")
                            # Still continue - show what columns exist
                            continue

                        print(f"  EIN column: {ein_col}, Business code column: {biz_col}")

                        matched = 0
                        for row in reader:
                            ein_raw = str(row.get(ein_col, "")).strip().lstrip("0")
                            if ein_raw in target_eins:
                                biz_code = str(row.get(biz_col, "")).strip()
                                if biz_code and biz_code != "nan" and biz_code != "":
                                    results[ein_raw] = biz_code
                                    matched += 1

                        print(f"  Matched {matched} EINs with business codes")

                # If we got results from the main form, break (don't need to try 'All')
                if results:
                    break

            except urllib.error.HTTPError as e:
                print(f"  HTTP {e.code}: {url}")
                continue
            except Exception as e:
                print(f"  Error: {e}")
                continue

        # If main form got results, don't need short form
        if results:
            break

    return results


def update_database(business_codes_by_year):
    """Update the database with business codes and industry sectors."""
    conn = sqlite3.connect(DB_PATH)

    updated = 0
    for year, codes in business_codes_by_year.items():
        for ein, biz_code in codes.items():
            sector = naics_to_sector(biz_code)
            cursor = conn.execute("""
                UPDATE form5500_filings
                SET naics_code = ?, industry_sector = ?
                WHERE ein = ? AND filing_year = ?
                  AND sponsor_state = 'MA' AND is_esop = 1
            """, (biz_code, sector, ein, year))
            updated += cursor.rowcount

    conn.commit()
    conn.close()
    return updated


def main():
    print("=" * 60)
    print("Fetching NAICS Business Codes from DOL Form 5500 Bulk Data")
    print("=" * 60)

    # Get our EINs
    ein_years = get_ma_esop_eins()
    all_eins = set(ein_years.keys())
    print(f"\nFound {len(all_eins)} unique MA ESOP EINs across {len(set(y for yrs in ein_years.values() for y in yrs))} years")

    # Get distinct years
    all_years = sorted(set(y for yrs in ein_years.values() for y in yrs))
    print(f"Years to process: {all_years}")

    # For efficiency, let's just process the most recent few years
    # Business codes rarely change, so we can backfill from the latest filing
    recent_years = [y for y in all_years if y >= 2020]
    print(f"\nProcessing recent years first: {recent_years}")

    business_codes_by_year = {}
    all_known_codes = {}  # EIN -> business_code (latest known)

    for year in sorted(recent_years, reverse=True):  # newest first
        print(f"\n--- Year {year} ---")
        codes = download_and_extract_business_codes(year, all_eins)
        business_codes_by_year[year] = codes

        # Track latest known code per EIN
        for ein, code in codes.items():
            if ein not in all_known_codes:
                all_known_codes[ein] = code

        print(f"  Total unique EINs with codes so far: {len(all_known_codes)}")

    # For older years, use the known codes (backfill)
    older_years = [y for y in all_years if y < 2020]
    if older_years:
        print(f"\n--- Backfilling older years {older_years} with known codes ---")
        for year in older_years:
            backfill = {}
            for ein in all_eins:
                if ein in all_known_codes:
                    backfill[ein] = all_known_codes[ein]
            business_codes_by_year[year] = backfill
            print(f"  Year {year}: backfilled {len(backfill)} EINs")

    # Update database
    print(f"\n--- Updating database ---")
    updated = update_database(business_codes_by_year)
    print(f"Updated {updated} filing records with business codes")

    # Summary
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT industry_sector, COUNT(*) as cnt
        FROM form5500_filings
        WHERE sponsor_state='MA' AND is_esop=1 AND filing_year=2024
          AND industry_sector IS NOT NULL AND industry_sector != ''
        GROUP BY industry_sector
        ORDER BY cnt DESC
    """).fetchall()
    conn.close()

    print(f"\n{'=' * 60}")
    print(f"Industry Sector Distribution (2024 MA ESOPs):")
    print(f"{'=' * 60}")
    for r in rows:
        print(f"  {r['industry_sector']}: {r['cnt']} plans")

    total_with = sum(r['cnt'] for r in rows)
    print(f"\nTotal with industry data: {total_with}")
    print("Done!")


if __name__ == "__main__":
    main()
