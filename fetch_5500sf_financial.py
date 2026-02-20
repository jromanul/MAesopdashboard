"""
Fetch financial data from Form 5500-SF (Short Form) filings for MA ESOPs
that are missing asset/financial data.

Small plans (<100 participants) file Form 5500-SF which includes financial
data directly on the form. This script downloads the DOL EFAST2 5500-SF
bulk data and extracts financial fields for our MA ESOP plans.

Also tries Schedule I data as a secondary source.
"""

import csv
import io
import os
import sqlite3
import ssl
import sys
import urllib.request
import zipfile

DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

# Financial field mappings — includes SF_ prefixed fields from 5500-SF
FINANCIAL_FIELDS = {
    "total_assets": [
        "SF_TOT_ASSETS_EOY_AMT", "TOT_ASSETS_EOY_AMT", "TOTAL_ASSETS_EOY_AMT",
        "SF_TOT_ASSETS_BOY_AMT", "TOT_ASSETS_BOY_AMT", "TOTAL_ASSETS_BOY_AMT",
        "SF_NET_ASSETS_EOY_AMT", "NET_ASSETS_EOY_AMT",
    ],
    "total_liabilities": [
        "SF_TOT_LIABILITIES_EOY_AMT", "TOT_LIABILITIES_EOY_AMT", "TOTAL_LIABILITIES_EOY_AMT",
        "SF_TOT_LIABILITIES_BOY_AMT", "TOT_LIABILITIES_BOY_AMT", "TOTAL_LIABILITIES_BOY_AMT",
    ],
    "employer_contributions": [
        "SF_EMPLR_CONTRIB_INCOME_AMT", "EMPLR_CONTRIB_INCOME_AMT", "EMPLR_CONTRIB_AMT",
        "EMPLOYER_CONTRIB_AMT", "EMPLR_CONTRIB_CUR_YR_AMT",
    ],
    "participant_contributions": [
        "SF_PARTICIP_CONTRIB_INCOME_AMT", "PARTICIPANT_CONTRIB_AMT", "PARTCP_CONTRIB_AMT",
        "PARTCPNT_CONTRIB_AMT", "EE_CONTRIB_AMT",
    ],
    "benefits_paid": [
        "SF_TOT_DISTRIB_BNFT_AMT", "TOT_DISTRIB_BNFT_AMT", "BENEFIT_PAYMENT_AMT",
        "BENEFITS_PAID_AMT", "TOTAL_DISTRIB_BNFT_AMT",
    ],
    "net_income": [
        "SF_NET_INCOME_AMT", "NET_INCOME_AMT", "NET_INCM_AMT", "NET_GAIN_LOSS_AMT",
    ],
    "employer_securities": [
        "EMPLR_SEC_EOY_AMT", "EMPLR_SECURITIES_EOY_AMT",
        "EMPLOYER_SECURITIES_EOY_AMT",
        "EMPLR_SEC_BOY_AMT", "EMPLR_SECURITIES_BOY_AMT",
    ],
}

# EIN and Plan Number fields — includes SF_ prefixed from 5500-SF
EIN_FIELDS = ["SF_SPONS_EIN", "SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN",
              "SCH_I_EIN", "SF_LAST_RPT_SPONS_EIN"]
PN_FIELDS = ["SF_PLAN_NUM", "PLAN_NUM", "PN", "SCH_I_PN", "SF_LAST_RPT_PLAN_NUM"]


def normalize_ein(ein_raw):
    if not ein_raw:
        return ""
    return str(ein_raw).strip().lstrip("0") or "0"


def normalize_pn(pn_raw):
    if not pn_raw:
        return ""
    return str(pn_raw).strip().lstrip("0") or "0"


def safe_money(val):
    if val is None:
        return None
    try:
        s = str(val).strip().replace(",", "").replace("$", "")
        if not s or s in ("-", ".", "N/A", "", "nan"):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def get_field(row, candidates):
    for c in candidates:
        if c in row and row[c]:
            v = str(row[c]).strip()
            if v and v not in ("", "nan", "None"):
                return v
        for k in row:
            if k.upper() == c.upper() and row[k]:
                v = str(row[k]).strip()
                if v and v not in ("", "nan", "None"):
                    return v
    return None


def get_missing_plans():
    """Get EIN/PN keys for plans missing financial data."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT ein, plan_num, filing_year
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
          AND total_assets IS NULL
    """).fetchall()
    conn.close()

    missing = {}
    for r in rows:
        key = (normalize_ein(r["ein"]), normalize_pn(r["plan_num"]))
        if key not in missing:
            missing[key] = []
        missing[key].append(r["filing_year"])
    return missing


def download_and_extract(url, target_eins_pns):
    """Download a DOL zip file, extract CSV, and find financial data for target EINs."""
    results = {}

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MassCEO-Research/1.0"
        })

        with urllib.request.urlopen(req, context=ctx, timeout=180) as resp:
            zip_data = resp.read()
            print(f"    Downloaded {len(zip_data) / 1e6:.1f} MB")

        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_files:
                print("    No CSV found in zip")
                return results

            csv_name = csv_files[0]
            print(f"    Extracting: {csv_name}")

            with zf.open(csv_name) as f:
                text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
                reader = csv.DictReader(text)

                headers = reader.fieldnames
                if headers:
                    print(f"    Columns ({len(headers)}): {headers[:8]}...")

                # Find EIN and PN columns
                ein_col = None
                pn_col = None
                for h in headers:
                    hu = h.upper().strip()
                    if hu in [x.upper() for x in EIN_FIELDS]:
                        ein_col = h
                    if hu in [x.upper() for x in PN_FIELDS]:
                        pn_col = h

                if not ein_col:
                    print(f"    WARNING: No EIN column found!")
                    return results

                print(f"    EIN col: {ein_col}, PN col: {pn_col}")

                matched = 0
                for row in reader:
                    ein_raw = normalize_ein(row.get(ein_col, ""))
                    pn_raw = normalize_pn(row.get(pn_col, "")) if pn_col else ""

                    key = (ein_raw, pn_raw)
                    if key not in target_eins_pns:
                        # Try matching just by EIN (some forms may have different PN format)
                        ein_only_match = False
                        for tk in target_eins_pns:
                            if tk[0] == ein_raw:
                                key = tk
                                ein_only_match = True
                                break
                        if not ein_only_match:
                            continue

                    # Extract financial fields
                    fin_data = {}
                    for field_name, candidates in FINANCIAL_FIELDS.items():
                        val = safe_money(get_field(row, candidates))
                        if val is not None:
                            fin_data[field_name] = val

                    if fin_data:
                        results[key] = fin_data
                        matched += 1

                print(f"    Matched {matched} plans with financial data")

    except urllib.error.HTTPError as e:
        print(f"    HTTP {e.code}: {url}")
    except Exception as e:
        print(f"    Error: {e}")

    return results


def update_database(all_results):
    """Update the database with financial data for plans that are missing it."""
    conn = sqlite3.connect(DB_PATH)
    updated_total = 0

    for year, results in all_results.items():
        for (ein, pn), fin_data in results.items():
            if not fin_data:
                continue

            set_clauses = []
            params = []
            for fname, fval in fin_data.items():
                set_clauses.append(f"{fname} = ?")
                params.append(fval)

            # Only update where total_assets IS NULL (don't overwrite Schedule H data)
            params.extend([ein, year])

            # Build WHERE clause - handle EIN matching with leading zeros stripped
            where_parts = ["sponsor_state = 'MA'", "is_esop = 1",
                           "total_assets IS NULL", "filing_year = ?"]

            sql = f"""
                UPDATE form5500_filings
                SET {', '.join(set_clauses)}
                WHERE sponsor_state = 'MA' AND is_esop = 1
                  AND total_assets IS NULL AND filing_year = ?
                  AND (ein = ? OR LTRIM(ein, '0') = ?)
            """
            params.extend([ein, ein])

            cursor = conn.execute(sql, params)
            if cursor.rowcount > 0:
                updated_total += cursor.rowcount

    conn.commit()
    conn.close()
    return updated_total


def main():
    print("=" * 60)
    print("Fetching Financial Data from Form 5500-SF (Short Form)")
    print("=" * 60)

    missing = get_missing_plans()
    print(f"\nFound {len(missing)} unique EIN/PN combinations missing financial data")

    if not missing:
        print("No plans missing financial data!")
        return

    # Get all years that have missing data
    all_missing_years = set()
    for key, years in missing.items():
        all_missing_years.update(years)
    all_missing_years = sorted(all_missing_years)
    print(f"Years with missing data: {all_missing_years}")

    all_results = {}

    # Try Form 5500-SF
    print("\n--- Downloading Form 5500-SF data ---")
    for year in sorted(all_missing_years, reverse=True):
        print(f"\n  Year {year}:")
        urls = [
            f"https://askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_5500_SF_{year}_Latest.zip",
            f"https://askebsa.dol.gov/FOIA%20Files/{year}/All/F_5500_SF_{year}_All.zip",
        ]
        for url in urls:
            print(f"  Trying: {url}")
            results = download_and_extract(url, missing)
            if results:
                if year not in all_results:
                    all_results[year] = {}
                all_results[year].update(results)
                break

    # Also try Schedule I (small plans)
    print("\n--- Downloading Schedule I data ---")
    for year in sorted(all_missing_years, reverse=True):
        if year < 2020:
            continue
        print(f"\n  Year {year}:")
        urls = [
            f"https://askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_SCH_I_{year}_Latest.zip",
            f"https://askebsa.dol.gov/FOIA%20Files/{year}/All/F_SCH_I_{year}_All.zip",
        ]
        for url in urls:
            print(f"  Trying: {url}")
            results = download_and_extract(url, missing)
            if results:
                if year not in all_results:
                    all_results[year] = {}
                # Only add if not already covered by 5500-SF
                for key, data in results.items():
                    if key not in all_results[year]:
                        all_results[year][key] = data
                break

    # Summary of what we found
    total_found = sum(len(r) for r in all_results.values())
    print(f"\n--- Summary ---")
    print(f"Total plans with new financial data: {total_found}")
    for year in sorted(all_results.keys()):
        print(f"  {year}: {len(all_results[year])} plans")

    if total_found > 0:
        print(f"\n--- Updating database ---")
        updated = update_database(all_results)
        print(f"Updated {updated} filing records with financial data")

        # Backfill older years from newer data
        print(f"\n--- Backfilling older years ---")
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row

        # Get plans still missing data
        still_missing = conn.execute("""
            SELECT id, ein, plan_num, filing_year
            FROM form5500_filings
            WHERE sponsor_state = 'MA' AND is_esop = 1
              AND total_assets IS NULL
        """).fetchall()

        backfill_count = 0
        for row in still_missing:
            ein = row["ein"]
            pn = row["plan_num"]
            year = row["filing_year"]

            # Find the nearest year with data for this EIN/PN
            nearest = conn.execute("""
                SELECT total_assets, total_liabilities, employer_securities,
                       employer_contributions, benefits_paid, net_income
                FROM form5500_filings
                WHERE ein = ? AND plan_num = ? AND sponsor_state = 'MA'
                  AND is_esop = 1 AND total_assets IS NOT NULL
                ORDER BY ABS(filing_year - ?) ASC
                LIMIT 1
            """, (ein, pn, year)).fetchone()

            if nearest and nearest["total_assets"]:
                conn.execute("""
                    UPDATE form5500_filings
                    SET total_assets = ?, total_liabilities = ?,
                        employer_securities = ?, employer_contributions = ?,
                        benefits_paid = ?, net_income = ?
                    WHERE id = ?
                """, (nearest["total_assets"], nearest["total_liabilities"],
                      nearest["employer_securities"], nearest["employer_contributions"],
                      nearest["benefits_paid"], nearest["net_income"],
                      row["id"]))
                backfill_count += 1

        conn.commit()
        conn.close()
        print(f"Backfilled {backfill_count} records from neighboring years")
    else:
        print("No new financial data found.")

    # Final stats
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("""
        SELECT COUNT(*) FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = 2024
    """).fetchone()[0]
    with_data = conn.execute("""
        SELECT COUNT(*) FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = 2024
          AND total_assets IS NOT NULL
    """).fetchone()[0]
    conn.close()

    print(f"\n{'=' * 60}")
    print(f"Final 2024 Data Coverage:")
    print(f"  Plans with financial data: {with_data}/{total} ({with_data/total*100:.1f}%)")
    print(f"  Plans still missing: {total - with_data}/{total}")
    print("Done!")


if __name__ == "__main__":
    main()
