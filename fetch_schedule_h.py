"""
Fetch financial data from Schedule H filings for MA ESOPs
that are missing asset/financial data.

Plans with 100+ participants file Form 5500 (full form) and attach Schedule H
which contains detailed financial data. This script downloads the DOL EFAST2
Schedule H bulk data and extracts financial fields for our MA ESOP plans.

Also tries 5500-SF (short form) and Schedule I as secondary sources for
smaller plans.

DOL EFAST2 URLs:
  Schedule H:  https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_SCH_H_{year}_Latest.zip
  5500-SF:     https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_5500_SF_{year}_Latest.zip
  Schedule I:  https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_SCH_I_{year}_Latest.zip
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

# ── Schedule H financial field mappings ──
# Schedule H columns use SCH_H_ prefix
SCH_H_FIELDS = {
    "total_assets": [
        "SCH_H_TOT_ASSETS_EOY_AMT",
        "TOT_ASSETS_EOY_AMT",
        "TOTAL_ASSETS_EOY_AMT",
        "NET_ASSETS_EOY_AMT",
        "SCH_H_NET_ASSETS_EOY_AMT",
    ],
    "total_liabilities": [
        "SCH_H_TOT_LIABILITIES_EOY_AMT",
        "TOT_LIABILITIES_EOY_AMT",
        "TOTAL_LIABILITIES_EOY_AMT",
    ],
    "employer_contributions": [
        "SCH_H_RCVBL_EMPLR_CONTRIB_EOY_AMT",
        "SCH_H_EMPLR_CONTRIB_INCOME_AMT",
        "EMPLR_CONTRIB_INCOME_AMT",
        "EMPLR_CONTRIB_AMT",
    ],
    "participant_contributions": [
        "SCH_H_PARTICIP_CONTRIB_INCOME_AMT",
        "PARTICIPANT_CONTRIB_AMT",
        "PARTCP_CONTRIB_AMT",
    ],
    "benefits_paid": [
        "SCH_H_TOT_DISTRIB_BNFT_AMT",
        "TOT_DISTRIB_BNFT_AMT",
        "BENEFIT_PAYMENT_AMT",
    ],
    "net_income": [
        "SCH_H_NET_INCOME_AMT",
        "NET_INCOME_AMT",
        "NET_INCM_AMT",
    ],
    "employer_securities": [
        "SCH_H_EMPLR_SEC_EOY_AMT",
        "EMPLR_SEC_EOY_AMT",
        "EMPLR_SECURITIES_EOY_AMT",
        "EMPLOYER_SECURITIES_EOY_AMT",
    ],
}

# ── 5500-SF financial field mappings ──
SF_FIELDS = {
    "total_assets": [
        "SF_TOT_ASSETS_EOY_AMT", "TOT_ASSETS_EOY_AMT",
        "SF_NET_ASSETS_EOY_AMT", "NET_ASSETS_EOY_AMT",
    ],
    "total_liabilities": [
        "SF_TOT_LIABILITIES_EOY_AMT", "TOT_LIABILITIES_EOY_AMT",
    ],
    "employer_contributions": [
        "SF_EMPLR_CONTRIB_INCOME_AMT", "EMPLR_CONTRIB_INCOME_AMT",
    ],
    "participant_contributions": [
        "SF_PARTICIP_CONTRIB_INCOME_AMT", "PARTICIPANT_CONTRIB_AMT",
    ],
    "benefits_paid": [
        "SF_TOT_DISTRIB_BNFT_AMT", "TOT_DISTRIB_BNFT_AMT",
    ],
    "net_income": [
        "SF_NET_INCOME_AMT", "NET_INCOME_AMT",
    ],
    "employer_securities": [
        "EMPLR_SEC_EOY_AMT", "EMPLR_SECURITIES_EOY_AMT",
    ],
}

# ── Schedule I financial field mappings ──
SCH_I_FIELDS = {
    "total_assets": [
        "SCH_I_TOT_ASSETS_EOY_AMT", "TOT_ASSETS_EOY_AMT",
    ],
    "total_liabilities": [
        "SCH_I_TOT_LIABILITIES_EOY_AMT", "TOT_LIABILITIES_EOY_AMT",
    ],
    "employer_contributions": [
        "SCH_I_EMPLR_CONTRIB_AMT", "EMPLR_CONTRIB_AMT",
    ],
    "participant_contributions": [
        "SCH_I_PARTICIP_CONTRIB_AMT", "PARTICIPANT_CONTRIB_AMT",
    ],
    "benefits_paid": [
        "SCH_I_TOT_DISTRIB_BNFT_AMT", "TOT_DISTRIB_BNFT_AMT",
    ],
    "net_income": [
        "SCH_I_NET_INCOME_AMT", "NET_INCOME_AMT",
    ],
    "employer_securities": [
        "SCH_I_EMPLR_SEC_EOY_AMT", "EMPLR_SEC_EOY_AMT",
    ],
}

# ── EIN / Plan Number column candidates per form type ──
EIN_COLS = {
    "sch_h": ["SCH_H_EIN", "SPONS_DFE_EIN", "EIN"],
    "sf":    ["SF_SPONS_EIN", "SF_LAST_RPT_SPONS_EIN", "SPONS_DFE_EIN", "EIN"],
    "sch_i": ["SCH_I_EIN", "SPONS_DFE_EIN", "EIN"],
}
PN_COLS = {
    "sch_h": ["SCH_H_PN", "PLAN_NUM", "PN"],
    "sf":    ["SF_PLAN_NUM", "SF_LAST_RPT_PLAN_NUM", "PLAN_NUM", "PN"],
    "sch_i": ["SCH_I_PN", "PLAN_NUM", "PN"],
}


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


def find_column(headers, candidates):
    """Find the first matching column name (case-insensitive)."""
    headers_upper = {h.upper().strip(): h for h in headers}
    for c in candidates:
        if c.upper() in headers_upper:
            return headers_upper[c.upper()]
    return None


def get_field(row, candidates):
    """Get the first non-empty value from candidate column names."""
    for c in candidates:
        # Exact match
        if c in row and row[c]:
            v = str(row[c]).strip()
            if v and v not in ("", "nan", "None"):
                return v
        # Case-insensitive match
        for k in row:
            if k.upper() == c.upper() and row[k]:
                v = str(row[k]).strip()
                if v and v not in ("", "nan", "None"):
                    return v
    return None


def get_missing_plans():
    """Get EIN/PN keys for plans missing financial data, organized by year."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT DISTINCT ein, plan_num, filing_year
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
          AND total_assets IS NULL
    """).fetchall()
    conn.close()

    # Build lookup: (normalized_ein, normalized_pn) -> [years]
    missing = {}
    for r in rows:
        key = (normalize_ein(r["ein"]), normalize_pn(r["plan_num"]))
        if key not in missing:
            missing[key] = []
        missing[key].append(r["filing_year"])
    return missing


def download_and_extract(url, target_eins_pns, ein_candidates, pn_candidates, field_map):
    """Download a DOL zip file, extract CSV, and find financial data for target EINs."""
    results = {}

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MassCEO-Research/1.0"
        })

        print(f"    Downloading...")
        with urllib.request.urlopen(req, context=ctx, timeout=300) as resp:
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

                headers = reader.fieldnames or []
                print(f"    Columns ({len(headers)}): {headers[:10]}...")

                # Find EIN and PN columns
                ein_col = find_column(headers, ein_candidates)
                pn_col = find_column(headers, pn_candidates)

                if not ein_col:
                    print(f"    WARNING: No EIN column found in {headers[:20]}")
                    return results

                print(f"    Using EIN col: {ein_col}, PN col: {pn_col}")

                matched = 0
                scanned = 0
                for row in reader:
                    scanned += 1
                    ein_raw = normalize_ein(row.get(ein_col, ""))
                    pn_raw = normalize_pn(row.get(pn_col, "")) if pn_col else ""

                    key = (ein_raw, pn_raw)
                    if key not in target_eins_pns:
                        # Try matching just by EIN
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
                    for field_name, candidates in field_map.items():
                        val = safe_money(get_field(row, candidates))
                        if val is not None:
                            fin_data[field_name] = val

                    if fin_data:
                        # If we already have data for this key, keep the one with more fields
                        if key in results and len(results[key]) >= len(fin_data):
                            continue
                        results[key] = fin_data
                        matched += 1

                print(f"    Scanned {scanned:,} rows, matched {matched} plans with financial data")

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

            # Only update where total_assets IS NULL (don't overwrite existing data)
            sql = f"""
                UPDATE form5500_filings
                SET {', '.join(set_clauses)}
                WHERE sponsor_state = 'MA' AND is_esop = 1
                  AND total_assets IS NULL AND filing_year = ?
                  AND (LTRIM(ein, '0') = ? OR ein = ?)
            """
            params.extend([year, ein, ein])

            cursor = conn.execute(sql, params)
            if cursor.rowcount > 0:
                updated_total += cursor.rowcount
                print(f"      Updated EIN={ein}, PN={pn}, Year={year}: {list(fin_data.keys())}")

    conn.commit()
    conn.close()
    return updated_total


def backfill_from_neighbors():
    """For plans still missing data, copy from nearest year that has data."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

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
    return backfill_count


def main():
    print("=" * 70)
    print("Fetching Financial Data from Schedule H, 5500-SF, and Schedule I")
    print("=" * 70)

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

    # ── 1. Schedule H (primary source — large plans with 100+ participants) ──
    print("\n" + "=" * 70)
    print("PHASE 1: Downloading Schedule H data (primary source)")
    print("=" * 70)
    for year in sorted(all_missing_years, reverse=True):
        print(f"\n  Year {year}:")
        urls = [
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_SCH_H_{year}_Latest.zip",
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/All/F_SCH_H_{year}_All.zip",
        ]
        for url in urls:
            print(f"  Trying: {url}")
            results = download_and_extract(
                url, missing,
                ein_candidates=EIN_COLS["sch_h"],
                pn_candidates=PN_COLS["sch_h"],
                field_map=SCH_H_FIELDS,
            )
            if results:
                if year not in all_results:
                    all_results[year] = {}
                all_results[year].update(results)
                print(f"  >> Found {len(results)} plans for {year}")
                break

    # ── 2. Form 5500-SF (small plans < 100 participants) ──
    print("\n" + "=" * 70)
    print("PHASE 2: Downloading Form 5500-SF data (small plans)")
    print("=" * 70)
    for year in sorted(all_missing_years, reverse=True):
        # Skip years already fully covered
        year_missing = {k: v for k, v in missing.items() if year in v}
        already_found = all_results.get(year, {})
        still_need = {k: v for k, v in year_missing.items() if k not in already_found}
        if not still_need:
            print(f"\n  Year {year}: All plans already covered by Schedule H")
            continue

        print(f"\n  Year {year}: {len(still_need)} plans still need data")
        urls = [
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_5500_SF_{year}_Latest.zip",
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/All/F_5500_SF_{year}_All.zip",
        ]
        for url in urls:
            print(f"  Trying: {url}")
            results = download_and_extract(
                url, still_need,
                ein_candidates=EIN_COLS["sf"],
                pn_candidates=PN_COLS["sf"],
                field_map=SF_FIELDS,
            )
            if results:
                if year not in all_results:
                    all_results[year] = {}
                for key, data in results.items():
                    if key not in all_results[year]:
                        all_results[year][key] = data
                print(f"  >> Found {len(results)} additional plans for {year}")
                break

    # ── 3. Schedule I (alternate small plan schedule) ──
    print("\n" + "=" * 70)
    print("PHASE 3: Downloading Schedule I data (alternate small plan)")
    print("=" * 70)
    for year in sorted(all_missing_years, reverse=True):
        if year < 2019:
            continue
        year_missing = {k: v for k, v in missing.items() if year in v}
        already_found = all_results.get(year, {})
        still_need = {k: v for k, v in year_missing.items() if k not in already_found}
        if not still_need:
            print(f"\n  Year {year}: All plans already covered")
            continue

        print(f"\n  Year {year}: {len(still_need)} plans still need data")
        urls = [
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/Latest/F_SCH_I_{year}_Latest.zip",
            f"https://www.askebsa.dol.gov/FOIA%20Files/{year}/All/F_SCH_I_{year}_All.zip",
        ]
        for url in urls:
            print(f"  Trying: {url}")
            results = download_and_extract(
                url, still_need,
                ein_candidates=EIN_COLS["sch_i"],
                pn_candidates=PN_COLS["sch_i"],
                field_map=SCH_I_FIELDS,
            )
            if results:
                if year not in all_results:
                    all_results[year] = {}
                for key, data in results.items():
                    if key not in all_results[year]:
                        all_results[year][key] = data
                print(f"  >> Found {len(results)} additional plans for {year}")
                break

    # ── Summary ──
    total_found = sum(len(r) for r in all_results.values())
    print(f"\n{'=' * 70}")
    print(f"SUMMARY: Found financial data for {total_found} plan-years")
    print(f"{'=' * 70}")
    for year in sorted(all_results.keys()):
        print(f"  {year}: {len(all_results[year])} plans")

    if total_found > 0:
        print(f"\n--- Updating database ---")
        updated = update_database(all_results)
        print(f"Updated {updated} filing records with financial data")

        print(f"\n--- Backfilling from neighboring years ---")
        backfill_count = backfill_from_neighbors()
        print(f"Backfilled {backfill_count} records from neighboring years")
    else:
        print("No new financial data found.")

    # ── Final stats ──
    conn = sqlite3.connect(DB_PATH)
    print(f"\n{'=' * 70}")
    print("FINAL DATA COVERAGE:")
    print(f"{'=' * 70}")
    for yr in sorted(all_missing_years):
        total = conn.execute("""
            SELECT COUNT(*) FROM form5500_filings
            WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = ?
        """, (yr,)).fetchone()[0]
        with_data = conn.execute("""
            SELECT COUNT(*) FROM form5500_filings
            WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = ?
              AND total_assets IS NOT NULL
        """, (yr,)).fetchone()[0]
        pct = (with_data / total * 100) if total > 0 else 0
        bar = "#" * int(pct / 2) + "-" * (50 - int(pct / 2))
        print(f"  {yr}: {with_data:3d}/{total:3d} ({pct:5.1f}%) [{bar}]")

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
