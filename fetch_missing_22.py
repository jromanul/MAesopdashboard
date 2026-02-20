"""
Targeted fetch for the 22 MA ESOP plans still missing financial data.

Strategy:
1. Download F_5500_2024_Latest.zip (the main Form 5500 filing data) to check
   what form type these plans actually filed and get their ACK_IDs.
2. Search Schedule H, 5500-SF, and Schedule I using both padded and unpadded EINs.
3. Also try the "All" variants (which include amended filings).
"""

import csv
import io
import os
import sqlite3
import ssl
import urllib.request
import zipfile

DB_PATH = os.path.join(os.path.dirname(__file__) or ".", "data", "form5500_dashboard.db")

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


def download_zip_csv(url):
    """Download a zip file and return csv.DictReader for the CSV inside."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) MassCEO-Research/1.0"
    })

    print(f"  Downloading {url}...")
    with urllib.request.urlopen(req, context=ctx, timeout=300) as resp:
        zip_data = resp.read()
        print(f"  Downloaded {len(zip_data) / 1e6:.1f} MB")

    zf = zipfile.ZipFile(io.BytesIO(zip_data))
    csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if not csv_files:
        print("  No CSV found in zip")
        return None, None, None

    csv_name = csv_files[0]
    print(f"  Extracting: {csv_name}")
    f = zf.open(csv_name)
    text = io.TextIOWrapper(f, encoding="utf-8", errors="replace")
    reader = csv.DictReader(text)
    return reader, zf, f


def get_missing_eins():
    """Get the 22 missing plan EINs in multiple formats."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    missing = conn.execute("""
        SELECT ein, plan_num, plan_name, sponsor_name, total_participants
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = 2024
          AND total_assets IS NULL
        ORDER BY total_participants DESC
    """).fetchall()
    conn.close()

    plans = []
    for r in missing:
        ein_raw = r["ein"]
        ein_stripped = ein_raw.strip().lstrip("0") or "0"
        ein_padded = ein_raw.strip().zfill(9)
        plans.append({
            "ein_raw": ein_raw,
            "ein_stripped": ein_stripped,
            "ein_padded": ein_padded,
            "plan_num": r["plan_num"],
            "pn_padded": str(r["plan_num"]).strip().zfill(3),
            "plan_name": r["plan_name"],
            "sponsor_name": r["sponsor_name"],
            "participants": r["total_participants"],
        })

    return plans


def build_ein_lookup(plans):
    """Build a lookup dict with all EIN variants -> plan info."""
    lookup = {}
    for p in plans:
        for ein_variant in [p["ein_raw"], p["ein_stripped"], p["ein_padded"]]:
            ein_clean = ein_variant.strip()
            if ein_clean:
                lookup[ein_clean] = p
    return lookup


def search_main_5500(plans):
    """Search the main F_5500 filing to find what form type these plans filed."""
    print("\n" + "=" * 70)
    print("STEP 1: Checking main Form 5500 filing data for these EINs")
    print("=" * 70)

    ein_lookup = build_ein_lookup(plans)

    url = "https://www.askebsa.dol.gov/FOIA%20Files/2024/Latest/F_5500_2024_Latest.zip"
    reader, zf, f = download_zip_csv(url)
    if not reader:
        return {}

    headers = reader.fieldnames or []
    print(f"  Columns ({len(headers)}): {headers[:15]}...")

    # Find EIN and PN columns
    ein_col = None
    pn_col = None
    form_type_col = None
    ack_col = None

    for h in headers:
        hu = h.upper().strip()
        if hu in ("SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN"):
            ein_col = h
        if hu in ("PLAN_NUM", "PN"):
            pn_col = h
        if hu in ("FORM_PLAN_YEAR_BEGIN_DATE", "FORM_TYPE", "TYPE_PLAN_ENTITY_CD",
                   "SF_FILING_IND", "PLAN_ENTITY_CD"):
            if not form_type_col:
                form_type_col = h
        if hu == "ACK_ID":
            ack_col = h

    print(f"  EIN col: {ein_col}, PN col: {pn_col}")

    found = {}
    scanned = 0
    for row in reader:
        scanned += 1
        ein_raw = row.get(ein_col, "").strip()
        ein_stripped = ein_raw.lstrip("0") or "0"

        match = ein_lookup.get(ein_raw) or ein_lookup.get(ein_stripped)
        if not match:
            continue

        pn = row.get(pn_col, "").strip()
        ack = row.get(ack_col, "").strip() if ack_col else ""

        # Collect all columns for this row to understand the filing
        info = {
            "ack_id": ack,
            "ein_in_5500": ein_raw,
            "pn_in_5500": pn,
        }

        # Grab key columns
        for h in headers:
            hu = h.upper()
            if any(kw in hu for kw in ["TYPE", "FORM", "SF_", "FILING", "ADMIN",
                                        "PREPARER", "ENTITY", "PLAN_YEAR"]):
                val = row.get(h, "").strip()
                if val:
                    info[h] = val

        key = f"{ein_stripped}_{pn.lstrip('0')}"
        if key not in found:
            found[key] = info
            print(f"\n  FOUND: EIN={ein_raw} PN={pn} ACK={ack}")
            print(f"    Match: {match['sponsor_name']} - {match['plan_name'][:50]}")
            for k, v in sorted(info.items()):
                if k not in ("ack_id", "ein_in_5500", "pn_in_5500"):
                    print(f"    {k}: {v}")

    print(f"\n  Scanned {scanned:,} rows, found {len(found)} of our 22 plans")
    zf.close()
    return found


def search_schedule_h_with_ack(ack_ids):
    """Search Schedule H using ACK_IDs from the main form."""
    print("\n" + "=" * 70)
    print("STEP 2: Searching Schedule H by ACK_ID")
    print("=" * 70)

    url = "https://www.askebsa.dol.gov/FOIA%20Files/2024/Latest/F_SCH_H_2024_Latest.zip"
    reader, zf, f = download_zip_csv(url)
    if not reader:
        return {}

    headers = reader.fieldnames or []
    ack_col = None
    for h in headers:
        if h.upper().strip() == "ACK_ID":
            ack_col = h
            break

    if not ack_col:
        print("  No ACK_ID column found!")
        return {}

    print(f"  Looking for {len(ack_ids)} ACK_IDs...")

    found = {}
    for row in reader:
        ack = row.get(ack_col, "").strip()
        if ack in ack_ids:
            ein = row.get("SCH_H_EIN", "").strip()
            pn = row.get("SCH_H_PN", "").strip()

            # Extract financial fields
            fin_data = {}
            field_map = {
                "total_assets": ["TOT_ASSETS_EOY_AMT", "NET_ASSETS_EOY_AMT"],
                "total_liabilities": ["TOT_LIABILITIES_EOY_AMT"],
                "employer_contributions": ["EMPLR_CONTRIB_INCOME_AMT", "RCVBL_EMPLR_CONTRIB_EOY_AMT"],
                "benefits_paid": ["TOT_DISTRIB_BNFT_AMT"],
                "net_income": ["NET_INCOME_AMT", "NET_INCM_AMT"],
                "employer_securities": ["EMPLR_SEC_EOY_AMT"],
            }

            for fname, candidates in field_map.items():
                for c in candidates:
                    for k in row:
                        if c.upper() in k.upper():
                            val = safe_money(row[k])
                            if val is not None:
                                fin_data[fname] = val
                                break
                    if fname in fin_data:
                        break

            found[ack] = {"ein": ein, "pn": pn, "fin_data": fin_data}
            print(f"  FOUND in Sch H: ACK={ack} EIN={ein} PN={pn} Data={list(fin_data.keys())}")

    zf.close()
    return found


def deep_search_sf(plans):
    """Do a deep search of the 5500-SF using padded EINs."""
    print("\n" + "=" * 70)
    print("STEP 3: Deep search of 5500-SF with padded EINs")
    print("=" * 70)

    ein_lookup = build_ein_lookup(plans)

    # Also search by sponsor name keywords
    name_keywords = {}
    for p in plans:
        # Use first significant word of sponsor name
        name = p["sponsor_name"].upper().strip()
        words = [w for w in name.split() if len(w) > 3 and w not in ("INC.", "INC", "LLC", "CORP", "THE")]
        if words:
            name_keywords[words[0]] = p

    url = "https://www.askebsa.dol.gov/FOIA%20Files/2024/Latest/F_5500_SF_2024_Latest.zip"
    reader, zf, f = download_zip_csv(url)
    if not reader:
        return {}

    headers = reader.fieldnames or []

    # Find all possible EIN columns
    ein_cols = []
    pn_cols = []
    name_cols = []
    for h in headers:
        hu = h.upper()
        if "EIN" in hu:
            ein_cols.append(h)
        if "PLAN_NUM" in hu or hu == "PN":
            pn_cols.append(h)
        if "PLAN_NAME" in hu or "SPONS_NAME" in hu or "SPONSOR" in hu:
            name_cols.append(h)

    print(f"  EIN columns: {ein_cols}")
    print(f"  PN columns: {pn_cols}")
    print(f"  Name columns: {name_cols}")

    found = {}
    scanned = 0
    for row in reader:
        scanned += 1

        # Try all EIN columns
        matched_plan = None
        for ec in ein_cols:
            ein_val = row.get(ec, "").strip()
            if not ein_val:
                continue
            ein_stripped = ein_val.lstrip("0") or "0"

            match = ein_lookup.get(ein_val) or ein_lookup.get(ein_stripped)
            if match:
                matched_plan = match
                break

        if not matched_plan:
            continue

        # Check if this is a MA plan with ESOP
        state = ""
        for h in headers:
            if "STATE" in h.upper():
                state = row.get(h, "").strip().upper()
                if state == "MA":
                    break

        # Extract financial data
        fin_data = {}
        for h in headers:
            hu = h.upper()
            val = safe_money(row.get(h))
            if val is None:
                continue

            if "TOT_ASSETS" in hu and "EOY" in hu:
                fin_data["total_assets"] = val
            elif "NET_ASSETS" in hu and "EOY" in hu and "total_assets" not in fin_data:
                fin_data["total_assets"] = val
            elif "TOT_LIABILITIES" in hu and "EOY" in hu:
                fin_data["total_liabilities"] = val
            elif "EMPLR_CONTRIB" in hu:
                fin_data["employer_contributions"] = val
            elif "DISTRIB_BNFT" in hu:
                fin_data["benefits_paid"] = val
            elif "NET_INCOME" in hu or "NET_INCM" in hu:
                fin_data["net_income"] = val
            elif "EMPLR_SEC" in hu and "EOY" in hu:
                fin_data["employer_securities"] = val

        pn = ""
        for pc in pn_cols:
            pn = row.get(pc, "").strip()
            if pn:
                break

        plan_name_in_sf = ""
        for nc in name_cols:
            plan_name_in_sf = row.get(nc, "").strip()
            if plan_name_in_sf:
                break

        key = f"{matched_plan['ein_stripped']}_{matched_plan['plan_num']}"
        if fin_data and (key not in found or len(fin_data) > len(found.get(key, {}).get("fin_data", {}))):
            found[key] = {
                "ein": matched_plan["ein_padded"],
                "pn": pn,
                "fin_data": fin_data,
                "plan_name_in_sf": plan_name_in_sf,
            }
            print(f"  FOUND: {matched_plan['sponsor_name'][:40]} | EIN match | PN={pn}")
            print(f"    SF name: {plan_name_in_sf[:60]}")
            print(f"    Data: {fin_data}")

    print(f"\n  Scanned {scanned:,} rows, found {len(found)} plans")
    zf.close()
    return found


def update_database(results, year=2024):
    """Update the database with any found financial data."""
    if not results:
        print("\nNo new data to update.")
        return 0

    conn = sqlite3.connect(DB_PATH)
    updated = 0

    for key, info in results.items():
        fin_data = info.get("fin_data", {})
        if not fin_data or "total_assets" not in fin_data:
            continue

        ein_parts = key.split("_")
        ein = ein_parts[0]

        set_clauses = []
        params = []
        for fname, fval in fin_data.items():
            set_clauses.append(f"{fname} = ?")
            params.append(fval)

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
            updated += cursor.rowcount
            print(f"  Updated {cursor.rowcount} record(s) for EIN={ein}")

    conn.commit()
    conn.close()
    return updated


def main():
    print("=" * 70)
    print("TARGETED SEARCH: 22 Plans Missing Financial Data")
    print("=" * 70)

    plans = get_missing_eins()
    print(f"\nSearching for {len(plans)} plans:")
    for p in plans:
        print(f"  EIN={p['ein_padded']} PN={p['plan_num']} ({p['participants']} ptcp) {p['sponsor_name'][:40]}")

    # Step 1: Check main Form 5500 to understand these filings
    main_5500_info = search_main_5500(plans)

    # Collect ACK_IDs for Schedule H search
    ack_ids = set()
    for key, info in main_5500_info.items():
        if info.get("ack_id"):
            ack_ids.add(info["ack_id"])

    # Step 2: Search Schedule H using ACK_IDs
    sch_h_results = {}
    if ack_ids:
        sch_h_results = search_schedule_h_with_ack(ack_ids)

    # Step 3: Deep search 5500-SF with multiple EIN formats
    sf_results = deep_search_sf(plans)

    # Combine results
    all_results = {}
    for key, info in sf_results.items():
        all_results[key] = info
    for ack, info in sch_h_results.items():
        ein = info["ein"].lstrip("0")
        pn = info["pn"].lstrip("0")
        key = f"{ein}_{pn}"
        if key not in all_results or len(info["fin_data"]) > len(all_results.get(key, {}).get("fin_data", {})):
            all_results[key] = info

    print(f"\n{'=' * 70}")
    print(f"COMBINED RESULTS: Found data for {len(all_results)} plans")
    print(f"{'=' * 70}")
    for key, info in all_results.items():
        print(f"  {key}: {list(info['fin_data'].keys())}")

    # Update database
    if all_results:
        print(f"\n--- Updating database ---")
        updated = update_database(all_results)
        print(f"Updated {updated} records")

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
    still_missing = conn.execute("""
        SELECT plan_name, sponsor_name, total_participants
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = 2024
          AND total_assets IS NULL
        ORDER BY total_participants DESC
    """).fetchall()
    conn.close()

    print(f"\n{'=' * 70}")
    print(f"FINAL 2024 COVERAGE: {with_data}/{total} ({with_data/total*100:.1f}%)")
    print(f"{'=' * 70}")
    if still_missing:
        print(f"\nStill missing ({len(still_missing)} plans):")
        for r in still_missing:
            print(f"  {r[2]:>4d} ptcp - {r[1]} / {r[0][:50]}")

    print("\nDone!")


if __name__ == "__main__":
    main()
