"""
Re-derive MA ESOP plan financials directly from authoritative DOL EFAST2 filings.

DOL Form 5500 filings are the authoritative source. This script downloads the DOL
bulk Schedule H (large plans, >=100 participants) and Schedule I (small plans,
<100 participants) datasets, buckets every record by its real PLAN YEAR
(SCH_*_PLAN_YEAR_BEGIN_DATE), and overwrites each MA ESOP plan-year's financial
fields with the authoritative value.

Why this exists
---------------
An earlier pipeline captured small-plan (Schedule I) assets from the wrong field,
leaving ~640 plan-years materially understated (e.g. RCM Services showed $6,676 vs
the true $5.78M; Cape Cod Lumber $32M vs $71M). Re-deriving from DOL corrects them.

Rules (in priority order, per plan-year, matched on plan_year + EIN + plan_num):
  1. Schedule H value wins where present (audited large-plan form; the only form
     that itemizes employer securities, Part I line 1c).
  2. Otherwise Schedule I value (small plans; employer_securities is NOT reported
     on Schedule I, so it is set to NULL -> renders "—", never "$0").
  3. Participant counts are NEVER touched here (they come from the main Form 5500
     and are already accurate).
Integrity guards applied afterward (see form5500_analysis):
  - employer_securities that is 0/unconfirmed -> NULL
  - employer_securities > total_assets (impossible) -> NULL
  - cross-EIN duplicate rows (same plan filed under two EINs in a transition year)
    must be de-duplicated separately (see _find_cross_ein_dupes()).

Usage:
    python3 refetch_dol_financials.py            # download, save filtered files, re-derive
    python3 refetch_dol_financials.py --no-download   # reuse files already in CACHE_DIR
"""
from __future__ import annotations

import csv
import io
import os
import re
import ssl
import sqlite3
import sys
import urllib.request
import zipfile

import config
import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

CACHE_DIR = os.path.join("/tmp", "dol")
SCHEDULE_DIR = config.FORM5500_SCHEDULE_DIR
YEARS = list(range(2014, 2025))

SCH_I_ASSET_FIELDS = {
    "total_assets": ["SMALL_TOT_ASSETS_EOY_AMT"],
    "total_liabilities": ["SMALL_TOT_LIABILITIES_EOY_AMT"],
    "employer_contributions": ["SMALL_EMPLR_CONTRIB_INCOME_AMT"],
    "participant_contributions": ["SMALL_PARTICIPANT_CONTRIB_AMT"],
    "benefits_paid": ["SMALL_TOT_DISTRIB_BNFT_AMT"],
    "net_income": ["SMALL_NET_INCOME_AMT"],
}


def _download(url: str, dest: str) -> bool:
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"User-Agent": "MassCEO-Research/1.0"})
        with urllib.request.urlopen(req, context=ctx, timeout=240) as resp:
            data = resp.read()
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
            if not name:
                return False
            with zf.open(name) as f, open(dest, "wb") as out:
                out.write(f.read())
        return True
    except Exception as e:  # noqa: BLE001
        print(f"    download failed: {e}")
        return False


def fetch_bulk(download: bool = True):
    os.makedirs(CACHE_DIR, exist_ok=True)
    for sched in ("H", "I"):
        for y in YEARS:
            dest = os.path.join(CACHE_DIR, f"sch{sched}_{y}.csv")
            if os.path.exists(dest) or not download:
                continue
            for variant in (f"Latest/F_SCH_{sched}_{y}_Latest",
                            f"All/F_SCH_{sched}_{y}_All"):
                url = f"https://askebsa.dol.gov/FOIA%20Files/{y}/{variant}.zip"
                print(f"  Sch {sched} {y}: {url}")
                if _download(url, dest):
                    break


def _num(v):
    return fa._safe_money(v)


def build_maps():
    """Return {(plan_year, ein, pn): {field: value}} for Schedule H and Schedule I."""
    H, I = {}, {}
    for y in YEARS:
        hp = os.path.join(CACHE_DIR, f"schH_{y}.csv")
        if os.path.exists(hp):
            with open(hp, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f):
                    d = r.get("SCH_H_PLAN_YEAR_BEGIN_DATE", "")
                    if not d[:4].isdigit():
                        continue
                    py = int(d[:4])
                    if _num(fa._get_field(r, fa.SCHEDULE_FINANCIAL_FIELDS["total_assets"])) is None:
                        continue
                    k = (py, fa._normalize_ein(fa._get_field(r, fa.SCHEDULE_EIN_FIELDS)),
                         fa._normalize_pn(fa._get_field(r, fa.SCHEDULE_PN_FIELDS)))
                    H[k] = {fld: _num(fa._get_field(r, c))
                            for fld, c in fa.SCHEDULE_FINANCIAL_FIELDS.items()}
        ip = os.path.join(CACHE_DIR, f"schI_{y}.csv")
        if os.path.exists(ip):
            with open(ip, encoding="utf-8", errors="replace") as f:
                for r in csv.DictReader(f):
                    d = r.get("SCH_I_PLAN_YEAR_BEGIN_DATE", "")
                    if not d[:4].isdigit():
                        continue
                    py = int(d[:4])
                    if _num(fa._get_field(r, SCH_I_ASSET_FIELDS["total_assets"])) is None:
                        continue
                    k = (py, fa._normalize_ein(fa._get_field(r, ["SCH_I_EIN"])),
                         fa._normalize_pn(fa._get_field(r, ["SCH_I_PLAN_NUM"])))
                    I[k] = {fld: _num(fa._get_field(r, c))
                            for fld, c in SCH_I_ASSET_FIELDS.items()}
    return H, I


def rederive(H, I):
    conn = sqlite3.connect(str(config.DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM form5500_filings "
                        "WHERE sponsor_state='MA' AND is_esop=1").fetchall()
    from_h = from_i = 0
    for r in rows:
        k = (r["filing_year"], fa._normalize_ein(r["ein"]), fa._normalize_pn(r["plan_num"]))
        if k in H:                       # Schedule H priority
            rec = H[k]
            conn.execute(
                "UPDATE form5500_filings SET total_assets=?, total_liabilities=?, "
                "employer_contributions=?, participant_contributions=?, benefits_paid=?, "
                "net_income=?, employer_securities=? WHERE id=?",
                (rec["total_assets"], rec["total_liabilities"], rec["employer_contributions"],
                 rec["participant_contributions"], rec["benefits_paid"], rec["net_income"],
                 rec["employer_securities"], r["id"]))
            from_h += 1
        elif k in I:                     # Schedule I (no employer securities reported)
            rec = I[k]
            conn.execute(
                "UPDATE form5500_filings SET total_assets=?, total_liabilities=?, "
                "employer_contributions=?, participant_contributions=?, benefits_paid=?, "
                "net_income=?, employer_securities=NULL WHERE id=?",
                (rec["total_assets"], rec["total_liabilities"], rec["employer_contributions"],
                 rec["participant_contributions"], rec["benefits_paid"], rec["net_income"],
                 r["id"]))
            from_i += 1
    conn.commit()
    conn.close()
    print(f"Re-derived: {from_h} from Schedule H, {from_i} from Schedule I")


def find_cross_ein_dupes():
    """Same plan filed under two EINs in a transition year -> double counts."""
    conn = sqlite3.connect(str(config.DB_PATH))
    rows = conn.execute(
        "SELECT filing_year, total_assets, total_participants, "
        "GROUP_CONCAT(DISTINCT ltrim(ein,'0')) eins, GROUP_CONCAT(DISTINCT sponsor_name) names "
        "FROM form5500_filings WHERE sponsor_state='MA' AND is_esop=1 AND total_assets>0 "
        "GROUP BY filing_year, total_assets, total_participants "
        "HAVING COUNT(DISTINCT ltrim(ein,'0'))>1").fetchall()
    conn.close()
    return rows


def main():
    download = "--no-download" not in sys.argv
    print("Downloading DOL Schedule H + Schedule I bulk data..." if download
          else "Using cached DOL files...")
    fetch_bulk(download)
    H, I = build_maps()
    print(f"Authoritative records: Schedule H={len(H)}, Schedule I={len(I)}")
    rederive(H, I)
    # Integrity guards (0/unconfirmed + >assets employer_securities -> NULL)
    fa.nullify_unreported_employer_securities()
    fa.recompute_annual_summaries()
    dupes = find_cross_ein_dupes()
    if dupes:
        print("\nWARNING: cross-EIN duplicate plan-years remain (resolve manually, "
              "keep the continuing EIN):")
        for d in dupes:
            print(f"  {d[0]} {d[4]} EINs={d[3]} assets={d[1]}")
    print("\nDone. Financials re-derived from authoritative DOL filings.")


if __name__ == "__main__":
    main()
