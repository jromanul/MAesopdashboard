"""
Re-derive MA ESOP plan financials directly from authoritative DOL EFAST2 filings.

DOL Form 5500 filings are the authoritative source. This script downloads the DOL
bulk main-form, Schedule H (large plans) and Schedule I (small plans) datasets for
every form year, finds the return each database row represents, and overwrites
that row's financial fields with the values filed on that return's own schedule.

Rules, per database row (see dol_bulk.py for why each matters):
  1. The row's return is chosen with dol_bulk.canonical_return(): plan year
     beginning in the form year, ESOP-coded, full rather than short year.
  2. Its schedule is joined by ACK_ID — never by (EIN, plan number, plan year),
     which pairs a plan's regular return with its short final return.
  3. End-of-year fields only. A blank is stored as NULL ("not reported").
  4. Employer securities: Schedule H line 1d(1), or Schedule I line 3d for small
     plans. An impossible value (negative, or above total assets) is NULL.
  5. Participant counts are NOT changed here. They come from the main form and
     are verified instead: any row whose stored counts differ from its return is
     reported so it can be reviewed, since a difference can be a documented
     correction (see scan_dol_filers.PARTICIPANT_OVERRIDES).

Also writes the filtered schedule rows for every tracked filing to data/form5500/
(f_sch_h_<year>.csv, f_sch_i_<year>.csv). Those files are what
form5500_analysis.nullify_unreported_employer_securities() reads to tell an
explicitly reported $0 from a blank, and what a reseeded database imports.

Afterwards: unreported-securities guard, annual summaries recomputed, and a
check for the same plan filed under two EINs (resolve those by hand).

Usage:
    python3 refetch_dol_financials.py                 # download, re-derive
    python3 refetch_dol_financials.py --no-download   # reuse zips in DOL_CACHE_DIR
    python3 refetch_dol_financials.py --dry-run       # report changes, write nothing
"""
from __future__ import annotations

import csv
import os
import shutil
import sqlite3
import ssl
import sys
import urllib.request

import config
import dol_bulk
import form5500_analysis as fa
from scan_dol_filers import PARTICIPANT_OVERRIDES, drop_name_scoped_exclusions

import safe_console

safe_console.enable_utf8_stdout()

SCHEDULE_DIR = config.FORM5500_SCHEDULE_DIR
OPEN_YEAR = config.FORM5500_OPEN_FORM_YEAR
YEARS = list(range(2014, OPEN_YEAR + 1))
KINDS = ("F_5500", "F_SCH_H", "F_SCH_I")


def _download(url: str, dest: str) -> bool:
    req = urllib.request.Request(url, headers={"User-Agent": "MassCEO-Research/1.0"})
    tmp = dest + ".part"
    try:
        with urllib.request.urlopen(req, context=ssl.create_default_context(),
                                    timeout=600) as resp, open(tmp, "wb") as out:
            shutil.copyfileobj(resp, out)
        os.replace(tmp, dest)
        return True
    except Exception as e:  # noqa: BLE001
        print(f"    download failed: {e}")
        if os.path.exists(tmp):
            os.remove(tmp)
        return False


# DOL's "All" files (every filing version) are fetched for every form year: the
# "Latest" file can drop a filing in any year (see dol_bulk, rule 4).
ALL_VERSION_YEARS = YEARS


def fetch_bulk(download: bool = True):
    os.makedirs(dol_bulk.CACHE_DIR, exist_ok=True)
    for y in YEARS:
        for variant in dol_bulk.VARIANTS:
            if variant == "All" and y not in ALL_VERSION_YEARS:
                continue
            for kind in KINDS:
                if dol_bulk.has_zip(y, kind, variant) or not download:
                    continue
                url = dol_bulk.zip_url(y, kind, variant)
                print(f"  {kind} {y} {variant}: {url}")
                _download(url, dol_bulk.zip_path(y, kind, variant))


def locate_returns(conn) -> dict:
    """{db_row_id: (form_year, canonical_return, schedules)} for every tracked row."""
    rows = conn.execute("SELECT * FROM form5500_filings "
                        "WHERE sponsor_state='MA' AND is_esop=1").fetchall()
    by_year: dict = {}
    for r in rows:
        by_year.setdefault(r["filing_year"], []).append(r)

    located = {}
    for y in sorted(by_year):
        if not (dol_bulk.has_zip(y, "F_5500") or dol_bulk.has_zip(y, "F_5500", "All")):
            print(f"  {y}: no main-form zip cached; financials left as they are")
            continue
        keys = {(dol_bulk.norm_ein(r["ein"]), dol_bulk.norm_pn(r["plan_num"]))
                for r in by_year[y]}
        returns = dol_bulk.returns_for_year(y, extra_keys=keys)
        returns = drop_name_scoped_exclusions(y, returns)
        sched = dol_bulk.schedules_for_returns(returns)
        for r in by_year[y]:
            k = (dol_bulk.norm_ein(r["ein"]), dol_bulk.norm_pn(r["plan_num"]))
            cands = returns.get(k, [])
            if not cands:
                print(f"  {y} {r['sponsor_name']} EIN={r['ein']} PN={r['plan_num']}: "
                      f"no return in DOL bulk")
                continue
            x = dol_bulk.canonical_return(y, cands)
            located[r["id"]] = (y, x, sched.get(x["ack_id"], {}))
    return located


def rederive(conn, located: dict, dry_run: bool = False) -> dict:
    stats = {"SCH_H": 0, "SCH_I": 0, "none": 0, "changed": 0}
    count_diffs = []
    for row_id, (y, ret, sched) in located.items():
        r = conn.execute("SELECT * FROM form5500_filings WHERE id=?", (row_id,)).fetchone()
        money, src = dol_bulk.financials(sched)
        stats[src or "none"] += 1
        override = PARTICIPANT_OVERRIDES.get(
            (y, dol_bulk.norm_ein(r["ein"]), dol_bulk.norm_pn(r["plan_num"])), {})
        for f, v in (("total_participants", ret["total_participants"]),
                     ("active_participants", ret["active_participants"])):
            if f not in override and v is not None and r[f] is not None and int(v) != int(r[f]):
                count_diffs.append((y, r["sponsor_name"], f, r[f], int(v)))
        if not src:
            continue
        changed = [f for f in dol_bulk.MONEY_FIELDS
                   if (money[f] is None) != (r[f] is None)
                   or (money[f] is not None and abs(money[f] - r[f]) > 0.5)]
        if not changed:
            continue
        stats["changed"] += 1
        print(f"  {y} {r['sponsor_name'][:36]:36} [{src}] " + ", ".join(
            f"{f} {r[f]} -> {money[f]}" for f in changed))
        if not dry_run:
            conn.execute(
                "UPDATE form5500_filings SET " +
                ", ".join(f"{f}=?" for f in dol_bulk.MONEY_FIELDS) + " WHERE id=?",
                [money[f] for f in dol_bulk.MONEY_FIELDS] + [row_id])
    if not dry_run:
        conn.commit()
    if count_diffs:
        print(f"\n  {len(count_diffs)} participant count(s) differ from the return "
              f"(not changed here — review):")
        for y, name, f, dbv, dolv in count_diffs:
            print(f"    {y} {name}: {f} DB={dbv} DOL={dolv}")
    return stats


def save_filtered_schedules(located: dict):
    """Write each tracked filing's raw Schedule H / I row to data/form5500/."""
    os.makedirs(SCHEDULE_DIR, exist_ok=True)
    for kind, prefix in (("SCH_H", "f_sch_h"), ("SCH_I", "f_sch_i")):
        per_year: dict = {}
        for y, _ret, sched in located.values():
            if kind in sched:
                per_year.setdefault(y, []).append(sched[kind]["_raw"])
        for y, raws in sorted(per_year.items()):
            path = os.path.join(SCHEDULE_DIR, f"{prefix}_{y}.csv")
            cols = list(raws[0].keys())
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                w.writeheader()
                for raw in sorted(raws, key=lambda d: d.get("ACK_ID", "")):
                    w.writerow(raw)
    print(f"  filtered schedules written to {SCHEDULE_DIR}")


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
    dry_run = "--dry-run" in sys.argv
    print("Downloading DOL bulk data..." if download
          else f"Using cached DOL files in {dol_bulk.CACHE_DIR}...")
    fetch_bulk(download)
    conn = sqlite3.connect(str(config.DB_PATH))
    conn.row_factory = sqlite3.Row
    located = locate_returns(conn)
    print(f"Located the filed return for {len(located)} tracked plan-years")
    stats = rederive(conn, located, dry_run)
    conn.close()
    print(f"Re-derived: {stats['SCH_H']} from Schedule H, {stats['SCH_I']} from "
          f"Schedule I, {stats['none']} with no schedule; {stats['changed']} rows changed")
    if dry_run:
        print("Dry run: nothing written.")
        return
    save_filtered_schedules(located)
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
