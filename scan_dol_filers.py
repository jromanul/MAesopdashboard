#!/usr/bin/env python3
"""
Scan DOL EFAST2 bulk data for a form year: find MA ESOP filers missing from our
database, verify stored fields against the authoritative source, and optionally
import new filers (used to keep the open form year current, e.g. 2025).

What it does per --year:
  1. Downloads F_5500, F_5500_SF, F_SCH_H and F_SCH_I for the year into
     DOL_CACHE_DIR (default /tmp/dol; --no-download reuses the cache).
  2. Filters to MA sponsors whose TYPE_PENSION_BNFT_CODE contains 2O, 2P or 2Q
     (ESOP / leveraged ESOP / S-corporation ESOP) — same rule as the dataset.
  3. Diffs against form5500_filings for that filing_year, bucketing:
       NEW (EIN never seen in DB) / RETURNING (EIN known from other years)
       / POSSIBLE_DUPE (another EIN already holds this plan for the year —
       the acquired plan filed under the acquirer's number, or a re-filing
       under a second plan number) / EXCLUDED (reviewed and deliberately not
       tracked; see EXCLUDED_EINS and KNOWN_NOT_TRACKED).
  4. Verifies every common row field by field against the return that row
     represents (dol_bulk.canonical_return) and that return's own schedule,
     joined by ACK_ID: participants, active participants, city, and the
     end-of-year financials.
  5. --import-new: backs up the DB and inserts the NEW/RETURNING rows (never
     the dupes or exclusions) with their financials, sponsor casing and curated
     industry inherited from the plan's earlier rows. The open form year never
     gets an annual-summary row (see recompute_annual_summaries).

Usage:
    python3 scan_dol_filers.py --year 2024
    python3 scan_dol_filers.py --year 2025 --import-new
    python3 scan_dol_filers.py --year 2025 --no-download --import-new
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import sqlite3
import ssl
import sys
import urllib.request
from datetime import datetime, timezone

import config
import dol_bulk
import form5500_analysis as fa

import safe_console

safe_console.enable_utf8_stdout()

DB_PATH = config.DB_PATH
CACHE_DIR = dol_bulk.CACHE_DIR
KINDS = ("F_5500", "F_5500_SF", "F_SCH_H", "F_SCH_I")
MONEY_FIELDS = dol_bulk.MONEY_FIELDS


# EINs whose ESOP-coded filings are deliberately NOT tracked, keyed by normalized
# (leading-zero-stripped) EIN. These are large publicly traded employers whose
# ordinary 401(k)/savings plan carries an ESOP code (2O) purely because company
# stock is one investment option alongside 2J/2K — confirmed by reading the raw
# F_5500 PLAN_NAME. They are not closely held employee-owned companies, and each
# would add thousands of participants, materially overstating MA employee
# ownership. Listed here so a later `--import-new` run cannot silently re-add
# them. See FORM5500_METHODOLOGY.
EXCLUDED_EINS: dict[str, str] = {
    "42271897": "Cabot Corporation (NYSE: CBT) - 'CABOT 401(K) PLAN', 2O alongside 2J/2K",
    "43234558": "Waters Technologies (NYSE: WAT) - 'THE WATERS EMPLOYEE INVESTMENT PLAN'",
    "880706021": "Crane NXT, Co. (NYSE: CXT) - 'CRANE NXT, CO. SAVINGS AND INVESTMENT PLAN'",
    "42456637": "State Street Corporation (NYSE: STT) - 'SALARY SAVINGS PROGRAM', 2O alongside 2J/2K",
    "951778500": "Raytheon Company - 'RAYTHEON SAVINGS AND INVESTMENT PLAN', 2O alongside 2J/2K",
    "60570975": "RTX / United Technologies - 'EMPLOYEE SAVINGS PLAN', 2P alongside 2J/2K",
    "140689340": "General Electric (NYSE: GE) - 'GE RETIREMENT SAVINGS PLAN', 2O alongside 2J/2K",
}

# Individual plans reviewed and deliberately not tracked, keyed by normalized
# (EIN, plan number). Unlike EXCLUDED_EINS these sponsors may have other plans,
# or other years of the same plan, that ARE tracked, so the key is the plan and
# an optional set of years limits it further. Reviewed 2026-10-26 against the
# September 2026 DOL release; each was surfaced by a full 2014-2025 rescan.
# An entry may carry a third element, a plan-name substring: the exclusion then
# applies only to returns whose PLAN_NAME contains it, so a second plan filed
# under the same EIN and plan number is not excluded with it.
KNOWN_NOT_TRACKED: dict[tuple[str, str], tuple] = {
    # Not ESOPs, whatever the code says.
    ("352223136", "1"): (None, "Ranella Hirsch MD PC 401(k): two-person 401(k); 2Q with no ESOP code"),
    ("42765092", "1"): (None, "Berman Donovan Stern & Wood, LLP profit-sharing plan: a partnership "
                              "has no stock for an ESOP to hold; 2P is a coding error"),
    ("42193872", "3"): (None, "Nypro Inc. Defined Contribution Plan: not an ESOP - no employer "
                              "securities on any return; 2P appears on the 2014 return only"),
    ("41782600", "2"): (None, "Rockland Trust 'EE Savings PS and Stock Ownership Plan': a 401(k) "
                              "savings plan with no ESOP code at a public bank (surfaces only in "
                              "the name-based coverage check)"),
    ("42079638", "1"): ({2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025},
                        "Haberman Insurance: the 401(k) and ESOP was renamed '401(K) PROFIT SHARING' "
                        "and sold its company stock during 2017 (counted for 2017); from 2018 it is "
                        "an ordinary 401(k)"),
    # Bank ESOPs at publicly traded or acquired banks are counted like every
    # other ESOP (owner's decision, 2026-10-28): Wellesley Bank, Peoples Federal
    # and Chicopee's 2017 final return, once excluded here as "investor-owned",
    # are tracked.
    # The same plan filed under a second EIN or plan number in a transition
    # year. The copy already tracked carries identical figures.
    ("41366970", "7"): ({2021}, "The Gillette Company: original 2021 return under the old EIN; the "
                                "plan year is tracked from its amended re-filing under EIN 813268325"),
    ("42401206", "2"): ({2020}, "Northern Tree Holdco: duplicate of the EIN 851665706 return"),
    ("884313116", "2"): ({2022}, "Feinmann, Inc.: duplicate of the EIN 043104386 return"),
    ("42606179", "2"): ({2022}, "Erland Construction, Inc.: duplicate of the EIN 043579044 return"),
    ("861384153", "2"): ({2020}, "CourtSmart Digital Systems: duplicate of the EIN 043358086 return"),
    ("43067724", "3"): ({2024, 2025}, "Cambridge Bancorp ESOP filed under Eastern Bank's EIN and "
                                      "plan number after the acquisition; Cambridge's own EIN "
                                      "042777442 return is tracked. Scoped by plan name: Eastern's "
                                      "own ESOP files under the same number and IS tracked",
                        "CAMBRIDGE BANCORP"),
    ("43298009", "3"): ({2024}, "Northern Construction Holding: administrative re-filing of the "
                                "PN 002 return already tracked"),
    ("332432954", "1"): ({2024}, "Green Tech Enterprises: superseded original; corrected re-filing "
                                 "is tracked as PN 002"),
}

# Plans confirmed to be a different plan from the one already tracked under the
# same EIN, so a return is not held back as a possible duplicate merely because
# its EIN files another tracked plan that year. Keyed by (EIN, plan number).
DISTINCT_PLANS: dict[tuple[str, str], str] = {
    ("41782600", "1"): "Peoples Federal Savings Bank ESOP, filed by Rockland Trust after the 2015 "
                       "acquisition; Rockland's plan 004 is the Central Co-operative Bank ESOP",
    ("43130808", "4"): "Meetinghouse Bank ESOP, filed by East Boston Savings Bank after the 2017 "
                       "acquisition; EBSB's own ESOP is plan 003",
    ("41858810", "4"): "United Bank's own ESOP (effective 2005); plan 003 under the same EIN is the "
                       "New England Bank ESOP it acquired in 2012",
}

# Plan-years whose stored participant counts deliberately differ from the
# return. Empty since the dataset moved to the year-end total (line 6f): the
# three entries it held (Shawmut Group 2017, Forge Group 2022, Sullivan
# Investment 2023) substituted a year-end count for a start-of-year line 5 that
# predated the plan's population, and line 6f now gives those counts directly.
PARTICIPANT_OVERRIDES: dict[tuple[int, str, str], dict] = {}

# Companies new to the dataset have no prior row to inherit casing from, and
# .title() mangles initialisms ("FE" -> "Fe") and camel-cased brands. Keyed by
# EIN, or by (EIN, plan number) where one EIN files more than one company's plan.
NEW_COMPANY_NAMES: dict = {
    ("41782600", "1"): ("Peoples Federal Savings Bank", "Rockland"),  # filed by Rockland Trust
    ("41858810", "4"): ("United Bank (United Financial Bancorp)", "West Springfield"),
    ("208447891", "5"): ("LSB Corporation (RiverBank)", "North Andover"),  # filed by People's United
    ("113431358", "5"): ("Essex County Gas Company (National Grid)", "Amesbury"),  # filed by KeySpan
    ("41663150", "20"): ("Essex County Gas Company (National Grid)", "Amesbury"),
    "43444078": ("BostonBean Coffee Company, Inc.", "Woburn"),
    "364615460": ("Helical Drilling, Inc.", "Braintree"),
    "200508239": ("Wildlife Acoustics, Inc.", "Maynard"),
    "921304786": ("FE Holdings, Inc.", "Mansfield"),
    "931156047": ("Atlantic Holdings, Inc. (Atlantic Sintered)", "Wrentham"),
}

# A city as one return spells it (a typo or an abbreviation), and the spelling
# the same sponsor uses on its other returns. Applied on import and accepted by
# verification; map_utils aliases them too.
CITY_SPELLINGS: dict[str, str] = {
    "RANDDOLPH": "Randolph",               # Econocorp 2022
    "SO. LANCASTER": "South Lancaster",    # James Monroe Corp. 2020-2023
    "W. SPRINGFIELD": "West Springfield",  # New England Bank 2014
    "WODBURN": "Woburn",                   # Atlantic Plywood, plan-year-2019 final return
}

# Plans whose return address is not where the covered company is based. Only
# Massachusetts-based ESOPs are counted (owner's decision, 2026-10-28), and
# "based" means the headquarters of the employer the plan covers - normally,
# but not always, the address on its Form 5500. Keyed by (EIN, plan number):
# (years or None, Massachusetts-based?, (home city, ZIP) or None, reason). A True entry
# counts a Massachusetts company whose parent or administrator files from
# another state; a False entry leaves out a company based elsewhere whose
# return carries a Massachusetts address (an acquirer's, lawyer's or
# administrator's). Each entry was researched and independently reviewed.
MA_BASED_OVERRIDES: dict[tuple[str, str], tuple] = {
    # Massachusetts companies whose ESOP returns are filed from another state.
    ("41366970", "7"): (None, True, ("Boston", "02127"),
                        "The Gillette Company: 2016-2021 returns are mailed from Procter & Gamble's "
                        "Cincinnati benefits office; Gillette stayed at 1 Gillette Park, Boston, "
                        "where the same EIN's other plan files"),
    ("41549430", "3"): (None, True, ("Plympton", "02367"),
                        "Litecontrol Corporation, a Plympton lighting maker bought by Hubbell in "
                        "2014; its 2014 return is mailed c/o Hubbell, Shelton, CT"),
    ("455266171", "3"): (None, True, ("Plympton", "02367"),
                         "Litecontrol's ESOP, final 2015 return filed by Hubbell's Newco Lighting "
                         "from Shelton, CT"),
    ("43050515", "2"): (None, True, ("Agawam", "01001"),
                        "Environmental Compliance Services (Agawam): ESOP wound down in 2017-2018 by "
                        "ECS Holdco from West Hartford, CT after the company's sale"),
    ("41858810", "4"): ({2014}, True, ("West Springfield", "01089"),
                        "United Bank's own ESOP: the bank was in West Springfield until its April "
                        "2014 merger with Rockville Financial; the return is filed from Glastonbury, CT"),
    ("61213065", "19"): (None, True, ("Belmont", "02478"),
                         "Belmont Savings Bank's ESOP, wound down 2018-2020 by People's United "
                         "(Bridgeport, CT) after it acquired BSB Bancorp"),
    ("208447891", "5"): (None, True, ("North Andover", "01845"),
                         "LSB Corporation / RiverBank's ESOP, final 2014 return filed by People's "
                         "United from Bridgeport, CT"),
    ("113431358", "5"): (None, True, ("Amesbury", "01913"),
                         "Essex County Gas Company's legacy ESOP (Amesbury), filed by its parent "
                         "KeySpan from New York"),
    ("41663150", "20"): (None, True, ("Amesbury", "01913"),
                         "the same Essex County Gas Company ESOP, filed by National Grid USA "
                         "Service Company from 2018"),
    # Companies based elsewhere whose returns carry a Massachusetts address.
    ("461710155", "3"): (None, False, None,
                         "Coastway Community Bank was based in Warwick, RI; HarborOne wound its "
                         "ESOP down from Brockton in 2018-2019"),
    ("41858810", "3"): ({2014}, False, None,
                        "New England Bank's ESOP: New England Bancshares was an Enfield, CT bank; "
                        "United Bank wound the plan down from West Springfield"),
    ("20452307", "2"): (None, False, None,
                        "Dynamic Air Corporation is in West Kennebunk, ME; its 2020 and 2023 "
                        "returns use a Boston mail-forwarding box"),
    ("880825163", "3"): (None, False, None,
                         "Forge Group, Inc. is a Maryland/DC insurance holding company (Chevy "
                         "Chase / Bethesda); its returns use a Worcester P.O. box"),
    ("842403043", "3"): (None, False, None,
                         "Manganaro North America is run from Beltsville, MD (its executives, back "
                         "office and the same EIN's 401(k) all file from there); the ESOP's Woburn "
                         "address is its Manganaro Northeast subsidiary's office"),
    ("50309452", "2"): (None, False, None,
                        "Gripnail Corporation is in East Providence, RI; its returns use a Taunton "
                        "address only during the 2019-2022 wind-down"),
}


def is_ksop_codes(codes: str) -> bool:
    """A KSOP is an ESOP with a 401(k) feature: code 2J alongside the ESOP code.
    (2K, a 401(m) match or after-tax arrangement, often accompanies it but is
    not part of the test: Strem Chemicals' and Haberman's 401(k)-and-ESOP plans
    carry 2J without it.)"""
    return "2J" in (codes or "").upper()


def city_spelling(city: str) -> str:
    c = (city or "").strip()
    return CITY_SPELLINGS.get(c.upper(), c)


dol_bulk.EXTRA_PLAN_KEYS.update(k for k, v in MA_BASED_OVERRIDES.items() if v[1])


def _ma_override(ein: str, pn: str, year: int):
    hit = MA_BASED_OVERRIDES.get((ein, pn))
    return hit if hit and (hit[0] is None or year in hit[0]) else None


def is_ma_based(ein: str, pn: str, year: int, rec: dict) -> bool:
    """Is this plan-year's covered company based in Massachusetts? The return's
    mailing address decides (dol_bulk.is_ma_address) unless MA_BASED_OVERRIDES
    holds a reviewed answer for the plan and year."""
    hit = _ma_override(ein, pn, year)
    if hit:
        return hit[1]
    return dol_bulk.is_ma_address(rec["state"], rec["sponsor_zip"])


def not_tracked_reason(ein: str, pn: str, year: int, plan_name: str = "") -> str | None:
    if ein in EXCLUDED_EINS:
        return EXCLUDED_EINS[ein]
    hit = KNOWN_NOT_TRACKED.get((ein, pn))
    if not hit or (hit[0] is not None and year not in hit[0]):
        return None
    if len(hit) > 2 and hit[2].upper() not in (plan_name or "").upper():
        return None
    return hit[1]


def drop_name_scoped_exclusions(year: int, returns: dict) -> dict:
    """Remove the individual returns a plan-name-scoped exclusion covers, so the
    canonical return for that EIN and plan number is chosen from the rest."""
    out = {}
    for key, lst in returns.items():
        hit = KNOWN_NOT_TRACKED.get(key)
        if hit and len(hit) > 2 and (hit[0] is None or year in hit[0]):
            lst = [x for x in lst if hit[2].upper() not in x["plan_name"].upper()]
        if lst:
            out[key] = lst
    return out


def norm_key(ein, pn) -> tuple[str, str]:
    return dol_bulk.norm_ein(ein), dol_bulk.norm_pn(pn)


def norm_name(s) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", str(s or "").lower())
    stop = {"inc", "incorporated", "corp", "corporation", "llc", "co", "company",
            "the", "and", "of", "employee", "stock", "ownership", "plan", "trust", "esop"}
    return " ".join(t for t in s.split() if t not in stop)


def _download(url: str, dest: str) -> bool:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    tmp = dest + ".part"
    try:
        with urllib.request.urlopen(req, timeout=600, context=ssl.create_default_context()) as resp, \
                open(tmp, "wb") as f:
            shutil.copyfileobj(resp, f)
        os.replace(tmp, dest)
        return True
    except Exception as exc:
        print(f"    ! {url.rsplit('/', 1)[-1]}: {exc}")
        if os.path.exists(tmp):
            os.remove(tmp)
        return False


def fetch(year: int, no_download: bool):
    """Download the year's Latest and All files. Later form years already in the
    cache are also searched for late returns (dol_bulk.returns_for_year)."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    for variant in dol_bulk.VARIANTS:
        for kind in KINDS:
            if dol_bulk.has_zip(year, kind, variant) or no_download:
                continue
            print(f"    downloading {kind}_{year}_{variant}.zip ...")
            _download(dol_bulk.zip_url(year, kind, variant),
                      dol_bulk.zip_path(year, kind, variant))


def sf_esop_returns(year: int) -> dict:
    """MA ESOP-coded Form 5500-SF returns. ESOPs are not eligible to file the SF,
    so this is normally empty; it is checked so a mis-filed plan is not missed."""
    out: dict = {}
    if not dol_bulk.has_zip(year, "F_5500_SF"):
        return out
    for r in dol_bulk.iter_rows(year, "F_5500_SF"):
        if not dol_bulk.is_ma_address(r.get("SF_SPONS_US_STATE"), r.get("SF_SPONS_US_ZIP")) or \
                not dol_bulk.has_esop_code(r.get("SF_TYPE_PENSION_BNFT_CODE")):
            continue
        k = norm_key(r.get("SF_SPONS_EIN"), r.get("SF_PLAN_NUM"))
        out.setdefault(k, []).append({
            "form": "5500-SF", "ack_id": r["ACK_ID"], "sponsor_name": r["SF_SPONSOR_NAME"],
            "sponsor_city": r["SF_SPONS_US_CITY"], "ein_raw": r["SF_SPONS_EIN"],
            "pn_raw": r["SF_PLAN_NUM"], "plan_year_begin": r["SF_PLAN_YEAR_BEGIN_DATE"],
            "total_participants": dol_bulk.to_num(r.get("SF_TOT_PARTCP_BOY_CNT"))})
    return out


def _prior_row(conn, ein: str, year: int, pn: str | None = None):
    """The most recent earlier row of the EIN (of the plan, when ``pn`` is given);
    failing that, its nearest later one, so a year recovered before a plan's
    first tracked year (a late or dropped return) still reads like the plan's
    other years."""
    cols = "SELECT sponsor_name, sponsor_city, naics_code, industry_sector FROM form5500_filings "
    where, args = "WHERE ltrim(ein,'0')=? ", [ein]
    if pn is not None:
        where, args = where + "AND ltrim(plan_num,'0')=? ", args + [pn]
    return (conn.execute(cols + where + "AND filing_year<? ORDER BY filing_year DESC LIMIT 1",
                         args + [year]).fetchone()
            or conn.execute(cols + where + "AND filing_year>? ORDER BY filing_year LIMIT 1",
                            args + [year]).fetchone())


def inherit_city(prior_city: str | None, filed_city: str) -> str:
    """The earlier row's spelling of the city, unless the plan has since moved.

    Casing is inherited so a plan reads the same in every year, but an address
    change on the return is real information and wins.
    """
    filed_city = city_spelling(filed_city)
    if prior_city and prior_city.strip().lower() == filed_city.lower():
        return prior_city
    return filed_city.strip().title() if filed_city else (prior_city or "")


def build_record(conn, year: int, ret: dict, sched: dict, now: str) -> dict:
    """A form5500_filings row for one return, with inherited casing/industry."""
    money, _src = dol_bulk.financials(sched)
    # The plan's own rows first; then a name reviewed for the plan; then the
    # EIN's other rows (one EIN can file several companies' plans: Rockland
    # Trust files acquired banks' ESOPs) and the prior sponsor's.
    prior = _prior_row(conn, ret["ein"], year, ret["pn"])
    named = NEW_COMPANY_NAMES.get((ret["ein"], ret["pn"]))
    if not prior and not named:
        prior = _prior_row(conn, ret["ein"], year)
        if not prior and ret.get("last_rpt_ein"):
            prior = _prior_row(conn, ret["last_rpt_ein"], year)
    hit = _ma_override(ret["ein"], ret["pn"], year)
    if prior:
        sponsor, naics, sector = prior["sponsor_name"], prior["naics_code"], prior["industry_sector"]
        city = inherit_city(prior["sponsor_city"], ret["sponsor_city"])
    else:
        sponsor, city = named or NEW_COMPANY_NAMES.get(
            ret["ein"], (ret["sponsor_name"].title(), city_spelling(ret["sponsor_city"]).title()))
        naics = ret["naics_code"]
        sector = fa.NAICS_SECTORS.get(naics[:2], "") if naics else ""
    if not naics:
        naics = ret["naics_code"]
        sector = sector or (fa.NAICS_SECTORS.get(naics[:2], "") if naics else "")
    zip_code = ret["sponsor_zip"]
    if hit and hit[1] and hit[2]:
        city, zip_code = hit[2]   # filed from another state; the row carries the company's home
    codes = ret["codes"]
    return {
        "filing_year": year, "ein": ret["ein"], "plan_num": ret["pn"],
        "plan_name": ret["plan_name"], "sponsor_name": sponsor, "sponsor_city": city,
        "sponsor_state": "MA", "sponsor_zip": zip_code,
        "type_plan_entity": ret["type_plan_entity"], "type_pension_bnft": codes,
        "is_esop": 1, "is_ksop": 1 if is_ksop_codes(codes) else 0,
        "total_participants": None if ret["total_participants"] is None else int(ret["total_participants"]),
        "active_participants": None if ret["active_participants"] is None else int(ret["active_participants"]),
        **{f: money.get(f) for f in MONEY_FIELDS},
        "naics_code": naics, "industry_sector": sector,
        "plan_eff_date": ret["plan_eff_date"], "fetched": now,
    }


def gather(year: int, db_keys: set | None = None):
    """Every return that can represent a plan in the form year, and their schedules.

    Reads the year's Latest and All files, later forms for late returns, and the
    previous form for short final years (dol_bulk.returns_for_year).
    ``db_keys`` pulls in the returns of plans already tracked even when their
    latest return dropped its ESOP code, so they are verified, not lost.
    Massachusetts-based plans that file from another state are always read
    (dol_bulk.EXTRA_PLAN_KEYS).
    """
    returns = dol_bulk.returns_for_year(year, extra_keys=db_keys)
    returns = drop_name_scoped_exclusions(year, returns)
    return returns, dol_bulk.schedules_for_returns(returns)


def classify(conn, year: int, returns: dict):
    """Bucket the form year's returns that the database does not hold."""
    db_year = {norm_key(r["ein"], r["plan_num"]): dict(r) for r in conn.execute(
        "SELECT * FROM form5500_filings WHERE filing_year=? AND is_esop=1", (year,))}
    db_eins_all = {dol_bulk.norm_ein(e) for (e,) in conn.execute(
        "SELECT DISTINCT ein FROM form5500_filings")}
    db_names_year = {norm_name(r["sponsor_name"]): k[0] for k, r in db_year.items()}
    db_eins_year = {k[0] for k in db_year}
    new_rows, returning, dupes, excluded = [], [], [], []
    for key, lst in returns.items():
        if key in db_year:
            continue
        rec = dol_bulk.canonical_return(year, lst)
        if not dol_bulk.has_esop_code(rec["codes"]):
            continue
        if rec["plan_year_begin"][:4] != str(year):
            continue  # a next-year return filed early on this form; belongs to that year
        if not is_ma_based(key[0], key[1], year, rec):
            hit = _ma_override(key[0], key[1], year)
            if hit:  # reviewed: a company based elsewhere, with a Massachusetts address
                excluded.append((key, rec, f"not Massachusetts-based: {hit[3]}"))
            continue
        why = not_tracked_reason(key[0], key[1], year, rec["plan_name"])
        if why:
            excluded.append((key, rec, why))
            continue
        same_name = db_names_year.get(norm_name(rec["sponsor_name"]))
        if rec["last_rpt_ein"] and rec["last_rpt_ein"] != key[0] and rec["last_rpt_ein"] in db_eins_year:
            dupes.append((key, rec, f"LAST_RPT_SPONS_EIN {rec['last_rpt_ein']} already tracked for {year}"))
        elif same_name and same_name != key[0]:
            dupes.append((key, rec, f"name matches existing {year} EIN {same_name}"))
        elif key[0] in db_eins_year and key not in DISTINCT_PLANS:
            dupes.append((key, rec, f"EIN already tracked for {year} under another plan number"))
        elif key[0] in db_eins_all:
            returning.append((key, rec))
        else:
            new_rows.append((key, rec))
    gone = [k for k in db_year if k not in returns]
    return db_year, new_rows, returning, dupes, excluded, gone


def verify(year: int, db_year: dict, returns: dict, sched: dict):
    mismatches, overrides = [], []
    for key, row in db_year.items():
        lst = returns.get(key)
        if not lst:
            continue
        ret = dol_bulk.canonical_return(year, lst)
        label = f"{row['sponsor_name']} (EIN {row['ein']})"
        ov = PARTICIPANT_OVERRIDES.get((year, key[0], key[1]), {})
        for fld in ("total_participants", "active_participants"):
            dolv, dbv = ret[fld], row[fld]
            if dolv is None or dbv is None or int(dolv) == int(dbv):
                continue
            (overrides if fld in ov else mismatches).append(
                (label, fld, dbv, int(dolv), "F_5500"))
        filed_elsewhere = not dol_bulk.is_ma_address(ret["state"], ret["sponsor_zip"])
        if ret["sponsor_city"] and row["sponsor_city"] and not filed_elsewhere and \
                city_spelling(ret["sponsor_city"]).lower() != str(row["sponsor_city"]).strip().lower():
            mismatches.append((label, "sponsor_city", row["sponsor_city"], ret["sponsor_city"], "F_5500"))
        if not filed_elsewhere and (row["sponsor_zip"] or "") != ret["sponsor_zip"].strip():
            mismatches.append((label, "sponsor_zip", row["sponsor_zip"], ret["sponsor_zip"], "F_5500"))
        if (row["type_pension_bnft"] or "") != ret["codes"]:
            mismatches.append((label, "type_pension_bnft", row["type_pension_bnft"], ret["codes"], "F_5500"))
        if int(row["is_ksop"] or 0) != int(is_ksop_codes(ret["codes"])):
            mismatches.append((label, "is_ksop", row["is_ksop"], int(is_ksop_codes(ret["codes"])), "F_5500"))
        money, src = dol_bulk.financials(sched.get(ret["ack_id"], {}))
        for fld in MONEY_FIELDS:
            av, dbv = money.get(fld), row[fld]
            if av is None and dbv is None:
                continue
            if av is None or dbv is None or abs(av - dbv) > 1.0:
                mismatches.append((label, fld, dbv, av, src or "no schedule"))
    return mismatches, overrides


def import_rows(conn, year: int, rows: list, sched: dict) -> int:
    now = datetime.now(timezone.utc).isoformat()
    n = 0
    for key, rec in rows:
        record = build_record(conn, year, rec, sched.get(rec["ack_id"], {}), now)
        cols = ", ".join(record)
        ph = ", ".join(":" + c for c in record)
        cur = conn.execute(f"INSERT OR IGNORE INTO form5500_filings ({cols}) VALUES ({ph})", record)
        if cur.rowcount:
            n += 1
            a = record["total_assets"]
            print(f"  + {record['sponsor_name'][:36]:36} PN={record['plan_num']:>3} "
                  f"tot={record['total_participants']} act={record['active_participants']} "
                  f"assets={('$' + format(a, ',.0f')) if a is not None else '-'}")
    conn.execute("INSERT OR REPLACE INTO form5500_meta(key, value) VALUES (?, ?)",
                 (f"filers_{year}_imported", now))
    conn.commit()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--import-new", action="store_true",
                    help="insert NEW/RETURNING filers into the DB (backs up first)")
    ap.add_argument("--no-download", action="store_true", help="reuse the DOL_CACHE_DIR cache")
    args = ap.parse_args()
    y = args.year

    print(f"=== DOL form-year {y}: download & filter ===")
    fetch(y, args.no_download)
    if not dol_bulk.has_zip(y, "F_5500"):
        print(f"No DOL main-form file for {y} (it may not be published yet).")
        sys.exit(0)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    db_keys = {norm_key(e, p) for e, p in conn.execute(
        "SELECT ein, plan_num FROM form5500_filings WHERE filing_year=? AND is_esop=1", (y,))}
    returns, sched = gather(y, db_keys)
    sf = sf_esop_returns(y)
    print(f"    F_5500: {sum(len(v) for v in returns.values())} returns "
          f"({len(returns)} plans); 5500-SF: {len(sf)}; schedules joined: {len(sched)}")

    db_year, new_rows, returning, dupes, excluded, gone = classify(conn, y, returns)

    print(f"\n=== {y} coverage vs DB ===")
    print(f"  DOL bulk MA ESOP plans : {len(returns)}")
    print(f"  in DB for {y}          : {len(db_year)}")
    print(f"  NEW (EIN never in DB)  : {len(new_rows)}")
    print(f"  RETURNING (known EIN, no {y} row): {len(returning)}")
    print(f"  possible dupes (NOT importable): {len(dupes)}")
    print(f"  deliberately excluded  : {len(excluded)}")
    print(f"  in DB but not in bulk  : {len(gone)}")
    for key, rec, why in excluded:
        print(f"    EXCLUDED: {rec['sponsor_name']} EIN={rec['ein_raw']} PN={rec['pn_raw']} - {why}")
    for bucket, lst in (("NEW", new_rows), ("RETURNING", returning)):
        for key, rec in lst:
            print(f"    {bucket}: {rec['sponsor_name']} ({rec['sponsor_city']}) EIN={rec['ein_raw']} "
                  f"PN={rec['pn_raw']} PY={rec['plan_year_begin']} received={rec['date_received']} "
                  f"partcp={rec['total_participants']}")
    for key, rec, why in dupes:
        print(f"    DUPE?: {rec['sponsor_name']} EIN={rec['ein_raw']} PN={rec['pn_raw']} - {why}")
    for k in gone:
        r = db_year[k]
        print(f"    DB-only: {r['sponsor_name']} EIN={r['ein']} PN={r['plan_num']} "
              f"(amended away / withdrawn / different form year?)")
    for k, lst in sf.items():
        x = lst[0]
        print(f"    5500-SF ESOP return (review by hand): {x['sponsor_name']} EIN={x['ein_raw']}")

    # Name-based coverage check: returns named like an ESOP that carry no ESOP
    # code, are not already tracked for this year, and are not known exclusions.
    named = []
    for key, lst in dol_bulk.ma_esop_named_returns_without_code(y).items():
        if key in db_year or not_tracked_reason(key[0], key[1], y,
                                                  lst[0]["plan_name"] if lst else ""):
            continue
        rec = dol_bulk.canonical_return(y, lst)
        if rec["plan_year_begin"][:4] == str(y):
            named.append((key, rec))
    print(f"\n=== {y} name-based coverage check ===")
    if named:
        print(f"  {len(named)} MA return(s) named like an ESOP but carrying no ESOP code "
              f"(review; not imported automatically):")
        for key, rec in named:
            print(f"    NAME_ONLY: {rec['sponsor_name']} EIN={rec['ein_raw']} PN={rec['pn_raw']} "
                  f"codes={rec['codes'] or '-'} plan={rec['plan_name']}")
    else:
        print("  no MA return is named like an ESOP without carrying an ESOP code ✓")

    mismatches, overrides = verify(y, db_year, returns, sched)
    print(f"\n=== {y} field verification ===")
    if mismatches:
        print(f"  {len(mismatches)} field mismatches:")
        for label, fld, dbv, av, src in mismatches:
            print(f"    {label}: {fld} DB={dbv} DOL={av} [{src}]")
    else:
        print("  all common rows match DOL source values (within $1) ✓")
    for label, fld, dbv, av, src in overrides:
        print(f"    documented override: {label}: {fld} DB={dbv} DOL={av}")

    report = os.path.join(os.path.dirname(__file__) or ".", f"dol_scan_{y}_report.csv")
    with open(report, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["bucket", "sponsor_name", "city", "ein", "plan_num", "source",
                    "participants", "detail"])
        for bucket, lst in (("NEW", new_rows), ("RETURNING", returning)):
            for key, rec in lst:
                w.writerow([bucket, rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                            rec["pn_raw"], rec["form"], rec["total_participants"],
                            f"plan year {rec['plan_year_begin']}, received {rec['date_received']}"])
        for key, rec, why in dupes:
            w.writerow(["POSSIBLE_DUPE", rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                        rec["pn_raw"], rec["form"], rec["total_participants"], why])
        for key, rec, why in excluded:
            w.writerow(["EXCLUDED", rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                        rec["pn_raw"], rec["form"], rec["total_participants"], why])
        for k in gone:
            r = db_year[k]
            w.writerow(["DB_ONLY", r["sponsor_name"], r["sponsor_city"], r["ein"],
                        r["plan_num"], "db", r["total_participants"], "not in fresh bulk"])
        for label, fld, dbv, av, src in mismatches:
            w.writerow(["FIELD_MISMATCH", label, "", "", "", src, "", f"{fld}: DB={dbv} DOL={av}"])
        for label, fld, dbv, av, src in overrides:
            w.writerow(["OVERRIDE", label, "", "", "", src, "", f"{fld}: DB={dbv} DOL={av}"])
        for key, rec in named:
            w.writerow(["NAME_ONLY", rec["sponsor_name"], rec["sponsor_city"], rec["ein_raw"],
                        rec["pn_raw"], rec["form"], rec["total_participants"],
                        f"named like an ESOP, codes {rec['codes'] or '-'}: {rec['plan_name']}"])
    print(f"\nReport: {report}")

    if args.import_new and (new_rows or returning):
        bak = DB_PATH + f".pre-{y}import.bak"
        if not os.path.exists(bak):
            shutil.copy2(DB_PATH, bak)
            print(f"DB backed up -> {bak}")
        n = import_rows(conn, y, new_rows + returning, sched)
        print(f"Imported {n} new {y} filings (dupes and exclusions skipped).")
        fa.recompute_annual_summaries()
    conn.close()


# ── Legacy helpers ───────────────────────────────
# Kept only because the one-off apply_2024_refresh.py and import_2025_strays.py
# (already applied; kept as the record of those changes) import them. New code
# should use dol_bulk instead.
SCH_VARIANTS = {"sch_h": ["{y}/Latest/F_SCH_H_{y}_Latest", "{y}/All/F_SCH_H_{y}_All"],
                "sch_i": ["{y}/Latest/F_SCH_I_{y}_Latest", "{y}/All/F_SCH_I_{y}_All"]}
SCH_I_FIELDS = {f: [c] for f, c in dol_bulk.SCH_I_FIELDS.items() if f != "employer_securities"}
to_num = dol_bulk.to_num


def pick_col(headers: list[str], candidates: list[str]) -> str | None:
    up = {h.upper().strip(): h for h in headers}
    for cand in candidates:
        for name in (cand, "SF_" + cand):
            if name in up:
                return up[name]
    return None


def fetch_zip_rows(year: int, variants: list[str], no_download: bool):
    kind = variants[0].rsplit("/", 1)[-1].replace(f"_{year}_Latest", "")
    if not dol_bulk.has_zip(year, kind) and not no_download:
        _download(dol_bulk.zip_url(year, kind), dol_bulk.zip_path(year, kind))
    return dol_bulk.iter_rows(year, kind) if dol_bulk.has_zip(year, kind) else None


def index_schedule(reader, fields: dict, keep_keys=None) -> dict:
    headers = reader.fieldnames or []
    ein_c = pick_col(headers, ["SCH_H_EIN", "SCH_I_EIN", "SPONS_DFE_EIN", "EIN"])
    pn_c = pick_col(headers, ["SCH_H_PN", "SCH_I_PLAN_NUM", "PLAN_NUM", "PN"])
    out = {}
    if not ein_c:
        return out
    fmap = {db: pick_col(headers, cands) for db, cands in fields.items()}
    for row in reader:
        key = norm_key(row.get(ein_c), row.get(pn_c) if pn_c else "1")
        if keep_keys is None or key in keep_keys:
            out[key] = {db: to_num(row.get(c)) for db, c in fmap.items() if c}
    return out


if __name__ == "__main__":
    main()
