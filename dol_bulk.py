"""
Shared reader for DOL EFAST2 Form 5500 bulk datasets.

Every refresh script needs the same four things from DOL's bulk zips: the
Massachusetts ESOP returns in a form year's main-form file, the Schedule H / I
rows attached to them, a rule for choosing between several returns a plan files
on one form, and the financial fields those schedules carry. They used to be
re-implemented per script, and the copies drifted in ways that put wrong figures
into the database (see "WHY THIS MODULE EXISTS" below). This module is the one
implementation; the scripts call it.

Files are read straight out of the zips DOL publishes, cached in
``DOL_CACHE_DIR`` (environment variable; default ``/tmp/dol``) under the names
scan_dol_filers.py has always used, e.g. ``2024_Latest_F_SCH_H_2024_Latest.zip``.

WHY THIS MODULE EXISTS
----------------------
Three matching rules here are load-bearing; each fixes a defect that reached
published figures before October 2026.

1. Schedules are joined to their return by ACK_ID, not by (EIN, plan number,
   plan year). A form year's bulk file holds a plan's regular return and, for a
   plan that terminates early the next year, its short final return too. Both
   carry the same EIN and plan number, and an (EIN, PN, year) join pairs the
   participants of one with the dollars of the other. Twelve historical rows
   were mixed this way (Atlantic Plywood 2018 showed 0 active participants
   beside a full year of financials). ACK_ID identifies the filing exactly.

2. Financials are read from end-of-year fields only. The older field lists fell
   back to the beginning-of-year column when the end-of-year one was blank, and
   for employer contributions fell back to EMPLR_CONTRIB_EOY_AMT, which is the
   contributions *receivable* balance (Schedule H line 1b(1)), not the year's
   contributions. A blank end-of-year value is stored as NULL ("not reported").

3. Employer securities come from Schedule H line 1d(1) for large plans and from
   Schedule I line 3d for small plans. Schedule I does itemize company stock —
   "current value of employer securities remaining in the plan at the end of
   the plan year" — which the dataset previously treated as unavailable, leaving
   the figure blank for roughly two-thirds of plans.

4. DOL's "Latest" file is not a complete list of filings. It keeps one accepted
   return per (EIN, plan number, plan year) across the 5500 and the 5500-SF, so
   a filing disappears from it when (a) a *different* plan is later filed under
   the same EIN and plan number — Eastern Bank's own 2024 ESOP return was
   displaced by the acquired Cambridge Bancorp plan filed under Eastern's number
   four months later; Enfield Enterprises' ESOP by its 401(k) on the 5500-SF —
   or (b) the newest version of a filing was rejected with errors, even though
   an earlier version was accepted (Atlantic Sintered 2023). The "All" file keeps
   every version, so returns are read from both (see returns_for_year).

5. A plan-year's only return can be filed years late on a later form (DOL's
   Delinquent Filer program): Econocorp filed plan years 2016-2019 on the 2022
   form. returns_for_year() also looks for those on every later form cached.

6. A return can also arrive on the PREVIOUS year's form: a plan that ends early
   in a year files its short final return on the form then current (Georgetown
   Savings Bank's plan year 2018, on the 2017 form). Every return counts in the
   form year its plan year begins, so returns_for_year() reads the previous
   form for those too, in every year.

7. Participants are the year-end total (line 6f), on the same date as the
   active count (line 6a(2)) and the dollar figures. Line 5, the start-of-year
   total, is kept on each record as participants_boy for reference: it is
   blank-in-effect for a plan's first year and runs below the active count
   whenever a plan grew during the year.
"""
from __future__ import annotations

import csv
import io
import os
import re
import zipfile

csv.field_size_limit(10 ** 9)

CACHE_DIR = os.environ.get("DOL_CACHE_DIR", "/tmp/dol")
DOL_BASE_URL = "https://www.askebsa.dol.gov/FOIA%20Files"

# Type-of-pension-benefit codes that mark an ESOP: 2O (ESOP), 2P (leveraged
# ESOP), 2Q (S-corporation ESOP; normally accompanies 2P, but a plan winding
# down can file 2Q alone). See config.FORM5500_METHODOLOGY.
ESOP_CODES = ("2O", "2P", "2Q")

# Massachusetts ZIP codes begin 010-027 (055 is the IRS's Andover box). A
# return whose state reads MA but whose ZIP lies elsewhere is a typo on the
# return, not a Massachusetts sponsor: The Severn Group's plan-year-2023 return
# says "UPPER MARLBORO, MA 20774" (Maryland), the state on all its other returns.
MA_ZIP3 = {f"{n:03d}" for n in range(10, 28)} | {"055"}

# (EIN, plan number) keys whose returns are read on every form whatever their
# address: Massachusetts companies whose plans are filed from another state.
# Filled by scan_dol_filers from its reviewed MA_BASED_OVERRIDES.
EXTRA_PLAN_KEYS: set = set()

# Schedule H (large plans) — Part I asset/liability statement and Part II
# income/expense lines, end-of-year values only.
SCH_H_FIELDS = {
    "total_assets": "TOT_ASSETS_EOY_AMT",                   # line 1f
    "total_liabilities": "TOT_LIABILITIES_EOY_AMT",         # line 1k
    "employer_securities": "EMPLR_SEC_EOY_AMT",             # line 1d(1)
    "employer_contributions": "EMPLR_CONTRIB_INCOME_AMT",   # line 2a(1)(A)
    "participant_contributions": "PARTICIPANT_CONTRIB_AMT",  # line 2a(1)(B)
    "benefits_paid": "TOT_DISTRIB_BNFT_AMT",                # line 2e(4)
    "net_income": "NET_INCOME_AMT",                         # line 2k
}

# Schedule I (small plans) uses its own SMALL_* column names.
SCH_I_FIELDS = {
    "total_assets": "SMALL_TOT_ASSETS_EOY_AMT",             # line 1a
    "total_liabilities": "SMALL_TOT_LIABILITIES_EOY_AMT",   # line 1b
    "employer_securities": "SMALL_EMPLR_SEC_EOY_AMT",       # line 3d
    "employer_contributions": "SMALL_EMPLR_CONTRIB_INCOME_AMT",  # line 2a(1)
    "participant_contributions": "SMALL_PARTICIPANT_CONTRIB_AMT",  # line 2a(2)
    "benefits_paid": "SMALL_TOT_DISTRIB_BNFT_AMT",          # line 2e
    "net_income": "SMALL_NET_INCOME_AMT",                   # line 2k
}

MONEY_FIELDS = list(SCH_H_FIELDS)


def norm_ein(v) -> str:
    return re.sub(r"\D", "", str(v or "")).lstrip("0")


def norm_pn(v) -> str:
    return re.sub(r"\D", "", str(v or "")).lstrip("0") or "1"


def to_num(v):
    s = str(v or "").strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# DOL publishes each dataset twice: "Latest" (one accepted version per filing)
# and "All" (every version, including amendments and rejected submissions).
VARIANTS = ("Latest", "All")

# EFAST2 processing status of a submission. Accepted returns are preferred; a
# rejected one stands only when no accepted version exists (Atlantic Sintered's
# 2024 return was never accepted, but the plan and its figures are real).
# PROCESSING_STOPPED submissions were never processed and are not filings.
STATUS_RANK = {"FILING_RECEIVED": 2, "": 2, "FILING_ERROR": 1}


def zip_path(year: int, kind: str, variant: str = "Latest") -> str:
    """Cached path of a bulk zip; kind is F_5500, F_5500_SF, F_SCH_H or F_SCH_I."""
    return os.path.join(CACHE_DIR, f"{year}_{variant}_{kind}_{year}_{variant}.zip")


def zip_url(year: int, kind: str, variant: str = "Latest") -> str:
    return f"{DOL_BASE_URL}/{year}/{variant}/{kind}_{year}_{variant}.zip"


def has_zip(year: int, kind: str, variant: str = "Latest") -> bool:
    return os.path.exists(zip_path(year, kind, variant))


def cached_form_years(kind: str = "F_5500") -> list[int]:
    """Form years with at least one variant of `kind` in the cache, ascending."""
    years = set()
    if os.path.isdir(CACHE_DIR):
        for name in os.listdir(CACHE_DIR):
            m = re.match(rf"(\d{{4}})_(Latest|All)_{kind}_\1_(Latest|All)\.zip$", name)
            if m:
                years.add(int(m.group(1)))
    return sorted(years)


def iter_rows(year: int, kind: str, variant: str = "Latest"):
    """csv.DictReader over the CSV inside a cached bulk zip."""
    zf = zipfile.ZipFile(zip_path(year, kind, variant))
    inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig",
                                           errors="replace"))


def has_esop_code(codes: str) -> bool:
    codes = str(codes or "").upper()
    return any(c in codes for c in ESOP_CODES)


def _main_record(r: dict, form_year: int | None = None, variant: str = "Latest") -> dict:
    return {
        "form": "F_5500",
        "form_year": form_year,
        "variant": variant,
        "filing_status": str(r.get("FILING_STATUS") or "").strip().upper(),
        "ack_id": r["ACK_ID"],
        "ein": norm_ein(r["SPONS_DFE_EIN"]),
        "pn": norm_pn(r["SPONS_DFE_PN"]),
        "ein_raw": r["SPONS_DFE_EIN"].strip(),
        "pn_raw": r["SPONS_DFE_PN"].strip(),
        "plan_year_begin": r["FORM_PLAN_YEAR_BEGIN_DATE"].strip(),
        "plan_year_end": r["FORM_TAX_PRD"].strip(),
        "state": r["SPONS_DFE_MAIL_US_STATE"].strip().upper(),
        "codes": str(r["TYPE_PENSION_BNFT_CODE"] or "").upper(),
        "sponsor_name": r["SPONSOR_DFE_NAME"].strip(),
        "sponsor_city": r["SPONS_DFE_MAIL_US_CITY"].strip(),
        "sponsor_zip": r["SPONS_DFE_MAIL_US_ZIP"].strip(),
        "plan_name": r["PLAN_NAME"].strip(),
        "type_plan_entity": r["TYPE_PLAN_ENTITY_CD"].strip(),
        "naics_code": r["BUSINESS_CODE"].strip(),
        "plan_eff_date": r["PLAN_EFF_DATE"].strip(),
        # Line 6f (total at end of year) and line 6a(2) (active at end of
        # year): both on the date the dollar figures describe. See rule 7.
        "total_participants": to_num(r["TOT_ACT_RTD_SEP_BENEF_CNT"]),
        "active_participants": to_num(r["TOT_ACTIVE_PARTCP_CNT"]),
        "participants_boy": to_num(r["TOT_PARTCP_BOY_CNT"]),
        "date_received": r["DATE_RECEIVED"].strip(),
        "amended": r["AMENDED_IND"].strip(),
        "final": r["FINAL_FILING_IND"].strip(),
        "short_year": r["SHORT_PLAN_YR_IND"].strip(),
        "last_rpt_ein": norm_ein(r["LAST_RPT_SPONS_EIN"]),
        "last_rpt_pn": r["LAST_RPT_PLAN_NUM"].strip(),
    }


def is_ma_address(state: str | None, zip_code: str | None) -> bool:
    """Mailing state MA, and a ZIP (when one is given) in Massachusetts' range."""
    if str(state or "").strip().upper() != "MA":
        return False
    z = str(zip_code or "").strip()[:3]
    return len(z) < 3 or not z.isdigit() or z in MA_ZIP3


def _is_ma_esop(r: dict) -> bool:
    return (is_ma_address(r.get("SPONS_DFE_MAIL_US_STATE"), r.get("SPONS_DFE_MAIL_US_ZIP"))
            and has_esop_code(r.get("TYPE_PENSION_BNFT_CODE")))


def ma_esop_returns(year: int, extra_keys: set | None = None,
                    all_versions: bool = True) -> dict:
    """{(ein, pn): [return, ...]} for MA ESOP-coded main-form returns on one form.

    Reads the Latest file and, when it is cached and ``all_versions`` is set, the
    All file too (deduplicated by ACK_ID), so a filing Latest dropped is still
    found. PROCESSING_STOPPED submissions are skipped.

    ``extra_keys`` also pulls in returns for those (ein, pn) keys whatever their
    state or codes, so a tracked plan whose latest return dropped its ESOP code
    or moved state is still found (and can be flagged) rather than vanishing.
    """
    out: dict = {}
    seen: set = set()
    variants = [v for v in VARIANTS if has_zip(year, "F_5500", v)
                and (all_versions or v == "Latest")]
    for variant in variants:
        for r in iter_rows(year, "F_5500", variant):
            ack = r.get("ACK_ID")
            if ack in seen:
                continue
            take = _is_ma_esop(r)
            if not take:
                k = (norm_ein(r.get("SPONS_DFE_EIN")), norm_pn(r.get("SPONS_DFE_PN")))
                take = k in EXTRA_PLAN_KEYS or bool(extra_keys and k in extra_keys)
            if not take:
                continue
            rec = _main_record(r, year, variant)
            if rec["filing_status"] not in STATUS_RANK:
                continue
            seen.add(ack)
            out.setdefault((rec["ein"], rec["pn"]), []).append(rec)
    return out


_LATER_FORM_CACHE: dict = {}


def _ma_esop_returns_cached(form_year: int) -> dict:
    if form_year not in _LATER_FORM_CACHE:
        _LATER_FORM_CACHE[form_year] = ma_esop_returns(form_year)
    return _LATER_FORM_CACHE[form_year]


def returns_for_year(year: int, extra_keys: set | None = None) -> dict:
    """{(ein, pn): [return, ...]} for every return that can represent a plan in
    form year ``year``:

    - every return on the year's own form (Latest and All versions);
    - MA ESOP returns on any LATER cached form whose plan year begins in ``year``
      (late and delinquent filings: Econocorp's 2016-2019 plan years arrived on
      the 2022 form);
    - MA ESOP returns on the PREVIOUS form whose plan year begins in ``year``
      (short final years filed early; rule 6).
    """
    out: dict = {}
    if has_zip(year, "F_5500") or has_zip(year, "F_5500", "All"):
        for k, lst in ma_esop_returns(year, extra_keys).items():
            out.setdefault(k, []).extend(lst)
    later = [f for f in cached_form_years() if f > year]
    sources = [(f, str(year)) for f in later]
    if (year - 1) in cached_form_years():
        sources.append((year - 1, str(year)))
    for form_year, pyb in sources:
        for k, lst in _ma_esop_returns_cached(form_year).items():
            hits = [x for x in lst if x["plan_year_begin"][:4] == pyb]
            if hits:
                have = {x["ack_id"] for x in out.get(k, [])}
                out.setdefault(k, []).extend(x for x in hits if x["ack_id"] not in have)
    return out


# Plan-name test for the coverage check: an ESOP whose return carries no ESOP
# code would be invisible to ma_esop_returns(), so every refresh also looks for
# Massachusetts returns *named* like an ESOP and reports any the code test missed.
ESOP_NAME_RE = re.compile(
    r"\bE\.?S\.?O\.?P\b|\bKSOP\b|EMPLOYEES?'?\s+STOCK|STOCK\s+OWNERSHIP|STOCK\s+BONUS",
    re.IGNORECASE)


def ma_esop_named_returns_without_code(year: int) -> dict:
    """{(ein, pn): [return, ...]} for MA returns named like an ESOP but carrying
    no ESOP code — the population the name-based coverage check reviews.
    Latest and All versions are both read."""
    out: dict = {}
    seen: set = set()
    for variant in VARIANTS:
        if not has_zip(year, "F_5500", variant):
            continue
        for r in iter_rows(year, "F_5500", variant):
            if r.get("ACK_ID") in seen:
                continue
            if not is_ma_address(r.get("SPONS_DFE_MAIL_US_STATE"), r.get("SPONS_DFE_MAIL_US_ZIP")):
                continue
            if has_esop_code(r.get("TYPE_PENSION_BNFT_CODE")):
                continue
            if not ESOP_NAME_RE.search(r.get("PLAN_NAME") or ""):
                continue
            rec = _main_record(r, year, variant)
            if rec["filing_status"] not in STATUS_RANK:
                continue
            seen.add(r["ACK_ID"])
            out.setdefault((rec["ein"], rec["pn"]), []).append(rec)
    return out


def schedules_by_ack(year: int, ack_ids: set) -> dict:
    """{ACK_ID: {"SCH_H": {...}} and/or {"SCH_I": {...}}} for the given filings
    on one form year's schedule files (Latest and All versions)."""
    out: dict = {}
    for kind, fields, pyb_col in (("SCH_H", SCH_H_FIELDS, "SCH_H_PLAN_YEAR_BEGIN_DATE"),
                                  ("SCH_I", SCH_I_FIELDS, "SCH_I_PLAN_YEAR_BEGIN_DATE")):
        for variant in VARIANTS:
            if not has_zip(year, f"F_{kind}", variant):
                continue
            for r in iter_rows(year, f"F_{kind}", variant):
                ack = r["ACK_ID"]
                if ack not in ack_ids or kind in out.get(ack, {}):
                    continue
                rec = {f: to_num(r.get(col)) for f, col in fields.items()}
                rec["plan_year_begin"] = r.get(pyb_col, "")
                rec["_raw"] = r
                out.setdefault(ack, {})[kind] = rec
    return out


def schedules_for_returns(returns: dict) -> dict:
    """Schedules for every return in a returns_for_year() result, each read from
    the schedule files of the form year the return was filed on."""
    by_form: dict = {}
    for lst in returns.values():
        for x in lst:
            by_form.setdefault(x.get("form_year"), set()).add(x["ack_id"])
    out: dict = {}
    for form_year, acks in by_form.items():
        if form_year is None:
            continue
        for ack, sched in schedules_by_ack(form_year, acks).items():
            out.setdefault(ack, {}).update(sched)
    return out


def canonical_return(form_year: int, returns: list[dict]) -> dict:
    """The return that represents a plan for a form year.

    In order of preference: a plan year beginning in the form year (a plan's
    next-year final return can be filed on the same form); a return carrying an
    ESOP code; an accepted rather than rejected submission; a full rather than
    short plan year (Savogran 2024 filed a full
    year and a one-day-later short final year; Erland 2023 a short transition
    year and then its new Nov-Oct fiscal year); then the later plan year and
    the later receipt date.
    """
    def rank(x):
        return (x["plan_year_begin"][:4] == str(form_year),
                has_esop_code(x["codes"]),
                STATUS_RANK.get(x.get("filing_status", ""), 0),
                x["short_year"] != "1",
                x["plan_year_begin"],
                x["date_received"])
    return max(returns, key=rank)


def financials(sched: dict) -> tuple[dict, str | None]:
    """Strict end-of-year financials for one filing's schedules.

    Schedule H wins where a filing carries both. Employer securities are kept
    only when they are a possible value (0 <= securities <= total assets); an
    impossible figure is a filer error and is stored as NULL rather than shown.
    """
    if "SCH_H" in sched:
        src, rec = "SCH_H", sched["SCH_H"]
    elif "SCH_I" in sched:
        src, rec = "SCH_I", sched["SCH_I"]
    else:
        return {}, None
    money = {f: rec.get(f) for f in MONEY_FIELDS}
    es, ta = money.get("employer_securities"), money.get("total_assets")
    if es is not None and (es < 0 or (ta is not None and es > ta)):
        money["employer_securities"] = None
    return money, src
