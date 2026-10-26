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


def zip_path(year: int, kind: str) -> str:
    """Cached path of a bulk zip; kind is F_5500, F_5500_SF, F_SCH_H or F_SCH_I."""
    return os.path.join(CACHE_DIR, f"{year}_Latest_{kind}_{year}_Latest.zip")


def zip_url(year: int, kind: str) -> str:
    return f"{DOL_BASE_URL}/{year}/Latest/{kind}_{year}_Latest.zip"


def has_zip(year: int, kind: str) -> bool:
    return os.path.exists(zip_path(year, kind))


def iter_rows(year: int, kind: str):
    """csv.DictReader over the CSV inside a cached bulk zip."""
    zf = zipfile.ZipFile(zip_path(year, kind))
    inner = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    return csv.DictReader(io.TextIOWrapper(zf.open(inner), encoding="utf-8-sig",
                                           errors="replace"))


def has_esop_code(codes: str) -> bool:
    codes = str(codes or "").upper()
    return any(c in codes for c in ESOP_CODES)


def _main_record(r: dict) -> dict:
    return {
        "form": "F_5500",
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
        # Line 5 (beginning of year) and line 6a(2) (active at end of year) —
        # the pairing the dataset has used for every year.
        "total_participants": to_num(r["TOT_PARTCP_BOY_CNT"]),
        "active_participants": to_num(r["TOT_ACTIVE_PARTCP_CNT"]),
        "date_received": r["DATE_RECEIVED"].strip(),
        "amended": r["AMENDED_IND"].strip(),
        "final": r["FINAL_FILING_IND"].strip(),
        "short_year": r["SHORT_PLAN_YR_IND"].strip(),
        "last_rpt_ein": norm_ein(r["LAST_RPT_SPONS_EIN"]),
        "last_rpt_pn": r["LAST_RPT_PLAN_NUM"].strip(),
    }


def ma_esop_returns(year: int, extra_keys: set | None = None) -> dict:
    """{(ein, pn): [return, ...]} for MA ESOP-coded main-form returns in a form year.

    ``extra_keys`` also pulls in returns for those (ein, pn) keys whatever their
    state or codes, so a tracked plan whose latest return dropped its ESOP code
    or moved state is still found (and can be flagged) rather than vanishing.
    """
    out: dict = {}
    for r in iter_rows(year, "F_5500"):
        rec = None
        if r.get("SPONS_DFE_MAIL_US_STATE", "").strip().upper() == "MA" and \
                has_esop_code(r.get("TYPE_PENSION_BNFT_CODE")):
            rec = _main_record(r)
        elif extra_keys:
            k = (norm_ein(r.get("SPONS_DFE_EIN")), norm_pn(r.get("SPONS_DFE_PN")))
            if k in extra_keys:
                rec = _main_record(r)
        if rec:
            out.setdefault((rec["ein"], rec["pn"]), []).append(rec)
    return out


# Plan-name test for the coverage check: an ESOP whose return carries no ESOP
# code would be invisible to ma_esop_returns(), so every refresh also looks for
# Massachusetts returns *named* like an ESOP and reports any the code test missed.
ESOP_NAME_RE = re.compile(
    r"\bE\.?S\.?O\.?P\b|\bKSOP\b|EMPLOYEES?'?\s+STOCK|STOCK\s+OWNERSHIP|STOCK\s+BONUS",
    re.IGNORECASE)


def ma_esop_named_returns_without_code(year: int) -> dict:
    """{(ein, pn): [return, ...]} for MA returns named like an ESOP but carrying
    no ESOP code — the population the name-based coverage check reviews."""
    out: dict = {}
    for r in iter_rows(year, "F_5500"):
        if r.get("SPONS_DFE_MAIL_US_STATE", "").strip().upper() != "MA":
            continue
        if has_esop_code(r.get("TYPE_PENSION_BNFT_CODE")):
            continue
        if not ESOP_NAME_RE.search(r.get("PLAN_NAME") or ""):
            continue
        rec = _main_record(r)
        out.setdefault((rec["ein"], rec["pn"]), []).append(rec)
    return out


def schedules_by_ack(year: int, ack_ids: set) -> dict:
    """{ACK_ID: {"SCH_H": {...}} and/or {"SCH_I": {...}}} for the given filings."""
    out: dict = {}
    for kind, fields, pyb_col in (("SCH_H", SCH_H_FIELDS, "SCH_H_PLAN_YEAR_BEGIN_DATE"),
                                  ("SCH_I", SCH_I_FIELDS, "SCH_I_PLAN_YEAR_BEGIN_DATE")):
        if not has_zip(year, f"F_{kind}"):
            continue
        for r in iter_rows(year, f"F_{kind}"):
            if r["ACK_ID"] not in ack_ids:
                continue
            rec = {f: to_num(r.get(col)) for f, col in fields.items()}
            rec["plan_year_begin"] = r.get(pyb_col, "")
            rec["_raw"] = r
            out.setdefault(r["ACK_ID"], {})[kind] = rec
    return out


def canonical_return(form_year: int, returns: list[dict]) -> dict:
    """The return that represents a plan for a form year.

    In order of preference: a plan year beginning in the form year (a plan's
    next-year final return can be filed on the same form); a return carrying an
    ESOP code; a full rather than short plan year (Savogran 2024 filed a full
    year and a one-day-later short final year; Erland 2023 a short transition
    year and then its new Nov-Oct fiscal year); then the later plan year and
    the later receipt date.
    """
    def rank(x):
        return (x["plan_year_begin"][:4] == str(form_year),
                has_esop_code(x["codes"]),
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
