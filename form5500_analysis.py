"""
Form 5500 Massachusetts ESOP Analysis Module
=============================================
Downloads and processes DOL EFAST2 bulk filing data to build a unique
Massachusetts-specific ESOP dataset.

Data source: DOL Form 5500 bulk datasets
https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import sqlite3
import threading
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import config

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────

# EINs of plans that filed in 2024 but appear to be zombie / winding-down:
# $0 (or near-$0) total assets and/or 0 active participants with minimal assets.
# These are excluded from Overview metrics but still appear in the full dataset
# and Year-over-Year analysis.
ZOMBIE_PLAN_EINS: set[str] = {
    "42175284",   # Green International Affiliates ($0 assets, 0 active)
    "42348065",   # Janis Research ($0 assets, 0 active)
    "42760904",   # Process Cooling Systems ($0 assets, 59 active)
    "42942412",   # Washburn-Garfield Corporation ($0 assets, 0 active)
    "43484613",   # Tri-Wire ($62 assets, 0 active, 539 total participants)
    "42501424",   # B&B Engineering Corporation ($39K assets, 0 active)
}

FIELD_MAPPINGS = {
    "sponsor_name": ["SPONSOR_DFE_NAME", "SPONS_DFE_NAME", "SPONS_SIGNED_NAME",
                     "SPONS_DFE_MAIL_US_ADDRESS"],
    "sponsor_state": ["SPONS_DFE_MAIL_US_STATE", "SPONS_DFE_LOC_US_STATE",
                      "SPONSOR_DFE_MAIL_US_STATE"],
    "sponsor_city": ["SPONS_DFE_MAIL_US_CITY", "SPONS_DFE_LOC_US_CITY",
                     "SPONSOR_DFE_MAIL_US_CITY"],
    "sponsor_zip": ["SPONS_DFE_MAIL_US_ZIP", "SPONS_DFE_LOC_US_ZIP",
                    "SPONSOR_DFE_MAIL_US_ZIP"],
    "plan_name": ["PLAN_NAME", "PLAN_NM"],
    "plan_eff_date": ["PLAN_EFF_DATE", "PLAN_EFFECTIVE_DT"],
    "ein": ["SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN"],
    "plan_num": ["PLAN_NUM", "PN"],
    "type_plan_entity": ["TYPE_PLAN_ENTITY_CD", "TYPE_PLAN_ENTITY"],
    "type_pension_bnft": ["TYPE_PENSION_BNFT_CD", "TYPE_PENSION_BENEFIT_CD"],
    "total_participants": ["TOT_PARTCP_BOY_CNT", "TOTAL_PARTCP_BOY_CNT",
                           "TOT_PARTCP_EOY_CNT"],
    "active_participants": ["TOT_ACTIVE_PARTCP_CNT", "TOTAL_ACTIVE_PARTCP_CNT",
                            "ACTIVE_PARTCP_CNT"],
    "naics_code": ["BUSINESS_CD", "NAICS_CD", "SIC_CD"],
    "total_assets": ["TOT_ASSETS_BOY_AMT", "TOT_ASSETS_EOY_AMT",
                     "TOTAL_ASSETS_BOY_AMT", "NET_ASSETS_EOY_AMT"],
    "total_liabilities": ["TOT_LIABILITIES_BOY_AMT", "TOT_LIABILITIES_EOY_AMT",
                          "TOTAL_LIABILITIES_BOY_AMT"],
    "employer_contributions": ["EMPLOYER_CONTRIB_AMT", "EMPLR_CONTRIB_AMT",
                               "EMPLR_CONTRIB_CUR_YR_AMT"],
    "participant_contributions": ["PARTCP_CONTRIB_AMT", "PARTCPNT_CONTRIB_AMT"],
    "benefits_paid": ["TOT_DISTRIB_BNFT_AMT", "BENEFIT_PAYMENT_AMT"],
    "net_income": ["NET_INCOME_AMT", "NET_INCM_AMT"],
}

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

SCHEDULE_FINANCIAL_FIELDS = {
    "total_assets": [
        "TOT_ASSETS_EOY_AMT", "TOT_ASSETS_BOY_AMT",
        "TOTAL_ASSETS_EOY_AMT", "TOTAL_ASSETS_BOY_AMT",
        "NET_ASSETS_EOY_AMT",
    ],
    "total_liabilities": [
        "TOT_LIABILITIES_EOY_AMT", "TOT_LIABILITIES_BOY_AMT",
        "TOTAL_LIABILITIES_EOY_AMT", "TOTAL_LIABILITIES_BOY_AMT",
    ],
    "employer_contributions": [
        "EMPLR_CONTRIB_INCOME_AMT", "EMPLR_CONTRIB_AMT", "EMPLOYER_CONTRIB_AMT",
        "EMPLR_CONTRIB_CUR_YR_AMT", "EMPLR_CONTRIB_EOY_AMT",
    ],
    "participant_contributions": [
        "PARTICIPANT_CONTRIB_AMT", "PARTCP_CONTRIB_AMT", "PARTCPNT_CONTRIB_AMT",
        "EE_CONTRIB_AMT",
    ],
    "benefits_paid": [
        "TOT_DISTRIB_BNFT_AMT", "BENEFIT_PAYMENT_AMT",
        "BENEFITS_PAID_AMT",
    ],
    "net_income": [
        "NET_INCOME_AMT", "NET_INCM_AMT",
        "NET_GAIN_LOSS_AMT",
    ],
    "employer_securities": [
        "EMPLR_SEC_EOY_AMT", "EMPLR_SEC_BOY_AMT",
        "EMPLR_SECURITIES_EOY_AMT", "EMPLOYER_SECURITIES_EOY_AMT",
        "EMPLR_SECURITIES_BOY_AMT", "EMPLOYER_SECURITIES_BOY_AMT",
    ],
}

SCHEDULE_EIN_FIELDS = ["SCH_H_EIN", "SCH_I_EIN", "SPONS_DFE_EIN", "SPONSOR_DFE_EIN", "EIN"]
SCHEDULE_PN_FIELDS = ["SCH_H_PN", "SCH_I_PN", "PLAN_NUM", "PN"]


# ── Database Layer ────────────────────────────────

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """One connection per thread."""
    if not hasattr(_local, "f5500_conn") or _local.f5500_conn is None:
        _local.f5500_conn = sqlite3.connect(str(config.DB_PATH), check_same_thread=False)
        _local.f5500_conn.row_factory = sqlite3.Row
        _local.f5500_conn.execute("PRAGMA journal_mode=WAL")
    return _local.f5500_conn


def init_form5500_tables():
    """Create Form 5500 tables if they don't exist."""
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS form5500_filings (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_year             INTEGER NOT NULL,
            ein                     TEXT,
            plan_num                TEXT,
            plan_name               TEXT,
            sponsor_name            TEXT,
            sponsor_city            TEXT,
            sponsor_state           TEXT,
            sponsor_zip             TEXT,
            type_plan_entity        TEXT,
            type_pension_bnft       TEXT,
            is_esop                 INTEGER DEFAULT 0,
            is_ksop                 INTEGER DEFAULT 0,
            total_participants      INTEGER,
            active_participants     INTEGER,
            total_assets            REAL,
            total_liabilities       REAL,
            employer_contributions  REAL,
            participant_contributions REAL,
            benefits_paid           REAL,
            net_income              REAL,
            naics_code              TEXT,
            industry_sector         TEXT,
            plan_eff_date           TEXT,
            fetched                 TEXT,
            UNIQUE(filing_year, ein, plan_num)
        );

        CREATE TABLE IF NOT EXISTS form5500_annual_summary (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_year             INTEGER NOT NULL UNIQUE,
            ma_plan_count           INTEGER,
            ma_esop_count           INTEGER,
            ma_ksop_count           INTEGER,
            ma_total_participants   INTEGER,
            ma_active_participants  INTEGER,
            ma_total_assets         REAL,
            ma_avg_plan_assets      REAL,
            ma_total_contributions  REAL,
            ma_avg_participants     REAL,
            us_total_esop_count     INTEGER,
            us_total_participants   INTEGER,
            us_total_assets         REAL,
            processed_at            TEXT
        );

        CREATE TABLE IF NOT EXISTS form5500_meta (
            key     TEXT PRIMARY KEY,
            value   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_f5500_year ON form5500_filings(filing_year);
        CREATE INDEX IF NOT EXISTS idx_f5500_state ON form5500_filings(sponsor_state);
        CREATE INDEX IF NOT EXISTS idx_f5500_esop ON form5500_filings(is_esop);
    """)
    for col, tbl in [
        ("employer_securities REAL", "form5500_filings"),
        ("ma_total_benefits_paid REAL", "form5500_annual_summary"),
        ("ma_total_employer_securities REAL", "form5500_annual_summary"),
    ]:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def set_meta(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO form5500_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value)
    )
    conn.commit()


def get_meta(key: str) -> str | None:
    row = _get_conn().execute(
        "SELECT value FROM form5500_meta WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else None


# ── Data Insertion ────────────────────────────────

def clear_form5500_data():
    conn = _get_conn()
    conn.execute("DELETE FROM form5500_filings")
    conn.execute("DELETE FROM form5500_annual_summary")
    conn.execute("DELETE FROM form5500_meta")
    conn.commit()


def insert_filings(records: list[dict]):
    if not records:
        return
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    for r in records:
        r.setdefault("fetched", now)
        r.setdefault("is_ksop", 0)
        r.setdefault("industry_sector", "")
        r.setdefault("employer_securities", None)
    conn.executemany("""
        INSERT OR REPLACE INTO form5500_filings
        (filing_year, ein, plan_num, plan_name, sponsor_name, sponsor_city,
         sponsor_state, sponsor_zip, type_plan_entity, type_pension_bnft,
         is_esop, is_ksop, total_participants, active_participants,
         total_assets, total_liabilities, employer_contributions,
         participant_contributions, benefits_paid, net_income,
         naics_code, industry_sector, plan_eff_date, fetched, employer_securities)
        VALUES
        (:filing_year, :ein, :plan_num, :plan_name, :sponsor_name, :sponsor_city,
         :sponsor_state, :sponsor_zip, :type_plan_entity, :type_pension_bnft,
         :is_esop, :is_ksop, :total_participants, :active_participants,
         :total_assets, :total_liabilities, :employer_contributions,
         :participant_contributions, :benefits_paid, :net_income,
         :naics_code, :industry_sector, :plan_eff_date, :fetched, :employer_securities)
    """, records)
    conn.commit()


def insert_annual_summary(summary: dict):
    conn = _get_conn()
    summary["processed_at"] = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO form5500_annual_summary
        (filing_year, ma_plan_count, ma_esop_count, ma_ksop_count,
         ma_total_participants, ma_active_participants, ma_total_assets,
         ma_avg_plan_assets, ma_total_contributions, ma_avg_participants,
         us_total_esop_count, us_total_participants, us_total_assets,
         processed_at)
        VALUES
        (:filing_year, :ma_plan_count, :ma_esop_count, :ma_ksop_count,
         :ma_total_participants, :ma_active_participants, :ma_total_assets,
         :ma_avg_plan_assets, :ma_total_contributions, :ma_avg_participants,
         :us_total_esop_count, :us_total_participants, :us_total_assets,
         :processed_at)
    """, summary)
    conn.commit()


# ── Query Functions ───────────────────────────────

def get_annual_summaries() -> list[dict]:
    rows = _get_conn().execute(
        "SELECT * FROM form5500_annual_summary ORDER BY filing_year"
    ).fetchall()
    return [dict(r) for r in rows]


def _zombie_clause() -> str:
    """Return SQL clause that excludes zombie-plan EINs."""
    placeholders = ",".join(f"'{e}'" for e in ZOMBIE_PLAN_EINS)
    return f" AND CAST(ein AS TEXT) NOT IN ({placeholders})"


def get_ma_filings(year: int | None = None, *,
                   exclude_zombie: bool = False) -> list[dict]:
    base = ("SELECT * FROM form5500_filings WHERE sponsor_state = 'MA' "
            "AND is_esop = 1")
    params: list = []
    if year:
        base += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        base += _zombie_clause()
    base += " ORDER BY " + ("total_assets DESC" if year
                            else "filing_year DESC, total_assets DESC")
    rows = _get_conn().execute(base, params).fetchall()
    return [dict(r) for r in rows]


def get_latest_year() -> int | None:
    row = _get_conn().execute(
        "SELECT MAX(filing_year) as yr FROM form5500_annual_summary"
    ).fetchone()
    return row["yr"] if row and row["yr"] else None


def get_ma_filings_by_city(year: int | None = None, *,
                           exclude_zombie: bool = False) -> list[dict]:
    query = """
        SELECT sponsor_city, COUNT(*) as plan_count,
               SUM(total_participants) as total_partcp,
               SUM(total_assets) as total_assets
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        query += _zombie_clause()
    query += " GROUP BY sponsor_city ORDER BY plan_count DESC"
    rows = _get_conn().execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_ma_filings_by_industry(year: int | None = None, *,
                               exclude_zombie: bool = False) -> list[dict]:
    query = """
        SELECT industry_sector, COUNT(*) as plan_count,
               SUM(total_participants) as total_partcp,
               SUM(total_assets) as total_assets
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
              AND industry_sector IS NOT NULL AND industry_sector != ''
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        query += _zombie_clause()
    query += " GROUP BY industry_sector ORDER BY plan_count DESC"
    rows = _get_conn().execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_top_ma_esops(year: int | None = None, limit: int = 25) -> list[dict]:
    query = """
        SELECT plan_name, sponsor_name, sponsor_city, sponsor_zip,
               industry_sector, total_participants, active_participants,
               total_assets, employer_contributions, filing_year
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    query += " ORDER BY total_assets DESC LIMIT ?"
    params.append(limit)
    rows = _get_conn().execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_new_and_terminated(year: int) -> tuple[list[dict], list[dict]]:
    prev = year - 1
    conn = _get_conn()

    new_rows = conn.execute("""
        SELECT f.* FROM form5500_filings f
        WHERE f.sponsor_state = 'MA' AND f.is_esop = 1 AND f.filing_year = ?
        AND NOT EXISTS (
            SELECT 1 FROM form5500_filings f2
            WHERE f2.ein = f.ein AND f2.plan_num = f.plan_num AND f2.filing_year = ?
        )
        ORDER BY f.total_assets DESC
    """, (year, prev)).fetchall()

    # Enrich new plans with status based on filing history
    new_plans = []
    for r in new_rows:
        d = dict(r)
        prior_years = conn.execute("""
            SELECT COUNT(*) FROM form5500_filings
            WHERE ein = ? AND plan_num = ? AND sponsor_state = 'MA'
              AND is_esop = 1 AND filing_year < ?
        """, (d["ein"], d["plan_num"], year)).fetchone()[0]

        eff = str(d.get("plan_eff_date", "") or "")[:4]
        if prior_years == 0 and eff == str(year):
            d["yoy_status"] = "New ESOP (Started {})".format(year)
        elif prior_years == 0 and eff == str(prev):
            d["yoy_status"] = "New ESOP (Started {})".format(prev)
        elif prior_years == 0:
            d["yoy_status"] = "New Filing (Started {})".format(eff if eff else "N/A")
        else:
            d["yoy_status"] = "Returning"
        new_plans.append(d)

    term_rows = conn.execute("""
        SELECT f.* FROM form5500_filings f
        WHERE f.sponsor_state = 'MA' AND f.is_esop = 1 AND f.filing_year = ?
        AND NOT EXISTS (
            SELECT 1 FROM form5500_filings f2
            WHERE f2.ein = f.ein AND f2.plan_num = f.plan_num AND f2.filing_year = ?
        )
        ORDER BY f.total_assets DESC
    """, (prev, year)).fetchall()

    # Categorise plans that did not file in current year.
    # As of Apr 2026 DOL EFAST2 search + web research confirms:
    #   - "Confirmed Terminated" = ESOP terminated due to acquisition, merger,
    #     or plan wind-down.  Dict value gives the reason.
    #   - "Late Filer" = ESOP believed still active; no 2024 filing yet on DOL.
    #   - Acentech removed: filed 2024 ESOP (received 2026-02-25)
    _CONFIRMED_TERMINATED_EINS: dict[str, str] = {
        "43247749":   "Acquired by Artemis Capital Partners (Nov 2024)",
        "41866480":   "Taken private by MiddleGround Capital (May 2024)",
        "42597651":   "Acquired by Blackstone (Nov 2021)",
        "10367721":   "Acquired by BetterBody Foods (Dec 2024)",
        "42772059":   "Acquired by Ecolab (Nov 2024)",
        "42777442":   "Merged into Eastern Bankshares (Jul 2024)",
        "521405842":  "Acquired by PAE/Amentum (Nov 2020) — final filing",
        "43448069":   "Acquired by Qmerit (Sep 2021) — winding down",
        "42372206":   "Acquired by Ascensus Specialties (Apr 2021)",
        "822323992":  "Sold to CI Capital Partners (Sep 2021) — final filing",
        "43533865":   "ESOP terminated ($0 assets, 0 active participants)",
        "43053085":   "Privately held; no public ESOP evidence — likely terminated",
        "550796211":  "MD-based entity; ESOP status unconfirmed — likely terminated",
    }

    # Active ESOPs — filing likely delayed, not terminated
    _CONFIRMED_ACTIVE_EINS: set[str] = {
        "813645861",   # Shawmut Group — still 100% employee-owned
        "42880295",    # Web Industries — still 100% employee-owned
        "42471226",    # Aerodyne Research — employee-owned since 1985
        "42932946",    # Darmann Abrasive — ESOP since 1999
        "42472856",    # James Monroe Corp — 33% employee-owned
    }

    terminated: list[dict] = []
    late_filers: list[dict] = []

    for r in term_rows:
        d = dict(r)
        ein_norm = str(d["ein"]).lstrip("0") or "0"

        if ein_norm in _CONFIRMED_TERMINATED_EINS:
            d["yoy_status"] = "Confirmed Terminated"
            d["yoy_note"] = _CONFIRMED_TERMINATED_EINS[ein_norm]
            terminated.append(d)
        else:
            if ein_norm in _CONFIRMED_ACTIVE_EINS:
                d["yoy_status"] = "Late Filer (Active ESOP)"
            else:
                d["yoy_status"] = "Late Filer"
            d["yoy_note"] = ""
            late_filers.append(d)

    return new_plans, terminated, late_filers


def get_asset_distribution(year: int | None = None, *,
                           exclude_zombie: bool = False) -> list[float]:
    query = """
        SELECT total_assets FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
              AND total_assets IS NOT NULL AND total_assets > 0
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        query += _zombie_clause()
    rows = _get_conn().execute(query, params).fetchall()
    return [r["total_assets"] for r in rows]


def get_participant_distribution(year: int | None = None, *,
                                 exclude_zombie: bool = False) -> list[int]:
    query = """
        SELECT total_participants FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
              AND total_participants IS NOT NULL AND total_participants > 0
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        query += _zombie_clause()
    rows = _get_conn().execute(query, params).fetchall()
    return [r["total_participants"] for r in rows]


def has_data() -> bool:
    row = _get_conn().execute(
        "SELECT COUNT(*) as cnt FROM form5500_filings"
    ).fetchone()
    return row["cnt"] > 0 if row else False


def has_financial_data() -> bool:
    row = _get_conn().execute(
        "SELECT COUNT(*) as cnt FROM form5500_filings "
        "WHERE total_assets IS NOT NULL AND total_assets > 0"
    ).fetchone()
    return row["cnt"] > 0 if row else False


def get_financial_summary(year: int = None, *,
                          exclude_zombie: bool = False) -> dict:
    query = """
        SELECT
            COUNT(*) as plans_total,
            SUM(CASE WHEN total_assets IS NOT NULL AND total_assets > 0 THEN 1 ELSE 0 END) as plans_with_assets,
            SUM(COALESCE(total_assets, 0)) as total_assets,
            SUM(COALESCE(employer_securities, 0)) as total_employer_securities,
            SUM(COALESCE(employer_contributions, 0)) as total_employer_contributions,
            SUM(COALESCE(participant_contributions, 0)) as total_participant_contributions,
            SUM(COALESCE(benefits_paid, 0)) as total_benefits_paid,
            SUM(COALESCE(net_income, 0)) as total_net_income,
            SUM(COALESCE(total_participants, 0)) as total_participants
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    if exclude_zombie:
        query += _zombie_clause()
    row = _get_conn().execute(query, params).fetchone()
    if not row:
        return {}
    d = dict(row)
    plans_with = d.get("plans_with_assets") or 0
    total_assets = d.get("total_assets") or 0
    total_part = d.get("total_participants") or 0
    d["avg_assets_per_plan"] = total_assets / plans_with if plans_with > 0 else 0
    d["avg_assets_per_participant"] = total_assets / total_part if total_part > 0 else 0
    return d


def get_plans_with_financial_data(year: int = None) -> list:
    query = """
        SELECT * FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
              AND total_assets IS NOT NULL AND total_assets > 0
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    query += " ORDER BY total_assets DESC"
    rows = _get_conn().execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_asset_per_participant_distribution(year: int = None) -> list:
    query = """
        SELECT plan_name, sponsor_name, sponsor_city, total_assets,
               total_participants, employer_securities, employer_contributions,
               CAST(total_assets AS REAL) / total_participants as assets_per_participant
        FROM form5500_filings
        WHERE sponsor_state = 'MA' AND is_esop = 1
              AND total_assets IS NOT NULL AND total_assets > 0
              AND total_participants IS NOT NULL AND total_participants > 0
    """
    params: list = []
    if year:
        query += " AND filing_year = ?"
        params.append(year)
    query += " ORDER BY assets_per_participant DESC"
    rows = _get_conn().execute(query, params).fetchall()
    return [dict(r) for r in rows]


# ── Data Processing Logic ─────────────────────────

def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        s = str(val).strip().replace(",", "").replace("$", "")
        if not s or s in ("-", ".", "N/A", ""):
            return None
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _safe_money(val) -> float | None:
    if val is None:
        return None
    try:
        s = str(val).strip().replace(",", "").replace("$", "")
        if not s or s in ("-", ".", "N/A", ""):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def _get_field(row: dict, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in row and row[c]:
            return str(row[c]).strip()
        for k, v in row.items():
            if k.upper() == c.upper() and v:
                return str(v).strip()
    return None


def _extract_sponsor_from_plan_name(plan_name: str) -> str:
    if not plan_name or plan_name == "nan":
        return ""
    suffixes = [
        r"\s+EMPLOYEE\s+STOCK\s+OWNERSHIP\s+PLAN\b.*",
        r"\s+ESOP\b.*",
        r"\s+STOCK\s+BONUS\s+PLAN\b.*",
        r"\s+LEVERAGED\s+ESOP\b.*",
        r"\s+KSOP\b.*",
        r"\s+401\(K\).*ESOP\b.*",
        r"\s+SAVINGS?\s+AND\s+ESOP\b.*",
        r"\s+THRIFT\s+AND\s+ESOP\b.*",
        r"\s+STOCK\s+OWNERSHIP\s+PLAN\b.*",
    ]
    name = plan_name.strip()
    for pattern in suffixes:
        name = re.sub(pattern, "", name, flags=re.IGNORECASE).strip()
    name = name.rstrip(" -\u2013\u2014,;:")
    if name == name.upper() and len(name) > 3:
        name = name.title()
    return name


def _normalize_ein(ein_raw: str) -> str:
    if not ein_raw:
        return ""
    return str(ein_raw).strip().lstrip("0") or "0"


def _normalize_pn(pn_raw: str) -> str:
    if not pn_raw:
        return ""
    return str(pn_raw).strip().lstrip("0") or "0"


# ── Import from pre-processed CSV ────────────────

def import_from_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    logger.info("Importing %d records from %s", len(df), csv_path)

    records = []
    for _, row in df.iterrows():
        plan_name = str(row.get("plan_name", ""))
        sponsor_raw = str(row.get("sponsor_name", ""))

        if sponsor_raw.replace(".", "").replace("-", "").isdigit() or sponsor_raw in ("nan", ""):
            sponsor_name = _extract_sponsor_from_plan_name(plan_name)
            plan_num_raw = str(row.get("plan_num", ""))
            if plan_num_raw in ("nan", ""):
                plan_num = sponsor_raw if sponsor_raw not in ("nan", "") else ""
            else:
                plan_num = plan_num_raw
        else:
            sponsor_name = sponsor_raw
            plan_num = str(row.get("plan_num", ""))

        records.append({
            "filing_year": int(row.get("filing_year", 0)),
            "ein": str(row.get("ein", "")),
            "plan_num": plan_num if plan_num != "nan" else "",
            "plan_name": plan_name if plan_name != "nan" else "",
            "sponsor_name": sponsor_name,
            "sponsor_city": str(row.get("sponsor_city", "")).replace("nan", ""),
            "sponsor_state": str(row.get("sponsor_state", "")).replace("nan", ""),
            "sponsor_zip": str(row.get("sponsor_zip", "")).replace("nan", ""),
            "type_plan_entity": str(row.get("type_plan_entity", "")).replace("nan", ""),
            "type_pension_bnft": str(row.get("type_pension_bnft", "")).replace("nan", ""),
            "is_esop": int(row.get("is_esop", 0)),
            "is_ksop": int(row.get("is_ksop", 0)),
            "total_participants": _safe_int(row.get("total_participants")),
            "active_participants": _safe_int(row.get("active_participants")),
            "total_assets": _safe_money(row.get("total_assets")),
            "total_liabilities": _safe_money(row.get("total_liabilities")),
            "employer_contributions": _safe_money(row.get("employer_contributions")),
            "participant_contributions": _safe_money(row.get("participant_contributions")),
            "benefits_paid": _safe_money(row.get("benefits_paid")),
            "net_income": _safe_money(row.get("net_income")),
            "employer_securities": _safe_money(row.get("employer_securities")),
            "naics_code": str(row.get("naics_code", "")).replace("nan", ""),
            "industry_sector": str(row.get("industry_sector", "")).replace("nan", ""),
            "plan_eff_date": str(row.get("plan_eff_date", "")).replace("nan", ""),
        })

    insert_filings(records)

    years = sorted(set(r["filing_year"] for r in records))
    for yr in years:
        yr_records = [r for r in records if r["filing_year"] == yr]
        us_summary = {
            "us_total_esop_count": 0,
            "us_total_participants": 0,
            "us_total_assets": 0.0,
        }
        summary = compute_annual_summary(yr, yr_records, us_summary)
        insert_annual_summary(summary)

    set_meta("last_import", datetime.now(timezone.utc).isoformat())
    set_meta("source", csv_path)
    logger.info("Import complete: %d records across %d years", len(records), len(years))


def import_from_summary_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        summary = {
            "filing_year": int(row.get("filing_year", 0)),
            "ma_plan_count": int(row.get("ma_plan_count", 0)),
            "ma_esop_count": int(row.get("ma_esop_count", 0)),
            "ma_ksop_count": int(row.get("ma_ksop_count", 0)),
            "ma_total_participants": int(row.get("ma_total_participants", 0)),
            "ma_active_participants": int(row.get("ma_active_participants", 0)),
            "ma_total_assets": float(row.get("ma_total_assets", 0)),
            "ma_avg_plan_assets": float(row.get("ma_avg_plan_assets", 0)),
            "ma_total_contributions": float(row.get("ma_total_contributions", 0)),
            "ma_avg_participants": float(row.get("ma_avg_participants", 0)),
            "us_total_esop_count": int(row.get("us_total_esop_count", 0)),
            "us_total_participants": int(row.get("us_total_participants", 0)),
            "us_total_assets": float(row.get("us_total_assets", 0)),
        }
        insert_annual_summary(summary)
    set_meta("last_import", datetime.now(timezone.utc).isoformat())
    set_meta("source", csv_path)


def compute_annual_summary(filing_year: int, ma_records: list[dict],
                           us_summary: dict) -> dict:
    esop_count = len(ma_records)
    ksop_count = sum(1 for r in ma_records if r.get("is_ksop"))
    total_partcp = sum(r.get("total_participants") or 0 for r in ma_records)
    active_partcp = sum(r.get("active_participants") or 0 for r in ma_records)
    total_assets = sum(r.get("total_assets") or 0 for r in ma_records)
    total_contrib = sum(r.get("employer_contributions") or 0 for r in ma_records)
    avg_assets = total_assets / esop_count if esop_count > 0 else 0
    avg_partcp = total_partcp / esop_count if esop_count > 0 else 0

    return {
        "filing_year": filing_year,
        "ma_plan_count": esop_count,
        "ma_esop_count": esop_count - ksop_count,
        "ma_ksop_count": ksop_count,
        "ma_total_participants": total_partcp,
        "ma_active_participants": active_partcp,
        "ma_total_assets": total_assets,
        "ma_avg_plan_assets": avg_assets,
        "ma_total_contributions": total_contrib,
        "ma_avg_participants": avg_partcp,
        "us_total_esop_count": us_summary.get("us_total_esop_count", 0),
        "us_total_participants": us_summary.get("us_total_participants", 0),
        "us_total_assets": us_summary.get("us_total_assets", 0),
    }


# ── Schedule H/I Financial Data Import ────────────

def import_schedule_csv(csv_path: str, schedule_type: str = "H",
                        filing_year: int = None) -> dict:
    stats = {
        "matched": 0, "updated": 0, "skipped": 0, "total_rows": 0,
        "year": filing_year, "schedule_type": schedule_type, "path": csv_path,
    }

    if not os.path.exists(csv_path):
        logger.warning("Schedule CSV not found: %s", csv_path)
        return stats

    if filing_year is None:
        year_match = re.search(r'(20\d{2})', os.path.basename(csv_path))
        if year_match:
            filing_year = int(year_match.group(1))
            stats["year"] = filing_year
        else:
            logger.warning("Cannot determine filing year from filename: %s", csv_path)
            return stats

    conn = _get_conn()

    existing = {}
    rows = conn.execute(
        "SELECT id, ein, plan_num FROM form5500_filings "
        "WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = ?",
        (filing_year,)
    ).fetchall()
    for r in rows:
        key = (_normalize_ein(r["ein"]), _normalize_pn(r["plan_num"]))
        existing[key] = r["id"]

    if not existing:
        logger.warning("No existing MA ESOP filings for year %d to match against", filing_year)
        return stats

    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            batch_updates = []

            for row in reader:
                stats["total_rows"] += 1

                ein_raw = _get_field(row, SCHEDULE_EIN_FIELDS) or ""
                pn_raw = _get_field(row, SCHEDULE_PN_FIELDS) or ""
                key = (_normalize_ein(ein_raw), _normalize_pn(pn_raw))

                if key not in existing:
                    stats["skipped"] += 1
                    continue

                stats["matched"] += 1

                fin_data = {}
                for field_name, candidates in SCHEDULE_FINANCIAL_FIELDS.items():
                    val = _safe_money(_get_field(row, candidates))
                    if val is not None:
                        fin_data[field_name] = val

                if not fin_data:
                    continue

                stats["updated"] += 1
                row_id = existing[key]

                set_clauses = []
                params = []
                for fname, fval in fin_data.items():
                    set_clauses.append(f"{fname} = COALESCE(?, {fname})")
                    params.append(fval)
                params.append(row_id)

                batch_updates.append((
                    f"UPDATE form5500_filings SET {', '.join(set_clauses)} WHERE id = ?",
                    params
                ))

                if len(batch_updates) >= 500:
                    for sql, p in batch_updates:
                        conn.execute(sql, p)
                    conn.commit()
                    batch_updates = []

            if batch_updates:
                for sql, p in batch_updates:
                    conn.execute(sql, p)
                conn.commit()

    except Exception as e:
        logger.error("Error importing schedule CSV %s: %s", csv_path, e)

    set_meta(f"schedule_{schedule_type.lower()}_{filing_year}_imported",
             datetime.now(timezone.utc).isoformat())

    return stats


def import_all_schedule_csvs(schedule_dir: str = None) -> list:
    schedule_dir = schedule_dir or config.FORM5500_SCHEDULE_DIR
    if not os.path.isdir(schedule_dir):
        return []

    results = []
    for filename in sorted(os.listdir(schedule_dir)):
        if not filename.lower().endswith(".csv"):
            continue
        fn_lower = filename.lower()

        sch_type = None
        if any(p.lower() in fn_lower for p in config.FORM5500_SCHEDULE_H_PATTERNS):
            sch_type = "H"
        elif any(p.lower() in fn_lower for p in config.FORM5500_SCHEDULE_I_PATTERNS):
            sch_type = "I"

        if sch_type is None:
            continue

        year_match = re.search(r'(20\d{2})', filename)
        filing_year = int(year_match.group(1)) if year_match else None

        full_path = os.path.join(schedule_dir, filename)
        result = import_schedule_csv(full_path, schedule_type=sch_type,
                                      filing_year=filing_year)
        results.append(result)

    return results


def recompute_annual_summaries():
    conn = _get_conn()
    years = [r[0] for r in conn.execute(
        "SELECT DISTINCT filing_year FROM form5500_filings "
        "WHERE sponsor_state = 'MA' AND is_esop = 1 ORDER BY filing_year"
    ).fetchall()]

    for yr in years:
        row = conn.execute("""
            SELECT
                COUNT(*) as ma_plan_count,
                SUM(CASE WHEN is_ksop = 1 THEN 1 ELSE 0 END) as ma_ksop_count,
                SUM(COALESCE(total_participants, 0)) as ma_total_participants,
                SUM(COALESCE(active_participants, 0)) as ma_active_participants,
                SUM(COALESCE(total_assets, 0)) as ma_total_assets,
                SUM(COALESCE(employer_contributions, 0)) as ma_total_contributions,
                SUM(COALESCE(benefits_paid, 0)) as ma_total_benefits_paid,
                SUM(COALESCE(employer_securities, 0)) as ma_total_employer_securities
            FROM form5500_filings
            WHERE sponsor_state = 'MA' AND is_esop = 1 AND filing_year = ?
        """, (yr,)).fetchone()

        if not row:
            continue

        plan_count = row["ma_plan_count"] or 0
        ksop_count = row["ma_ksop_count"] or 0
        total_assets = row["ma_total_assets"] or 0

        existing = conn.execute(
            "SELECT us_total_esop_count, us_total_participants, us_total_assets "
            "FROM form5500_annual_summary WHERE filing_year = ?", (yr,)
        ).fetchone()

        us_esop = existing["us_total_esop_count"] if existing else 0
        us_part = existing["us_total_participants"] if existing else 0
        us_assets = existing["us_total_assets"] if existing else 0

        summary = {
            "filing_year": yr,
            "ma_plan_count": plan_count,
            "ma_esop_count": plan_count - ksop_count,
            "ma_ksop_count": ksop_count,
            "ma_total_participants": row["ma_total_participants"] or 0,
            "ma_active_participants": row["ma_active_participants"] or 0,
            "ma_total_assets": total_assets,
            "ma_avg_plan_assets": total_assets / plan_count if plan_count > 0 else 0,
            "ma_total_contributions": row["ma_total_contributions"] or 0,
            "ma_avg_participants": (row["ma_total_participants"] or 0) / plan_count if plan_count > 0 else 0,
            "us_total_esop_count": us_esop or 0,
            "us_total_participants": us_part or 0,
            "us_total_assets": us_assets or 0,
        }
        insert_annual_summary(summary)

        conn.execute("""
            UPDATE form5500_annual_summary
            SET ma_total_benefits_paid = ?,
                ma_total_employer_securities = ?
            WHERE filing_year = ?
        """, (row["ma_total_benefits_paid"] or 0,
              row["ma_total_employer_securities"] or 0,
              yr))

    conn.commit()
    set_meta("last_recompute", datetime.now(timezone.utc).isoformat())


# Initialize on import
init_form5500_tables()
