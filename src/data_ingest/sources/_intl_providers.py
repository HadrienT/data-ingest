"""Download and parse logic for the `intl-rates` source, one provider each.

Leading underscore so the registry's auto-discovery skips this module.

Every provider here is a central bank or a treasury publishing for free, with
no API key: the ECB Data Portal, the Bank of England's Statistical Interactive
Database, the SNB data portal, the Bank of Japan's time-series API and the
Japanese Ministry of Finance. Each has two functions:

- `parse_*` turns the provider's raw text into (series_id, date, value) rows.
  Pure — the unit tests feed it recorded payloads, no network needed.
- `fetch_*` downloads for a window and hands the text to its parser.

All values are in percent per annum, like FRED's. A missing observation
(the provider's "-", ".", empty cell) is dropped, never stored as NaN: a NaN
in a DOUBLE column survives every row count and poisons every later AVG.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

import requests

logger = logging.getLogger("data_ingest.intl_rates")

Row = Tuple[str, date, float]

#: Some of these servers refuse the default python-requests agent.
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; data-ingest; +https://github.com/HadrienT/data-ingest)"}
_TIMEOUT_S = 60


def _get(url: str, params: Optional[Mapping[str, str]] = None) -> str:
    response = requests.get(url, params=params, headers=_HEADERS, timeout=_TIMEOUT_S)
    response.raise_for_status()
    return response.text


def _number(raw: str) -> Optional[float]:
    raw = (raw or "").strip().strip('"')
    if raw in ("", "-", ".", "NaN", "ND"):
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value == value else None  # NaN is not a value


def _since(rows: Iterable[Row], start: Optional[date]) -> List[Row]:
    return [r for r in rows if start is None or r[1] >= start]


# ── ECB Data Portal (SDMX REST, CSV) ────────────────────────────────────────
# https://data.ecb.europa.eu/help/api/data

ECB_URL = "https://data-api.ecb.europa.eu/service/data/{flow}/{key}"


def parse_ecb_csv(text: str, series: Mapping[str, str]) -> List[Row]:
    """`series` maps the ECB's full series KEY to our series_id. A monthly
    TIME_PERIOD ("2026-08") is dated the first of its month, FRED's
    convention for monthly series."""
    rows: List[Row] = []
    for record in csv.DictReader(io.StringIO(text)):
        series_id = series.get(record.get("KEY", ""))
        value = _number(record.get("OBS_VALUE", ""))
        if series_id is None or value is None:
            continue
        period = record["TIME_PERIOD"]
        day = date.fromisoformat(period if len(period) == 10 else f"{period}-01")
        rows.append((series_id, day, value))
    return rows


def fetch_ecb(series: Mapping[str, str], start: Optional[date]) -> List[Row]:
    """One request per dataflow; keys of a flow are OR-ed with `+` in the
    last dimension, which is how SDMX asks for several series at once."""
    by_flow: Dict[Tuple[str, str], List[str]] = {}
    for key in series:
        flow, rest = key.split(".", 1)
        prefix, last = rest.rsplit(".", 1)
        by_flow.setdefault((flow, prefix), []).append(last)
    rows: List[Row] = []
    for (flow, prefix), lasts in by_flow.items():
        params = {"format": "csvdata"}
        if start is not None:
            # Month granularity, so a monthly series' last value (dated the 1st)
            # is not cut off by a window starting mid-month.
            params["startPeriod"] = start.strftime("%Y-%m")
        url = ECB_URL.format(flow=flow, key=f"{prefix}.{'+'.join(lasts)}")
        rows += parse_ecb_csv(_get(url, params), series)
    return _since(rows, start.replace(day=1) if start else None)


# ── Bank of England, Statistical Interactive Database ───────────────────────
# https://www.bankofengland.co.uk/boeapps/database/

BOE_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
_BOE_FIRST = date(1975, 1, 1)


def parse_boe_csv(text: str, series: Mapping[str, str]) -> List[Row]:
    """CSV with a DATE column ("22 Sep 2026") then one column per code."""
    rows: List[Row] = []
    reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
    for record in reader:
        raw_date = (record.get("DATE") or "").strip()
        if not raw_date:
            continue
        day = datetime.strptime(raw_date, "%d %b %Y").date()
        for code, series_id in series.items():
            value = _number(record.get(code, ""))
            if value is not None:
                rows.append((series_id, day, value))
    return rows


def fetch_boe(series: Mapping[str, str], start: Optional[date]) -> List[Row]:
    params = {
        "csv.x": "yes",
        "Datefrom": (start or _BOE_FIRST).strftime("%d/%b/%Y"),
        "Dateto": "now",
        "SeriesCodes": ",".join(series),
        "CSVF": "TN",  # tabular, no titles: DATE + one column per code
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }
    return _since(parse_boe_csv(_get(BOE_URL, params), series), start)


# ── Swiss National Bank data portal ─────────────────────────────────────────
# https://data.snb.ch/en/help/api — a cube is downloaded whole (it is small).

SNB_URL = "https://data.snb.ch/api/cube/{cube}/data/csv/en"


def parse_snb_csv(text: str, series: Mapping[str, str]) -> List[Row]:
    """Semicolon CSV: a few metadata lines, a blank line, then
    "Date";"D0";"Value". `series` maps the D0 code to our series_id."""
    body = text.lstrip("﻿").replace("\r", "").split("\n\n", 1)[-1]
    rows: List[Row] = []
    for record in csv.DictReader(io.StringIO(body), delimiter=";"):
        series_id = series.get(record.get("D0", ""))
        value = _number(record.get("Value", ""))
        if series_id is None or value is None:
            continue
        rows.append((series_id, date.fromisoformat(record["Date"]), value))
    return rows


def fetch_snb(cube: str, series: Mapping[str, str], start: Optional[date]) -> List[Row]:
    return _since(parse_snb_csv(_get(SNB_URL.format(cube=cube)), series), start)


# ── Bank of Japan time-series API ───────────────────────────────────────────
# https://www.stat-search.boj.or.jp/info/api_manual_en.pdf

BOJ_URL = "https://www.stat-search.boj.or.jp/api/v1/getDataCode"
_BOJ_FIRST = date(1985, 7, 1)


def parse_boj_csv(text: str, series: Mapping[str, str]) -> List[Row]:
    """A STATUS/MESSAGE header, then one line per observation:
    code, name, unit, frequency, category, last update, YYYYMMDD, value."""
    rows: List[Row] = []
    for line in csv.reader(io.StringIO(text)):
        if len(line) < 8:
            continue
        series_id = series.get(line[0])
        value = _number(line[7])
        if series_id is None or value is None or not line[6].isdigit():
            continue
        rows.append((series_id, datetime.strptime(line[6], "%Y%m%d").date(), value))
    return rows


def fetch_boj(db: str, series: Mapping[str, str], start: Optional[date]) -> List[Row]:
    params = {
        "format": "csv",
        "lang": "en",
        "db": db,
        "code": ",".join(series),
        "startDate": (start or _BOJ_FIRST).strftime("%Y%m"),
    }
    text = _get(BOJ_URL, params)
    if "STATUS,200" not in text:
        raise RuntimeError(f"BoJ API refused the request: {text[:200]!r}")
    return _since(parse_boj_csv(text, series), start)


# ── Japanese Ministry of Finance, JGB interest rates ────────────────────────
# https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/

MOF_CURRENT_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
MOF_HISTORY_URL = (
    "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv"
)


def parse_mof_csv(text: str, prefix: str = "JPY.JGB_") -> List[Row]:
    """Two header lines, then "2026/9/18,1.578,…" with one column per tenor
    (1Y … 40Y); "-" where a tenor did not exist yet; a trailing notice line."""
    lines = text.replace("\r", "").split("\n")
    header_at = next(i for i, l in enumerate(lines) if l.startswith("Date,"))
    tenors = lines[header_at].split(",")[1:]
    rows: List[Row] = []
    for line in csv.reader(lines[header_at + 1:]):
        if not line or not line[0][:1].isdigit():
            continue
        day = datetime.strptime(line[0], "%Y/%m/%d").date()
        for tenor, raw in zip(tenors, line[1:]):
            value = _number(raw)
            if value is not None:
                rows.append((f"{prefix}{tenor.strip()}", day, value))
    return rows


def fetch_mof(start: Optional[date], today: date) -> List[Row]:
    """The current month is in jgbcme.csv, every earlier month in
    jgbcme_all.csv: the history file is read only when the window reaches
    back before the first of the current month."""
    rows = parse_mof_csv(_get(MOF_CURRENT_URL))
    if start is None or start < today.replace(day=1):
        rows += parse_mof_csv(_get(MOF_HISTORY_URL))
    return _since(rows, start)
