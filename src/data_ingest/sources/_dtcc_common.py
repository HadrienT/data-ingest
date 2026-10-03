"""Download and parse logic shared by the DTCC sources (`dtcc-swaptions`,
`dtcc-swap-rates`, `dtcc-fx-options`).

Leading underscore so the registry's auto-discovery skips this module.

US swap dealers must report every swap to a swap data repository, and the
repository must publish the price and size of each one (CFTC Part 43, "public
price dissemination"). DTCC's repository does so in one cumulative file per
UTC day and per asset class; the interest-rate one holds about 25,000 messages
a day: every swap, swaption, cap and FRA traded by a US person, with its
rates, dates, notional and premium — and no counterparty. The foreign-exchange
one holds about 70,000: forwards, non-deliverable forwards and options.

It is the only free source of traded interest-rate option prices: there is no
free swaption volatility surface, but there are the trades one is built from.

Two things the files do not say and that were found by looking at them:

- **Old files are not downloadable.** A file stays about two years, then moves
  to cold storage: the bucket still answers a HEAD with 200 but a GET with 403
  (`InvalidObjectState`). A backfill therefore reaches back two years, and a
  day that was not ingested in time is lost.
- **The product is in the UPI columns.** `Product name`, `Option Type` and
  `Option Style` are empty; `UPI FISN` ("NA/O Call Epn OIS USD") and
  `UPI Underlier Name` ("NA/Swap OIS USD") carry it.

Each source has a `parse_*` function, pure, fed with recorded rows by the unit
tests, and shares `fetch_day` for the download.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Dict, Iterator, List, Optional

import requests

from ..core.source import Window

logger = logging.getLogger("data_ingest.dtcc")

#: One cumulative file per asset class and UTC day.
RATES = "RATES"
FOREX = "FOREX"
DTCC_URL = "https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/CFTC_CUMULATIVE_{}_{:%Y_%m_%d}.zip"

#: How far back a full run asks. Files older than about two years answer 403
#: (cold storage) and are skipped like any other missing day.
FULL_HISTORY_DAYS = 740

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; data-ingest; +https://github.com/HadrienT/data-ingest)"}
_TIMEOUT_S = 120

Record = Dict[str, str]


def read_rates_zip(payload: bytes) -> List[Record]:
    """The rows of one cumulative file, keyed by the CSV header. The name is
    from when the rates file was the only one read; every asset class has the
    same layout."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        with archive.open(archive.namelist()[0]) as handle:
            # utf-8-sig: some files start with a byte-order mark, which would
            # otherwise end up inside the first column's name.
            return list(csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig", errors="replace")))


@lru_cache(maxsize=16)
def download_day(day: date, asset_class: str = RATES) -> Optional[bytes]:
    """The zip published for `day`, or None when there is no file to download:
    not published yet, or moved to cold storage (both answer 403, S3's reply
    for an object that cannot be read).

    The last days are cached (1.5 MB each) so that the two DTCC sources, run
    one after the other over a routine week by `ingest run`, download each
    day once. A two-year backfill of both downloads everything twice.
    """
    response = requests.get(DTCC_URL.format(asset_class, day), headers=_HEADERS, timeout=_TIMEOUT_S)
    if response.status_code in (403, 404):
        return None
    response.raise_for_status()
    return response.content


def fetch_day(day: date, asset_class: str = RATES) -> Optional[List[Record]]:
    """The rows published for `day`, or None when there is no file."""
    payload = download_day(day, asset_class)
    return None if payload is None else read_rates_zip(payload)


def window_days(window: Window) -> Iterator[date]:
    """Every calendar day of the window (weekend files exist: they hold the
    late reports), oldest first."""
    today = datetime.now(timezone.utc).date()
    if window.full or window.start is None:
        start = today - timedelta(days=FULL_HISTORY_DAYS)
    else:
        start = window.start
    end = min(window.end or today, today)
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def number(raw: Optional[str]) -> Optional[float]:
    """"250,000,000+" -> 250000000.0. The trailing "+" marks a capped notional
    (see `is_capped`); empty or unparseable -> None, never NaN."""
    text = (raw or "").strip().replace(",", "").rstrip("+")
    if not text:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    return value if value == value and abs(value) != float("inf") else None


def is_capped(raw: Optional[str]) -> bool:
    """Large notionals are published capped, with a trailing "+": the true
    size is larger and unknown. A premium, on the other hand, is published in
    full — so premium / notional is meaningless on a capped row."""
    return (raw or "").strip().endswith("+")


def iso_date(raw: Optional[str]) -> Optional[date]:
    try:
        return date.fromisoformat((raw or "").strip()[:10])
    except ValueError:
        return None


def timestamp(raw: Optional[str]) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def boolean(raw: Optional[str]) -> Optional[bool]:
    value = (raw or "").strip().upper()
    if value == "TRUE":
        return True
    if value == "FALSE":
        return False
    return None


def text(raw: Optional[str]) -> Optional[str]:
    value = (raw or "").strip()
    return value or None
