"""Daily OHLCV for the S&P 500 constituents, from yfinance.

The original pipeline this grew out of ran as a GCP Cloud Function writing to
BigQuery; the download and normalisation logic is carried over unchanged.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

from ._yfinance_common import download_daily_ohlcv
from ..config import DATA_DIR
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.sp500_prices")

MARKET_TZ = "America/New_York"
MARKET_CALENDAR = "NYSE"

#: yfinance's capitalised names, mapped to the table's lowercase columns.
FIELDS = {
    "Date": "date",
    "Ticker": "ticker",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


class SP500Prices(Source):
    name = "sp500-prices"
    description = "Daily OHLCV for S&P 500 constituents (yfinance)"
    write_mode = WriteMode.UPSERT
    # The US session closes at 16:00 New York. Paris is 5 or 6 hours ahead
    # depending on how the two DST calendars line up, so a 22:00 run lands
    # before the close for a couple of weeks a year; the upsert is idempotent
    # and the lookback means the next run corrects it.
    schedule = "Mon..Fri 22:00"
    lookback_days = 5

    table = TableSpec(
        schema="prices",
        name="sp500_daily",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("open", "DOUBLE PRECISION"),
            Column("high", "DOUBLE PRECISION"),
            Column("low", "DOUBLE PRECISION"),
            Column("close", "DOUBLE PRECISION"),
            Column("volume", "BIGINT"),
        ),
        primary_key=("date", "ticker"),
        # The primary key serves "everything on this date"; the common query is
        # "one ticker over a range", which needs the reverse order.
        indexes=(("ticker", "date"),),
    )

    def tickers(self) -> List[str]:
        path = DATA_DIR / "tickers.csv"
        return pd.read_csv(path, header=None)[0].dropna().astype(str).tolist()

    def fetch(self, window: Window) -> pd.DataFrame:
        tickers = self.tickers()
        logger.info("Fetching %s tickers", len(tickers))

        if window.full:
            start, end = "2000-01-01", datetime.now(ZoneInfo(MARKET_TZ)).strftime("%Y-%m-%d")
        else:
            session = _last_trading_day()
            start = (window.start or session.date()).strftime("%Y-%m-%d")
            end = ((window.end or session.date()) + timedelta(days=1)).strftime("%Y-%m-%d")

        raw = download_daily_ohlcv(tickers, start, end)
        if raw.empty:
            return raw
        return raw.rename(columns=FIELDS)[list(FIELDS.values())]


def _last_trading_day(reference_dt: Optional[datetime] = None) -> datetime:
    now = reference_dt or datetime.now(ZoneInfo(MARKET_TZ))
    try:
        calendar = mcal.get_calendar(MARKET_CALENDAR)
        schedule = calendar.schedule(start_date=(now - timedelta(days=10)).date(), end_date=now.date())
        if schedule.empty:
            raise ValueError("Empty market schedule")
        last_session = schedule.index[-1].to_pydatetime().date()
        return datetime.combine(last_session, datetime.min.time(), tzinfo=ZoneInfo(MARKET_TZ))
    except Exception as exc:
        logger.warning("Falling back to weekday logic: %s", exc)
        current = now - timedelta(days=1)
        while current.isoweekday() not in range(1, 6):
            current -= timedelta(days=1)
        return current
