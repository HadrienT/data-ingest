"""Daily OHLCV for the CAC 40, DAX, FTSE 100 and Nikkei 225 — every
constituent and the index itself — from yfinance, like `sp500-prices`.

Prices are stored in the ISO currency of the line, recorded per row: London
lines quote in pence (GBp) and are divided by 100 into pounds; a few FTSE
constituents quote in USD or EUR and stay so. The currency is per ticker, not
per market, because of exactly those.

No options: Yahoo publishes option chains for US listings only, so these
markets have prices and nothing to calibrate a volatility surface on.

A row whose close is missing is dropped: Yahoo returns the current session
with a NaN close until it has settled, and a NaN stored in a DOUBLE column
survives every row count and poisons every later average.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from ._constituents import all_members
from ._yfinance_common import download_daily_ohlcv
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.intl_equity_prices")

FIELDS = {
    "Date": "date",
    "Ticker": "ticker",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}


class IntlEquityPrices(Source):
    name = "intl-equity-prices"
    description = "Daily OHLCV for CAC 40, DAX, FTSE 100, Nikkei 225 constituents and indices (yfinance)"
    write_mode = WriteMode.UPSERT
    # After London's close (16:30 local, 15:30/16:30 UTC); Tokyo and the
    # continent closed earlier. The lookback absorbs a late correction.
    schedule = "30 18 * * 1-5"
    lookback_days = 5

    table = TableSpec(
        schema="prices",
        name="intl_equity_daily",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("open", "DOUBLE PRECISION"),
            Column("high", "DOUBLE PRECISION"),
            Column("low", "DOUBLE PRECISION"),
            Column("close", "DOUBLE PRECISION"),
            Column("volume", "BIGINT"),
            Column("currency", "TEXT", nullable=False),
        ),
        primary_key=("date", "ticker"),
        indexes=(("ticker", "date"),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        members = {m.ticker: m for m in all_members()}  # Airbus: same line, once
        today = datetime.now(timezone.utc).date()
        start = "2000-01-01" if window.full else (window.start or today).isoformat()
        end = (today + timedelta(days=1)).isoformat()
        logger.info("Fetching %s tickers from %s", len(members), start)
        raw = _download_with_retry(list(members), start, end)
        if raw.empty:
            return raw
        return normalise(raw.rename(columns=FIELDS)[list(FIELDS.values())], members)


#: Yahoo rate-limits a burst of ~400 symbols: some come back empty inside an
#: otherwise successful chunk, which the chunk-level retry of
#: download_daily_ohlcv does not see. Those are asked again after a pause.
RETRY_PASSES = 2
RETRY_PAUSE_S = 30.0


def _download_with_retry(tickers, start, end, pause_s: float = RETRY_PAUSE_S) -> pd.DataFrame:
    frames = [download_daily_ohlcv(tickers, start, end)]
    got = set(frames[0]["Ticker"]) if not frames[0].empty else set()
    missing = [t for t in tickers if t not in got]
    for attempt in range(1, RETRY_PASSES + 1):
        # Everything missing is an outage (or a market holiday everywhere),
        # not a partial rate limit: nothing to gain by hammering again.
        if not missing or len(missing) == len(tickers):
            break
        logger.info("Retrying %d tickers without data (pass %d): %s", len(missing), attempt, missing)
        time.sleep(pause_s)
        again = download_daily_ohlcv(missing, start, end)
        if not again.empty:
            frames.append(again)
            got |= set(again["Ticker"])
        missing = [t for t in missing if t not in got]
    if missing:
        # On a routine run this can be a local holiday (Tokyo closed, Europe
        # open); on a backfill it is a ticker to look at.
        logger.warning("No data for %d tickers: %s", len(missing), missing)
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def normalise(frame: pd.DataFrame, members: dict) -> pd.DataFrame:
    """Drop unsettled rows; convert sub-unit quotes (pence) to the ISO currency."""
    frame = frame[frame["close"].notna() & frame["ticker"].isin(members)].copy()
    divisor = frame["ticker"].map(lambda t: members[t].divisor)
    for col in ("open", "high", "low", "close"):
        frame[col] = frame[col] / divisor
    frame["currency"] = frame["ticker"].map(lambda t: members[t].currency)
    return frame.reset_index(drop=True)
