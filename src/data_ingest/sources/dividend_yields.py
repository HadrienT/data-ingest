"""Daily trailing dividend yield, from yfinance.

Exists specifically to unblock quant-modeling's vol-surface pipeline: Dupire
needs a dividend yield to convert spot into a forward, and until this
source existed there was nowhere to read one from -- the API called
yfinance live, on every request, for a number that changes at most a
handful of times a year.

yfinance has no bulk endpoint for this (unlike download_daily_ohlcv's
multi-ticker download): fast_info is a per-Ticker object, so this is a
per-ticker loop, same shape as options_chain.py's.  Same universe as
options-chain-snapshot by construction (import, not a second list to keep
in sync) -- this exists to serve the same tickers, and only those, until
something else needs it too.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import pandas as pd
import yfinance as yf

from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode
from .options_chain import DEFAULT_TICKERS

logger = logging.getLogger("data_ingest.dividend_yields")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5


class DividendYields(Source):
    name = "dividend-yields"
    description = "Daily trailing dividend yield for the vol-surface ticker universe (yfinance)"
    write_mode = WriteMode.UPSERT
    schedule = "0 22 * * 1-5"
    lookback_days = 1  # a point-in-time snapshot of "today's known trailing yield", not a series to backfill

    table = TableSpec(
        schema="prices",
        name="dividend_yields",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("trailing_yield", "DOUBLE PRECISION"),
        ),
        primary_key=("date", "ticker"),
        indexes=(("ticker", "date"),),
    )

    def tickers(self) -> List[str]:
        return list(DEFAULT_TICKERS)

    def fetch(self, window: Window) -> pd.DataFrame:
        today = datetime.now(timezone.utc).date()
        rows = []
        for ticker in self.tickers():
            yield_value = _fetch_one_yield(ticker)
            if yield_value is not None:
                rows.append({"date": today, "ticker": ticker, "trailing_yield": yield_value})

        if not rows:
            return pd.DataFrame(columns=self.table.column_names)
        return pd.DataFrame(rows)


#: Above this, treat the field as a data error rather than a real yield --
#: no equity or broad-index ETF legitimately yields 20%+ trailing.
_MAX_PLAUSIBLE_YIELD = 0.20


def _fetch_one_yield(ticker: str) -> Optional[float]:
    """trailingAnnualDividendYield from .info -- already a fraction (0.0074,
    not 7.4 or 0.74), unlike fast_info, which doesn't even carry a dividend
    yield field in current yfinance versions (silently returns None there,
    not an error -- caught once by actually running this against live data
    before trusting it)."""
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            info = yf.Ticker(ticker).info
            dy = info.get("trailingAnnualDividendYield")
            if dy is None:
                return 0.0
            dy = float(dy)
            if not (0.0 <= dy <= _MAX_PLAUSIBLE_YIELD):
                logger.warning("dividend-yields: implausible yield %.4f for %s, treating as 0", dy, ticker)
                return 0.0
            return dy
        except Exception as exc:
            last_error = exc
            logger.warning("dividend-yields: attempt %d for %s failed: %s", attempt, ticker, exc)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    logger.error("dividend-yields: giving up on %s: %s", ticker, last_error)
    return None
