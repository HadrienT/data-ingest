"""Daily bars as quoted on the day — adjusted for nothing — for the S&P 500,
a set of broad and sector ETFs and the S&P 500 total-return index.

This is the price table of a backtest; `prices.sp500_daily` is not (its
closes are restated by Yahoo at every split). The convention, the universe
and its survivorship limit are in `_equity_raw_common`; in short:

- `open`, `high`, `low`, `close`: the price quoted that day, in USD. Apple's
  close of 2000-01-03 is 111.94, not the 0.9994 that four later splits make
  of it. `volume`: the number of shares traded that day.
- Dividends and splits are in `prices.dividends` and `prices.splits`
  (`equity-dividends`, `equity-splits`), never folded into a price.
- **A day without a quote has no row.** Nothing is filled or interpolated.
- **A bar that fails a control is not stored**, and the run's log says which
  (`check_bars`): a price at or below zero, a high under the low, a close
  outside the day's range. A total return far outside the ticker's usual
  ones is stored, and logged.
- **The total-return benchmark** is SPY with its dividends, which a backtest
  can hold; `^SP500TR`, the index itself, is there to compare with.

UPSERT on (date, ticker). A stored bar does not change when a split happens
later, so a routine run over a few days is enough; it starts at the last
stored date after an outage.
"""

from __future__ import annotations

import pandas as pd

from . import _equity_raw_common as raw
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode


class EquityRawPrices(Source):
    name = "equity-raw-prices"
    description = "Daily bars as quoted, unadjusted for splits and dividends: S&P 500, ETFs, S&P 500 TR (yfinance)"
    write_mode = WriteMode.UPSERT
    # After sp500-prices, well after the 16:00 New York close.
    schedule = "15 22 * * 1-5"
    lookback_days = 5
    catch_up_on = "date"

    table = TableSpec(
        schema="prices",
        name="raw_daily",
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
        indexes=(("ticker", "date"),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        frame = raw.download(window)
        if frame.empty:
            return frame
        kept, report = raw.check_bars(raw.bars(frame), raw.splits(frame), raw.dividends(frame))
        raw.log_report(self.name, report)
        return kept
