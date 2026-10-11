"""Cash dividends by ex-date, for the universe of `equity-raw-prices`.

`amount` is in USD **per share as counted on the ex-date**: what a holder of
one share that morning receives. Yahoo restates past dividends for later
splits (Apple's 0.47 of 2014-11-06 becomes 0.1175 after the 2020 split);
that restatement is undone here, see `_equity_raw_common`.

A backtest credits `amount × shares held` in cash on `ex_date`. The payment
date, a few weeks later, is not published by this source. Special dividends
come like ordinary ones, with nothing to tell them apart.

UPSERT on (ticker, ex_date). A routine run looks back a month: a dividend is
sometimes published a few days after it was detached.
"""

from __future__ import annotations

import pandas as pd

from . import _equity_raw_common as raw
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode


class EquityDividends(Source):
    name = "equity-dividends"
    description = "Cash dividends by ex-date, per share as counted that day: S&P 500 and ETFs (yfinance)"
    write_mode = WriteMode.UPSERT
    schedule = "20 22 * * 1-5"
    lookback_days = 30
    catch_up_on = "ex_date"

    table = TableSpec(
        schema="prices",
        name="dividends",
        columns=(
            Column("ticker", "TEXT", nullable=False),
            Column("ex_date", "DATE", nullable=False),
            Column("amount", "DOUBLE PRECISION", nullable=False),
        ),
        primary_key=("ticker", "ex_date"),
        indexes=(("ex_date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        return raw.dividends(raw.download(window))
