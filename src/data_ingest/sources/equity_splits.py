"""Stock splits by effective date, for the universe of `equity-raw-prices`.

`ratio` is new shares per old share: 2 for a two-for-one, 0.1 for a
one-for-ten reverse split. On `date`, a backtest multiplies the position by
`ratio`; the close of `prices.raw_daily` drops by the same factor between
the day before and that day.

**A spin-off comes from Yahoo as a split with an odd ratio** (GE, 1.281 on
2023-01-04, for GE HealthCare; AT&T, 1.324 on 2022-04-11, for Warner Bros.
Discovery). Applied as a split, it gives the holder more shares of the
parent in place of the shares of the new company: the total return is
right, the share count is not the real one. See `_equity_raw_common`.

UPSERT on (ticker, date). A routine run starts at the last split stored, so
none is missed after an outage.
"""

from __future__ import annotations

import pandas as pd

from . import _equity_raw_common as raw
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode


class EquitySplits(Source):
    name = "equity-splits"
    description = "Stock splits by effective date, ratio = new shares per old share: S&P 500 and ETFs (yfinance)"
    write_mode = WriteMode.UPSERT
    schedule = "25 22 * * 1-5"
    lookback_days = 30
    catch_up_on = "date"

    table = TableSpec(
        schema="prices",
        name="splits",
        columns=(
            Column("ticker", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("ratio", "DOUBLE PRECISION", nullable=False),
        ),
        primary_key=("ticker", "date"),
        indexes=(("date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        return raw.splits(raw.download(window))
