"""Which ticker belongs to which market: the index constituent lists, as a table.

quant-modeling reads it to offer "market, then ticker" and to know each
ticker's currency. It covers the S&P 500 (data/tickers.csv, which has symbols
only — no names) and CAC 40, DAX, FTSE 100, Nikkei 225 (data/constituents/,
with names, the index itself, and the quote currency checked on Yahoo).

A ticker can sit in two markets (Airbus is in the CAC 40 and the DAX), hence
the key (market, ticker). Rows are upserted, so a company that LEAVES an index
stays in the table with its old `as_of`: a consumer takes, per market, the
rows of the latest `as_of` — the current composition — and the older ones
remain as the record of past compositions.
"""

from __future__ import annotations

import pandas as pd

from ._constituents import all_members
from ..config import DATA_DIR
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode


class EquityUniverse(Source):
    name = "equity-universe"
    description = "Index constituents by market: S&P 500, CAC 40, DAX, FTSE 100, Nikkei 225"
    write_mode = WriteMode.UPSERT
    # The lists only change through a commit; a weekly run re-asserts them.
    schedule = "0 6 * * 1"
    lookback_days = 0

    table = TableSpec(
        schema="prices",
        name="equity_universe",
        columns=(
            Column("market", "TEXT", nullable=False),
            Column("ticker", "TEXT", nullable=False),
            Column("name", "TEXT"),
            Column("kind", "TEXT", nullable=False),
            Column("currency", "TEXT", nullable=False),
            Column("as_of", "DATE", nullable=False),
        ),
        primary_key=("market", "ticker"),
        indexes=(("ticker",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        rows = [
            (m.market, m.ticker, m.name or None, m.kind, m.currency, m.as_of)
            for m in all_members()
        ]
        # The S&P 500 list predates this table and carries no names and no
        # date: the run date stands in as its as_of (the list in force today).
        # It also holds a few index symbols (^GSPC, ^DJI… and the CAC 40, DAX,
        # FTSE 100 and Nikkei 225 indices): a ^ symbol is an index, and one
        # that belongs to another market is listed there only, in its own
        # currency — not as a USD member of the S&P 500.
        elsewhere = {m.ticker for m in all_members()}
        sp500 = pd.read_csv(DATA_DIR / "tickers.csv", header=None)[0].dropna().astype(str)
        today = pd.Timestamp.now(tz="UTC").date().isoformat()
        rows += [
            ("SP500", t, None, "index" if t.startswith("^") else "equity", "USD", today)
            for t in sp500
            if t not in elsewhere
        ]
        frame = pd.DataFrame(rows, columns=[c.name for c in self.table.columns])
        frame["as_of"] = pd.to_datetime(frame["as_of"]).dt.date
        return frame
