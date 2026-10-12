"""One-minute bars of the recorded order books: trades, top of book, spread
and activity, per pair.

Computed **from** the files the capture service writes, never the other way
round (`quant-microstructure`, ADR-015): each run replays the day files of its
window (`capture/bars.py`) and replaces their bars. The day being recorded is
replayed from its start every time, so its last bar is the minute in progress
and is completed by the next run.

The rows outlive the files: when the oldest days are removed to make room
(`capture/housekeeping.py`), their bars stay.

A plain table, not a TimescaleDB hypertable: a pair is 1,440 rows a day, half
a million a year, which the primary key serves without help. The extension
would mean another Postgres image for the whole store.

A day file is cut on the receive time and a bar on the exchange's: a file
starts with the book as it stood a moment before midnight, and its first
messages may have left the exchange the day before. Only the bars of the
file's own day are kept from it, so that no minute is written twice.

`open` to `close` and `vwap` are trade prices, NULL in a minute without a
trade. `buy_volume` and `sell_volume` split the volume by the side of the
aggressor. `bid` and `ask` are the top of the book when the minute ended;
`spread_mean` is the spread averaged over `quoted_seconds`, the time of the
minute during which the book was known and not crossed. `gaps` counts the
holes of the feed that fell in the minute: the book was unknown from each
until the fresh one that follows it.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from ..capture.bars import minute_bars
from ..capture.files import events_file, stem_of
from ..config import CAPTURE_DIR
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.microstructure_bars")


def scan(root: Path, since: Optional[date]) -> List[tuple]:
    """The bars of every recorded day from `since` on whose events are still
    there, each day from its own file."""
    rows: List[tuple] = []
    for path in sorted(root.glob("*/*/*.json")):
        try:
            sidecar = json.loads(path.read_text())
            day = date.fromisoformat(sidecar["day"])
            events = events_file(stem_of(path))
            if events is None or (since is not None and day < since):
                continue
            bars = minute_bars(events, sidecar["price_decimals"], sidecar["qty_decimals"])
        except (OSError, EOFError, ValueError, KeyError) as exc:
            # Read while the capture was replacing or compressing it, or not a sidecar.
            logger.warning("Skipping %s: %s", path, exc)
            continue
        rows.extend((sidecar["venue"], sidecar["pair"], day, *bar) for bar in bars if bar[0].date() == day)
    return rows


class MicrostructureBars(Source):
    name = "microstructure-bars"
    description = "One-minute bars of the recorded order books: trades, top of book, spread, activity"
    write_mode = WriteMode.UPSERT
    schedule = "7 * * * *"
    # The day being recorded, and the one before it, whose last minutes were
    # written after its last run.
    lookback_days = 1
    catch_up_on = "date"

    table = TableSpec(
        schema="microstructure",
        name="bars_1m",
        columns=(
            Column("venue", "TEXT", nullable=False),
            Column("pair", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("ts", "TIMESTAMPTZ", nullable=False),
            Column("open", "DOUBLE PRECISION"),
            Column("high", "DOUBLE PRECISION"),
            Column("low", "DOUBLE PRECISION"),
            Column("close", "DOUBLE PRECISION"),
            Column("volume", "DOUBLE PRECISION", nullable=False),
            Column("buy_volume", "DOUBLE PRECISION", nullable=False),
            Column("sell_volume", "DOUBLE PRECISION", nullable=False),
            Column("trades", "BIGINT", nullable=False),
            Column("vwap", "DOUBLE PRECISION"),
            Column("bid", "DOUBLE PRECISION"),
            Column("ask", "DOUBLE PRECISION"),
            Column("spread_mean", "DOUBLE PRECISION"),
            Column("quoted_seconds", "DOUBLE PRECISION", nullable=False),
            Column("adds", "BIGINT", nullable=False),
            Column("cancels", "BIGINT", nullable=False),
            Column("gaps", "BIGINT", nullable=False),
        ),
        primary_key=("venue", "pair", "ts"),
        indexes=(("date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        since = None if window.full else window.start
        # dtype=object: None stays None (not NaN) and integers stay integers,
        # which is what COPY needs for the nullable columns.
        return pd.DataFrame(scan(CAPTURE_DIR, since), columns=self.table.column_names, dtype=object)
