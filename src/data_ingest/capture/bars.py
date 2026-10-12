"""One-minute bars, computed from a day file by replaying it.

The files are the source of truth and Postgres only holds what is computed
from them (`quant-microstructure`, ADR-015). A bar says what a minute of the
exchange's clock held:

- the trades: first, highest, lowest and last price, the volume and how much
  of it was bought and sold by the aggressor, the number of fills and their
  volume-weighted price;
- the top of the book when the minute ended, and the spread averaged over the
  time the book was known during the minute (`quoted_seconds`);
- the activity: orders added and cancelled, and the holes of the feed.

A hole is a number skipped in the file, followed by a Clear and a fresh book
(`service.py`). Between the last event before it and that Clear the book is
unknown: that time counts in no spread, and a minute that ends inside it has
no top of book. The orders of the fresh book are not counted as added.

Only the minutes that hold an event have a bar.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .book import Book
from .events import RECORD, EventType, Side
from .files import record_chunks

MINUTE = 60_000_000_000

COLUMNS = ("ts", "open", "high", "low", "close", "volume", "buy_volume", "sell_volume", "trades", "vwap",
           "bid", "ask", "spread_mean", "quoted_seconds", "adds", "cancels", "gaps")


class _Bar:
    __slots__ = ("minute", "open", "high", "low", "close", "volume", "buy_volume", "notional", "trades",
                 "spread_time", "quoted", "adds", "cancels", "gaps")

    def __init__(self, minute: int):
        self.minute = minute
        self.open = self.high = self.low = self.close = None
        self.volume = self.buy_volume = self.notional = self.trades = 0
        self.spread_time = self.quoted = 0
        self.adds = self.cancels = self.gaps = 0


def minute_bars(path: Path, price_decimals: int, qty_decimals: int) -> List[tuple]:
    """The bars of a day file, open or compressed, as tuples in the order of
    `COLUMNS`. Prices and volumes are in the units of the pair, not in ticks
    and lots. The file of the open day may end in the middle of a record:
    what is whole is read."""
    # Divided, not multiplied by 0.01: 8340709 / 100 is the float nearest to 83407.09.
    ticks, lots = 10 ** price_decimals, 10 ** qty_decimals
    book = Book()
    best = book.best
    buy, sell = Side.BUY, Side.SELL
    add, cancel, trade, clear = EventType.ADD, EventType.CANCEL, EventType.TRADE, EventType.CLEAR
    rows: List[tuple] = []

    bar: Optional[_Bar] = None
    last_seq = None
    #: Time and top of the book after the last event; `known` is False from a
    #: hole to the Clear that follows it.
    at = 0
    bid = ask = None
    known = False
    #: Receive time of the Clear whose book is being loaded.
    loading = None

    def close_bar() -> None:
        b = bar
        quoted = known and bid is not None and ask is not None
        rows.append((
            datetime.fromtimestamp(b.minute * 60, tz=timezone.utc),
            None if b.open is None else b.open / ticks,
            None if b.high is None else b.high / ticks,
            None if b.low is None else b.low / ticks,
            None if b.close is None else b.close / ticks,
            b.volume / lots,
            b.buy_volume / lots,
            (b.volume - b.buy_volume) / lots,
            b.trades,
            b.notional / b.volume / ticks if b.volume else None,
            bid / ticks if quoted else None,
            ask / ticks if quoted else None,
            b.spread_time / b.quoted / ticks if b.quoted else None,
            b.quoted / 1e9,
            b.adds,
            b.cancels,
            b.gaps,
        ))

    for chunk in record_chunks(path, strict=False):
        for ts, ts_recv, seq, order_id, price, qty, kind, side in RECORD.iter_unpack(chunk):
            hole = last_seq is not None and seq != last_seq + 1
            last_seq = seq
            if hole:
                known = False

            # The time since the last event, with the book as it was left.
            minute = ts // MINUTE
            if bar is None:
                bar = _Bar(minute)
            elif minute > bar.minute:
                if known and bid is not None and ask is not None and ask > bid:
                    elapsed = (bar.minute + 1) * MINUTE - at
                    bar.spread_time += (ask - bid) * elapsed
                    bar.quoted += elapsed
                close_bar()
                bar = _Bar(minute)
                at = minute * MINUTE
            if ts > at:
                if known and bid is not None and ask is not None and ask > bid:
                    bar.spread_time += (ask - bid) * (ts - at)
                    bar.quoted += ts - at
                at = ts
            if hole:
                bar.gaps += 1

            try:
                if kind == add:
                    book.add(order_id, Side(side), price, qty)
                    if ts_recv != loading:
                        loading = None
                        bar.adds += 1
                elif kind == trade:
                    book.reduce(order_id, qty)
                    if bar.open is None:
                        bar.open = bar.high = bar.low = price
                    elif price > bar.high:
                        bar.high = price
                    elif price < bar.low:
                        bar.low = price
                    bar.close = price
                    bar.volume += qty
                    bar.notional += price * qty
                    bar.trades += 1
                    if side == buy:
                        bar.buy_volume += qty
                elif kind == cancel:
                    book.reduce(order_id, qty)
                    bar.cancels += 1
                elif kind == clear:
                    book.clear()
                    known = True
                    loading = ts_recv
                else:
                    continue
            except ValueError:
                continue  # an event a strict book refuses: `capture-verify` counts them
            bid, ask = best(buy), best(sell)

    if bar is not None:
        close_bar()
    return rows
