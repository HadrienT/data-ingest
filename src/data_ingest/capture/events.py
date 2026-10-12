"""The event format of `quant-microstructure`, written from Python.

A capture produces the files that repository replays, so the layout below is
its `feed/event_file.hpp` and nothing else: an 8-byte header (the magic
"QMEV", then the version as a 32-bit integer) followed by 50-byte records,
every integer little-endian. Prices are whole ticks, quantities whole lots and
times nanoseconds since the Unix epoch; the tick and the lot are not in the
file (format version 1 has no room for them) and travel in the sidecar that
`writer.py` puts next to each file.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import BinaryIO, Iterator

MAGIC = b"QMEV"
VERSION = 1
HEADER = struct.Struct("<4sI")
#: ts, ts_recv, seq, order_id, price, qty, type, side.
RECORD = struct.Struct("<qqQQqqBB")

assert HEADER.size == 8 and RECORD.size == 50


class EventType(IntEnum):
    ADD = 0
    CANCEL = 1
    TRADE = 2
    LEVEL = 3
    CLEAR = 4


class Side(IntEnum):
    BUY = 0
    SELL = 1


@dataclass(frozen=True)
class Event:
    """One record. `side` of a TRADE is the aggressor's; `order_id` and
    `price` are then those of the resting order it hit."""

    ts: int
    ts_recv: int
    seq: int
    order_id: int
    price: int
    qty: int
    type: EventType
    side: Side

    def pack(self) -> bytes:
        return RECORD.pack(self.ts, self.ts_recv, self.seq, self.order_id, self.price, self.qty, self.type, self.side)


def write_header(out: BinaryIO) -> None:
    out.write(HEADER.pack(MAGIC, VERSION))


def read_events(stream: BinaryIO) -> Iterator[Event]:
    """Every event of a file. Raises ValueError on anything that is not a
    complete version-1 event file, like the C++ reader."""
    header = stream.read(HEADER.size)
    if len(header) != HEADER.size:
        raise ValueError("event file: missing header")
    magic, version = HEADER.unpack(header)
    if magic != MAGIC:
        raise ValueError("event file: not an event file")
    if version != VERSION:
        raise ValueError("event file: unknown format version")
    while True:
        record = stream.read(RECORD.size)
        if not record:
            return
        if len(record) != RECORD.size:
            raise ValueError("event file: truncated record")
        ts, ts_recv, seq, order_id, price, qty, kind, side = RECORD.unpack(record)
        yield Event(ts, ts_recv, seq, order_id, price, qty, EventType(kind), Side(side))
