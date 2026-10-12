"""One event file per pair and per UTC day, and what goes with it.

    <root>/bitstamp/btcusd/btcusd-2026-10-12.qmev            the events
    <root>/bitstamp/btcusd/btcusd-2026-10-12.json            the sidecar
    <root>/bitstamp/btcusd/btcusd-2026-10-12.checkpoints.gz  the exchange's snapshots

The event file is the source of truth (`quant-microstructure`, ADR-015). Each
one starts with a Clear and the whole book, so a day replays on its own, and
numbers its events from 1. A hole in the feed skips one number, then the
stream goes on with a Clear and a fresh book.

The sidecar says what the file cannot say in format version 1 (which kind of
stream, the tick and the lot) and how the day went: counts, time range, holes,
defects. It is rewritten in place while the day is open.

The checkpoints are the books the exchange published during the day, one JSON
line each, to check a replay against (`verify.py`).
"""

from __future__ import annotations

import gzip
import json
import os
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

from .bitstamp import Instrument, Snapshot
from .events import HEADER, RECORD, Event, write_header


def day_stem(root: Path, instrument: Instrument, day: date) -> Path:
    """Path of a day's files, without the extension."""
    return root / instrument.venue / instrument.pair / f"{instrument.pair}-{day.isoformat()}"


class DayWriter:
    """Appends to the files of one day. Reopening a day that already has a
    file resumes it: a record cut short by a crash is dropped and numbering
    goes on from the last one."""

    def __init__(self, root: Path, instrument: Instrument, day: date):
        self.instrument = instrument
        self.day = day
        stem = day_stem(root, instrument, day)
        stem.parent.mkdir(parents=True, exist_ok=True)
        self.path = stem.with_name(stem.name + ".qmev")
        self.sidecar_path = stem.with_name(stem.name + ".json")
        self.checkpoints_path = stem.with_name(stem.name + ".checkpoints.gz")

        #: How the day went: holes, reconnections, defects, checkpoints.
        self.quality: Counter = Counter()
        self.seq = 0
        self.events = 0
        self.first_ts_recv: Optional[int] = None
        self.last_ts_recv: Optional[int] = None

        previous = self._read_sidecar()
        self._file = open(self.path, "ab")
        size = self._file.tell()
        if size < HEADER.size:
            self._file.truncate(0)
            write_header(self._file)
        else:
            whole = (size - HEADER.size) // RECORD.size
            self._file.truncate(HEADER.size + whole * RECORD.size)
            self.events = whole
            if whole:
                with open(self.path, "rb") as tail:
                    tail.seek(HEADER.size + (whole - 1) * RECORD.size)
                    last = RECORD.unpack(tail.read(RECORD.size))
                    self.seq, self.last_ts_recv = last[2], last[1]
                    tail.seek(HEADER.size)
                    self.first_ts_recv = RECORD.unpack(tail.read(RECORD.size))[1]
            self.quality.update(previous.get("quality", {}))

    def _read_sidecar(self) -> dict:
        try:
            return json.loads(self.sidecar_path.read_text())
        except (OSError, ValueError):
            return {}

    def write(self, events: Iterable[Event]) -> None:
        """Appends events, numbering them (their own `seq` is ignored)."""
        write = self._file.write
        for event in events:
            self.seq += 1
            write(RECORD.pack(event.ts, event.ts_recv, self.seq, event.order_id, event.price, event.qty,
                              event.type, event.side))
            self.events += 1
            if self.first_ts_recv is None:
                self.first_ts_recv = event.ts_recv
            self.last_ts_recv = event.ts_recv

    def gap(self) -> None:
        """Marks a hole: the next event skips a number. Nothing to mark in a
        file that is still empty."""
        if self.events:
            self.seq += 1
            self.quality["gaps"] += 1

    def checkpoint(self, snapshot: Snapshot) -> None:
        line = json.dumps({"time_us": snapshot.time_us, "orders": snapshot.orders}, separators=(",", ":"))
        # One gzip member per checkpoint: the file stays readable while it grows.
        with open(self.checkpoints_path, "ab") as out:
            out.write(gzip.compress(line.encode() + b"\n"))

    def flush(self) -> None:
        self._file.flush()

    def save(self, closed: bool = False) -> None:
        """Flushes the events and rewrites the sidecar."""
        self._file.flush()
        sidecar = {
            "format": "QMEV",
            "version": 1,
            "kind": "l3",
            "venue": self.instrument.venue,
            "pair": self.instrument.pair,
            "day": self.day.isoformat(),
            "tick_size": self.instrument.tick_size,
            "lot_size": self.instrument.lot_size,
            "price_decimals": self.instrument.price_decimals,
            "qty_decimals": self.instrument.qty_decimals,
            "events": self.events,
            "last_seq": self.seq,
            "first_ts_recv": self.first_ts_recv,
            "last_ts_recv": self.last_ts_recv,
            "closed": closed,
            "quality": dict(sorted(self.quality.items())),
        }
        temporary = self.sidecar_path.with_name(self.sidecar_path.name + ".tmp")
        temporary.write_text(json.dumps(sidecar, indent=2) + "\n")
        os.replace(temporary, self.sidecar_path)

    def close(self, closed: bool = False) -> None:
        self.save(closed)
        self._file.close()
