"""Where the files of a day are, and how to read its events.

    <root>/bitstamp/btcusd/btcusd-2026-10-12.qmev            the events, while the day is open
    <root>/bitstamp/btcusd/btcusd-2026-10-12.qmev.xz         the same, once the day is over
    <root>/bitstamp/btcusd/btcusd-2026-10-12.json            the sidecar
    <root>/bitstamp/btcusd/btcusd-2026-10-12.checkpoints.gz  the exchange's snapshots

The open day is a plain file, so it can be appended to and read at any offset.
A day that is over is compressed whole (`housekeeping.py`): `xz -d` gives back
the file `quant-microstructure` reads, byte for byte. When the oldest days are
removed to make room, the sidecar stays: it is what says the day was recorded.
"""

from __future__ import annotations

import lzma
from datetime import date
from pathlib import Path
from typing import BinaryIO, Iterator, Optional

from .events import HEADER, MAGIC, RECORD, VERSION

EVENTS = ".qmev"
PACKED = ".qmev.xz"
SIDECAR = ".json"
CHECKPOINTS = ".checkpoints.gz"

_CHUNK = RECORD.size * 65536


def day_stem(root: Path, venue: str, pair: str, day: date) -> Path:
    """Path of a day's files, without the extension."""
    return root / venue / pair / f"{pair}-{day.isoformat()}"


def stem_of(path: Path) -> Path:
    """The stem of any file of a day."""
    for suffix in (PACKED, EVENTS, CHECKPOINTS, SIDECAR):
        if path.name.endswith(suffix):
            return path.with_name(path.name[:-len(suffix)])
    raise ValueError(f"{path}: not a file of a recorded day")


def with_suffix(stem: Path, suffix: str) -> Path:
    # Not Path.with_suffix: the stem has a dot-free name, the suffixes have two.
    return stem.with_name(stem.name + suffix)


def day_of(stem: Path) -> date:
    """The day a stem names: its last ten characters."""
    return date.fromisoformat(stem.name[-10:])


def events_file(stem: Path) -> Optional[Path]:
    """The events of a day, open or compressed; None once they were removed."""
    for suffix in (EVENTS, PACKED):
        path = with_suffix(stem, suffix)
        if path.exists():
            return path
    return None


def open_events(path: Path) -> BinaryIO:
    return lzma.open(path, "rb") if path.name.endswith(PACKED) else open(path, "rb")


def record_chunks(path: Path, strict: bool = True) -> Iterator[bytes]:
    """The records of an event file, open or compressed, in blocks of whole
    records. Raises ValueError if it is not a version-1 event file.

    The file of the open day is being appended to, and may end in the middle
    of a record: `strict=False` stops before it instead of raising.
    """
    with open_events(path) as stream:
        header = stream.read(HEADER.size)
        if len(header) != HEADER.size or HEADER.unpack(header) != (MAGIC, VERSION):
            raise ValueError(f"{path}: not a version {VERSION} event file")
        while True:
            chunk = stream.read(_CHUNK)
            cut = len(chunk) % RECORD.size
            if cut:
                if strict:
                    raise ValueError(f"{path}: truncated record")
                chunk = chunk[:-cut]
            if not chunk:
                return
            yield chunk
            if cut:
                return
