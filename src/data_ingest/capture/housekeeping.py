"""What keeps the recordings from filling the disk.

Two things, done by the capture service between two messages (it is the one
that owns the files):

- a day that is over is compressed. The open day stays a plain file, which can
  be appended to and read at any offset; a closed one never changes again;
- when the whole directory weighs more than its budget, the oldest days go,
  whatever the pair. Their events and checkpoints are deleted, their sidecar
  stays, and so do the rows computed from them in Postgres.

Nothing is ever taken from the day being recorded.
"""

from __future__ import annotations

import logging
import lzma
import os
import shutil
from datetime import date
from pathlib import Path
from typing import Collection, Dict, List, Tuple

from .files import CHECKPOINTS, EVENTS, PACKED, day_of, stem_of, with_suffix

logger = logging.getLogger("data_ingest.capture")

#: Level 6 of xz: four times smaller on BTC/USD, at a few megabytes a second.
PRESET = 6


def compress(path: Path) -> Path:
    """Replaces a day's event file by its compressed copy. The copy is
    complete before the original goes: a crash leaves one or the other."""
    packed = with_suffix(stem_of(path), PACKED)
    temporary = packed.with_name(packed.name + ".tmp")
    with open(path, "rb") as source, lzma.open(temporary, "wb", preset=PRESET) as target:
        shutil.copyfileobj(source, target, 1 << 20)
    os.replace(temporary, packed)
    path.unlink()
    return packed


def _closed_days(root: Path, today: date, busy: Collection[Path]) -> Dict[Tuple[date, Path], List[Path]]:
    """The event and checkpoint files of every day that is over, by day then
    stem: the order in which they are removed."""
    days: Dict[Tuple[date, Path], List[Path]] = {}
    for suffix in (EVENTS, PACKED, CHECKPOINTS):
        for path in root.glob(f"*/*/*{suffix}"):
            stem = stem_of(path)
            try:
                day = day_of(stem)
            except ValueError:
                continue  # not one of ours
            if day < today and with_suffix(stem, EVENTS) not in busy:
                days.setdefault((day, stem), []).append(path)
    return dict(sorted(days.items()))


def tidy(root: Path, today: date, busy: Collection[Path] = (), max_bytes: int = 0) -> Dict[str, int]:
    """Compresses the days before `today` and removes the oldest ones while
    the directory weighs more than `max_bytes` (0: keep everything).

    `busy` holds the event files a recorder still has open: after a night
    without the feed, the file of the day before is closed by the first
    message of the morning, not by midnight.
    """
    done = {"compressed": 0, "removed": 0}
    for paths in _closed_days(root, today, busy).values():
        for path in paths:
            if path.name.endswith(EVENTS):
                before = path.stat().st_size
                packed = compress(path)
                done["compressed"] += 1
                logger.info("Compressed %s: %d to %d bytes", packed.name, before, packed.stat().st_size)

    if max_bytes > 0:
        total = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
        for (_, stem), paths in _closed_days(root, today, busy).items():
            if total <= max_bytes:
                break
            for path in paths:
                total -= path.stat().st_size
                path.unlink()
            done["removed"] += 1
            logger.warning("Removed the events of %s: the recordings weigh more than %d bytes", stem.name, max_bytes)
    return done
