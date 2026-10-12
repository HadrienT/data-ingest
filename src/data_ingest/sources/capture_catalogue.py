"""The catalogue of what the capture service recorded: one row per event file,
with its time range, its size and how the day went.

The files are the source of truth and the database only describes them
(`quant-microstructure`, ADR-015): this source reads the sidecar the capture
writes next to each day file (`capture/writer.py`) and never the other way
round. The capture itself does not touch Postgres, so it keeps recording when
the database is down; this run catches up afterwards.

`price_decimals` and `qty_decimals` turn the integers of a file into prices
and sizes: a price of 8340709 with two decimals is 83407.09. `gaps` is the
number of holes in the numbering of the file, each followed by a Clear and a
fresh book; `checkpoints_failed` the number of times the recorded book was
not the one the exchange published. `path` is relative to `CAPTURE_DIR`.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

from ..config import CAPTURE_DIR
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.capture_catalogue")


def _time(nanoseconds: Optional[int]) -> Optional[datetime]:
    if nanoseconds is None:
        return None
    return datetime.fromtimestamp(nanoseconds / 1e9, tz=timezone.utc)


def catalogue_row(sidecar: dict, events_path: Path, root: Path) -> tuple:
    """One row of the table from one sidecar."""
    quality = sidecar.get("quality", {})
    count = quality.get("latency_count", 0)
    return (
        sidecar["venue"],
        sidecar["pair"],
        date.fromisoformat(sidecar["day"]),
        str(events_path.relative_to(root)),
        sidecar["kind"],
        sidecar["price_decimals"],
        sidecar["qty_decimals"],
        events_path.stat().st_size if events_path.exists() else None,
        sidecar["events"],
        _time(sidecar.get("first_ts_recv")),
        _time(sidecar.get("last_ts_recv")),
        quality.get("gaps", 0),
        quality.get("disconnections", 0),
        quality.get("chain_breaks", 0),
        quality.get("checkpoints_ok", 0),
        quality.get("checkpoints_failed", 0),
        quality.get("crossed_book", 0),
        quality.get("unknown_order", 0) + quality.get("duplicate_order", 0) + quality.get("empty_order", 0),
        quality["latency_ns_sum"] / count / 1e6 if count else None,
        quality["latency_ns_max"] / 1e6 if count else None,
        bool(sidecar.get("closed")),
    )


def scan(root: Path, since: Optional[date]) -> List[tuple]:
    rows = []
    for path in sorted(root.glob("*/*/*.json")):
        try:
            sidecar = json.loads(path.read_text())
            if since is not None and date.fromisoformat(sidecar["day"]) < since:
                continue
            rows.append(catalogue_row(sidecar, path.with_suffix(".qmev"), root))
        except (OSError, ValueError, KeyError) as exc:
            # Read while the capture was replacing it, or not a sidecar.
            logger.warning("Skipping %s: %s", path, exc)
    return rows


class CaptureCatalogue(Source):
    name = "capture-catalogue"
    description = "Catalogue of the recorded order book files: time range, size, holes, checks"
    write_mode = WriteMode.UPSERT
    schedule = "*/15 * * * *"
    # The open day changes at every run; closed days only if a file is replaced.
    lookback_days = 3

    table = TableSpec(
        schema="microstructure",
        name="capture_files",
        columns=(
            Column("venue", "TEXT", nullable=False),
            Column("pair", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("path", "TEXT", nullable=False),
            Column("kind", "TEXT", nullable=False),
            Column("price_decimals", "INTEGER", nullable=False),
            Column("qty_decimals", "INTEGER", nullable=False),
            Column("bytes", "BIGINT"),
            Column("events", "BIGINT", nullable=False),
            Column("first_ts_recv", "TIMESTAMPTZ"),
            Column("last_ts_recv", "TIMESTAMPTZ"),
            Column("gaps", "BIGINT", nullable=False),
            Column("disconnections", "BIGINT", nullable=False),
            Column("chain_breaks", "BIGINT", nullable=False),
            Column("checkpoints_ok", "BIGINT", nullable=False),
            Column("checkpoints_failed", "BIGINT", nullable=False),
            Column("crossed_book", "BIGINT", nullable=False),
            Column("defects", "BIGINT", nullable=False),
            Column("latency_ms_mean", "DOUBLE PRECISION"),
            Column("latency_ms_max", "DOUBLE PRECISION"),
            Column("closed", "BOOLEAN", nullable=False),
        ),
        primary_key=("venue", "pair", "date"),
        indexes=(("date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        since = None if window.full else window.start
        # dtype=object: None stays None (not NaN or NaT) and integers stay
        # integers, which is what COPY needs for the nullable columns.
        return pd.DataFrame(scan(CAPTURE_DIR, since), columns=self.table.column_names, dtype=object)
