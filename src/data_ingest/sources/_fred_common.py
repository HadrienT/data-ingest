"""Shared fetch logic for every FRED-backed source.

Leading underscore so the registry's auto-discovery (data_ingest.registry)
skips this module -- it has nothing that is itself a Source.
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd
from fredapi import Fred

from ..config import require
from ..core.source import Window

logger = logging.getLogger("data_ingest.fred_common")


def fetch_fred_series(series_ids: List[str], window: Window) -> pd.DataFrame:
    """Rows shaped (series_id, date, value) for every id in `series_ids`.

    One unavailable or empty series is logged and skipped rather than sinking
    the whole run -- a discontinued or mistyped id should not take the other
    series with it.
    """
    client = Fred(api_key=require("FRED_API_KEY"))
    start = None if window.full else window.start

    frames = []
    for series_id in series_ids:
        try:
            observations = client.get_series(series_id, observation_start=start)
        except Exception as exc:
            logger.warning("Skipping %s: %s", series_id, exc)
            continue
        if observations is None or observations.empty:
            logger.info("%s returned no observations", series_id)
            continue
        # Series.reset_index has no `names` argument; name the axis first.
        frame = observations.rename("value").rename_axis("date").reset_index()
        frame["series_id"] = series_id
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    data["date"] = pd.to_datetime(data["date"]).dt.date
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    return data[["series_id", "date", "value"]]
