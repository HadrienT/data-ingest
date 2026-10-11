"""Daily factor returns from Kenneth French's data library.

What a backtest regresses a strategy's returns on, to tell an alpha from a
known premium. Three files, each a `dataset`:

| dataset    | factors                               | since      |
|------------|---------------------------------------|------------|
| `ff3`      | `Mkt-RF`, `SMB`, `HML`, `RF`          | 1926-07-01 |
| `ff5`      | `Mkt-RF`, `SMB`, `HML`, `RMW`, `CMA`, `RF` | 1963-07-01 |
| `momentum` | `UMD`                                 | 1926-11-03 |

- **`value` is in percent, as published**: 0.79 is 0.79 %, a daily return.
  `RF` is the one-month T-bill rate brought to a daily simple rate.
- **`SMB` is not the same number in `ff3` and `ff5`** (the five-factor one
  averages three size sorts): take every factor of a regression from one
  dataset. `Mkt-RF` and `RF` are identical in both.
- `UMD` is the column the momentum file calls `Mom`.
- `date` is the library's own: US trading days.
- A missing value (-99.99 or -999 in the files) is a missing row.

The library is rebuilt from CRSP about once a month, two months behind, and
**its history is revised**: a new CRSP vintage moves past values in the
second decimal. VERSIONED, like `fred-macro`: a run stores the values that
differ from the last ones held, with `observed_at`, and
`factors.french_daily_latest` is the current vintage. A window means nothing
here, each file is the whole history.
"""

from __future__ import annotations

import io
import logging
import zipfile
from typing import Dict, List

import pandas as pd
import requests

from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.french_factors")

LIBRARY_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/{}_CSV.zip"

#: dataset -> the library's file name.
DATASETS: Dict[str, str] = {
    "ff3": "F-F_Research_Data_Factors_daily",
    "ff5": "F-F_Research_Data_5_Factors_2x3_daily",
    "momentum": "F-F_Momentum_Factor_daily",
}
#: The library's column names that are stored under another one.
RENAMED = {"Mom": "UMD"}
MISSING = (-99.99, -999.0)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; data-ingest; +https://github.com/HadrienT/data-ingest)"}
_TIMEOUT_S = 120


def parse_factors(text: str, dataset: str) -> pd.DataFrame:
    """The rows of one file of the library: a few lines of description, a
    header starting with a comma, one line per day, a copyright line."""
    lines = text.splitlines()
    header = next((i for i, line in enumerate(lines) if line.startswith(",")), None)
    if header is None:
        raise ValueError(f"{dataset}: no header line in the file")
    factors = [RENAMED.get(name.strip(), name.strip()) for name in lines[header].split(",")[1:]]

    rows: List[tuple] = []
    for line in lines[header + 1:]:
        cells = [cell.strip() for cell in line.split(",")]
        if len(cells[0]) != 8 or not cells[0].isdigit():
            if rows:
                break  # the end of the daily block
            continue
        day = pd.Timestamp(cells[0]).date()
        for factor, cell in zip(factors, cells[1:]):
            value = float(cell)
            if value not in MISSING:
                rows.append((dataset, factor, day, value))
    return pd.DataFrame(rows, columns=["dataset", "factor", "date", "value"])


def download(dataset: str) -> str:
    response = requests.get(LIBRARY_URL.format(DATASETS[dataset]), headers=_HEADERS, timeout=_TIMEOUT_S)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        return archive.read(archive.namelist()[0]).decode("latin-1")


class FrenchFactors(Source):
    name = "french-factors"
    description = "Daily Fama-French 3 and 5 factors, momentum and RF, in percent (K. French's data library, revisions kept)"
    write_mode = WriteMode.VERSIONED
    # The library moves about once a month; a weekly look is plenty.
    schedule = "0 9 * * 6"
    lookback_days = 0

    table = TableSpec(
        schema="factors",
        name="french_daily",
        columns=(
            Column("dataset", "TEXT", nullable=False),
            Column("factor", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION", nullable=False),
        ),
        primary_key=("dataset", "factor", "date"),
        indexes=(("date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        frames = [parse_factors(download(dataset), dataset) for dataset in DATASETS]
        frame = pd.concat(frames, ignore_index=True)
        logger.info("%s factor returns, %s to %s", len(frame), frame["date"].min(), frame["date"].max())
        return frame
