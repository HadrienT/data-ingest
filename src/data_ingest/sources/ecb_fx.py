"""Euro foreign-exchange reference rates from the ECB — every currency the ECB
publishes (about 30 live ones), daily since 1999, free and without a key.

Each value is the number of units of `currency` for one euro (EUR/USD 1.1367
is stored as currency=USD, value=1.1367). Any cross rate follows by division:
USD/JPY = (JPY per EUR) / (USD per EUR). The rates are set once a day around
14:10 CET from the concertation between central banks and never revised,
hence UPSERT. Currencies the ECB stopped quoting (those that joined the euro,
the rouble since March 2022) keep their history and simply stop.

Complements `fx-rates` (FRED H.10, noon New York, USD pairs): one reference
per region, and the ECB's covers every currency this project prices in.
"""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import List, Optional

import pandas as pd

from ._intl_providers import _get, _number
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

ECB_EXR_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A"


def parse_exr_csv(text: str) -> List[tuple]:
    """(currency, date, units per EUR) rows; missing observations dropped."""
    rows = []
    for record in csv.DictReader(io.StringIO(text)):
        value = _number(record.get("OBS_VALUE", ""))
        if value is None or not record.get("CURRENCY"):
            continue
        rows.append((record["CURRENCY"], date.fromisoformat(record["TIME_PERIOD"]), value))
    return rows


class EcbFx(Source):
    name = "ecb-fx"
    description = "Euro FX reference rates, every currency the ECB publishes (daily since 1999)"
    write_mode = WriteMode.UPSERT
    # Published around 14:10 CET on TARGET business days.
    schedule = "0 16 * * 1-5"
    lookback_days = 10

    table = TableSpec(
        schema="fx",
        name="ecb_reference_rates",
        columns=(
            Column("currency", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION", nullable=False),
        ),
        primary_key=("currency", "date"),
        indexes=(("date",),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        params = {"format": "csvdata"}
        start: Optional[date] = None if window.full else window.start
        if start is not None:
            params["startPeriod"] = start.isoformat()
        rows = parse_exr_csv(_get(ECB_EXR_URL, params))
        return pd.DataFrame(rows, columns=["currency", "date", "value"])
