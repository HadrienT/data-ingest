"""Par swap rates of the day, from the swaps actually traded: the median fixed
rate of the spot-starting swaps reported to DTCC's repository, per tenor.

FRED has Treasury yields and the SOFR fixing but no swap curve, and dealer
quotes are not free. The trades are: a SOFR swap curve from 1 to 30 years
comes out of the 5,000 or so USD OIS swaps traded each day. It is what a
swaption's forward rate and annuity must be computed on (`dtcc-swaptions`
comes from the same file, so the two are consistent), and a swap curve next to
the Treasury one for anything else.

A row is a (date, product, tenor): the median, the quartiles and the number
of trades behind it. The median, because the file mixes at-market swaps with
off-market ones (compressions, unwinds, legs of a package); what can be
recognised is removed first:

- only new trades (`NEWT` / `TRAD`);
- spot-starting: effective within a week of the execution date;
- no other payment (an upfront fee means an off-market rate) and not flagged
  as having non-standard terms;
- a maturity within three weeks of a whole number of years of the tenor grid.

A tenor with fewer than `MIN_TRADES` trades is not stored: no number is
better than a number resting on one trade.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import date
from typing import Dict, Iterable, List, Mapping, Optional

import pandas as pd

from . import _dtcc_common as dtcc
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.dtcc")

#: UPI FISN of the swaps a curve is built from -> their currency.
PRODUCTS: Mapping[str, str] = {
    "NA/Swap OIS USD": "USD",  # SOFR
}

TENORS_YEARS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 15, 20, 25, 30)
MIN_TRADES = 3
_SPOT_START_DAYS = 7
_TENOR_TOLERANCE_YEARS = 0.06
#: A fixed rate outside (0, 25 %) is a notation error, not a swap rate.
_MAX_RATE = 0.25


def _tenor(effective: date, maturity: date) -> Optional[int]:
    years = (maturity - effective).days / 365.25
    for tenor in TENORS_YEARS:
        if abs(years - tenor) <= _TENOR_TOLERANCE_YEARS:
            return tenor
    return None


def parse_swap_rates(
    records: Iterable[dtcc.Record], report_date: date, products: Mapping[str, str] = PRODUCTS
) -> List[tuple]:
    """(date, product, currency, tenor, median, lower quartile, upper quartile,
    trades) rows, one per tenor with enough trades."""
    rates: Dict[tuple, List[float]] = defaultdict(list)
    for record in records:
        product = (record.get("UPI FISN") or "").strip()
        if product not in products:
            continue
        if (record.get("Action type") or "").strip() != "NEWT" or (record.get("Event type") or "").strip() != "TRAD":
            continue
        if dtcc.boolean(record.get("Non-standardized term indicator")) or dtcc.text(record.get("Other payment amount")):
            continue
        executed = dtcc.timestamp(record.get("Execution Timestamp"))
        effective = dtcc.iso_date(record.get("Effective Date"))
        maturity = dtcc.iso_date(record.get("Expiration Date"))
        rate = dtcc.number(record.get("Fixed rate-Leg 1"))
        if rate is None:
            rate = dtcc.number(record.get("Fixed rate-Leg 2"))
        if executed is None or effective is None or maturity is None or rate is None:
            continue
        if not 0.0 < rate < _MAX_RATE:
            continue
        if not 0 <= (effective - executed.date()).days <= _SPOT_START_DAYS:
            continue
        tenor = _tenor(effective, maturity)
        if tenor is not None:
            rates[(product, tenor)].append(rate)

    rows = []
    for (product, tenor), values in sorted(rates.items()):
        if len(values) < MIN_TRADES:
            continue
        lower, median, upper = statistics.quantiles(values, n=4, method="inclusive")
        rows.append((report_date, product, products[product], tenor, median, lower, upper, len(values)))
    return rows


class DtccSwapRates(Source):
    name = "dtcc-swap-rates"
    description = "Par swap rates by tenor (SOFR, 1Y to 30Y): median fixed rate of the day's traded swaps, from DTCC"
    write_mode = WriteMode.UPSERT
    schedule = "35 5 * * *"
    lookback_days = 7

    table = TableSpec(
        schema="rates",
        name="dtcc_swap_rates",
        columns=(
            Column("date", "DATE", nullable=False),
            Column("product", "TEXT", nullable=False),
            Column("currency", "TEXT", nullable=False),
            Column("tenor_years", "INTEGER", nullable=False),
            Column("rate", "DOUBLE PRECISION", nullable=False),
            Column("rate_p25", "DOUBLE PRECISION", nullable=False),
            Column("rate_p75", "DOUBLE PRECISION", nullable=False),
            Column("trades", "INTEGER", nullable=False),
        ),
        primary_key=("date", "product", "tenor_years"),
        indexes=(("product", "tenor_years", "date"),),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        rows: List[tuple] = []
        for day in dtcc.window_days(window):
            records = dtcc.fetch_day(day)
            if records is None:
                logger.info("[%s] no file for %s", self.name, day)
                continue
            rows.extend(parse_swap_rates(records, day))
        return pd.DataFrame(rows, columns=self.table.column_names)
