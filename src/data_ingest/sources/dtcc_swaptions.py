"""European swaption trades published by DTCC's swap data repository — every
currency, every message of the day, as reported.

What a consumer gets is what the market paid: expiry, underlying swap,
strike, notional and premium of each trade. Dividing the premium by the
notional and inverting Bachelier's formula gives the implied volatility — the
input a rates model is calibrated on, which nobody publishes for free. About
1,500 rows a day, 700 to 800 of them on USD SOFR swaps.

Rows are stored raw, the way `sec-fundamentals` stores XBRL tags: turning
them into a volatility is the consumer's job, and so are the rules it takes.
The ones found on real files:

- **Keep `action_type = 'NEWT'` and `event_type = 'TRAD'`** for prices.
  Novations, amendments, terminations and exercises are messages about an
  older trade (novations often show a zero premium). A correction (`CORR`) or
  a cancellation (`EROR`) points to the message it replaces through
  `original_dissemination_id`.
- **Skip `notional_capped` rows** when dividing premium by notional: the
  notional is published capped, the premium is not.
- **Straddles traded on a platform** (`platform` other than `BILT`): both
  legs carry the premium of the *whole* straddle. A call and a put with the
  same expiry, underlier, strike, notional and premium is one straddle; halve
  the premium, or every such vol comes out doubled.
- **`strike` is as published**: almost always a decimal (0.049), under
  `strike_notation` '3' or '1' alike, but a few rows hold a percentage (4.9).
  Read it against the forward.
- **Long expiries** (beyond about five years) look quoted with a premium paid
  at expiry rather than up front; the file does not say which.
- `underlier_maturity` is missing on about a quarter of the rows.

Key: `dissemination_id`, DTCC's identifier of the message. UPSERT, and a
routine run re-reads a week: a file is cumulative over its UTC day, so the
current day is partial until midnight and is completed by the next run.

Files stay downloadable about two years (see `_dtcc_common`): `--full`
backfills that much and no more.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Iterable, List, Optional

import pandas as pd

from . import _dtcc_common as dtcc
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.dtcc")

#: "NA/O Call Epn OIS USD": an option (O), a call, European (Epn), on a USD
#: OIS swap. "P" is a put, "Opt" another option type (a chooser, a straddle
#: reported as one product).
_SWAPTION = re.compile(r"^NA/O (Call|P|Opt) Epn\b")
_OPTION_TYPE = {"Call": "call", "P": "put", "Opt": "other"}
#: UPI Underlier Name of a swaption: "NA/Swap OIS USD", "NA/Swap Fxd Flt EUR".
_SWAP_UNDERLIER = "NA/Swap "


def parse_swaptions(records: Iterable[dtcc.Record], report_date: date) -> List[tuple]:
    """One row per European swaption message of the file, in the table's
    column order. Options whose underlier is not a swap are left out; rows
    without a dissemination identifier or a currency are dropped: they cannot
    be keyed or used."""
    rows = []
    for record in records:
        product = (record.get("UPI FISN") or "").strip()
        match = _SWAPTION.match(product)
        dissemination_id = dtcc.text(record.get("Dissemination Identifier"))
        currency = dtcc.text(record.get("Notional currency-Leg 1"))
        underlier = dtcc.text(record.get("UPI Underlier Name"))
        # "NA/O Call Epn USD" on "USD-SOFR CME Term" is an option on an index
        # (a caplet), not on a swap.
        if match is None or underlier is None or not underlier.startswith(_SWAP_UNDERLIER):
            continue
        if dissemination_id is None or currency is None:
            continue
        notional = record.get("Notional amount-Leg 1")
        fixed_rate: Optional[float] = dtcc.number(record.get("Fixed rate-Leg 1"))
        if fixed_rate is None:
            fixed_rate = dtcc.number(record.get("Fixed rate-Leg 2"))
        rows.append(
            (
                dissemination_id,
                dtcc.text(record.get("Original Dissemination Identifier")),
                report_date,
                dtcc.text(record.get("Action type")),
                dtcc.text(record.get("Event type")),
                dtcc.timestamp(record.get("Execution Timestamp")),
                currency,
                product,
                _OPTION_TYPE[match.group(1)],
                underlier,
                dtcc.iso_date(record.get("Effective Date")),
                dtcc.iso_date(record.get("Expiration Date")),
                dtcc.iso_date(record.get("First exercise date")),
                dtcc.iso_date(record.get("Maturity date of the underlier")),
                dtcc.number(record.get("Strike Price")),
                dtcc.text(record.get("Strike price notation")),
                fixed_rate,
                dtcc.number(notional),
                dtcc.is_capped(notional),
                dtcc.number(record.get("Option Premium Amount")),
                dtcc.text(record.get("Option Premium Currency")),
                dtcc.text(record.get("Platform identifier")),
                dtcc.boolean(record.get("Package indicator")),
                dtcc.text(record.get("Cleared")),
            )
        )
    return rows


class DtccSwaptions(Source):
    name = "dtcc-swaptions"
    description = "European swaption trades (expiry, underlier, strike, notional, premium) from DTCC's public dissemination"
    write_mode = WriteMode.UPSERT
    # The file of a UTC day is complete after midnight UTC.
    schedule = "30 5 * * *"
    lookback_days = 7

    table = TableSpec(
        schema="rates",
        name="dtcc_swaptions",
        columns=(
            Column("dissemination_id", "TEXT", nullable=False),
            Column("original_dissemination_id", "TEXT"),
            Column("report_date", "DATE", nullable=False),
            Column("action_type", "TEXT"),
            Column("event_type", "TEXT"),
            Column("execution_timestamp", "TIMESTAMPTZ"),
            Column("currency", "TEXT", nullable=False),
            Column("product", "TEXT", nullable=False),
            Column("option_type", "TEXT", nullable=False),
            Column("underlier", "TEXT"),
            Column("effective_date", "DATE"),
            Column("expiry", "DATE"),
            Column("first_exercise_date", "DATE"),
            Column("underlier_maturity", "DATE"),
            Column("strike", "DOUBLE PRECISION"),
            Column("strike_notation", "TEXT"),
            Column("fixed_rate", "DOUBLE PRECISION"),
            Column("notional", "DOUBLE PRECISION"),
            Column("notional_capped", "BOOLEAN", nullable=False),
            Column("premium", "DOUBLE PRECISION"),
            Column("premium_currency", "TEXT"),
            Column("platform", "TEXT"),
            Column("package", "BOOLEAN"),
            Column("cleared", "TEXT"),
        ),
        primary_key=("dissemination_id",),
        indexes=(("report_date",), ("currency", "expiry")),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        rows: List[tuple] = []
        for day in dtcc.window_days(window):
            records = dtcc.fetch_day(day)
            if records is None:
                logger.info("[%s] no file for %s", self.name, day)
                continue
            rows.extend(parse_swaptions(records, day))
        # dtype=object: None stays None (not NaN) and booleans stay booleans,
        # which is what COPY needs for the nullable DATE and BOOLEAN columns.
        frame = pd.DataFrame(rows, columns=self.table.column_names, dtype=object)
        # A message re-published on a later day keeps its identifier: the
        # latest file wins.
        return frame.drop_duplicates(subset=["dissemination_id"], keep="last")
