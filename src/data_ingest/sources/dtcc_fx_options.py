"""Vanilla FX option trades published by DTCC's swap data repository — every
currency pair, every message of the day, as reported.

Implied FX volatilities are not published for free; the trades they come from
are. The foreign-exchange file of the public price dissemination (see
`_dtcc_common`) holds about 5,000 vanilla option messages a day, a quarter of
them on EUR/USD: strike, expiry, the two amounts exchanged at exercise and the
premium paid. Premium over notional, inverted through Garman-Kohlhagen's
formula with a forward read on the two currencies' curves, is an implied
volatility.

Rows are stored raw, as `dtcc-swaptions` does: turning them into a
volatility is the consumer's job, and so are the rules it takes. The ones
found on real files:

- **Keep `action_type = 'NEWT'` and `event_type = 'TRAD'`** for prices:
  terminations, exercises, novations and compressions are messages about an
  older trade. A correction (`CORR`) or a cancellation (`EROR`) points to the
  message it replaces through `original_dissemination_id`.
- **`option_type` is about the first currency of `pair`**: "put" on
  "EUR USD" is the right to sell euros. The unambiguous statement is the four
  `call_*` / `put_*` columns: what the holder receives and what it delivers
  at exercise. Their ratio is the strike.
- **`strike` is quoted as `strike_pair` says** ("EUR/USD": dollars per euro),
  which is not always the market convention of the pair (a few rows say
  "USD/EUR"); `strike_pair` itself is empty on about one row in ten. The
  ratio of the two amounts settles it.
- **One trade can be several rows**: reported by the venue and again by a
  clearing or reporting entity (`platform` "XXXX", "XLCH"), under another
  identifier, often with one of the two amounts missing. Same execution
  time, expiry, strike and premium: count it once.
- **Skip `notional_capped` rows** when dividing premium by notional: large
  amounts are published capped (a trailing "+"), the premium is not.
- **The premium is in `premium_currency`**, either currency of the pair.
- **A package** (`package`, six rows in ten) is a leg of a strategy — a
  straddle, a risk reversal — or an option traded with its delta hedge.
  Each leg carries its own premium here (136 of the 141 EUR/USD call-and-put
  pairs of 2026-10-01), unlike the platform straddles of the rates file.
- **The exercise style is not published.** OTC vanilla FX options are
  European by market convention; the file gives no way to check a given row.
- **Trades are short-dated**: seven in ten expire within three months and
  one in twenty beyond a year. A longer volatility is an extrapolation.
- A sanity check on 2026-10-01: EUR/USD options within 2 % of the money give
  6 to 7 % of volatility from one month to one year.

Barrier, digital and non-deliverable options are left out: their premium
does not invert to a Garman-Kohlhagen volatility.

Key: `dissemination_id`, DTCC's identifier of the message. UPSERT, and a
routine run re-reads a week: a file is cumulative over its UTC day, so the
current day is partial until midnight and is completed by the next run.

Files stay downloadable about two years (see `_dtcc_common`): `--full`
backfills that much and no more, and downloads about 3 MB a day.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Iterable, List

import pandas as pd

from . import _dtcc_common as dtcc
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.dtcc")

#: "NA/O Van Put EUR USD": an option (O), vanilla, a put, on EUR against USD.
_VANILLA = re.compile(r"^NA/O Van (Call|Put) ([A-Z]{3}) ([A-Z]{3})$")


def parse_fx_options(records: Iterable[dtcc.Record], report_date: date) -> List[tuple]:
    """One row per vanilla FX option message of the file, in the table's
    column order. Rows without a dissemination identifier are dropped: they
    cannot be keyed."""
    rows = []
    for record in records:
        product = (record.get("UPI FISN") or "").strip()
        match = _VANILLA.match(product)
        dissemination_id = dtcc.text(record.get("Dissemination Identifier"))
        if match is None or dissemination_id is None:
            continue
        notional_1 = record.get("Notional amount-Leg 1")
        notional_2 = record.get("Notional amount-Leg 2")
        rows.append(
            (
                dissemination_id,
                dtcc.text(record.get("Original Dissemination Identifier")),
                report_date,
                dtcc.text(record.get("Action type")),
                dtcc.text(record.get("Event type")),
                dtcc.timestamp(record.get("Execution Timestamp")),
                product,
                f"{match.group(2)} {match.group(3)}",
                match.group(1).lower(),
                dtcc.text(record.get("Call currency")),
                dtcc.number(record.get("Call amount")),
                dtcc.text(record.get("Put currency")),
                dtcc.number(record.get("Put amount")),
                dtcc.number(record.get("Strike Price")),
                dtcc.text(record.get("Strike price currency/currency pair")),
                dtcc.text(record.get("Strike price notation")),
                dtcc.iso_date(record.get("Effective Date")),
                dtcc.iso_date(record.get("Expiration Date")),
                dtcc.iso_date(record.get("Maturity date of the underlier")),
                dtcc.number(notional_1),
                dtcc.text(record.get("Notional currency-Leg 1")),
                dtcc.number(notional_2),
                dtcc.text(record.get("Notional currency-Leg 2")),
                dtcc.is_capped(notional_1)
                or dtcc.is_capped(notional_2)
                or dtcc.is_capped(record.get("Call amount"))
                or dtcc.is_capped(record.get("Put amount")),
                dtcc.number(record.get("Option Premium Amount")),
                dtcc.text(record.get("Option Premium Currency")),
                dtcc.text(record.get("Platform identifier")),
                dtcc.boolean(record.get("Package indicator")),
            )
        )
    return rows


class DtccFxOptions(Source):
    name = "dtcc-fx-options"
    description = "Vanilla FX option trades (pair, strike, expiry, amounts, premium) from DTCC's public dissemination"
    write_mode = WriteMode.UPSERT
    # The file of a UTC day is complete after midnight UTC.
    schedule = "40 5 * * *"
    lookback_days = 7

    table = TableSpec(
        schema="fx",
        name="dtcc_options",
        columns=(
            Column("dissemination_id", "TEXT", nullable=False),
            Column("original_dissemination_id", "TEXT"),
            Column("report_date", "DATE", nullable=False),
            Column("action_type", "TEXT"),
            Column("event_type", "TEXT"),
            Column("execution_timestamp", "TIMESTAMPTZ"),
            Column("product", "TEXT", nullable=False),
            Column("pair", "TEXT", nullable=False),
            Column("option_type", "TEXT", nullable=False),
            Column("call_currency", "TEXT"),
            Column("call_amount", "DOUBLE PRECISION"),
            Column("put_currency", "TEXT"),
            Column("put_amount", "DOUBLE PRECISION"),
            Column("strike", "DOUBLE PRECISION"),
            Column("strike_pair", "TEXT"),
            Column("strike_notation", "TEXT"),
            Column("effective_date", "DATE"),
            Column("expiry", "DATE"),
            Column("settlement_date", "DATE"),
            Column("notional_1", "DOUBLE PRECISION"),
            Column("currency_1", "TEXT"),
            Column("notional_2", "DOUBLE PRECISION"),
            Column("currency_2", "TEXT"),
            Column("notional_capped", "BOOLEAN", nullable=False),
            Column("premium", "DOUBLE PRECISION"),
            Column("premium_currency", "TEXT"),
            Column("platform", "TEXT"),
            Column("package", "BOOLEAN"),
        ),
        primary_key=("dissemination_id",),
        indexes=(("report_date",), ("pair", "expiry")),
    )

    def fetch(self, window: Window) -> pd.DataFrame:
        rows: List[tuple] = []
        for day in dtcc.window_days(window):
            records = dtcc.fetch_day(day, dtcc.FOREX)
            if records is None:
                logger.info("[%s] no file for %s", self.name, day)
                continue
            rows.extend(parse_fx_options(records, day))
        # dtype=object: None stays None (not NaN) and booleans stay booleans,
        # which is what COPY needs for the nullable DATE and BOOLEAN columns.
        frame = pd.DataFrame(rows, columns=self.table.column_names, dtype=object)
        # A message re-published on a later day keeps its identifier: the
        # latest file wins.
        return frame.drop_duplicates(subset=["dissemination_id"], keep="last")
