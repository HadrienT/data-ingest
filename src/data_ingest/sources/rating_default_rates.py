"""One-year default rates by rating, from the statistics the rating agencies
must report to ESMA (the EU's markets authority), which publishes them in its
central repository, CEREP: https://registers.esma.europa.eu/cerep-publication/

For each agency and each calendar year, one row per rating category: how many
of the corporate issuers rated in that category on 1 January defaulted during
the year, and that number as a percentage of them. It is the *historical*
frequency of default — what a regulatory capital formula asks for — as opposed
to the probability a credit spread implies, which also pays for the risk.

Why ESMA and not the agencies' own default studies: those are the usual
reference, but their terms forbid storing or redistributing the tables. ESMA's
legal notice allows reproduction with acknowledgement of the source, and the
figures are the agencies' own, in a regulated format. Averaged over 2000-2025,
S&P's rates here (BBB 0.11 %, BB 0.53 %, B 2.96 %) are those of its study of
1981-2024 (0.14 %, 0.56 %, 2.93 %) to a few basis points.

What a consumer has to know:

- **Scope**: corporate ratings (corporates, financial institutions, insurers),
  long-term, by rating category (AAA, AA, ... — not by notch), of the three
  global agencies. Each agency keeps its own scale: `rating` is `BBB` for S&P
  and Fitch, `Baa` for Moody's, and the scales end with the agency's default
  and withdrawn states (`SD`, `D`, `NR`, `RD`, `WD`, `WR`), stored as published.
- **The agencies' EU entities**: the cohort is the ratings issued or endorsed
  by the entity registered with ESMA, a subset of the agency's global book —
  a few names only in the top category, so its rate is zero every year.
- **A year with no statistics is absent**, not zero: S&P starts in 2000.
- `default_rate` is in percent, as published; ESMA does not publish the
  denominator, and `defaults / default_rate` only recovers it to rounding.

ESMA updates the repository twice a year and agencies do resubmit past
periods, hence UPSERT over a lookback of three years.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional

import pandas as pd
import requests

from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

log = logging.getLogger(__name__)

CEREP_URL = "https://registers.esma.europa.eu/cerep-publication/"
_HEADERS = {"User-Agent": "data-ingest (personal research)", "Content-Type": "application/json"}
_TIMEOUT_S = 120
#: Between two requests: the repository answers a query in about ten seconds
#: and is nobody's API.
_PAUSE_S = 0.5

#: Our name for the agency -> the code of its entity in CEREP.
AGENCIES: Dict[str, str] = {
    "S&P": "STPGB",
    "Moody's": "MDYGB",
    "Fitch": "FITGB",
}

#: CEREP's first period.
FIRST_YEAR = 1989

COLUMNS = ["agency", "period_start", "period_end", "rating", "defaults", "default_rate"]


def _epoch_ms(day: date) -> str:
    """CEREP's code for a period boundary: midnight UTC, in milliseconds."""
    return str(int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000))


def _one(code: str) -> dict:
    return {"filterList": [{"code": code, "selected": True}]}


def query(cra: str, year: int) -> dict:
    """The body CEREP's page posts for: this agency, corporate ratings, long
    term, the calendar year, by rating category, in percentages."""
    scale_and_unit = {
        "filterList": [{"code": "C", "selected": True}, {"code": "P", "selected": True}]
    }
    return {
        "filters": {
            "cra": _one(cra),
            "begOfPrd": _one(_epoch_ms(date(year, 1, 1))),
            "endOfPrd": _one(_epoch_ms(date(year, 12, 31))),
            "ratingType": _one("C"),
            "timeHorizon": _one("L"),
            "categories": scale_and_unit,
            "percentage": scale_and_unit,
        }
    }


def parse_default_rates(payload: Mapping, agency: str, year: int) -> List[tuple]:
    """Rows of one answer of `searchStatistics/2`. No statistics for the
    period (the agency did not report it, or changed its scale during it)
    comes back as an empty or missing map: no rows."""
    rates = (payload.get("defaultRates") or {}).get("defMap") or {}
    rows = []
    for rating, cell in rates.items():
        rows.append(
            (
                agency,
                date(year, 1, 1),
                date(year, 12, 31),
                rating,
                int(cell["numberOfRatings"]),
                float(cell["percentageOfRatings"]),
            )
        )
    return rows


def published_years(filters: Mapping) -> List[int]:
    """The calendar years CEREP has: those whose 31 December is among the
    period ends it offers."""
    years = []
    for item in filters["filters"]["endOfPrd"]["filterList"]:
        day, month, year = item["name"].split("/")
        if (day, month) == ("31", "12"):
            years.append(int(year))
    return sorted(years)


def _post(path: str, body: Mapping) -> dict:
    response = requests.post(CEREP_URL + path, json=body, headers=_HEADERS, timeout=_TIMEOUT_S)
    response.raise_for_status()
    return response.json()


class RatingDefaultRates(Source):
    name = "rating-default-rates"
    description = (
        "One-year corporate default rates by rating category of S&P, Moody's and Fitch "
        "(ESMA CEREP, yearly)"
    )
    write_mode = WriteMode.UPSERT
    # ESMA publishes twice a year; a monthly look costs nine requests.
    schedule = "0 7 2 * *"
    lookback_days = 3 * 366

    table = TableSpec(
        schema="credit",
        name="rating_default_rates",
        columns=(
            Column("agency", "TEXT", nullable=False),
            Column("period_start", "DATE", nullable=False),
            Column("period_end", "DATE", nullable=False),
            Column("rating", "TEXT", nullable=False),
            Column("defaults", "INTEGER", nullable=False),
            Column("default_rate", "DOUBLE PRECISION", nullable=False),
        ),
        primary_key=("agency", "period_start", "period_end", "rating"),
        indexes=(("rating", "period_start"),),
    )

    def years(self, window: Window, published: Iterable[int]) -> List[int]:
        first: Optional[int] = None if window.full or window.start is None else window.start.year
        last: Optional[int] = None if window.full or window.end is None else window.end.year
        return [
            y
            for y in published
            if y >= FIRST_YEAR and (first is None or y >= first) and (last is None or y <= last)
        ]

    def fetch(self, window: Window) -> pd.DataFrame:
        years = self.years(window, published_years(_post("filters", {})))
        rows: List[tuple] = []
        for agency, cra in AGENCIES.items():
            for year in years:
                time.sleep(_PAUSE_S)
                found = parse_default_rates(_post("searchStatistics/2", query(cra, year)), agency, year)
                if not found:
                    log.info("rating-default-rates: no statistics for %s in %d", agency, year)
                rows.extend(found)
        return pd.DataFrame(rows, columns=COLUMNS)
