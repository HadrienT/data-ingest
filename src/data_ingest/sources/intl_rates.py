"""Reference interest rates for EUR, GBP, CHF and JPY, from their central banks
and treasuries — the non-USD counterpart of what `fred-macro` carries for USD.

What is free, and therefore what is here, per currency:

| Ccy | Overnight benchmark (+ compounded averages) | Government curve                    |
|-----|---------------------------------------------|-------------------------------------|
| EUR | €STR, compounded 1W–12M; ECB deposit rate   | ECB euro-area AAA zero-coupon curve |
| GBP | SONIA; Bank Rate                            | BoE nominal gilt par yields 5/10/20Y|
| CHF | SARON, compounded 1M/3M/6M                  | — (the SNB stopped publishing it)   |
| JPY | TONA (uncollateralised overnight call rate) | MoF JGB par yields 1Y–40Y           |

What is NOT free, and therefore not here: OIS swap curves in any currency
(€STR/SONIA/SARON/TONA beyond a few months) and daily Euribor fixings (EMMI
licence; the ECB publishes only a monthly average, which is stored as such).

Every series is stored in one long table, `(series_id, date, value)` in percent,
the shape of `macro.fred_series`, so a consumer reads both the same way. Values
are fixings of the past, corrected rather than revised, hence UPSERT (unlike
FRED's macro indicators). Each provider is fetched independently: one being
down costs its own series, not the others'; only all of them failing fails the
run, so Airflow retries and the failure is visible.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

import pandas as pd

from . import _intl_providers as p
from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.intl_rates")


@dataclass(frozen=True)
class Series:
    series_id: str  # our id: <CCY>.<NAME>
    upstream: str  # the provider's code
    kind: str  # overnight | compounded | policy | interbank_monthly | govt_zero | govt_par
    tenor_years: Optional[float]  # None for an overnight or policy rate
    description: str


def _t(label: str) -> float:
    n, unit = int(label[:-1]), label[-1]
    return {"W": n / 52, "M": n / 12, "Y": float(n)}[unit]


_ECB_CURVE_TENORS = ("3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y", "30Y")
_ESTR_COMPOUNDED = {"1W": "F16", "1M": "F24", "3M": "F32", "6M": "F40", "12M": "F57"}
_EURIBOR = {"1M": "EURIBOR1MD_", "3M": "EURIBOR3MD_", "6M": "EURIBOR6MD_", "12M": "EURIBOR1YD_"}
_JGB_TENORS = ("1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y", "8Y", "9Y", "10Y", "15Y", "20Y", "25Y", "30Y", "40Y")

ECB_SERIES: List[Series] = [
    Series("EUR.ESTR", "EST.B.EU000A2X2A25.WT", "overnight", None, "Euro short-term rate (€STR)"),
    *[
        Series(f"EUR.ESTR_CA_{t}", f"EST.B.EU000A2QQ{code}.CR", "compounded", _t(t),
               f"Compounded €STR average rate, {t} (backward-looking)")
        for t, code in _ESTR_COMPOUNDED.items()
    ],
    Series("EUR.ECB_DFR", "FM.D.U2.EUR.4F.KR.DFR.LEV", "policy", None, "ECB deposit facility rate"),
    *[
        Series(f"EUR.EURIBOR_{t}_MONTHLY", f"FM.M.U2.EUR.RT.MM.{code}.HSTA", "interbank_monthly", _t(t),
               f"Euribor {t}, monthly average (daily fixings are licensed by EMMI)")
        for t, code in _EURIBOR.items()
    ],
    *[
        Series(f"EUR.AAA_SPOT_{t}", f"YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{t}", "govt_zero", _t(t),
               f"Euro area AAA government zero-coupon spot rate, {t} (ECB Svensson fit)")
        for t in _ECB_CURVE_TENORS
    ],
]

BOE_SERIES: List[Series] = [
    Series("GBP.SONIA", "IUDSOIA", "overnight", None, "Sterling overnight index average (SONIA)"),
    Series("GBP.BANK_RATE", "IUDBEDR", "policy", None, "Bank of England Bank Rate"),
    Series("GBP.GILT_PAR_5Y", "IUDSNPY", "govt_par", 5.0, "Nominal gilt par yield, 5Y (BoE curve)"),
    Series("GBP.GILT_PAR_10Y", "IUDMNPY", "govt_par", 10.0, "Nominal gilt par yield, 10Y (BoE curve)"),
    Series("GBP.GILT_PAR_20Y", "IUDLNPY", "govt_par", 20.0, "Nominal gilt par yield, 20Y (BoE curve)"),
]

SNB_CUBE = "zirepo"
SNB_SERIES: List[Series] = [
    Series("CHF.SARON", "H0", "overnight", None, "Swiss average rate overnight (SARON), close of trading"),
    Series("CHF.SARON_CR_1M", "H6", "compounded", _t("1M"), "SARON 1M compound rate (backward-looking)"),
    Series("CHF.SARON_CR_3M", "H7", "compounded", _t("3M"), "SARON 3M compound rate (backward-looking)"),
    Series("CHF.SARON_CR_6M", "H8", "compounded", _t("6M"), "SARON 6M compound rate (backward-looking)"),
]

BOJ_DB = "FM01"
BOJ_SERIES: List[Series] = [
    Series("JPY.TONA", "STRDCLUCON", "overnight", None,
           "Tokyo overnight average rate (TONA): uncollateralised overnight call rate, average"),
]

MOF_SERIES: List[Series] = [
    Series(f"JPY.JGB_{t}", t, "govt_par", _t(t), f"JGB par yield, {t} (Ministry of Finance)")
    for t in _JGB_TENORS
]

CATALOG: List[Series] = ECB_SERIES + BOE_SERIES + SNB_SERIES + BOJ_SERIES + MOF_SERIES


class IntlRates(Source):
    name = "intl-rates"
    description = "EUR/GBP/CHF/JPY reference rates and government curves (ECB, BoE, SNB, BoJ, MoF)"
    write_mode = WriteMode.UPSERT
    # After the European and Japanese publications of the day; weekdays, like
    # the other sources. SNB and BoJ publish with a lag of several days, hence
    # a long lookback — upserting a few hundred rows twice costs nothing.
    schedule = "30 21 * * 1-5"
    lookback_days = 45

    table = TableSpec(
        schema="macro",
        name="intl_rates",
        columns=(
            Column("series_id", "TEXT", nullable=False),
            Column("date", "DATE", nullable=False),
            Column("value", "DOUBLE PRECISION"),
        ),
        primary_key=("series_id", "date"),
        indexes=(("date",),),
    )

    def providers(self, window: Window) -> List[tuple[str, Callable[[], List[p.Row]]]]:
        start = None if window.full else window.start
        today = datetime.now(timezone.utc).date()
        return [
            ("ECB", lambda: p.fetch_ecb({s.upstream: s.series_id for s in ECB_SERIES}, start)),
            ("BoE", lambda: p.fetch_boe({s.upstream: s.series_id for s in BOE_SERIES}, start)),
            ("SNB", lambda: p.fetch_snb(SNB_CUBE, {s.upstream: s.series_id for s in SNB_SERIES}, start)),
            ("BoJ", lambda: p.fetch_boj(BOJ_DB, {s.upstream: s.series_id for s in BOJ_SERIES}, start)),
            ("MoF", lambda: p.fetch_mof(start, today)),
        ]

    def fetch(self, window: Window) -> pd.DataFrame:
        rows: List[p.Row] = []
        failed: List[str] = []
        providers = self.providers(window)
        for name, fetch in providers:
            try:
                got = fetch()
            except Exception as exc:  # noqa: BLE001 — one provider down must not sink the others
                logger.warning("intl-rates: %s failed: %s", name, exc)
                failed.append(name)
                continue
            logger.info("intl-rates: %s returned %d observations", name, len(got))
            rows += got
        if len(failed) == len(providers):
            raise RuntimeError(f"intl-rates: every provider failed ({', '.join(failed)})")
        if not rows:
            return pd.DataFrame(columns=["series_id", "date", "value"])
        known = {s.series_id for s in CATALOG}
        data = pd.DataFrame(rows, columns=["series_id", "date", "value"])
        data = data[data["series_id"].isin(known)]
        # The MoF current-month and history files can overlap: one row per key.
        return data.drop_duplicates(["series_id", "date"], keep="last").reset_index(drop=True)
