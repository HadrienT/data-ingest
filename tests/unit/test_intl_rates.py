"""intl-rates: each provider's parser on a payload recorded from the live API
(trimmed), and the source's contract. The `network` test at the bottom asks
every provider for real."""

from datetime import date

import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _intl_providers as p
from data_ingest.sources import intl_rates
from data_ingest.sources.intl_rates import CATALOG, IntlRates

ECB_CSV = """KEY,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS
EST.B.EU000A2X2A25.WT,B,EU000A2X2A25,2026-09-22,2.437,A
EST.B.EU000A2X2A25.WT,B,EU000A2X2A25,2026-09-23,2.439,A
FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA,M,U2,2026-08,2.5131429,A
YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y,B,U2,2026-09-23,,M
OTHER.KEY,B,U2,2026-09-23,9.9,A
"""

BOE_CSV = """DATE,IUDSOIA,IUDMNPY
22 Sep 2026,3.7305,5.1907
23 Sep 2026,3.7310,
"""

SNB_CSV = (
    '﻿"CubeId";"zirepo"\r\n"PublishingDate";"2026-09-21 09:00"\r\n\r\n'
    '"Date";"D0";"Value"\r\n"2026-09-15";"H0";"-0.040425"\r\n'
    '"2026-09-15";"H1";"-0.03627"\r\n"2026-09-15";"H7";""\r\n'
)

BOJ_CSV = """STATUS,200
MESSAGEID,M181000I
MESSAGE,Successfully completed
STRDCLUCON,"Call Rate, Uncollateralized Overnight, Average (Daily)",percent per annum,DAILY,Call Rate,20260924,20260916,0.977
STRDCLUCON,"Call Rate, Uncollateralized Overnight, Average (Daily)",percent per annum,DAILY,Call Rate,20260924,20260917,
"""

MOF_CSV = (
    "Interest Rate,,,,(Unit : %)\r\n"
    "Date,1Y,2Y,10Y,40Y\r\n"
    "1974/9/24,10.327,9.362,-,-\r\n"
    "2026/9/18,1.578,1.849,2.981,4.033\r\n"
    '"  ※If you cannot download the latest csv data, please clear the browser\'s cache",,,,\r\n'
)


def test_ecb_rows_are_mapped_and_monthly_periods_dated_the_first():
    series = {
        "EST.B.EU000A2X2A25.WT": "EUR.ESTR",
        "FM.M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA": "EUR.EURIBOR_3M_MONTHLY",
        "YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y": "EUR.AAA_SPOT_10Y",
    }
    rows = p.parse_ecb_csv(ECB_CSV, series)
    assert rows == [
        ("EUR.ESTR", date(2026, 9, 22), 2.437),
        ("EUR.ESTR", date(2026, 9, 23), 2.439),
        ("EUR.EURIBOR_3M_MONTHLY", date(2026, 8, 1), 2.5131429),
    ]  # the empty OBS_VALUE and the unrequested key are dropped


def test_boe_empty_cells_are_dropped_not_stored_as_nan():
    rows = p.parse_boe_csv(BOE_CSV, {"IUDSOIA": "GBP.SONIA", "IUDMNPY": "GBP.GILT_PAR_10Y"})
    assert rows == [
        ("GBP.SONIA", date(2026, 9, 22), 3.7305),
        ("GBP.GILT_PAR_10Y", date(2026, 9, 22), 5.1907),
        ("GBP.SONIA", date(2026, 9, 23), 3.7310),
    ]


def test_snb_cube_with_crlf_and_metadata_header():
    rows = p.parse_snb_csv(SNB_CSV, {"H0": "CHF.SARON", "H7": "CHF.SARON_CR_3M"})
    assert rows == [("CHF.SARON", date(2026, 9, 15), -0.040425)]


def test_boj_skips_the_status_header_and_missing_values():
    rows = p.parse_boj_csv(BOJ_CSV, {"STRDCLUCON": "JPY.TONA"})
    assert rows == [("JPY.TONA", date(2026, 9, 16), 0.977)]


def test_mof_tenors_before_they_existed_and_the_trailing_notice_are_skipped():
    rows = p.parse_mof_csv(MOF_CSV)
    assert ("JPY.JGB_1Y", date(1974, 9, 24), 10.327) in rows
    assert not any(r[0] == "JPY.JGB_10Y" and r[1].year == 1974 for r in rows)
    assert ("JPY.JGB_40Y", date(2026, 9, 18), 4.033) in rows
    assert len(rows) == 2 + 4


def test_catalog_ids_are_unique_and_currency_prefixed():
    ids = [s.series_id for s in CATALOG]
    assert len(ids) == len(set(ids))
    assert {i.split(".")[0] for i in ids} == {"EUR", "GBP", "CHF", "JPY"}


def test_table_shape_matches_fred_series():
    table = IntlRates.table
    assert table.qualified == "macro.intl_rates"
    assert table.primary_key == ("series_id", "date")


def _fake(monkeypatch, outcomes):
    def providers(self, window):
        def make(outcome):
            def run():
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome
            return run
        return [(name, make(o)) for name, o in outcomes.items()]
    monkeypatch.setattr(IntlRates, "providers", providers)


def test_one_provider_down_costs_only_its_own_series(monkeypatch):
    _fake(monkeypatch, {
        "ECB": [("EUR.ESTR", date(2026, 9, 23), 2.439)],
        "BoE": RuntimeError("503"),
    })
    frame = IntlRates().fetch(Window.recent(5))
    assert list(frame["series_id"]) == ["EUR.ESTR"]


def test_every_provider_down_fails_the_run(monkeypatch):
    _fake(monkeypatch, {"ECB": RuntimeError("down"), "BoE": RuntimeError("down")})
    with pytest.raises(RuntimeError, match="every provider failed"):
        IntlRates().fetch(Window.recent(5))


def test_overlapping_rows_are_deduplicated_and_unknown_ids_dropped(monkeypatch):
    _fake(monkeypatch, {
        "MoF": [
            ("JPY.JGB_10Y", date(2026, 9, 1), 2.9),
            ("JPY.JGB_10Y", date(2026, 9, 1), 2.9),
            ("JPY.JGB_99Y", date(2026, 9, 1), 1.0),
        ],
    })
    frame = IntlRates().fetch(Window.recent(5))
    assert len(frame) == 1 and frame.iloc[0]["series_id"] == "JPY.JGB_10Y"


@pytest.mark.network
def test_every_catalogued_series_is_published():
    frame = IntlRates().fetch(Window.recent(45))
    assert not frame.empty and not frame["value"].isna().any()
    missing = {s.series_id for s in CATALOG} - set(frame["series_id"])
    # Monthly Euribor averages can be up to ~7 weeks old; everything else is
    # daily (SNB and BoJ publish with a lag of about a week).
    assert not missing, f"not published in the last 45 days: {sorted(missing)}"
    assert isinstance(frame, pd.DataFrame) and intl_rates.CATALOG


# ── ecb-fx ───────────────────────────────────────────────────────────────────

EXR_CSV = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-09-24,1.1367
EXR.D.JPY.EUR.SP00.A,D,JPY,EUR,SP00,A,2026-09-24,180.57
EXR.D.RUB.EUR.SP00.A,D,RUB,EUR,SP00,A,2022-03-01,
"""


def test_ecb_fx_rows_are_units_per_euro_and_gaps_dropped():
    from data_ingest.sources.ecb_fx import parse_exr_csv

    rows = parse_exr_csv(EXR_CSV)
    assert rows == [("USD", date(2026, 9, 24), 1.1367), ("JPY", date(2026, 9, 24), 180.57)]
    # a cross rate by division: USD/JPY = JPY per EUR / USD per EUR
    assert rows[1][2] / rows[0][2] == pytest.approx(158.855, rel=1e-4)


@pytest.mark.network
def test_ecb_fx_publishes_the_project_currencies():
    from data_ingest.sources.ecb_fx import EcbFx

    frame = EcbFx().fetch(Window.recent(10))
    assert {"USD", "GBP", "JPY", "CHF"} <= set(frame["currency"])
    assert not frame["value"].isna().any()
