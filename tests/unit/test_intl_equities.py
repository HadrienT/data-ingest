"""equity-universe and intl-equity-prices: the committed constituent lists are
complete and well-formed, pence become pounds, unsettled rows are dropped."""

from datetime import date

import pandas as pd
import pytest

from data_ingest.core.source import Window
from data_ingest.sources import _constituents as c
from data_ingest.sources.equity_universe import EquityUniverse
from data_ingest.sources.intl_equity_prices import IntlEquityPrices, normalise

EXPECTED = {"CAC40": 40, "DAX": 40, "FTSE100": 100, "NIKKEI225": 225}


@pytest.mark.parametrize("market", sorted(EXPECTED))
def test_each_list_has_its_index_and_every_constituent(market):
    members = c.members(market)
    assert [m.kind for m in members].count("index") == 1
    assert [m.kind for m in members].count("equity") == EXPECTED[market]
    assert len({m.ticker for m in members}) == len(members)
    assert all(m.name and m.quote_currency and m.as_of for m in members)


def test_yahoo_symbol_conventions():
    assert all(m.ticker.endswith(".T") for m in c.members("NIKKEI225") if m.kind == "equity")
    assert all(m.ticker.endswith(".L") for m in c.members("FTSE100") if m.kind == "equity")
    # A share class is written with a dash on Yahoo.
    assert "BT-A.L" in {m.ticker for m in c.members("FTSE100")}


def test_pence_are_stored_as_pounds_other_quotes_untouched():
    members = {
        "AZN.L": c.Member("FTSE100", "AZN.L", "AstraZeneca", "equity", "GBp", "2026-09-24"),
        "^FTSE": c.Member("FTSE100", "^FTSE", "FTSE 100", "index", "GBP", "2026-09-24"),
        "X.L": c.Member("FTSE100", "X.L", "A USD line", "equity", "USD", "2026-09-24"),
    }
    raw = pd.DataFrame(
        {
            "date": [date(2026, 9, 24)] * 3,
            "ticker": ["AZN.L", "^FTSE", "X.L"],
            "open": [12400.0, 10700.0, 50.0],
            "high": [12500.0, 10720.0, 51.0],
            "low": [12300.0, 10680.0, 49.0],
            "close": [12460.0, 10710.31, 50.5],
            "volume": [1, 0, 2],
        }
    )
    out = normalise(raw, members).set_index("ticker")
    assert out.loc["AZN.L", "close"] == pytest.approx(124.60)
    assert out.loc["AZN.L", "currency"] == "GBP"
    assert out.loc["^FTSE", "close"] == pytest.approx(10710.31)
    assert out.loc["X.L", ["close", "currency"]].tolist() == [50.5, "USD"]


def test_an_unsettled_close_is_dropped_not_stored_as_nan():
    members = {"^N225": c.Member("NIKKEI225", "^N225", "Nikkei 225", "index", "JPY", "2026-09-24")}
    raw = pd.DataFrame(
        {"date": [date(2026, 9, 23), date(2026, 9, 24)], "ticker": ["^N225"] * 2,
         "open": [1.0, 1.0], "high": [1.0, 1.0], "low": [1.0, 1.0],
         "close": [45000.0, float("nan")], "volume": [0, 0]}
    )
    out = normalise(raw, members)
    assert list(out["date"]) == [date(2026, 9, 23)]


def test_universe_covers_five_markets_and_keys_are_unique():
    frame = EquityUniverse().fetch(Window.recent(1))
    assert set(frame["market"]) == {"SP500", *EXPECTED}
    assert not frame.duplicated(["market", "ticker"]).any()
    # Airbus is in two indices: two rows, one per market.
    assert set(frame.loc[frame["ticker"] == "AIR.PA", "market"]) == {"CAC40", "DAX"}
    assert frame["currency"].str.fullmatch(r"[A-Z]{3}").all()  # ISO, never "GBp"


def test_price_table_shape():
    table = IntlEquityPrices.table
    assert table.qualified == "prices.intl_equity_daily"
    assert table.primary_key == ("date", "ticker")
    assert "currency" in [col.name for col in table.columns]


def test_tickers_missing_from_a_partial_download_are_asked_again(monkeypatch):
    from data_ingest.sources import intl_equity_prices as mod

    calls = []

    def fake(tickers, start, end):
        calls.append(list(tickers))
        served = tickers[:1] if len(calls) == 1 else tickers  # first pass: rate-limited
        return pd.DataFrame({"Date": [date(2026, 9, 24)] * len(served), "Ticker": served,
                             "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 0})

    monkeypatch.setattr(mod, "download_daily_ohlcv", fake)
    out = mod._download_with_retry(["A", "B", "C"], "2026-09-24", "2026-09-25", pause_s=0)
    assert calls == [["A", "B", "C"], ["B", "C"]]
    assert sorted(out["Ticker"]) == ["A", "B", "C"]


def test_a_total_outage_is_not_retried(monkeypatch):
    from data_ingest.sources import intl_equity_prices as mod

    calls = []
    monkeypatch.setattr(mod, "download_daily_ohlcv", lambda t, s, e: calls.append(t) or pd.DataFrame())
    assert mod._download_with_retry(["A", "B"], "s", "e", pause_s=0).empty
    assert len(calls) == 1
