from datetime import date

import pandas as pd

from data_ingest.sources.options_chain import (
    DEFAULT_TICKERS,
    OptionsChainSnapshot,
    _normalize_side,
)


def _raw_side(**overrides):
    row = {
        "strike": 100.0,
        "bid": 1.2,
        "ask": 1.4,
        "lastPrice": 1.3,
        "volume": 42,
        "openInterest": 500,
        "impliedVolatility": 0.25,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_normalize_side_maps_yfinance_columns_to_table_columns():
    out = _normalize_side(_raw_side(), "call", "AAPL", date(2026, 1, 16), date(2025, 12, 1))

    assert list(out.columns) == OptionsChainSnapshot.table.column_names
    row = out.iloc[0]
    assert row["date"] == date(2025, 12, 1)
    assert row["ticker"] == "AAPL"
    assert row["expiry"] == date(2026, 1, 16)
    assert row["option_type"] == "call"
    assert row["strike"] == 100.0
    assert row["bid"] == 1.2
    assert row["ask"] == 1.4
    assert row["last_price"] == 1.3
    assert row["volume"] == 42
    assert row["open_interest"] == 500
    assert row["implied_volatility"] == 0.25


def test_normalize_side_drops_non_positive_strikes():
    out = _normalize_side(_raw_side(strike=0.0), "call", "AAPL", date(2026, 1, 16), date(2025, 12, 1))
    assert out.empty


def test_normalize_side_treats_zero_implied_volatility_as_missing():
    out = _normalize_side(_raw_side(impliedVolatility=0.0), "put", "AAPL", date(2026, 1, 16), date(2025, 12, 1))
    assert pd.isna(out.iloc[0]["implied_volatility"])


def test_normalize_side_handles_empty_and_none_input():
    assert _normalize_side(None, "call", "AAPL", date(2026, 1, 16), date(2025, 12, 1)).empty
    assert _normalize_side(pd.DataFrame(), "call", "AAPL", date(2026, 1, 16), date(2025, 12, 1)).empty


def test_normalize_side_skips_a_frame_missing_expected_columns():
    broken = pd.DataFrame([{"strike": 100.0}])  # missing bid/ask/etc.
    assert _normalize_side(broken, "call", "AAPL", date(2026, 1, 16), date(2025, 12, 1)).empty


def test_table_primary_key_matches_what_upsert_needs_to_dedupe_a_rerun():
    assert OptionsChainSnapshot.table.primary_key == ("date", "ticker", "expiry", "option_type", "strike")


def test_tickers_defaults_to_the_fixed_liquid_universe(monkeypatch):
    monkeypatch.delenv("OPTIONS_CHAIN_TICKERS", raising=False)
    assert OptionsChainSnapshot().tickers() == DEFAULT_TICKERS


def test_tickers_honours_the_env_override(monkeypatch):
    monkeypatch.setenv("OPTIONS_CHAIN_TICKERS", "aapl, msft ,, nvda")
    assert OptionsChainSnapshot().tickers() == ["AAPL", "MSFT", "NVDA"]
