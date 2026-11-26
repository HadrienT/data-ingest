"""options-chain-iv on synthetic days: European parity, the discount curve,
OTM selection, flags, and the American branch (quant-modeling WP 31, §4.6)."""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest
from synthetic_options import (
    DAY, SPOT, SPY_DIVIDENDS, SPY_SIGMA, RATE, day_inputs, smile, spx_chain, spy_chain, ttm_to,
)

from data_ingest.core.spec import WriteMode
from data_ingest.sources import options_chain_iv as iv
from data_ingest.sources.options_chain_iv import Flag


@pytest.fixture(scope="module")
def european_day():
    return iv.compute(day_inputs(spx_chain(noisy_expiry=date(2027, 6, 17))))


# ------------------------------------------------------------------ times


def test_valuation_is_the_close_of_the_last_session():
    # Thanksgiving holds the previous session's quotes; the day after closes at 13:00.
    assert iv.valuation_time(date(2026, 11, 26)) == pd.Timestamp("2026-11-25 21:00", tz="UTC")
    assert iv.valuation_time(date(2026, 11, 27)) == pd.Timestamp("2026-11-27 18:00", tz="UTC")


def test_am_settlement_is_a_session_earlier_than_pm():
    am = iv.settlement_time(date(2026, 12, 18), "am")
    pm = iv.settlement_time(date(2026, 12, 18), "pm")
    assert am == pd.Timestamp("2026-12-18 14:30", tz="UTC")
    assert (pm - am) == pd.Timedelta(hours=6, minutes=30)


# -------------------------------------------------------------- European


def test_the_curve_comes_from_the_days_european_parity(european_day):
    _, day = european_day
    accepted = day.expiries[(day.expiries["status"] == "accepted")]
    assert (accepted["discount_source"] == "parity").all()
    for row in accepted.itertuples():
        assert row.discount == pytest.approx(np.exp(-RATE * row.ttm), rel=5e-4)
        assert row.forward == pytest.approx(SPOT * np.exp((RATE - 0.011) * row.ttm), rel=2e-4)
    assert {load["curve_source"] for load in day.loads} == {"parity"}


def test_european_vols_recover_thesmile(european_day):
    quotes, _ = european_day
    clean = quotes[(quotes["flags"] & int(Flag.EXPIRY_REJECTED | Flag.IV_FAILED)) == 0]
    assert (clean["exercise"] == "european").all() and (clean["early_exercise_premium"] == 0).all()
    error = (clean["implied_vol"] - smile(clean["log_moneyness"])).abs()
    # The quotes are noisy within 30 % of a half-spread: so is the vol, by
    # that much over vega, on top of the forward's own error.
    noise = 0.3 * (clean["ask"] - clean["bid"]) / 2.0 / clean["vega"]
    assert (error <= noise + 0.001).all()


def test_one_side_per_strike_the_otm_one_and_both_near_the_money(european_day):
    quotes, _ = european_day
    used = quotes[(quotes["flags"] & int(Flag.NOT_OTM)) == 0]
    assert (used.loc[used["strike"] < used["forward"], "option_type"] == "put").all()
    assert (used.loc[used["strike"] >= used["forward"], "option_type"] == "call").all()
    diagnostic = quotes[(quotes["flags"] & int(Flag.NOT_OTM)) != 0]
    assert len(diagnostic) > 0
    assert (diagnostic["log_moneyness"].abs() < iv.BOTH_SIDES_BAND).all()
    assert not quotes.duplicated(["expiry", "root", "option_type", "strike"]).any()


def test_a_noisy_expiry_is_rejected_and_its_quotes_flagged(european_day):
    quotes, day = european_day
    noisy = day.expiries[day.expiries["expiry"] == date(2027, 6, 17)].iloc[0]
    assert noisy["status"] == "rejected" and noisy["rejection_reason"] == "parity_dispersion"
    flags = quotes.loc[quotes["expiry"] == date(2027, 6, 17), "flags"]
    assert ((flags & int(Flag.EXPIRY_REJECTED)) != 0).all()


def test_an_unknown_root_is_rejected_withoutquotes(european_day):
    quotes, day = european_day
    spxo = day.expiries[day.expiries["root"] == "SPXO"].iloc[0]
    assert spxo["rejection_reason"] == "unknown_root"
    assert (quotes["root"] != "SPXO").all()


# --------------------------------------------------------------- American


@pytest.fixture(scope="module")
def american_day():
    chain = pd.concat([spx_chain(), spy_chain()], ignore_index=True)
    return iv.compute(day_inputs(chain))


def test_american_vols_are_deamericanized_back_to_sigma(american_day):
    quotes, day = american_day
    spy = quotes[quotes["ticker"] == "SPY"]
    assert (spy["exercise"] == "american").all() and (spy["forward_source"] == "carry").all()
    assert spy["implied_vol"].notna().mean() > 0.95
    priced = spy[spy["implied_vol"].notna() & (spy["log_moneyness"].abs() < 0.25)]
    # The DF comes from the SPX curve, not the exact RATE: a little slack.
    assert (priced["implied_vol"] - SPY_SIGMA).abs().max() < 0.002
    assert (priced.loc[priced["option_type"] == "put", "early_exercise_premium"] > 0).any()
    expiries = day.expiries[day.expiries["ticker"] == "SPY"]
    assert (expiries["status"] == "accepted").all()
    assert expiries["carry_forward_gap"].abs().max() < 0.5


def test_without_a_european_chain_the_curve_falls_back_to_fred():
    quotes, day = iv.compute(day_inputs(spy_chain(expiries=(date(2027, 1, 15),))))
    assert {load["curve_source"] for load in day.loads} == {"fred"}
    assert ((quotes["flags"] & int(Flag.RATE_FALLBACK)) != 0).all()
    assert (day.expiries["discount_source"] == "fred").all()


def test_an_american_ticker_without_a_close_is_rejected():
    _, day = iv.compute(day_inputs(spy_chain(expiries=(date(2027, 1, 15),)), closes={}))
    assert day.expiries.iloc[0]["rejection_reason"] == "no_spot"


# ---------------------------------------------------------- quote flags


def test_quote_flags():
    expiry = date(2027, 1, 15)
    ttm = ttm_to(expiry)
    exp = iv._Expiry(ttm, 100.0, 0.99, "european", "parity", 0, np.array([110.0]))
    group = pd.DataFrame({
        "date": DAY, "ticker": "X", "expiry": expiry, "root": "X",
        "option_type": ["call", "call", "call", "call", "put"],
        "strike": [105.0, 110.0, 120.0, 150.0, 90.0],
        "bid": [2.0, 1.0, 0.0, 0.01, 1.0],
        "ask": [2.1, 1.1, 0.3, 0.02, 2.5],
    })
    rows = iv.quote_rows(group, exp, np.array([False, True, False, False, False])).set_index("strike")["flags"]
    assert rows[105.0] == 0
    assert rows[110.0] == Flag.STALE | Flag.PARITY_OUTLIER
    assert rows[120.0] & Flag.NO_BID and rows[120.0] & Flag.WIDE_SPREAD
    assert rows[150.0] & Flag.BELOW_TWO_TICKS
    assert rows[90.0] & Flag.WIDE_SPREAD


def test_tick_sizes():
    assert list(iv.tick_size("SPXW", np.array([1.0, 5.0]))) == [0.05, 0.10]
    assert list(iv.tick_size("XSP", np.array([1.0, 5.0]))) == [0.01, 0.01]
    assert list(iv.tick_size("AAPL", np.array([1.0, 5.0]))) == [0.01, 0.05]


# ------------------------------------------------------------- dividends


def test_dividends_project_the_last_amount_at_the_last_frequency():
    projected = iv.project_dividends(SPY_DIVIDENDS, DAY, date(2027, 6, 30))
    assert [a for _, a in projected] == [1.89, 1.89, 1.89]
    assert projected[0][0] == date(2026, 9, 18) + timedelta(days=91)


def test_known_future_dividends_are_kept_and_projection_follows_them():
    history = pd.concat([SPY_DIVIDENDS, pd.DataFrame({"ex_date": [date(2026, 12, 18)], "amount": [2.05]})])
    projected = iv.project_dividends(history.reset_index(drop=True), DAY, date(2027, 3, 31))
    assert projected[0] == (date(2026, 12, 18), 2.05)
    assert projected[1][1] == 2.05


def test_a_ticker_that_stopped_paying_projects_nothing():
    old = pd.DataFrame({"ex_date": [date(2023, 3, 1), date(2023, 6, 1)], "amount": [0.5, 0.5]})
    assert iv.project_dividends(old, DAY, date(2027, 12, 31)) == []
    assert iv.project_dividends(None, DAY, date(2027, 12, 31)) == []


# ------------------------------------------------------------- the source


def test_the_source_is_computed_after_its_inputs():
    source = iv.OptionsChainIV()
    assert "options-chain-snapshot" in source.inputs
    assert source.write_mode is WriteMode.SNAPSHOT
    assert tuple(source.table.snapshot_key) == ("ticker", "date")
    with pytest.raises(TypeError):
        source.fetch(None)


def test_flag_bits_are_stable():
    """Part of the contract in docs/sources.md: a bit never changes meaning."""
    assert {f.name: f.value for f in Flag} == {
        "NO_BID": 1, "WIDE_SPREAD": 2, "BELOW_TWO_TICKS": 4, "STALE": 8, "PARITY_OUTLIER": 16,
        "EXPIRY_REJECTED": 32, "IV_FAILED": 64, "NOT_OTM": 128, "SHORT_MATURITY": 256, "RATE_FALLBACK": 512,
    }
