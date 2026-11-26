"""Black-76 and its inversion, parity fits, discount curves and the
de-Americanization tree (quant-modeling WP 31, §4.6)."""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from data_ingest.options import american, black, parity, rates

# ------------------------------------------------------------------ Black


def test_round_trip_to_1e_12_over_the_whole_grid():
    """Black price -> our IV -> price, over |k| <= 3 and T from one day to
    five years, wherever the price carries any time value to invert."""
    forward, discount = 100.0, 0.93
    k = np.linspace(-3.0, 3.0, 121)
    ttm = np.array([1 / 365, 7 / 365, 0.1, 0.5, 1.0, 2.0, 5.0])
    kk, tt = np.meshgrid(k, ttm)
    strike = forward * np.exp(kk)
    for vol in (0.05, 0.2, 0.5, 1.0):
        for is_call in (True, False):
            price = black.price(forward, strike, tt, vol, discount, is_call)
            iv = black.implied_vol(price, forward, strike, tt, discount, is_call)
            intrinsic = np.maximum(forward - strike, 0.0) if is_call else np.maximum(strike - forward, 0.0)
            time_value = price / discount - intrinsic
            # Where the time value is lost to rounding, there is no vol to
            # recover; everywhere else there must be one.
            invertible = (time_value > 1e-11 * np.maximum(price / discount, 1.0)) & (price > 1e-250)
            assert np.isfinite(iv[invertible]).all()
            repriced = black.price(forward, strike, tt, iv, discount, is_call)
            ok = np.isfinite(iv)
            assert np.all(np.abs(repriced[ok] - price[ok]) <= 1e-12 * np.maximum(price[ok], 1.0))
            # Within a standard deviation of the money, every quote inverts.
            assert ok[np.abs(kk) <= vol * np.sqrt(tt)].all()


def test_implied_vol_is_nan_outside_blacks_range():
    # At intrinsic, above the forward, zero, negative, missing.
    prices = np.array([10.0 * 0.95, 110.0 * 0.95 + 0.01, 0.0, -1.0, np.nan])
    iv = black.implied_vol(prices, 110.0, 100.0, 1.0, 0.95, True)
    assert np.isnan(iv).all()


def test_put_and_call_at_one_vol_satisfy_parity():
    c = black.price(105.0, 100.0, 0.7, 0.3, 0.97, True)
    p = black.price(105.0, 100.0, 0.7, 0.3, 0.97, False)
    assert c - p == pytest.approx(0.97 * (105.0 - 100.0), abs=1e-12)


def test_vega_matches_a_finite_difference():
    bump = 1e-6
    numeric = (black.price(100, 90, 1.0, 0.25 + bump, 0.95, True) - black.price(100, 90, 1.0, 0.25 - bump, 0.95, True)) / (2 * bump)
    assert black.vega(100, 90, 1.0, 0.25, 0.95) == pytest.approx(numeric, rel=1e-7)


# ----------------------------------------------------------------- parity


def synthetic_european_chain(forward, discount, ttm, strikes, vol=0.2, skew=-0.3, spread=0.02, seed=0,
                             outliers=()):
    """Calls and puts at Black prices on a skewed smile, quoted around the
    price with relative noise inside a spread; `outliers` strikes get a call
    mid moved by five spreads."""
    rng = np.random.default_rng(seed)
    rows = []
    for k in strikes:
        sigma = vol + skew * np.log(k / forward) * 0.2
        for is_call in (True, False):
            price = float(black.price(forward, k, ttm, sigma, discount, is_call))
            half = max(spread * price, 0.05) / 2.0
            mid = price + rng.uniform(-0.3, 0.3) * half
            if is_call and k in outliers:
                mid += 10.0 * half
            rows.append({"option_type": "call" if is_call else "put", "strike": float(k),
                         "bid": max(mid - half, 0.0), "ask": mid + half})
    return pd.DataFrame(rows)


def test_parity_recovers_forward_and_discount_within_their_error():
    forward, discount = 7250.0, 0.952
    chain = synthetic_european_chain(forward, discount, 1.0, np.arange(6500, 8000, 25.0))
    fit = parity.fit(chain)
    assert fit is not None
    assert abs(fit.discount - discount) < 4 * fit.discount_se + 1e-4
    assert fit.forward == pytest.approx(forward, rel=2e-4)
    assert fit.dispersion < fit.half_band


def test_parity_flags_the_outliers_and_resists_them():
    forward, discount = 7250.0, 0.952
    strikes = np.arange(6500, 8000, 25.0)
    bad = (7000.0, 7300.0, 7600.0)
    chain = synthetic_european_chain(forward, discount, 1.0, strikes, outliers=bad)
    fit = parity.fit(chain)
    assert set(bad) <= set(fit.outlier_strikes)
    assert fit.forward == pytest.approx(forward, rel=3e-4)


def test_parity_with_a_given_discount_fits_the_forward_alone():
    forward, discount = 725.0, 0.995
    chain = synthetic_european_chain(forward, discount, 0.1, np.arange(650, 800, 1.0))
    fit = parity.fit(chain, discount=discount)
    assert fit.discount == discount and fit.discount_se == 0.0
    assert fit.forward == pytest.approx(forward, abs=0.05)


def test_parity_needs_enough_two_sided_pairs():
    chain = synthetic_european_chain(100.0, 0.99, 0.5, [95.0, 100.0, 105.0])
    assert parity.fit(chain) is None
    one_sided = synthetic_european_chain(100.0, 0.99, 0.5, np.arange(80, 120, 1.0))
    one_sided.loc[one_sided["option_type"] == "put", "bid"] = 0.0
    assert parity.fit(one_sided) is None


# ------------------------------------------------------------------ rates


def test_parity_curve_keeps_reliable_nodes_and_drops_outliers():
    nodes = [(t, np.exp(-0.045 * t), 0.0001 * t, 0.2) for t in (0.1, 0.25, 0.5, 1.0, 2.0)]
    nodes.append((0.75, np.exp(-0.10 * 0.75), 0.0001, 0.2))      # far from its neighbours
    nodes.append((0.01, np.exp(-0.5 * 0.01), 0.00001, 0.2))      # too short
    nodes.append((0.3, np.exp(-0.045 * 0.3), 0.01, 0.2))         # slope too uncertain
    nodes.append((0.4, np.exp(-0.045 * 0.4), 0.0001, 2.0))       # dispersion too wide
    curve = rates.from_parity(nodes, date(2026, 11, 25))
    assert curve.source == "parity"
    assert list(np.round(curve.ttm, 6)) == [0.1, 0.25, 0.5, 1.0, 2.0]
    assert curve.rate(0.6) == pytest.approx(0.045)
    assert curve.discount(5.0) == pytest.approx(np.exp(-0.045 * 5.0))  # flat beyond the last node


def test_parity_curve_needs_three_nodes():
    assert rates.from_parity([(0.5, 0.98, 0.0001, 0.1), (1.0, 0.96, 0.0001, 0.1)], date(2026, 11, 25)) is None


def test_fred_curve_converts_to_continuous_rates():
    curve = rates.from_fred({"SOFR": (date(2026, 11, 24), 3.94), "DGS1": (date(2026, 11, 24), 4.59)})
    assert curve.source == "fred" and curve.as_of == date(2026, 11, 24)
    assert curve.rate(1 / 365) == pytest.approx(np.log1p(0.0394 / 360) * 365)
    assert curve.rate(1.0) == pytest.approx(2 * np.log1p(0.0459 / 2))
    assert rates.from_fred({}) is None


# ------------------------------------------------------------------- tree


def test_leisen_reimer_american_put_matches_the_reference():
    """S = K = 100, r = 5 %, sigma = 20 %, T = 1: 6.0904 (Leisen & Reimer
    1996, a 15,000-step binomial reference)."""
    carry = american.Carry(spot=100.0, rate=0.05)
    am, eu = american.tree_prices(carry, 1.0, [100.0], [0.2], [False], steps=401)
    assert am[0] == pytest.approx(6.0904, abs=2e-3)
    assert eu[0] == pytest.approx(black.price(carry.forward(1.0), 100.0, 1.0, 0.2, carry.discount(1.0), False), abs=1e-4)


def test_an_american_call_without_dividends_is_european():
    carry = american.Carry(spot=100.0, rate=0.05)
    am, eu = american.tree_prices(carry, 1.0, [80.0, 100.0, 120.0], [0.3] * 3, [True] * 3)
    assert np.allclose(am, eu, atol=1e-12)


@pytest.mark.parametrize("carry", [
    american.Carry(spot=700.0, rate=0.045, dividends=((0.06, 1.9), (0.31, 1.8), (0.56, 1.9), (0.81, 1.9))),
    american.Carry(spot=700.0, rate=0.045, dividend_yield=0.011),
])
def test_deamericanization_recovers_sigma_and_blacks_price(carry):
    """Synthetic American prices at a known sigma and known dividends: the
    de-Americanization finds sigma again, and its European price is Black's."""
    ttm, sigma = 1.06, 0.21
    strike = np.arange(500.0, 900.0, 10.0)
    forward = carry.forward(ttm)
    is_call = strike >= forward
    am, _ = american.tree_prices(carry, ttm, strike, np.full(strike.shape, sigma), is_call)
    result = american.deamericanize(am, strike, is_call, carry, ttm)
    assert result.converged.all()
    assert np.abs(result.implied_vol - sigma).max() < 1e-5
    reference = black.price(forward, strike, ttm, sigma, carry.discount(ttm), is_call)
    assert np.abs(result.price_european - reference).max() < 2e-3
    assert (result.early_exercise_premium >= 0).all()
    assert result.early_exercise_premium[~is_call].max() > 0.1  # puts carry a premium at 4.5 %
