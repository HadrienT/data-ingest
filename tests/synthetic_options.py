"""Synthetic option-chain days for the options-chain-iv tests: a European
SPX chain at a known forward and discount on a skewed smile, and an American
SPY chain whose early exercise comes from the tree at a known sigma."""

from datetime import date

import numpy as np
import pandas as pd

from data_ingest.options import american, black
from data_ingest.sources import options_chain_iv as iv

DAY = date(2026, 11, 25)
RATE = 0.048
SPOT = 7200.0
SPY_SPOT = 717.0


def smile(k):
    return 0.18 - 0.25 * k + 0.4 * k * k


def quotes(ticker, root, expiry, forward, discount, ttm, strikes, spread=0.01, seed=0, noise=0.3, price=None):
    """Two-sided quotes around a price per (strike, side): Black on the
    smile by default, or `price(strike, is_call)`."""
    rng = np.random.default_rng(seed)
    rows = []
    for k in strikes:
        for is_call in (True, False):
            value = price(k, is_call) if price else float(
                black.price(forward, k, ttm, smile(np.log(k / forward)), discount, is_call))
            # A cheap option still has a positive bid, so its mid is its value.
            half = min(max(spread * value, 0.05) / 2.0, 0.5 * value)
            mid = value + rng.uniform(-noise, noise) * half
            rows.append({"date": DAY, "ticker": ticker, "expiry": expiry, "root": root,
                         "option_type": "call" if is_call else "put", "strike": float(k),
                         "bid": max(mid - half, 0.0), "ask": mid + half, "last_trade_date": pd.NaT})
    return pd.DataFrame(rows)


def ttm_to(expiry, settlement="pm"):
    return iv.years_between(iv.valuation_time(DAY), iv.settlement_time(expiry, settlement))


SPX_EXPIRIES = [date(2027, 1, 15), date(2027, 3, 19), date(2027, 6, 17), date(2027, 12, 17), date(2028, 12, 15)]


def spx_chain(noisy_expiry=None):
    frames = []
    for i, expiry in enumerate(SPX_EXPIRIES):
        ttm = ttm_to(expiry, "am")
        forward, discount = SPOT * np.exp((RATE - 0.011) * ttm), np.exp(-RATE * ttm)
        strikes = np.arange(round(forward * 0.6, -1), forward * 1.4, 25.0)
        noise = 30.0 if expiry == noisy_expiry else 0.3
        frames.append(quotes("^SPX", "SPX", expiry, forward, discount, ttm, strikes, seed=i, noise=noise))
    # A root whose settlement is not known.
    frames.append(quotes("^SPX", "SPXO", date(2027, 1, 20), SPOT, 0.99, 0.15, np.arange(7000, 7400, 25.0)))
    return pd.concat(frames, ignore_index=True)


SPY_DIVIDENDS = pd.DataFrame({
    "ex_date": [date(2025, 12, 19), date(2026, 3, 20), date(2026, 6, 18), date(2026, 9, 18)],
    "amount": [1.99, 1.80, 1.90, 1.89],
})
SPY_SIGMA = 0.2


def spy_chain(expiries=(date(2027, 1, 15), date(2027, 6, 17), date(2027, 12, 17))):
    frames = []
    session = iv.valuation_time(DAY).tz_convert(iv.NEW_YORK).date()
    for i, expiry in enumerate(expiries):
        ttm = ttm_to(expiry)
        cash = iv.project_dividends(SPY_DIVIDENDS, session, expiry)
        carry = american.Carry(SPY_SPOT, RATE, dividends=tuple(((d - session).days / 365.0, a) for d, a in cash))
        strikes = np.arange(560.0, 880.0, 5.0)

        def price(k, is_call, carry=carry, ttm=ttm):
            # The European price is Black's, the early exercise the tree's:
            # what the de-Americanization assumes, so sigma must come back.
            am, eu = american.tree_prices(carry, ttm, [k], [SPY_SIGMA], [is_call])
            european = black.price(carry.forward(ttm), k, ttm, SPY_SIGMA, carry.discount(ttm), is_call)
            return float(european + am[0] - eu[0])

        frames.append(quotes("SPY", "SPY", expiry, None, None, ttm, strikes, seed=10 + i, noise=0.0, price=price))
    return pd.concat(frames, ignore_index=True)


def day_inputs(chain, **overrides):
    values = dict(
        day=DAY, chain=chain, stale=np.zeros(len(chain), dtype=bool), valuation=iv.valuation_time(DAY),
        closes={"^GSPC": SPOT, "SPY": SPY_SPOT}, dividends={"SPY": SPY_DIVIDENDS},
        fred={"SOFR": (DAY, 3.94), "DGS1": (DAY, 4.59), "DGS2": (DAY, 4.40)},
    )
    values.update(overrides)
    return iv.DayInputs(**values)
