"""Black-76 on the forward, and its inversion by Jäckel's "Let's Be Rational".

Every price here is the option's price today, `DF * E[payoff]`, against the
forward F to the expiry and the discount factor DF to the settlement:

    C = DF [F N(d1) - K N(d2)],  P = DF [K N(-d2) - F N(-d1)],
    d1 = (ln(F / K) + sigma^2 T / 2) / (sigma sqrt(T)),  d2 = d1 - sigma sqrt(T).

The implied vol is the sigma this formula maps to an observed price: a
quoting convention, which needs F and DF and no model of the dynamics.
"""

from __future__ import annotations

import math

import numpy as np
from py_lets_be_rational import implied_volatility_from_a_transformed_rational_guess as _lbr_implied_vol
from scipy.special import ndtr


def _d1_d2(forward, strike, vol, ttm):
    sd = vol * np.sqrt(ttm)
    d1 = (np.log(forward / strike) + 0.5 * sd * sd) / sd
    return d1, d1 - sd


def price(forward, strike, ttm, vol, discount, is_call):
    """Black-76 price, vectorised over every argument."""
    forward, strike, ttm, vol, discount, is_call = np.broadcast_arrays(
        *(np.asarray(a, dtype=float) for a in (forward, strike, ttm, vol, discount)), np.asarray(is_call, dtype=bool)
    )
    d1, d2 = _d1_d2(forward, strike, vol, ttm)
    call = forward * ndtr(d1) - strike * ndtr(d2)
    put = strike * ndtr(-d2) - forward * ndtr(-d1)
    return discount * np.where(is_call, call, put)


def vega(forward, strike, ttm, vol, discount):
    """dPrice/dsigma, the same for a call and a put."""
    forward, strike, ttm, vol, discount = (np.asarray(a, dtype=float) for a in (forward, strike, ttm, vol, discount))
    d1, _ = _d1_d2(forward, strike, vol, ttm)
    return discount * forward * np.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi) * np.sqrt(ttm)


def implied_vol(option_price, forward, strike, ttm, discount, is_call) -> np.ndarray:
    """The Black-76 vol of each price, NaN where there is none.

    There is none when the undiscounted price is outside Black's range, at or
    below the intrinsic value max(F - K, 0) (calls) or above F (calls) or K
    (puts), or when an input is missing. Let's Be Rational reaches machine
    precision in two iterations everywhere else, wings included (Jäckel
    2015), so no tolerance or fallback is needed.
    """
    args = np.broadcast_arrays(
        *(np.asarray(a, dtype=float) for a in (option_price, forward, strike, ttm, discount)),
        np.asarray(is_call, dtype=bool),
    )
    out = np.full(args[0].shape, np.nan)
    for i, (p, f, k, t, df, call) in enumerate(zip(*(a.ravel() for a in args))):
        if not (np.isfinite(p) and np.isfinite(f) and np.isfinite(k) and np.isfinite(t) and np.isfinite(df)):
            continue
        if p <= 0.0 or f <= 0.0 or k <= 0.0 or t <= 0.0 or df <= 0.0:
            continue
        undiscounted = p / df
        intrinsic = max(f - k, 0.0) if call else max(k - f, 0.0)
        upper = f if call else k
        if not (intrinsic < undiscounted < upper):
            continue
        vol = _lbr_implied_vol(undiscounted, f, k, t, 1 if call else -1)
        # Outside its domain the library returns a sentinel (+/-DBL_MAX,
        # a negative value) rather than raising.
        if np.isfinite(vol) and 0.0 < vol < 100.0:
            out.flat[i] = vol
    return out
