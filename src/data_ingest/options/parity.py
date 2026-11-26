"""Forward and discount factor of one European expiry, from put-call parity.

For a European call and put at one strike, C - P = DF F - DF K: a line in K
whose slope is -DF and whose intercept is DF F. The regression runs on the
strikes where both sides have a two-sided quote, within a band around the
money, weighted by 1 / (spread_C^2 + spread_P^2), with Huber's loss against
the outliers (Cohort, Corbetta, Martini & Laachir 2018, §4.1: "robust linear
regression leveraging the put-call parity for mid prices").

The slope is poorly determined on a short expiry or a narrow strike range:
on a two-day SPXW expiry, a 0.002 error on DF is a 40 % error on the implied
rate. So the fit reports the discount's standard error, and the caller can
refit with the discount fixed (`discount=`), from a curve built on the
expiries whose own slope is reliable. The fit then estimates F alone, as the
Huber location of K + (C - P) / DF.

Two checks follow:
- the bid/ask parity bounds, bid_C - ask_P <= DF (F - K) <= ask_C - bid_P
  (Madan et al. 2019), for every pair;
- the residuals' robust dispersion, the expiry's quality.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

#: Band around the money the regression uses, in |ln K / F0|.
PARITY_BAND = 0.10
#: Fewest two-sided pairs a fit needs.
MIN_PAIRS = 5
#: Huber's threshold, in robust standard deviations of the residuals.
HUBER_K = 1.345
#: A pair whose residual exceeds this many robust standard deviations, or
#: that breaks the bid/ask bounds, is a parity outlier.
OUTLIER_SIGMAS = 4.0
#: The plausible range of a discount factor: outside it, the fit is wrong.
DISCOUNT_RANGE = (0.5, 1.02)


@dataclass(frozen=True)
class ParityFit:
    forward: float
    discount: float
    #: Standard error of `discount`; 0 when it was given rather than fitted.
    discount_se: float
    n_pairs: int
    #: Robust standard deviation of the residuals of C - P, in price units.
    dispersion: float
    #: Median half-width of the pairs' bid/ask parity band, same units: the
    #: dispersion compares to it.
    half_band: float
    #: Strikes of the pairs flagged as parity outliers.
    outlier_strikes: np.ndarray
    #: Pairs outside the bid/ask parity bounds.
    bound_violations: int


def pairs(chain: pd.DataFrame) -> pd.DataFrame:
    """One row per strike where both sides have a two-sided quote: the
    columns bid_c, ask_c, bid_p, ask_p, indexed by strike."""
    quoted = chain[(chain["bid"] > 0) & (chain["ask"] > 0) & (chain["ask"] >= chain["bid"])]
    calls = quoted[quoted["option_type"] == "call"].set_index("strike")[["bid", "ask"]]
    puts = quoted[quoted["option_type"] == "put"].set_index("strike")[["bid", "ask"]]
    both = calls.add_suffix("_c").join(puts.add_suffix("_p"), how="inner")
    return both.sort_index()


def _robust_sigma(residuals: np.ndarray) -> float:
    mad = np.median(np.abs(residuals - np.median(residuals)))
    return 1.4826 * mad


def _huber(design: np.ndarray, y: np.ndarray, weights: np.ndarray, iterations: int = 50):
    """Weighted Huber regression of y on the design's columns by iteratively
    reweighted least squares, the scale re-estimated each pass from the
    weighted residuals' MAD. Returns (coefficients, their covariance,
    residuals)."""
    robust = np.ones_like(y)
    coef = np.zeros(design.shape[1])
    sqrt_w = np.sqrt(weights)
    for _ in range(iterations):
        w = weights * robust
        sw = np.sqrt(w)
        new, *_ = np.linalg.lstsq(design * sw[:, None], y * sw, rcond=None)
        residuals = y - design @ new
        scaled = residuals * sqrt_w
        scale = _robust_sigma(scaled)
        if scale <= 0:
            coef = new
            break
        u = np.abs(scaled) / (HUBER_K * scale)
        robust = np.where(u <= 1.0, 1.0, 1.0 / np.maximum(u, 1e-300))
        if np.allclose(new, coef, rtol=1e-12, atol=1e-12):
            coef = new
            break
        coef = new
    residuals = y - design @ coef
    # Sandwich-free WLS covariance on the final weights: enough to tell a
    # well-determined slope from a poorly determined one.
    w = weights * robust
    dof = max(len(y) - design.shape[1], 1)
    s2 = float(np.sum(w * residuals ** 2) / dof)
    try:
        cov = s2 * np.linalg.inv(design.T @ (design * w[:, None]))
    except np.linalg.LinAlgError:
        cov = np.full((design.shape[1], design.shape[1]), np.inf)
    return coef, cov, residuals


def fit(chain: pd.DataFrame, discount: Optional[float] = None) -> Optional[ParityFit]:
    """The parity fit of one expiry's quotes (columns option_type, strike,
    bid, ask), or None when fewer than MIN_PAIRS pairs can be used. With
    `discount`, DF is taken as given and only F is fitted."""
    both = pairs(chain)
    if len(both) < MIN_PAIRS:
        return None
    strike = both.index.to_numpy(dtype=float)
    mid_diff = ((both["bid_c"] + both["ask_c"]) - (both["bid_p"] + both["ask_p"])).to_numpy() / 2.0

    # First guess of the forward: where C - P crosses zero, DF taken as 1.
    nearest = int(np.argmin(np.abs(mid_diff)))
    forward0 = strike[nearest] + mid_diff[nearest]
    if forward0 <= 0:
        return None
    band = np.abs(np.log(strike / forward0)) <= PARITY_BAND
    if band.sum() < MIN_PAIRS:
        return None

    x, y = strike[band], mid_diff[band]
    spread_c = (both["ask_c"] - both["bid_c"]).to_numpy()[band]
    spread_p = (both["ask_p"] - both["bid_p"]).to_numpy()[band]
    # A zero spread (a locked quote) would take all the weight: floor it at
    # the smallest tick any of these products trades in.
    weights = 1.0 / np.maximum(spread_c ** 2 + spread_p ** 2, 0.01 ** 2)

    if discount is None:
        coef, cov, residuals = _huber(np.column_stack([np.ones_like(x), x]), y, weights)
        discount, discount_se = float(-coef[1]), float(np.sqrt(max(cov[1, 1], 0.0)))
        if not (DISCOUNT_RANGE[0] <= discount <= DISCOUNT_RANGE[1]):
            return ParityFit(np.nan, discount, discount_se, int(band.sum()), np.nan, np.nan, np.array([]), 0)
        forward = coef[0] / discount
    else:
        # DF F = y + DF K: a location problem in DF F.
        coef, _, residuals = _huber(np.ones((len(x), 1)), y + discount * x, weights)
        forward, discount_se = float(coef[0] / discount), 0.0

    # The bounds and the outliers, on every pair of the band.
    lower = (both["bid_c"] - both["ask_p"]).to_numpy()[band]
    upper = (both["ask_c"] - both["bid_p"]).to_numpy()[band]
    fitted = discount * (forward - x)
    outside = (fitted < lower) | (fitted > upper)
    dispersion = _robust_sigma(residuals)
    far = np.abs(residuals - np.median(residuals)) > OUTLIER_SIGMAS * max(dispersion, 1e-12)
    half_band = float(np.median((upper - lower) / 2.0))

    return ParityFit(
        forward=float(forward),
        discount=float(discount),
        discount_se=discount_se,
        n_pairs=int(band.sum()),
        dispersion=float(dispersion),
        half_band=half_band,
        outlier_strikes=x[outside | far],
        bound_violations=int(outside.sum()),
    )
