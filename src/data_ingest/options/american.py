"""De-Americanization of American option quotes on a Leisen-Reimer tree.

What OptionMetrics does on a Cox-Ross-Rubinstein tree, with the same aim:
turn an American quote into the European price Black's formula can invert.

    early_exercise_premium(sigma) = American_tree(sigma) - European_tree(sigma)
    price_european = mid - early_exercise_premium(sigma*)
    sigma* = Black^-1(price_european)

sigma* is found as the fixed point of sigma -> Black^-1(mid - premium(sigma)),
started at Black^-1(mid). At the fixed point the American tree reprices the
mid at sigma* up to the tree's own European bias, and taking the premium as
a difference of two prices on one tree cancels that bias to first order (a
control variate: Burkovska, Glau, Mahlstedt & Wohlmuth 2018 measure the
method's error). The premium varies slowly with sigma, so the iteration
contracts in a few steps.

The tree is Leisen-Reimer's (1996), whose price is smooth in sigma and
converges in O(1/n^2), on an odd number of steps. Dividends:
- a continuous yield `q` (an index ETF, from prices.dividend_yields);
- or discrete cash dividends (a stock), in the escrowed model: the tree runs
  on S* = S - PV(dividends to the expiry), and a node's exercise value adds
  back the present value of the dividends still to come, so that a call is
  exercised at the right dates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence, Tuple

import numpy as np

from . import black

#: Tree steps (odd, as Leisen-Reimer requires).
STEPS = 201
#: The fixed point stops when sigma moves less than this...
TOLERANCE = 1e-9
#: ...or after this many passes, the quote being flagged when it has not.
MAX_ITERATIONS = 30


@dataclass(frozen=True)
class Carry:
    """What drives one expiry's forward on an American underlying."""

    spot: float
    #: Continuously compounded rate to the expiry, -ln(DF) / T.
    rate: float
    #: Continuous dividend yield.
    dividend_yield: float = 0.0
    #: Discrete cash dividends as (time in years, amount), within (0, T].
    dividends: Sequence[Tuple[float, float]] = field(default_factory=tuple)

    def dividends_pv(self, ttm: float, after: float = 0.0) -> float:
        """Present value at time `after` of the dividends paid in (after, ttm]."""
        return float(sum(a * np.exp(-self.rate * (t - after)) for t, a in self.dividends if after < t <= ttm))

    def forward(self, ttm: float) -> float:
        escrowed = self.spot - self.dividends_pv(ttm)
        return escrowed * np.exp((self.rate - self.dividend_yield) * ttm)

    def discount(self, ttm: float) -> float:
        return float(np.exp(-self.rate * ttm))


def _peizer_pratt(z: np.ndarray, n: int) -> np.ndarray:
    """Peizer-Pratt method 2: the binomial probability matching N(z)."""
    x = z / (n + 1.0 / 3.0 + 0.1 / (n + 1.0))
    return 0.5 + np.sign(z) * np.sqrt(np.maximum(0.25 - 0.25 * np.exp(-x * x * (n + 1.0 / 6.0)), 0.0))


def tree_prices(carry: Carry, ttm: float, strike, vol, is_call, steps: int = STEPS):
    """American and European prices of each quote of one expiry on one
    Leisen-Reimer tree, vectorised over the quotes (strike, vol, is_call
    arrays of one length). Returns (american, european)."""
    strike = np.asarray(strike, dtype=float)
    vol = np.asarray(vol, dtype=float)
    is_call = np.asarray(is_call, dtype=bool)
    if steps % 2 == 0:
        steps += 1
    dt = ttm / steps
    r, q = carry.rate, carry.dividend_yield
    escrowed = carry.spot - carry.dividends_pv(ttm)

    sd = vol * np.sqrt(ttm)
    d1 = (np.log(escrowed / strike) + (r - q + 0.5 * vol * vol) * ttm) / sd
    d2 = d1 - sd
    p = _peizer_pratt(d2, steps)
    p_star = _peizer_pratt(d1, steps)
    growth = np.exp((r - q) * dt)
    up = growth * p_star / p
    down = (growth - p * up) / (1.0 - p)
    log_up, log_down = np.log(up), np.log(down)
    discount = np.exp(-r * dt)

    def node_spot(i: int) -> np.ndarray:
        """Escrowed spot at step i, shape (quotes, i + 1)."""
        j = np.arange(i + 1)
        return escrowed * np.exp(i * log_down[:, None] + j[None, :] * (log_up - log_down)[:, None])

    def payoff(spot: np.ndarray) -> np.ndarray:
        k = strike[:, None]
        return np.where(is_call[:, None], np.maximum(spot - k, 0.0), np.maximum(k - spot, 0.0))

    terminal = payoff(node_spot(steps))
    american, european = terminal, terminal.copy()
    p_col = p[:, None]
    for i in range(steps - 1, -1, -1):
        american = discount * (p_col * american[:, 1:] + (1.0 - p_col) * american[:, :-1])
        european = discount * (p_col * european[:, 1:] + (1.0 - p_col) * european[:, :-1])
        # Exercise on the full spot: the escrowed spot plus the dividends
        # still to come.
        spot = node_spot(i) + carry.dividends_pv(ttm, after=i * dt)
        american = np.maximum(american, payoff(spot))
    return american[:, 0], european[:, 0]


@dataclass(frozen=True)
class Deamericanized:
    price_european: np.ndarray
    early_exercise_premium: np.ndarray
    implied_vol: np.ndarray
    converged: np.ndarray


def deamericanize(mid, strike, is_call, carry: Carry, ttm: float, steps: int = STEPS) -> Deamericanized:
    """The European-equivalent price and implied vol of American mids of one
    expiry, by the fixed point described in the module docstring."""
    mid = np.asarray(mid, dtype=float)
    strike = np.asarray(strike, dtype=float)
    is_call = np.asarray(is_call, dtype=bool)
    forward, discount = carry.forward(ttm), carry.discount(ttm)

    vol = black.implied_vol(mid, forward, strike, ttm, discount, is_call)
    premium = np.zeros_like(mid)
    converged = np.zeros(mid.shape, dtype=bool)
    live = np.isfinite(vol)
    for _ in range(MAX_ITERATIONS):
        if not live.any():
            break
        idx = np.flatnonzero(live)
        american, european = tree_prices(carry, ttm, strike[idx], vol[idx], is_call[idx], steps)
        premium[idx] = np.maximum(american - european, 0.0)
        new = black.implied_vol(mid[idx] - premium[idx], forward, strike[idx], ttm, discount, is_call[idx])
        done = np.isfinite(new) & (np.abs(new - vol[idx]) < TOLERANCE)
        failed = ~np.isfinite(new)
        vol[idx] = new
        converged[idx[done]] = True
        live[idx[done | failed]] = False
    vol[~converged] = np.nan
    return Deamericanized(
        price_european=mid - premium,
        early_exercise_premium=premium,
        implied_vol=vol,
        converged=converged,
    )
