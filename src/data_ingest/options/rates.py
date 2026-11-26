"""The discount curve the implied vols use, one per day.

Two ways to build it, in order of preference (quant-modeling WP 31, D3):

1. **From the day's European parity** (`from_parity`): the discount factors
   the SPX, SPXW and XSP expiries imply, which are the option market's own
   financing rate (its box-spread rate) and valid for every USD underlying.
   Only the expiries whose slope is reliable are nodes: see `NODE_*`.
2. **Fallback, from FRED** (`from_fred`), when no European chain gives
   enough nodes that day (every day stored before SPX was captured): SOFR at
   the overnight point, then the Treasury constant-maturity yields from one
   month to thirty years. A curve directly read, no bootstrap. It sits below
   the box rate (SOFR 3.94 % against about 4.4-5.0 % implied by SPX on
   2026-11-25), so a quote priced on it is flagged.

The curve holds continuously compounded zero rates at its nodes and
interpolates them linearly in T, flat beyond the first and last node.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Sequence, Tuple

import numpy as np

#: A parity expiry is a node of the curve when it is at least this long...
NODE_MIN_TTM = 14.0 / 365.0
#: ...its implied rate's standard error is at most this...
NODE_MAX_RATE_SE = 0.0025
#: ...and its residual dispersion at most this fraction of its parity band.
NODE_MAX_DISPERSION = 0.5
#: A node further than this from the median of its neighbours is dropped.
NODE_MAX_DEVIATION = 0.0050
#: Fewest nodes a parity curve needs; below, the FRED fallback is used.
MIN_NODES = 3

#: FRED series of the fallback, with their maturity in years. SOFR is an
#: overnight ACT/360 rate; the DGS series are bond-equivalent (semiannual)
#: par yields, close enough to zero rates for a fallback.
FRED_NODES: Dict[str, float] = {
    "SOFR": 1.0 / 365.0,
    "DGS1MO": 1.0 / 12.0,
    "DGS3MO": 0.25,
    "DGS6MO": 0.5,
    "DGS1": 1.0,
    "DGS2": 2.0,
    "DGS3": 3.0,
    "DGS5": 5.0,
    "DGS7": 7.0,
    "DGS10": 10.0,
    "DGS20": 20.0,
    "DGS30": 30.0,
}


@dataclass(frozen=True)
class DiscountCurve:
    #: "parity" or "fred".
    source: str
    ttm: np.ndarray
    zero_rate: np.ndarray
    #: The latest date of the inputs the curve was built from.
    as_of: Optional[date] = None

    def rate(self, ttm) -> np.ndarray:
        return np.interp(np.asarray(ttm, dtype=float), self.ttm, self.zero_rate)

    def discount(self, ttm) -> np.ndarray:
        ttm = np.asarray(ttm, dtype=float)
        return np.exp(-self.rate(ttm) * ttm)


def _clean_nodes(ttm: np.ndarray, rate: np.ndarray) -> np.ndarray:
    """Mask of the nodes within NODE_MAX_DEVIATION of the median of their
    neighbours (two on each side, themselves included)."""
    keep = np.ones(len(ttm), dtype=bool)
    for i in range(len(ttm)):
        lo, hi = max(0, i - 2), min(len(ttm), i + 3)
        keep[i] = abs(rate[i] - np.median(rate[lo:hi])) <= NODE_MAX_DEVIATION
    return keep


def from_parity(nodes: Sequence[Tuple[float, float, float, float]], as_of: date) -> Optional[DiscountCurve]:
    """The curve of the day's European parity fits, from (ttm, discount,
    discount standard error, dispersion over the parity band) per expiry, or
    None when too few of them qualify as nodes."""
    rows = []
    for ttm, discount, discount_se, dispersion_ratio in nodes:
        if not (np.isfinite(discount) and discount > 0 and ttm >= NODE_MIN_TTM):
            continue
        if not np.isfinite(dispersion_ratio) or dispersion_ratio > NODE_MAX_DISPERSION:
            continue
        rate = -np.log(discount) / ttm
        rate_se = discount_se / discount / ttm
        if not np.isfinite(rate_se) or rate_se > NODE_MAX_RATE_SE:
            continue
        rows.append((ttm, rate, rate_se))
    if len(rows) < MIN_NODES:
        return None
    rows.sort()
    ttm = np.array([r[0] for r in rows])
    rate = np.array([r[1] for r in rows])
    se = np.array([r[2] for r in rows])
    keep = _clean_nodes(ttm, rate)
    ttm, rate, se = ttm[keep], rate[keep], se[keep]
    if len(ttm) < MIN_NODES:
        return None
    # Several expiries at one maturity (SPX and XSP on a monthly date):
    # their inverse-variance mean.
    unique, inverse = np.unique(np.round(ttm, 8), return_inverse=True)
    weights = 1.0 / np.maximum(se, 1e-6) ** 2
    merged = np.bincount(inverse, weights * rate) / np.bincount(inverse, weights)
    return DiscountCurve("parity", unique, merged, as_of)


def from_fred(values: Dict[str, Tuple[date, float]]) -> Optional[DiscountCurve]:
    """The fallback curve from the latest value (date, percent) of each
    FRED series of FRED_NODES, or None when none is available."""
    rows = []
    for series, ttm in FRED_NODES.items():
        if series not in values or values[series][1] is None or not np.isfinite(values[series][1]):
            continue
        quoted = values[series][1] / 100.0
        if series == "SOFR":
            continuous = np.log1p(quoted / 360.0) * 365.0
        else:
            continuous = 2.0 * np.log1p(quoted / 2.0)
        rows.append((ttm, continuous))
    if not rows:
        return None
    rows.sort()
    as_of = max(values[s][0] for s in FRED_NODES if s in values)
    return DiscountCurve("fred", np.array([r[0] for r in rows]), np.array([r[1] for r in rows]), as_of)
