"""Regression on real data: SPY's stored chain of 2026-11-25 (WP 31, §4.6).

Runs wherever that day is stored, the production database or a copy of it,
and is skipped elsewhere (CI's Postgres is empty). Nothing is written: the
day is computed in memory.

What it checks, and where it departs from WP 31's §4.6 as written:
- call and put vols agree near the forward: the median gap within
  |ln K/F| < 0.02 stays under half a vol point;
- the one-year ATM vol is about 20 %. §4.6 expected 21-23 %, read from the
  American mids at an assumed forward. De-Americanized at the market's
  forward, the European-equivalent vol is lower: 20.2 % on this day (FRED
  rates), against 19.6 % on SPX's own European chain;
- the carry forward and the de-Americanized parity forward agree to under
  40 bp a year of carry;
- the end-of-quarter expiries are **not** rejected: their parity, on the
  de-Americanized prices near the money, is as consistent as the
  monthlies'. Their defect is a share of aberrant quotes, mostly ITM calls,
  which the OTM selection leaves out; the pairs that break the parity
  bounds are flagged `parity_outlier`.
"""

from datetime import date

import numpy as np
import psycopg
import pytest

from data_ingest.config import dsn
from data_ingest.sources import options_chain_iv as iv

pytestmark = pytest.mark.postgres

DAY = date(2026, 11, 25)


@pytest.fixture(scope="module")
def spy():
    try:
        conn = psycopg.connect(dsn(), connect_timeout=3)
    except Exception as exc:
        pytest.skip(f"No Postgres reachable at {dsn()}: {exc}")
    with conn:
        stored = conn.execute("SELECT to_regclass('options.chain_snapshot')").fetchone()[0]
        if stored is None or conn.execute(
            "SELECT count(*) FROM options.chain_snapshot WHERE ticker = 'SPY' AND date = %s", (DAY,)
        ).fetchone()[0] == 0:
            pytest.skip(f"SPY's chain of {DAY} is not stored here")
        inputs = iv.load_inputs(conn, DAY)
    inputs.chain = inputs.chain[inputs.chain["ticker"] == "SPY"].reset_index(drop=True)
    inputs.stale = np.zeros(len(inputs.chain), dtype=bool)
    quotes, day = iv.compute(inputs)
    return quotes, day.expiries


def _gaps(quotes):
    near = quotes[(quotes["log_moneyness"].abs() < iv.BOTH_SIDES_BAND) & (quotes["ttm"] > 0.05)]
    pairs = near.pivot_table(index=["expiry", "strike"], columns="option_type", values="implied_vol").dropna()
    return (pairs["call"] - pairs["put"]).groupby(level=0).median()


def test_calls_and_puts_agree_near_the_forward(spy):
    quotes, _ = spy
    gaps = _gaps(quotes)
    assert len(gaps) >= 15
    assert gaps.abs().median() < 0.005
    assert gaps.abs().max() < 0.012


def test_the_one_year_atm_vol(spy):
    quotes, _ = spy
    expiry = quotes.loc[quotes["expiry"] == date(2027, 12, 17)]
    used = expiry[(expiry["flags"] & int(iv.Flag.NOT_OTM | iv.Flag.IV_FAILED)) == 0].sort_values("log_moneyness")
    atm = float(np.interp(0.0, used["log_moneyness"], used["implied_vol"]))
    assert 0.19 <= atm <= 0.215


def test_every_month_is_kept_and_its_carry_matches_its_parity(spy):
    _, expiries = spy
    long = expiries[expiries["ttm"] > 0.05]
    assert (long["status"] == "accepted").all()
    # The gap as an error on the carry r - q: under 40 bp a year (FRED's
    # rates on this day; 0.3 %/yr at two years).
    carry_error = (long["carry_forward_gap"] / long["forward"]).abs() / long["ttm"]
    assert carry_error.max() < 0.004
    for quarter_end in (date(2027, 3, 31), date(2027, 6, 30), date(2027, 9, 30)):
        assert quarter_end in set(long["expiry"])
