"""Strike coverage of one stored option-chain day, per ticker, root and expiry.

Reads `options.chain_snapshot` (and the underlying's close from
`prices.sp500_daily`) and prints, for each expiry:

- the number of strikes and of two-sided quotes (bid > 0 and ask > 0);
- the strike range as K/S;
- the strike spacing near the money;
- the median relative spread of the two-sided quotes within 2 % of the money.

Run against the production database to choose quant-modeling's primary
European surface (WP 31, D2), the result being written down in
docs/option-coverage.md:

    python scripts/measure_option_coverage.py --date 2026-11-26 --tickers ^SPX,^XSP,SPY
"""

from __future__ import annotations

import argparse
from datetime import date

import numpy as np
import pandas as pd
import psycopg

from data_ingest.config import dsn

#: The close that stands for each ticker's underlying: the index options are
#: on the S&P 500 (XSP at a tenth of it), whose close is ^GSPC's.
_UNDERLYING = {"^SPX": ("^GSPC", 1.0), "^XSP": ("^GSPC", 0.1)}


def _spot(conn: psycopg.Connection, ticker: str, day: date) -> float:
    symbol, scale = _UNDERLYING.get(ticker, (ticker, 1.0))
    row = conn.execute(
        "SELECT close FROM prices.sp500_daily WHERE ticker = %s AND date <= %s ORDER BY date DESC LIMIT 1",
        (symbol, day),
    ).fetchone()
    if row is None:
        raise SystemExit(f"no close stored for {symbol} on or before {day}")
    return float(row[0]) * scale


def coverage(chain: pd.DataFrame, spot: float) -> pd.DataFrame:
    """One row per (root, expiry) of a day's chain for one ticker."""
    chain = chain.assign(
        two_sided=(chain["bid"] > 0) & (chain["ask"] > 0),
        moneyness=chain["strike"] / spot,
    )
    chain["rel_spread"] = np.where(
        chain["two_sided"], (chain["ask"] - chain["bid"]) / ((chain["ask"] + chain["bid"]) / 2), np.nan
    )
    rows = []
    for (root, expiry), group in chain.groupby(["root", "expiry"]):
        strikes = np.sort(group["strike"].unique())
        near = strikes[np.abs(np.log(strikes / spot)) < 0.05]
        atm = group[np.abs(np.log(group["moneyness"])) < 0.02]
        rows.append({
            "root": root,
            "expiry": expiry,
            "calls": int((group["option_type"] == "call").sum()),
            "puts": int((group["option_type"] == "put").sum()),
            "two_sided": int(group["two_sided"].sum()),
            "k_over_s_min": round(float(group["moneyness"].min()), 3),
            "k_over_s_max": round(float(group["moneyness"].max()), 3),
            "atm_spacing": float(np.median(np.diff(near))) if len(near) > 1 else np.nan,
            "atm_rel_spread": round(float(atm["rel_spread"].median()), 4) if atm["two_sided"].any() else np.nan,
        })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--tickers", default="^SPX,^XSP,SPY")
    args = parser.parse_args()

    pd.set_option("display.width", 200)
    with psycopg.connect(dsn()) as conn:
        for ticker in [t.strip() for t in args.tickers.split(",") if t.strip()]:
            chain = pd.read_sql_query(  # noqa: pandas warns on a raw psycopg connection, harmlessly
                "SELECT root, expiry, option_type, strike, bid, ask FROM options.chain_snapshot "
                "WHERE ticker = %s AND date = %s",
                conn, params=(ticker, args.date),
            )
            if chain.empty:
                print(f"{ticker}: nothing stored on {args.date}\n")
                continue
            spot = _spot(conn, ticker, args.date)
            table = coverage(chain, spot)
            print(f"{ticker} on {args.date}, S = {spot:.2f}: {len(chain)} quotes, "
                  f"{int(table['two_sided'].sum())} two-sided, {len(table)} (root, expiry)")
            print(table.to_string(index=False))
            print()


if __name__ == "__main__":
    main()
