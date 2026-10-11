#!/usr/bin/env python3
"""Check `prices.raw_daily`, `prices.dividends` and `prices.splits` against a
fresh download: the total return rebuilt from the stored rows must be the one
of Yahoo's adjusted close.

    python scripts/verify_raw_prices.py                 # every ticker
    python scripts/verify_raw_prices.py AAPL GE SPY     # these

For each ticker and each pair of consecutive stored days, with `P` the stored
close, `D` the stored dividend and `s` the stored split ratio of the day:

    yahoo     s P_t / (P_{t-1} - s D_t) - 1      Yahoo's own adjustment
    book      s (P_t + D_t) / P_{t-1} - 1        dividend credited on the ex-date

are compared with `A_t / A_{t-1} - 1`, `A` the adjusted close just
downloaded. `yahoo` must match to rounding (Yahoo's prices are
single-precision floats, 1e-6 on a return): that is the check that no price,
dividend or split is missing or misdated. `book` is what a share-and-cash
book earns: the dividend is cash on the ex-date, where Yahoo reinvests it at
the previous close. The two differ by about (D/P)² on an ex-date, by
construction (1e-4 for a quarterly dividend, several percent for a spin-off
paid as a dividend), and the script prints by how much.

The tolerance is wider than rounding for one day: Yahoo's dividends around
the market closure of September 2001 do not quite add up to its own adjusted
close (4e-4 at worst, on 2001-09-10).

Exit status 1 when a ticker is beyond TOLERANCE on `yahoo`.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg

from data_ingest.config import dsn
from data_ingest.sources import _equity_raw_common as raw
from data_ingest.sources._yfinance_common import download_daily_ohlcv

#: Largest gap accepted between a rebuilt daily return and Yahoo's.
TOLERANCE = 1e-3


def compare(bars: pd.DataFrame, dividends: pd.DataFrame, splits: pd.DataFrame, adjusted: pd.DataFrame) -> pd.DataFrame:
    """One row per ticker: days compared, largest gap of each formula and
    the day of the largest `yahoo` gap."""
    frame = (
        bars[["ticker", "date", "close"]]
        .merge(dividends.rename(columns={"ex_date": "date"}), on=["ticker", "date"], how="left")
        .merge(splits, on=["ticker", "date"], how="left")
        .merge(adjusted, on=["ticker", "date"], how="inner")
        .sort_values(["ticker", "date"])
    )
    d, s = frame["amount"].fillna(0.0), frame["ratio"].fillna(1.0)
    by_ticker = frame.groupby("ticker")
    before, adjusted_before = by_ticker["close"].shift(1), by_ticker["adj_close"].shift(1)
    reference = frame["adj_close"] / adjusted_before - 1
    frame["yahoo"] = (s * frame["close"] / (before - s * d) - 1 - reference).abs()
    frame["book"] = (s * (frame["close"] + d) / before - 1 - reference).abs()
    frame = frame.dropna(subset=["yahoo"])
    worst = frame.loc[frame.groupby("ticker")["yahoo"].idxmax(), ["ticker", "date"]].set_index("ticker")["date"]
    out = frame.groupby("ticker").agg(days=("yahoo", "size"), yahoo=("yahoo", "max"), book=("book", "max"))
    out["worst_day"] = worst
    return out.reset_index()


def stored(tickers: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    where, params = ("WHERE ticker = ANY(%s)", (tickers,)) if tickers else ("", ())
    with psycopg.connect(dsn()) as conn:
        def read(sql: str, columns: list[str]) -> pd.DataFrame:
            return pd.DataFrame(conn.execute(sql.format(where), params).fetchall(), columns=columns)
        return (
            read("SELECT ticker, date, close FROM prices.raw_daily {}", ["ticker", "date", "close"]),
            read("SELECT ticker, ex_date, amount FROM prices.dividends {}", ["ticker", "ex_date", "amount"]),
            read("SELECT ticker, date, ratio FROM prices.splits {}", ["ticker", "date", "ratio"]),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("tickers", nargs="*", help="default: every ticker stored")
    args = parser.parse_args()

    bars, dividends, splits = stored(args.tickers)
    if bars.empty:
        print("nothing stored", file=sys.stderr)
        return 1
    tickers = sorted(bars["ticker"].unique())
    end = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
    fresh = download_daily_ohlcv(tickers, raw.HISTORY_START, end, actions=True)
    adjusted = fresh.rename(columns={"Ticker": "ticker", "Date": "date", "Adj Close": "adj_close"})
    adjusted = adjusted[["ticker", "date", "adj_close"]].dropna()

    result = compare(bars, dividends, splits, adjusted)
    failed = result[result["yahoo"] > TOLERANCE]
    print(f"{len(result)} tickers, {int(result['days'].sum())} daily returns compared")
    print(f"yahoo formula: largest gap {result['yahoo'].max():.2e}, median of the tickers' largest {result['yahoo'].median():.2e}")
    print(f"book formula:  largest gap {result['book'].max():.2e}, median of the tickers' largest {result['book'].median():.2e}")
    missing = sorted(set(tickers) - set(result["ticker"]))
    if missing:
        print(f"not on Yahoo today, not compared: {missing}")
    if not failed.empty:
        print(f"\n{len(failed)} tickers beyond {TOLERANCE:g} on the yahoo formula:")
        print(failed.sort_values("yahoo", ascending=False).to_string(index=False))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
