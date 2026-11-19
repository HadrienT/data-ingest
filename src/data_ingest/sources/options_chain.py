"""Daily snapshot of the full option chain, US equities, from yfinance.

Stage 1 only: this captures what yfinance reports for each (ticker, expiry,
strike, side) quote, unmodified. No re-inversion of a missing or implausible
implied_volatility, no bid/ask cleaning, no mid/TTM computation -- those are
derived quantities that belong to the consumer (quant-modeling's RawVolSurface,
in C++), which also has the spot price and rate a re-inversion needs. Mirrors
the "no cleaning happens here" split quant-modeling's own fetcher.py already
draws between fetching and cleaning.

Unlike sp500-prices, yfinance exposes no *history* of past option chains --
only the chain live at fetch time. So `window.full` and `--since` cannot
backfill anything; every run just fetches today's chain, same as a routine
run. Running this daily is what builds a history at all: each row is stamped
with the day it was captured (`date`), and a calibration that wants last
Tuesday's surface reads that day's rows back from storage rather than asking
yfinance, which no longer has them.

Universe: a small, fixed set of liquid, options-heavy underlyings by default,
overridable via OPTIONS_CHAIN_TICKERS -- deliberately *not* the full 519-name
sp500-prices universe. Every ticker's full chain is one call for the list of
expirations plus one more per expiration (a liquid single-stock name easily
has 15-20), so scaling this to hundreds of tickers multiplies out to
thousands of requests a day against an unofficial, rate-limited endpoint.
Start small and prove it holds up before widening the list.

Load registry: next to the chain, `<schema>.chain_snapshot_loads` holds one
row per stored (ticker, date) -- when it was last written, how many rows it
has and a checksum of them -- updated in the same transaction as the chain.
A consumer reads that one row to know whether a snapshot changed instead of
re-reading thousands of quotes. Both figures are computed by Postgres from
the stored rows, not from the frame: an upsert leaves in place the contracts a
re-run no longer lists, and the checksum has to describe them too.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import List, Optional

import numpy as np
import pandas as pd
import psycopg
import yfinance as yf
from psycopg import sql

from ..core.source import Source, Window
from ..core.spec import Column, TableSpec, WriteMode

logger = logging.getLogger("data_ingest.options_chain")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5

#: Liquid, options-heavy names: broad index/sector ETFs plus mega-cap single
#: names, chosen so a calibrated surface actually has enough strikes and
#: maturities to be worth fitting. Override with a comma-separated
#: OPTIONS_CHAIN_TICKERS to widen or replace this.
DEFAULT_TICKERS = [
    "SPY", "QQQ", "IWM",
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
]

#: yfinance's option_chain() column names -> the table's columns.
_RAW_TO_STORED = {
    "strike": "strike",
    "bid": "bid",
    "ask": "ask",
    "lastPrice": "last_price",
    "volume": "volume",
    "openInterest": "open_interest",
    "impliedVolatility": "implied_volatility",
}

#: Name of the load registry, in the chain's schema.
REGISTRY_TABLE = "chain_snapshot_loads"

#: The text whose md5 is the registry's checksum: every stored row of one
#: (ticker, date) as a Postgres row literal of these columns, joined by "|" in
#: primary-key order. Part of the contract in docs/sources.md: changing it
#: changes every checksum.
_CHECKSUM_COLUMNS = (
    "expiry", "option_type", "strike", "bid", "ask", "last_price",
    "volume", "open_interest", "implied_volatility",
)


class OptionsChainSnapshot(Source):
    name = "options-chain-snapshot"
    description = "Daily snapshot of the full option chain, US equities (yfinance)"
    write_mode = WriteMode.UPSERT
    # After the US close (16:00 New York), same reasoning as sp500-prices'
    # 22:00 Paris slot: close enough behind the close without chasing DST.
    schedule = "30 22 * * 1-5"
    lookback_days = 1  # there is nothing to look back over; see module docstring

    table = TableSpec(
        schema="options",
        name="chain_snapshot",
        columns=(
            Column("date", "DATE", nullable=False),  # day this snapshot was captured
            Column("ticker", "TEXT", nullable=False),
            Column("expiry", "DATE", nullable=False),
            Column("option_type", "TEXT", nullable=False),  # "call" or "put"
            Column("strike", "DOUBLE PRECISION", nullable=False),
            Column("bid", "DOUBLE PRECISION"),
            Column("ask", "DOUBLE PRECISION"),
            Column("last_price", "DOUBLE PRECISION"),
            Column("volume", "BIGINT"),
            Column("open_interest", "BIGINT"),
            Column("implied_volatility", "DOUBLE PRECISION"),
        ),
        primary_key=("date", "ticker", "expiry", "option_type", "strike"),
        # The common downstream query is "one ticker's whole surface on one
        # day"; the primary key already serves "everything on one day".
        indexes=(("ticker", "date"),),
    )

    # ------------------------------------------------------------ registry

    def _registry(self) -> sql.Composed:
        return sql.SQL("{}.{}").format(sql.Identifier(self.table.schema), sql.Identifier(REGISTRY_TABLE))

    def ensure_companions(self, conn: psycopg.Connection) -> None:
        """Creates the load registry and, when it is new, fills it for every
        snapshot already stored, so it holds one row per stored (ticker, date)
        from the moment it exists."""
        qualified = f"{self.table.schema}.{REGISTRY_TABLE}"
        existed = conn.execute("SELECT to_regclass(%s)", (qualified,)).fetchone()[0] is not None
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} (\n"
                "    ticker TEXT NOT NULL,\n"
                "    date DATE NOT NULL,\n"
                "    loaded_at TIMESTAMPTZ NOT NULL,\n"
                "    row_count INTEGER NOT NULL,\n"
                "    checksum TEXT NOT NULL,\n"
                "    PRIMARY KEY (ticker, date)\n"
                ")"
            ).format(self._registry())
        )
        if not existed:
            with conn.cursor() as cur:
                self._record_loads(cur, None)
                logger.info("%s created and backfilled: %s snapshots", qualified, cur.rowcount)

    def after_write(self, cur: psycopg.Cursor, frame: pd.DataFrame) -> None:
        pairs = frame[["ticker", "date"]].drop_duplicates()
        self._record_loads(cur, (list(pairs["ticker"]), list(pairs["date"])))

    def _record_loads(self, cur: psycopg.Cursor, pairs: Optional[tuple]) -> None:
        """Upserts the registry rows of `pairs` (tickers, dates), or of every
        stored snapshot when None, from the rows the chain holds now."""
        # The checksum hashes the text of the rows, so pin the two settings
        # that change it: the float output precision and the date format.
        cur.execute("SET LOCAL extra_float_digits = 1")
        cur.execute("SET LOCAL DateStyle = 'ISO, MDY'")

        chain = sql.SQL("{}.{}").format(sql.Identifier(self.table.schema), sql.Identifier(self.table.name))
        row = sql.SQL("ROW({})::text").format(
            sql.SQL(", ").join(sql.SQL("c.{}").format(sql.Identifier(n)) for n in _CHECKSUM_COLUMNS)
        )
        if pairs is None:
            scope, params = sql.SQL(""), ()
        else:
            scope = sql.SQL(
                "JOIN unnest(%s::text[], %s::date[]) AS t(ticker, date) "
                "ON c.ticker = t.ticker AND c.date = t.date "
            )
            params = pairs
        cur.execute(
            sql.SQL(
                "INSERT INTO {registry} (ticker, date, loaded_at, row_count, checksum) "
                "SELECT c.ticker, c.date, now(), count(*), "
                "md5(string_agg({row}, '|' ORDER BY c.expiry, c.option_type COLLATE \"C\", c.strike)) "
                "FROM {chain} c {scope}"
                "GROUP BY c.ticker, c.date "
                "ON CONFLICT (ticker, date) DO UPDATE SET "
                "loaded_at = EXCLUDED.loaded_at, row_count = EXCLUDED.row_count, checksum = EXCLUDED.checksum"
            ).format(registry=self._registry(), row=row, chain=chain, scope=scope),
            params,
        )

    # -------------------------------------------------------------- fetch

    def tickers(self) -> List[str]:
        configured = os.getenv("OPTIONS_CHAIN_TICKERS", "")
        if configured.strip():
            return [t.strip().upper() for t in configured.split(",") if t.strip()]
        return list(DEFAULT_TICKERS)

    def fetch(self, window: Window) -> pd.DataFrame:
        snapshot_date = date.today()
        if window.full or (window.start and window.start < snapshot_date):
            logger.warning(
                "options-chain-snapshot: yfinance has no historical option chains -- "
                "fetching today's (%s) snapshot regardless of the requested window",
                snapshot_date,
            )

        tickers = self.tickers()
        logger.info("Fetching option chains for %d tickers", len(tickers))

        frames = [_fetch_one_chain(ticker, snapshot_date) for ticker in tickers]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame(columns=self.table.column_names)
        return pd.concat(frames, ignore_index=True)


def _fetch_one_chain(ticker: str, snapshot_date: date) -> pd.DataFrame:
    expirations: Optional[List[str]] = None
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            expirations = yf.Ticker(ticker).options
            break
        except Exception as exc:  # yfinance raises a mix of exception types
            last_error = exc
            logger.warning("options-chain: attempt %d for %s failed: %s", attempt, ticker, exc)
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    if expirations is None:
        logger.error("options-chain: giving up on %s: %s", ticker, last_error)
        return pd.DataFrame()
    if not expirations:
        return pd.DataFrame()

    tick = yf.Ticker(ticker)
    sides = []
    for exp_str in expirations:
        try:
            expiry = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if expiry <= snapshot_date:
            continue  # expiring today or already past: nothing left to quote

        try:
            chain = tick.option_chain(exp_str)
        except Exception as exc:
            logger.warning("options-chain: failed to fetch %s %s: %s", ticker, exp_str, exc)
            continue

        sides.append(_normalize_side(chain.calls, "call", ticker, expiry, snapshot_date))
        sides.append(_normalize_side(chain.puts, "put", ticker, expiry, snapshot_date))

    sides = [s for s in sides if not s.empty]
    if not sides:
        return pd.DataFrame()
    return pd.concat(sides, ignore_index=True)


def _normalize_side(
    df: Optional[pd.DataFrame], option_type: str, ticker: str, expiry: date, snapshot_date: date
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    missing = [c for c in _RAW_TO_STORED if c not in df.columns]
    if missing:
        logger.warning("options-chain: %s %s %s missing columns %s", ticker, expiry, option_type, missing)
        return pd.DataFrame()

    out = df.rename(columns=_RAW_TO_STORED)[list(_RAW_TO_STORED.values())].copy()
    out["date"] = snapshot_date
    out["ticker"] = ticker
    out["expiry"] = expiry
    out["option_type"] = option_type

    out = out[pd.to_numeric(out["strike"], errors="coerce") > 0]
    for col in ("strike", "bid", "ask", "last_price", "implied_volatility"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ("volume", "open_interest"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype("int64")
    # yfinance reports 0 rather than a gap for a missing IV; 0 is not a valid
    # implied vol, so store it as absent like any other missing quote --
    # re-deriving it from price is a cleaning-stage concern, not this one's.
    out.loc[out["implied_volatility"] <= 0, "implied_volatility"] = np.nan

    return out[
        ["date", "ticker", "expiry", "option_type", "strike", "bid", "ask", "last_price", "volume", "open_interest", "implied_volatility"]
    ]
