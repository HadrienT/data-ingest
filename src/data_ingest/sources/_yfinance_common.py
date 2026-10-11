"""Shared yfinance download logic for every daily-OHLCV source.

Leading underscore so the registry's auto-discovery (data_ingest.registry)
skips this module -- it has nothing that is itself a Source.

Two conventions hold for every row it returns:

- **Prices are not adjusted for dividends** (`auto_adjust=False`, Yahoo's
  "Close"). yfinance 1.x adjusts by default, as of the download date: a
  routine run that rewrites the last few days would then store rows adjusted
  at different dates, and the return across an ex-date would be wrong. Yahoo
  still restates its whole history after a split, which a routine run does
  not: a split calls for a `--full` run.
- **A missing quote is a missing row.** yfinance pads a multi-ticker frame
  with NaN rows: a weekend for every ticker downloaded next to a crypto
  pair, a day one ticker failed. Stored, such a row would overwrite a good
  quote with NULL on the next upsert.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable, List, Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger("data_ingest.yfinance_common")

CHUNK_SIZE = 100
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5

#: yfinance's capitalised names -- callers rename/select from these.
RAW_FIELDS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
#: What `actions=True` adds: the dividend and the split ratio of the day (0
#: when there is none), and the close adjusted for both as of today.
ACTION_FIELDS = ["Adj Close", "Dividends", "Stock Splits"]


def download_daily_ohlcv(tickers: List[str], start: str, end: str, actions: bool = False) -> pd.DataFrame:
    """Daily OHLCV for every ticker, columns named per RAW_FIELDS, plus
    ACTION_FIELDS when `actions` is set.

    Downloads in chunks of CHUNK_SIZE tickers (yfinance's multi-ticker request
    gets unreliable well beyond that), retrying each chunk on failure. One
    chunk failing after every retry does not take the others with it -- it is
    simply missing from the result, same as a holiday with nothing published.
    A (date, ticker) without a single price is left out as well.
    """
    frames = []
    for batch in _chunked(tickers, CHUNK_SIZE):
        normalized = _normalize(_download_chunk(batch, start, end, actions), batch, actions)
        if not normalized.empty:
            frames.append(normalized)
    if not frames:
        return pd.DataFrame()
    data = pd.concat(frames, ignore_index=True)
    return data.dropna(subset=["Date", "Ticker"]).drop_duplicates(subset=["Date", "Ticker"], keep="last")


def _chunked(items: Iterable[str], size: int) -> Iterable[List[str]]:
    batch: List[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _download_chunk(tickers: List[str], start: str, end: str, actions: bool = False) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = yf.download(
                tickers=" ".join(tickers),
                start=start,
                end=end,
                interval="1d",
                group_by="ticker",
                threads=True,
                auto_adjust=False,
                actions=actions,
            )
            if not data.empty:
                return data
        except Exception as exc:
            last_error = exc
            logger.warning("Download attempt %s failed: %s", attempt, exc)
        time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    if last_error:
        logger.error("Download failed after %s attempts: %s", MAX_RETRIES, last_error)
    return pd.DataFrame()


def _normalize(df: pd.DataFrame, tickers: List[str], actions: bool = False) -> pd.DataFrame:
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        data = df.stack(level=0, future_stack=True).reset_index()
    else:
        data = df.reset_index().copy()
        data["Ticker"] = tickers[0] if tickers else "UNKNOWN"

    wanted = RAW_FIELDS + ACTION_FIELDS if actions else RAW_FIELDS
    data = data[[c for c in wanted if c in data.columns]]
    data["Date"] = pd.to_datetime(data["Date"]).dt.date
    for col in data.columns.difference(["Date", "Ticker"]):
        data[col] = pd.to_numeric(data[col], errors="coerce")
    # A day is kept when it has a price, or an event: a dividend can be
    # detached on a day the ticker did not trade.
    prices = [c for c in ("Open", "High", "Low", "Close") if c in data.columns]
    quoted = data[prices].notna().any(axis=1)
    for col in ("Dividends", "Stock Splits"):
        if col in data.columns:
            data[col] = data[col].fillna(0.0)
            quoted |= data[col] != 0
    data = data[quoted].copy()
    if "Volume" in data.columns:
        data["Volume"] = data["Volume"].fillna(0).astype("int64")
    return data
