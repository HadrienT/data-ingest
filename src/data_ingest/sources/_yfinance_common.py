"""Shared yfinance download logic for every daily-OHLCV source.

Leading underscore so the registry's auto-discovery (data_ingest.registry)
skips this module -- it has nothing that is itself a Source.
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


def download_daily_ohlcv(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    """Daily OHLCV for every ticker, columns named per RAW_FIELDS.

    Downloads in chunks of CHUNK_SIZE tickers (yfinance's multi-ticker request
    gets unreliable well beyond that), retrying each chunk on failure. One
    chunk failing after every retry does not take the others with it -- it is
    simply missing from the result, same as a holiday with nothing published.
    """
    frames = []
    for batch in _chunked(tickers, CHUNK_SIZE):
        normalized = _normalize(_download_chunk(batch, start, end), batch)
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


def _download_chunk(tickers: List[str], start: str, end: str) -> pd.DataFrame:
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


def _normalize(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    if isinstance(df.columns, pd.MultiIndex):
        data = df.stack(level=0, future_stack=True).reset_index()
    else:
        data = df.reset_index().copy()
        data["Ticker"] = tickers[0] if tickers else "UNKNOWN"

    wanted = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
    data = data[[c for c in wanted if c in data.columns]]
    data["Date"] = pd.to_datetime(data["Date"]).dt.date
    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    if "Volume" in data.columns:
        data["Volume"] = data["Volume"].fillna(0).astype("int64")
    return data
