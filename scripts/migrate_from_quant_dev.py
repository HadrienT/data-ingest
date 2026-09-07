#!/usr/bin/env python3
"""One-off migration of the price history out of the old quant-dev database.

The previous stack stored everything in a single `public.sp500_data` table.
This moves it into the schema-per-domain layout as `prices.sp500_daily`,
through the engine's own writer, and verifies the result on aggregates rather
than on a row count alone.

A row count is not enough: migrating this table out of BigQuery once matched
exactly at 4,150,962 rows while a million NULLs had silently become IEEE NaN,
which a count cannot see but which turns every later SUM into NaN.

    python scripts/migrate_from_quant_dev.py \
        --source-dsn "host=localhost port=5433 dbname=quantdev user=quantdev password=..."
"""

from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd
import psycopg

from data_ingest.config import dsn
from data_ingest.core.database import Database
from data_ingest.registry import get_source

logger = logging.getLogger("data_ingest.migrate")

CHECKS = """
SELECT COUNT(*)                                AS rows,
       COUNT(DISTINCT ticker)                  AS tickers,
       ROUND(SUM(close)::numeric, 2)           AS sum_close,
       SUM(volume)                             AS sum_volume,
       COUNT(*) FILTER (WHERE close IS NULL)   AS null_close,
       MIN(date)                               AS first_date,
       MAX(date)                               AS last_date
FROM {table}
"""


def summarise(conn: psycopg.Connection, table: str) -> dict:
    row = conn.execute(CHECKS.format(table=table)).fetchone()
    keys = ["rows", "tickers", "sum_close", "sum_volume", "null_close", "first_date", "last_date"]
    return dict(zip(keys, row))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-dsn", required=True, help="libpq DSN of the old quant-dev database")
    parser.add_argument("--source-table", default="public.sp500_data")
    parser.add_argument("--batch", type=int, default=500_000, help="rows per batch")
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    source = get_source("sp500-prices")
    target = Database(dsn())
    target.ensure_table(source.table, source.write_mode)

    with psycopg.connect(args.source_dsn) as origin:
        before = summarise(origin, args.source_table)
        logger.info("Source %s: %s", args.source_table, before)

        offset = 0
        while True:
            frame = pd.DataFrame(
                origin.execute(
                    f"SELECT date, ticker, open, high, low, close, volume "
                    f"FROM {args.source_table} ORDER BY date, ticker LIMIT %s OFFSET %s",
                    (args.batch, offset),
                ).fetchall(),
                columns=["date", "ticker", "open", "high", "low", "close", "volume"],
            )
            if frame.empty:
                break
            target.write(source.table, frame, source.write_mode)
            offset += len(frame)
            logger.info("Migrated %s / %s rows", offset, before["rows"])

    with target.connect() as conn:
        after = summarise(conn, source.table.qualified)
    logger.info("Target %s: %s", source.table.qualified, after)

    mismatches = [k for k in before if before[k] != after[k]]
    if mismatches:
        logger.error("Mismatch on %s. Do NOT drop the source table.", ", ".join(mismatches))
        for key in mismatches:
            logger.error("  %-12s source=%s target=%s", key, before[key], after[key])
        return 1

    logger.info("Every check matches. The source table is safe to drop.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
