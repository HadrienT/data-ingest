"""Command line for the ingestion engine.

    ingest list                       what sources exist
    ingest run sp500-prices           routine run over the source's lookback
    ingest run sp500-prices --full    backfill the whole history
    ingest run --all                  every source, routine window
    ingest status                     what is stored, per source
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from typing import List, Optional

from .config import LOG_LEVEL, dsn
from .core.database import Database
from .core.runner import run_source
from .core.source import Window
from .registry import all_sources, get_source

logger = logging.getLogger("data_ingest")


def cmd_list(args: argparse.Namespace) -> int:
    sources = all_sources()
    if not sources:
        if not args.porcelain:
            print("No sources registered.")
        return 0
    if args.porcelain:
        # Machine-readable name/schedule pairs, for scripting against the
        # registry without importing it (schedules are also read directly by
        # airflow/dags/data_ingest_dags.py, which runs in the same process).
        for source in sources:
            print(f"{source.name}\t{source.schedule}")
        return 0
    width = max(len(s.name) for s in sources)
    for source in sources:
        print(f"{source.name:<{width}}  {source.table.qualified:<22} {source.write_mode.value:<9} {source.schedule:<16} {source.description}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    db = Database(dsn())
    sources = all_sources() if args.all else [get_source(name) for name in args.source]

    failures: List[str] = []
    for source in sources:
        if args.full:
            window = Window.everything()
        elif args.since:
            window = Window(start=date.fromisoformat(args.since), end=date.today())
        else:
            window = source.default_window()
        try:
            run_source(db, source, window)
        except Exception:
            # With --all, one broken upstream must not stop the others.
            logger.exception("[%s] run failed", source.name)
            failures.append(source.name)

    if failures:
        logger.error("Failed sources: %s", ", ".join(failures))
        return 1
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    db = Database(dsn())
    for source in all_sources():
        if not db.table_exists(source.table):
            print(f"{source.name:<16} {source.table.qualified:<22} not created yet")
            continue
        count = db.row_count(source.table)
        first, last = db.bounds(source.table, "date")
        print(f"{source.name:<16} {source.table.qualified:<22} {count:>10} rows  {first} -> {last}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ingest", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    listing = sub.add_parser("list", help="show the registered sources")
    listing.add_argument("--porcelain", action="store_true", help="tab-separated name and schedule, for scripts")
    listing.set_defaults(func=cmd_list)

    run = sub.add_parser("run", help="ingest one or more sources")
    run.add_argument("source", nargs="*", help="source names; omit with --all")
    run.add_argument("--all", action="store_true", help="run every registered source")
    run.add_argument("--full", action="store_true", help="fetch the entire history")
    run.add_argument("--since", metavar="YYYY-MM-DD", help="fetch from this date to today")
    run.set_defaults(func=cmd_run)

    sub.add_parser("status", help="show what is stored").set_defaults(func=cmd_status)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "run" and not args.source and not args.all:
        parser.error("name at least one source, or pass --all")
    if args.command == "run" and args.full and args.since:
        parser.error("--full and --since are mutually exclusive")

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
