"""Running a single source's fetch/write pass.

Split out from the CLI so it has exactly one caller-facing contract: given a
connected `Database`, a `Source` and a `Window`, do the fetch, ensure the
table exists, write, and return the row count. Both `cli.cmd_run` and the
Airflow DAGs in `airflow/dags/data_ingest_dags.py` call this directly, so
there is one place that knows what "running a source" means rather than
Airflow shelling out to the CLI as a black box.

A source with `inputs` (computed from other sources' tables) receives the
database through `Source.frames`; `run_order` puts it after its inputs.

Exceptions are not caught here — that is a decision each caller makes for
itself. `cmd_run` wraps this per source so `--all` keeps going after one
failure; an Airflow task lets the exception propagate so the task fails and
Airflow's own retry/alerting takes over.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import List, Sequence

from .database import Database
from .source import Source, Window

logger = logging.getLogger("data_ingest")


def routine_window(db: Database, source: Source) -> Window:
    """The window of a routine run: the source's lookback, stretched back to
    the last date stored when the previous run is older than that.

    The scheduler does not catch up (`catchup=False`): when the machine was
    off for longer than the lookback, the next run alone would leave the days
    in between missing for good. See `Source.catch_up_on`.
    """
    window = source.default_window()
    column = source.catch_up_on
    if column is None or window.full or window.start is None or not db.table_exists(source.table):
        return window
    _, last = db.bounds(source.table, column)
    if last is None or last >= window.start:
        return window
    logger.info("[%s] last stored %s is %s, catching up from there", source.name, column, last)
    return replace(window, start=last)


def run_source(db: Database, source: Source, window: Window) -> int:
    """Fetches the window and writes it, one transaction per frame the
    source yields (see `Source.frames`). Returns the rows written."""
    logger.info("[%s] fetching %s", source.name, window)
    written, ready = 0, False
    for frame in source.frames(db, window):
        if frame.empty:
            continue
        if not ready:
            db.ensure_table(source.table, source.write_mode, companions=source.ensure_companions)
            ready = True
        written += db.write(source.table, frame, source.write_mode, after_write=source.after_write)
    if not ready:
        logger.info("[%s] nothing returned", source.name)
        return 0
    logger.info("[%s] %s rows written to %s", source.name, written, source.table.qualified)
    return written


def run_order(sources: Sequence[Source]) -> List[Source]:
    """The sources in an order that runs each after its `inputs`."""
    by_name = {s.name: s for s in sources}
    ordered: List[Source] = []
    seen: set = set()

    def visit(source: Source, path: tuple) -> None:
        if source.name in seen:
            return
        if source.name in path:
            raise ValueError(f"Sources depend on each other in a cycle: {' -> '.join((*path, source.name))}")
        for name in source.inputs:
            if name in by_name:
                visit(by_name[name], (*path, source.name))
        seen.add(source.name)
        ordered.append(source)

    for source in sources:
        visit(source, ())
    return ordered
