"""Running a single source's fetch/write pass.

Split out from the CLI so it has exactly one caller-facing contract: given a
connected `Database`, a `Source` and a `Window`, do the fetch, ensure the
table exists, write, and return the row count. Both `cli.cmd_run` and the
Airflow DAGs in `airflow/dags/data_ingest_dags.py` call this directly, so
there is one place that knows what "running a source" means rather than
Airflow shelling out to the CLI as a black box.

Exceptions are not caught here — that is a decision each caller makes for
itself. `cmd_run` wraps this per source so `--all` keeps going after one
failure; an Airflow task lets the exception propagate so the task fails and
Airflow's own retry/alerting takes over.
"""

from __future__ import annotations

import logging

from .database import Database
from .source import Source, Window

logger = logging.getLogger("data_ingest")


def run_source(db: Database, source: Source, window: Window) -> int:
    logger.info("[%s] fetching %s", source.name, window)
    frame = source.fetch(window)
    if frame.empty:
        logger.info("[%s] nothing returned", source.name)
        return 0

    db.ensure_table(source.table, source.write_mode)
    written = db.write(source.table, frame, source.write_mode)
    logger.info("[%s] %s rows written to %s", source.name, written, source.table.qualified)
    return written
