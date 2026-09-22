"""One Airflow DAG per registered source, generated from the registry.

Adding a source is still just a new file in `data_ingest/sources/` — nothing
here has to be hand-updated, the same "no list to keep in sync" reasoning
`scripts/install-timer.sh` used to apply to systemd timers now applies to
DAGs. Each DAG has one TaskFlow task that calls `run_source` directly, in
this process: `data_ingest` is installed into the Airflow image (see
`Dockerfile.airflow`), so there is no subprocess/CLI boundary between the
scheduler and the ingestion code.

`retries`/`retry_delay`/`execution_timeout` carry over what the old
`data-ingest@.service` systemd unit did (`StartLimitBurst=3`,
`RestartSec=120`, `TimeoutStartSec=1800`).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, Param, task

from data_ingest.config import dsn
from data_ingest.core.database import Database
from data_ingest.core.runner import run_source
from data_ingest.core.source import Window
from data_ingest.registry import all_sources, get_source

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# Fixed, safely-past start date: schedules are daily/weekday crons with no
# backfill semantics of their own (a source's `default_window` lookback
# handles a missed run), so catchup stays off and this date only matters as
# "before which Airflow has nothing to schedule".
START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")


def _build_dag(source_name: str) -> DAG:
    """One DAG for one source. The source is re-resolved by name inside the
    task body (via `get_source`, the same lookup the CLI uses) rather than
    closed over at build time, since the task runs later, in a fresh process.
    """
    source = get_source(source_name)
    dag_id = f"ingest_{source_name.replace('-', '_')}"

    with DAG(
        dag_id=dag_id,
        description=source.description or f"Ingest {source_name}",
        schedule=source.schedule,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["data-ingest", source.table.schema],
        params={"full": Param(False, type="boolean", title="Backfill the entire history")},
    ) as dag:

        # `params` is a recognized Airflow context key: TaskFlow injects the
        # DAG run's resolved params dict when a parameter is named `params`,
        # so `full` arrives as a native bool from the "Trigger DAG w/ config"
        # form — no Jinja templating of a CLI flag through a shell string.
        @task(task_id="run")
        def run(params: dict) -> int:
            db = Database(dsn())
            src = get_source(source_name)
            window = Window.everything() if params.get("full") else src.default_window()
            return run_source(db, src, window)

        run()

    return dag


for _source in all_sources():
    _dag = _build_dag(_source.name)
    globals()[_dag.dag_id] = _dag
