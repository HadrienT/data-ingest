# Operations

How the stack is scheduled, deployed and updated. First-time setup is in the
[README](../README.md#running-it).

## Scheduling

Airflow is the scheduler — `airflow/dags/data_ingest_dags.py` generates one
DAG per registered source at parse time (`ingest_<source>`, e.g.
`ingest_sp500_prices`), reading each source's `schedule` straight off the
registry, the same "nothing to hand-register" reasoning the engine already
applies everywhere else. A DAG's single task calls `run_source`
(`core/runner.py`) directly, in-process — the same function the CLI's `ingest
run` uses, not a shelled-out copy of it, so there's exactly one place that
knows what "running a source" means.

It runs as `LocalExecutor`: a scheduler, api-server, dag-processor and
triggerer, no Celery/Redis — the workload is a handful of daily batch runs on
one host, and distributing that across workers would be overhead for
identical throughput. Airflow's own metadata lives in a Postgres separate
from the data store (`airflow-postgres`), so its schema migrations can never
touch ingested data.

Retries (3, 2 minutes apart) and a 30-minute timeout per run mirror what the
old systemd unit did. A full backfill (`Window.everything()`, what `--full`
does on the CLI) is a checkbox on "Trigger DAG w/ config" in the UI, not a
flag you have to know to type over SSH.

`catchup` is off, so a host that was down runs each DAG once when it comes
back, over the source's lookback. For a source that declares `catch_up_on`
(the daily price sources), that run starts at the last date stored instead
when it is older: a two-week outage is fetched whole, not left as a hole.

## Deployment

**Updating is automatic**: merging into `main` deploys. Every two minutes a
systemd user timer runs `scripts/auto-deploy.sh`, which does nothing until
`origin/main` moves, waits for that commit's CI to be green, then
fast-forwards and runs `scripts/deploy.sh`. If the deploy fails (build, Airflow
health, a DAG import error) it goes back to the previous commit, deploys that
again and leaves the unit `failed`; the failed commit is not retried, push a
fix. Each attempt is listed under the repository's *Deployments* on GitHub.

```bash
cp deploy/autodeploy@.service deploy/autodeploy@.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now autodeploy@data-ingest.timer
journalctl --user -u 'autodeploy@*' -f
```

It only acts when this checkout is on a clean `main` (it is also where the
code is written): on a work branch, or with uncommitted changes, it says so in
the journal and waits. The rollback covers the code and the images, not the
data: a table a new source created, or rows it wrote, stay.

By hand — the same thing, from `main`:

```bash
scripts/deploy.sh           # pull, rebuild, restart what changed, verify
scripts/deploy.sh --check   # the same checks, nothing built or restarted
```

A restart is not enough: the Airflow image installs `data_ingest` when it is
built, and the DAG file reads the sources from that installed package, so a
new source only appears after a rebuild. The script refuses a branch other
than `main` or uncommitted changes (this checkout is also where the stack
runs), fixes nothing silently (a root-owned `airflow/logs` is reported with
the command to fix it), waits for the four Airflow services to be healthy,
fails on a DAG import error, and lists the DAGs still paused. It never starts
the `ingest` service, whose default command runs every source — which a bare
`docker compose up -d` does.

It also starts the capture. A deploy that changes the image (any commit that
touches `src/`, `pyproject.toml` or the README) recreates that container: a few seconds
of the feed are lost, the day's files say so (a hole, then a fresh book), and
the recording goes on in the same files.

The Airflow UI is at `http://127.0.0.1:8080` (login: `AIRFLOW_ADMIN_USER` /
`AIRFLOW_ADMIN_PASSWORD` from `.env`). DAGs start paused; unpause the ones you
want to run on their schedule.

Everything binds to `127.0.0.1`; nothing here is reachable off the server.
