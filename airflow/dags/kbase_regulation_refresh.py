"""Monthly refresh of the regulation collection of AgenticEnv's knowledge base.

The work runs on the host, not here: it needs the GPU (MinerU), `uv` and AgenticEnv's
checkout, none of which the Airflow containers have. The task opens an SSH session with
a key whose `authorized_keys` line forces one command, AgenticEnv's
`infra/scripts/kbase-regulation-refresh.sh`: whatever is sent, that is all the key can
run. The script fetches the regulators' sources again, re-ingests what changed,
evaluates, comments on AgenticEnv#71 and prints the same summary here.

Like `atlas_checks.py`, this file has nothing to do with `data_ingest` and imports none
of it; it lives here because this is where Airflow reads its DAGs.

Connection:
  kbase_refresh_ssh  SSH, created by `infra/scripts/install-refresh-access.sh` in
                     AgenticEnv. The extras hold the key and the host key.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import DAG, BaseHook, task

CONNECTION = "kbase_refresh_ssh"
# A full refresh took 33 minutes on 2026-11-13 (a few hundred polite requests, MinerU).
REFRESH_TIMEOUT_S = 3 * 3600


def outcome(returncode: int, stdout: str, stderr: str) -> str:
    """The summary to log, or an error. 255 is ssh itself: the script did not run."""
    if returncode == 255:
        raise RuntimeError(f"the refresh did not start (ssh exit 255): {stderr[-500:]}")
    if returncode != 0:
        raise RuntimeError(f"the refresh ran with failures (exit {returncode}):\n{stdout[-2000:]}")
    return stdout


def run_refresh() -> str:
    """The host's forced command ignores what is sent: the key can only start the refresh."""
    connection = BaseHook.get_connection(CONNECTION)
    extra = json.loads(connection.extra)
    with tempfile.TemporaryDirectory() as folder:
        key = Path(folder) / "key"
        key.touch(mode=0o600)
        key.write_text(extra["private_key"])
        known_hosts = Path(folder) / "known_hosts"
        known_hosts.write_text(extra["known_hosts"])
        done = subprocess.run(
            ["ssh", "-F", "none", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
             "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
             "-o", "ServerAliveInterval=60",
             f"{connection.login}@{connection.host}"],
            capture_output=True, text=True, timeout=REFRESH_TIMEOUT_S, check=False,
        )
    return outcome(done.returncode, done.stdout, done.stderr)


with DAG(
    dag_id="kbase_regulation_refresh",
    description="AgenticEnv kbase: fetch the regulators' texts again, re-ingest what changed",
    schedule="0 0 1 * *",  # first day of the month, 00:00 UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        # A failed run is worth a look rather than a blind retry: the script already
        # retries each request, and a second run would fetch everything again.
        "retries": 0,
        "execution_timeout": timedelta(seconds=REFRESH_TIMEOUT_S + 600),
    },
    tags=["agenticenv", "kbase"],
):

    @task
    def refresh() -> None:
        print(run_refresh())

    refresh()
