"""The DAG file needs Airflow only for its wiring, so a stub stands in for it here."""

import importlib.util
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def refresh():
    class Dag:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG, sdk.BaseHook = Dag, object
    sdk.task = lambda fn: lambda: None
    sys.modules.setdefault("airflow", types.ModuleType("airflow"))
    sys.modules["airflow.sdk"] = sdk
    sys.modules.setdefault("pendulum", types.SimpleNamespace(datetime=lambda *a, **k: None))
    path = Path(__file__).parents[2] / "airflow" / "dags" / "kbase_regulation_refresh.py"
    spec = importlib.util.spec_from_file_location("kbase_regulation_refresh", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    del sys.modules["airflow.sdk"]


def test_a_clean_run_returns_the_summary(refresh):
    assert refresh.outcome(0, "status OK\n- jurisdictions: recall@10 1.000", "").startswith("status OK")


def test_a_run_with_failed_steps_fails_the_task_with_its_summary(refresh):
    with pytest.raises(RuntimeError, match=r"exit 1\):\nstatus FAILED"):
        refresh.outcome(1, "status FAILED\n- quant: no report", "")


def test_ssh_failure_says_the_refresh_did_not_start(refresh):
    with pytest.raises(RuntimeError, match="did not start"):
        refresh.outcome(255, "", "Permission denied (publickey).")
