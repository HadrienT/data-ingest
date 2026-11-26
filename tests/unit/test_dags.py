"""The DAG file's wiring, with a stub standing in for Airflow: a source with
inputs is scheduled on their Assets, and every run publishes its own."""

import importlib.util
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def dags():
    built = {}

    class Dag:
        def __init__(self, **kwargs):
            self.kwargs, self.dag_id = kwargs, kwargs["dag_id"]
            built[self.dag_id] = self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class Asset:
        def __init__(self, name):
            self.name = name

        def __eq__(self, other):
            return isinstance(other, Asset) and other.name == self.name

        def __repr__(self):
            return f"Asset({self.name!r})"

    def task(**kwargs):
        def decorate(fn):
            def call():
                built[list(built)[-1]].task = kwargs
            return call
        return decorate

    sdk = types.ModuleType("airflow.sdk")
    sdk.DAG, sdk.Asset, sdk.task = Dag, Asset, task
    sdk.Param = lambda *a, **k: None
    saved = {name: sys.modules.get(name) for name in ("airflow", "airflow.sdk", "pendulum")}
    sys.modules.setdefault("airflow", types.ModuleType("airflow"))
    sys.modules["airflow.sdk"] = sdk
    sys.modules.setdefault("pendulum", types.SimpleNamespace(datetime=lambda *a, **k: None))
    path = Path(__file__).parents[2] / "airflow" / "dags" / "data_ingest_dags.py"
    spec = importlib.util.spec_from_file_location("data_ingest_dags", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module, built, Asset
    for name, value in saved.items():
        if value is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = value


def test_a_source_with_inputs_runs_on_their_assets(dags):
    _, built, Asset = dags
    schedule = built["ingest_options_chain_iv"].kwargs["schedule"]
    assert schedule == [
        Asset("data-ingest/options-chain-snapshot"), Asset("data-ingest/sp500-prices"),
        Asset("data-ingest/dividend-yields"), Asset("data-ingest/equity-dividends"),
    ]


def test_a_source_without_inputs_keeps_its_cron_and_publishes_its_asset(dags):
    _, built, Asset = dags
    snapshot = built["ingest_options_chain_snapshot"]
    assert snapshot.kwargs["schedule"] == "30 22 * * 1-5"
    assert snapshot.task["outlets"] == [Asset("data-ingest/options-chain-snapshot")]
