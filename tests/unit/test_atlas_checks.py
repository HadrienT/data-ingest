"""The DAG file needs Airflow only for its wiring, so a stub stands in for it here."""

import importlib.util
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def checks():
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
    path = Path(__file__).parents[2] / "airflow" / "dags" / "atlas_checks.py"
    spec = importlib.util.spec_from_file_location("atlas_checks", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    del sys.modules["airflow.sdk"]


@pytest.mark.parametrize(
    "glob,path,expected",
    [
        ("src/a.py", "src/a.py", True),
        ("src/a.py", "src/a.pyc", False),
        ("src/a.py", "x/src/a.py", False),
        ("src/a.py", "src/axpy", False),
        ("a+b(c).txt", "a+b(c).txt", True),
        ("a?.txt", ".txt", False),
        ("src/*.py", "src/a.py", True),
        ("src/*.py", "src/sub/a.py", False),
        ("packages/*/pyproject.toml", "packages/a/b/pyproject.toml", False),
        ("src/**", "src/a/b/c.py", True),
        ("src/**", "source/a.py", False),
        ("**/__init__.py", "__init__.py", True),
        ("**/__init__.py", "a/b/__init__.py", True),
        ("**/__init__.py", "a/x__init__.py", False),
        ("**/tests/**", "pkg/tests/sub/a.py", True),
        ("**/tests/**", "pkg/contests/a.py", False),
    ],
)
def test_matches(checks, glob, path, expected):
    assert checks.matches(glob, path) is expected


CONFIG = {
    "ignore": ["docs/**", "**/__init__.py"],
    "components": {"api": ["src/api/**"], "store": ["src/store/**", "src/api/db.py"]},
}


def test_classify(checks):
    files = ["docs/x.md", "src/api/__init__.py", "src/api/db.py", "README.md"]
    assert checks.classify(files, CONFIG) == {
        "components": {"api": ["src/api/db.py"], "store": ["src/api/db.py"]},
        "unmapped": ["README.md"],
    }


def report(**kwargs):
    base = {"repo": "r", "documented": "aaa", "head": "bbb", "commits": 1, "truncated": False, "components": {}, "unmapped": []}
    return {**base, **kwargs}


def test_nothing_to_review(checks):
    assert checks.review_body(report()) is None


def test_review_reports_components_unmapped_and_truncation(checks):
    body = checks.review_body(report(components={"api": ["a.py"]}, unmapped=["b"], truncated=True))
    assert "**api**: `a.py`" in body and "`b`" in body and "incomplete" in body


def test_known_gaps_stay_quiet_until_they_close(checks):
    probe = {"component": "c", "id": "p", "actual": "x"}
    assert checks.drift_body([{**probe, "status": "known"}, {**probe, "status": "ok"}]) is None
    assert "cleared" in checks.drift_body([{**probe, "status": "cleared"}])


class FakeHub:
    """The real sync, on an in-memory list of issues."""

    def __init__(self, checks):
        self.hub = checks.GitHub("t")
        self.issues = []
        self.hub.open_issues = lambda: [i for i in self.issues if i["state"] == "open"]
        self.hub.call = self.call

    def call(self, method, path, json=None, **kwargs):
        if method == "POST":
            self.issues.append({"number": len(self.issues) + 1, "state": "open", **json})
        else:
            self.issues[int(path.rsplit("/", 1)[1]) - 1].update(json)


def test_sync_opens_one_issue_updates_it_and_closes_it(checks):
    fake = FakeHub(checks)
    fake.hub.sync("review:r", "Review", "one")
    fake.hub.sync("review:r", "Review", "one")
    assert len(fake.issues) == 1
    fake.hub.sync("review:r", "Review", "two")
    assert len(fake.issues) == 1 and fake.issues[0]["body"].endswith("two")
    fake.hub.sync("review:other", "Review", "one")
    assert len(fake.issues) == 2
    fake.hub.sync("review:r", "Review", None)
    assert fake.issues[0]["state"] == "closed" and fake.issues[1]["state"] == "open"
