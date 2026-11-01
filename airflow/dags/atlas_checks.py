"""Nightly checks of the Atlas pages (HadrienT/atlas), one GitHub issue per finding.

`detect_changes` compares each page's `source_sha` with its repository, through
the GitHub API. `check_live_state` asks the host to run Atlas's read-only
probes, over SSH, because `systemctl --user` and `docker exec` only exist there.

Both only report: a person updates the pages. This file has nothing to do with
`data_ingest` and imports none of it; it only lives here because this is where
Airflow reads its DAGs. `tools/flag.mjs` in Atlas is the local twin of
`matches` and `classify`, and `tests/unit/test_atlas_checks.py` keeps its cases.

Connections:
  github_atlas     HTTP, the password is a fine-grained GitHub token.
  atlas_probe_ssh  SSH, set up by `tools/install-probe-access.sh` in Atlas. The
                   extras hold a key that can only run the probes, and the host key.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import DAG, BaseHook, task

ATLAS = "HadrienT/atlas"
API = "https://api.github.com"
TIMEOUT_S = 30
API_FILE_LIMIT = 300
PROBE_TIMEOUT_S = 120
TOKEN_WARNING_DAYS = 14


def matches(glob: str, path: str) -> bool:
    escaped = re.sub(r"[.+?^${}()|\[\]\\]", lambda m: "\\" + m[0], glob)
    tokens = {"**/": "(?:.*/)?", "**": ".*", "*": "[^/]*"}
    source = re.sub(r"\*\*/|\*\*|\*", lambda m: tokens[m[0]], escaped)
    return re.fullmatch(source, path) is not None


def classify(files: list[str], config: dict) -> dict:
    components: dict[str, list[str]] = {}
    unmapped: list[str] = []
    for file in files:
        if any(matches(glob, file) for glob in config.get("ignore", [])):
            continue
        hits = [cid for cid, globs in config["components"].items() if any(matches(g, file) for g in globs)]
        if not hits:
            unmapped.append(file)
        for cid in hits:
            components.setdefault(cid, []).append(file)
    return {"components": components, "unmapped": unmapped}


def review_body(report: dict) -> str | None:
    """The issue text for one repo's report, or None when there is nothing to review."""
    if not (report["components"] or report["unmapped"] or report["truncated"]):
        return None
    lines = [
        f"`{report['repo']}` has {report['commits']} commit(s) since `{report['documented']}`, now `{report['head']}`.",
        "",
    ]
    if report["truncated"]:
        lines += [f"GitHub stopped at {API_FILE_LIMIT} files, so this list is incomplete.", ""]
    for cid, files in sorted(report["components"].items()):
        lines.append(f"- **{cid}**: " + ", ".join(f"`{f}`" for f in files))
    if report["unmapped"]:
        lines += ["", "Not mapped to any component: " + ", ".join(f"`{f}`" for f in report["unmapped"])]
    lines += ["", "Update the page, then set `source_sha` and `META.commit` to the reviewed commit."]
    return "\n".join(lines)


def drift_body(results: list[dict]) -> str | None:
    """The issue text for one repo's probes. A known gap stays quiet until it closes."""
    findings = [r for r in results if r["status"] in ("mismatch", "cleared", "error")]
    if not findings:
        return None
    lines = ["| Status | Component | Probe | Observed |", "|---|---|---|---|"]
    lines += [f"| {r['status']} | {r['component']} | {r['id']} | `{r['actual'][:120]}` |" for r in findings]
    lines += ["", "`cleared` means a documented gap closed, so the page has to change."]
    return "\n".join(lines)


def expiry_body(expiry: str | None, now: datetime) -> str | None:
    """The issue text when the token is about to expire. GitHub sends the date with each answer."""
    if expiry is None:
        return None
    date = datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=UTC)
    if date - now > timedelta(days=TOKEN_WARNING_DAYS):
        return None
    return f"The `github_atlas` token expires on {date.date()}. Once it does, this issue cannot update and `detect_changes` fails. Replace it in Airflow."


class GitHub:
    def __init__(self, token: str):
        self.session = requests.Session()
        self.token_expiry: str | None = None
        self.session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})

    def call(self, method: str, path: str, **kwargs):
        response = self.session.request(method, f"{API}/{path}", timeout=TIMEOUT_S, **kwargs)
        response.raise_for_status()
        self.token_expiry = response.headers.get("github-authentication-token-expiration", self.token_expiry)
        return response

    def documented_repos(self) -> list[str]:
        entries = self.call("GET", f"repos/{ATLAS}/contents/repos").json()
        return [entry["name"] for entry in entries if entry["type"] == "dir"]

    def sources(self, repo: str) -> dict:
        raw = {"Accept": "application/vnd.github.raw+json"}
        return self.call("GET", f"repos/{ATLAS}/contents/repos/{repo}/sources.json", headers=raw).json()

    def report(self, repo: str) -> dict:
        config = self.sources(repo)
        sha, branch = config["source_sha"], config["branch"]
        compare = self.call("GET", f"repos/{config['repo']}/compare/{sha}...{branch}").json()
        head = self.call("GET", f"repos/{config['repo']}/commits/{branch}").json()["sha"]
        files = [file["filename"] for file in compare.get("files", [])]
        return {
            "repo": repo,
            "documented": sha[:7],
            "head": head[:7],
            "commits": compare["ahead_by"],
            "truncated": len(files) >= API_FILE_LIMIT,
            **classify(files, config),
        }

    def open_issues(self) -> list[dict]:
        issues: list[dict] = []
        for page in range(1, 100):
            batch = self.call("GET", f"repos/{ATLAS}/issues", params={"state": "open", "per_page": 100, "page": page}).json()
            issues += [issue for issue in batch if "pull_request" not in issue]
            if len(batch) < 100:
                return issues
        return issues

    def sync(self, key: str, title: str, body: str | None) -> None:
        """Keeps exactly one open issue for `key`: opens, updates or closes it."""
        marker = f"<!-- atlas-check:{key} -->"
        existing = next((i for i in self.open_issues() if marker in (i["body"] or "")), None)
        if body is None:
            if existing:
                self.call("PATCH", f"repos/{ATLAS}/issues/{existing['number']}", json={"state": "closed"})
        elif existing is None:
            self.call("POST", f"repos/{ATLAS}/issues", json={"title": title, "body": f"{marker}\n{body}"})
        elif existing["body"] != f"{marker}\n{body}":
            self.call("PATCH", f"repos/{ATLAS}/issues/{existing['number']}", json={"body": f"{marker}\n{body}"})


def github() -> GitHub:
    return GitHub(BaseHook.get_connection("github_atlas").password)


def report_failure(context: dict) -> None:
    """A failed task keeps an issue, so nobody has to open Airflow to learn about it. It closes on the next success."""
    name = context["task_instance"].task_id
    github().sync(f"failure:{name}", f"The nightly check {name} failed", f"`{name}` failed: `{str(context.get('exception'))[:300]}`")


def run_probes() -> list[dict]:
    """The host's forced command ignores what is sent: the key can only run the probes."""
    connection = BaseHook.get_connection("atlas_probe_ssh")
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
             f"{connection.login}@{connection.host}"],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT_S, check=False,
        )
    # probe.mjs exits with 0, 1 or 2 and prints its report each time. 255 is ssh itself.
    if done.returncode not in (0, 1, 2):
        raise RuntimeError(f"probes did not run, exit {done.returncode}: {done.stderr[-500:]}")
    return json.loads(done.stdout)


with DAG(
    dag_id="atlas_checks",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # two runs at once would both see no issue, and both open one
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=15),
        "on_failure_callback": report_failure,
    },
    tags=["atlas"],
):

    @task
    def detect_changes() -> None:
        hub = github()
        failed = []
        for repo in hub.documented_repos():
            try:
                report = hub.report(repo)
            except requests.RequestException as error:
                failed.append(f"{repo}: {error}")
                continue
            hub.sync(f"review:{repo}", f"Review the {repo} page", review_body(report))
        if failed:
            raise RuntimeError("; ".join(failed))
        hub.sync("token", "The Atlas GitHub token expires soon", expiry_body(hub.token_expiry, datetime.now(UTC)))
        hub.sync("failure:detect_changes", "", None)

    @task
    def check_live_state() -> None:
        results = run_probes()
        hub = github()
        for repo in sorted({r["repo"] for r in results}):
            mine = [r for r in results if r["repo"] == repo]
            hub.sync(f"drift:{repo}", f"Live state differs from the {repo} page", drift_body(mine))
        errors = [f"{r['repo']}/{r['id']}: {r['actual']}" for r in results if r["status"] == "error"]
        if errors:
            raise RuntimeError("; ".join(errors))
        hub.sync("failure:check_live_state", "", None)

    detect_changes()
    check_live_state()
