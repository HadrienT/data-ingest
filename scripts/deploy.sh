#!/usr/bin/env bash
# Rebuild and restart data-ingest's stack: the store, Airflow and the order
# book capture.
#
#   scripts/deploy.sh           deploy main (pull, build, restart, verify)
#   scripts/deploy.sh --check   every check, nothing built or restarted
#
# Why a rebuild and not a restart: the Airflow image installs the data_ingest
# package when it is BUILT (Dockerfile.airflow), and the DAG file generates one
# DAG per source from that installed registry. A new or changed source is
# invisible to Airflow until the image is rebuilt.
#
# This checkout is both where the code is written and where the stack runs
# (there is no separate prod folder), so the script only deploys a clean
# `main`: an image built from a dirty tree would run code that is in no commit.
#
# It never starts the `ingest` service: that is the manual CLI, and its
# default command is `run --all` — a bare `docker compose up -d` would run
# every source at once as a side effect.
#
# The capture is a connection that stays open: recreating it leaves a hole of
# a few seconds in the day's file (marked as such, and followed by a fresh
# book). Compose only recreates it when the image changed, that is when a
# commit touched src/, pyproject.toml or README.md (the Dockerfile copies them).
#
# Idempotent: with nothing new, the build is cached and no container is
# recreated. Newly registered DAGs start paused (AIRFLOW__CORE__DAGS_ARE_
# PAUSED_AT_CREATION): the script lists them with the command to enable them,
# it does not decide for you.
set -euo pipefail
cd "$(dirname "$0")/.."

CHECK_ONLY=0
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=1

STORE=(postgres airflow-postgres)
AIRFLOW=(airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer)
CAPTURE=(capture)

fail() {
  echo "✗ $*" >&2
  exit 1
}

# ── Preconditions ────────────────────────────────────────────────────────────
[[ -f .env ]] || fail ".env is missing: cp .env.example .env and fill it in (README, Running it)"
docker compose config --quiet || fail "docker compose config is invalid (a required variable of .env?)"

branch="$(git rev-parse --abbrev-ref HEAD)"
[[ "$branch" == "main" ]] || fail "on branch '$branch': the stack runs main — git switch main first"
if ! git diff --quiet || ! git diff --cached --quiet; then
  fail "uncommitted changes to tracked files: commit or stash them before deploying"
fi

# scripts/auto-deploy.sh sets DEPLOY_NO_PULL=1: it deploys exactly the commit
# whose CI it checked.
if [[ $CHECK_ONLY -eq 0 && -z "${DEPLOY_NO_PULL:-}" ]]; then
  # Non-fatal, like the other deploy scripts: offline, deploy the checkout.
  git pull --ff-only --quiet 2>/dev/null &&
    echo "→ synced with $(git rev-parse --abbrev-ref '@{u}')" ||
    echo "⚠ git pull skipped (offline or diverged) — deploying the current checkout"
fi

docker network inspect dataplatform >/dev/null 2>&1 || {
  [[ $CHECK_ONLY -eq 1 ]] && fail "docker network 'dataplatform' is missing (a deploy creates it)"
  docker network create dataplatform >/dev/null
  echo "→ created docker network dataplatform"
}

# Airflow runs as AIRFLOW_UID and writes into these bind mounts. Left to
# Docker they are created root-owned and the dag-processor crash-loops.
uid="$(grep -E '^AIRFLOW_UID=' .env | cut -d= -f2)"
uid="${uid:-50000}"
for d in airflow/logs airflow/plugins airflow/config; do
  mkdir -p "$d"
  owner="$(stat -c %u "$d")"
  [[ "$owner" == "$uid" ]] ||
    fail "$d is owned by uid $owner, Airflow runs as $uid: docker run --rm -v \"\$PWD/airflow:/a\" alpine chown -R $uid:0 /a/logs /a/plugins /a/config"
done

# The capture writes as CAPTURE_UID into this bind mount, and Airflow reads
# it. Left to Docker it is created root-owned and the capture cannot write.
env_value() { grep -E "^$1=" .env | cut -d= -f2- || true; }
capture_dir="$(env_value CAPTURE_DIR)"
capture_dir="${capture_dir:-./data/capture}"
capture_uid="$(env_value CAPTURE_UID)"
capture_uid="${capture_uid:-1000}"
[[ $CHECK_ONLY -eq 1 ]] || mkdir -p "$capture_dir"
if [[ -d "$capture_dir" ]]; then
  owner="$(stat -c %u "$capture_dir")"
  [[ "$owner" == "$capture_uid" ]] ||
    fail "$capture_dir is owned by uid $owner, the capture runs as $capture_uid: set CAPTURE_UID in .env or chown the directory"
fi

COMMIT_SHA="$(git rev-parse --short HEAD)"
if [[ $CHECK_ONLY -eq 1 ]]; then
  echo "✓ ready to deploy $COMMIT_SHA (--check: nothing built or restarted)"
  exit 0
fi

# ── Build and restart ────────────────────────────────────────────────────────
echo "→ deploying $COMMIT_SHA"
docker compose build
docker compose up -d "${STORE[@]}" airflow-init "${AIRFLOW[@]}" "${CAPTURE[@]}"

# ── Health gate ──────────────────────────────────────────────────────────────
echo -n "→ waiting for Airflow to be healthy "
healthy=0
for _ in $(seq 1 60); do
  healthy=0
  for s in "${AIRFLOW[@]}"; do
    id="$(docker compose ps -q "$s")"
    [[ -n "$id" ]] && [[ "$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null)" == "healthy" ]] &&
      healthy=$((healthy + 1))
  done
  [[ $healthy -eq ${#AIRFLOW[@]} ]] && break
  echo -n "."
  sleep 5
done
if [[ $healthy -ne ${#AIRFLOW[@]} ]]; then
  echo " FAILED" >&2
  docker compose ps -a >&2
  docker compose logs --tail 30 "${AIRFLOW[@]}" >&2
  exit 1
fi
echo " ok"

sql() { docker compose exec -T airflow-postgres psql -U airflow -d airflow -Atc "$1"; }
airflow() { docker compose exec -T airflow-scheduler airflow "$@"; }

# Wait until the dag-processor has parsed one DAG per registered source.
expected="$(docker compose exec -T airflow-scheduler python -c \
  'from data_ingest.registry import all_sources; print(len(all_sources()))')"
for _ in $(seq 1 24); do
  [[ "$(sql "SELECT count(*) FROM dag WHERE dag_id LIKE 'ingest\_%'")" -ge "$expected" ]] && break
  sleep 5
done

errors="$(airflow dags list-import-errors -o plain 2>/dev/null | grep -v '^No data found' || true)"
[[ -z "$errors" ]] || fail "DAG import errors:
$errors"
echo "→ $expected sources registered, no import error"

# The metadata DB is the truth for the pause state (`airflow dags list` can lag).
paused="$(sql "SELECT dag_id FROM dag WHERE dag_id LIKE 'ingest\_%' AND is_paused ORDER BY 1")"
if [[ -n "$paused" ]]; then
  echo "⚠ paused — they will not run until enabled (UI, or for each):"
  while read -r dag; do
    echo "    docker compose exec airflow-scheduler airflow dags unpause $dag"
  done <<<"$paused"
fi

# The capture has no dependency to wait for: it is recording or it is not.
id="$(docker compose ps -q "${CAPTURE[@]}")"
[[ -n "$id" && "$(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null)" == "true" ]] ||
  fail "the capture is not running: docker compose logs --tail 30 capture"
echo "→ capture running, writing to $capture_dir"

docker image prune -f >/dev/null
echo "✓ deployed $COMMIT_SHA"
