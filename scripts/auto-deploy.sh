#!/usr/bin/env bash
# Continuous deployment, pulled by the server: deploy origin/main when it moves
# and its CI is green, and go back to the previous commit if the deploy fails.
#
#   scripts/auto-deploy.sh      run by the autodeploy@ systemd timer, every 2 min
#
# Pulled rather than pushed because the server has no open port and the
# repository is public: a self-hosted Actions runner would let a pull request
# run code here.
#
# One tick:
#   1. nothing new on origin/main, or that commit already failed  → exit
#   2. its GitHub checks are still running                         → exit, next tick
#   3. a check failed                                              → remember it, exit
#   4. fast-forward, run scripts/deploy.sh (build, restart, health gate)
#   5. on failure: back to the previous commit, deploy.sh again, exit 1
#
# A commit that failed is not retried; push a fix and the new commit is tried.
# Each attempt is recorded as a GitHub deployment (repository → Deployments).
# This file is the same in every repository that deploys this way.
set -euo pipefail
cd "$(dirname "$0")/.."

BRANCH="${AUTODEPLOY_BRANCH:-main}"
DEPLOY="${AUTODEPLOY_CMD:-scripts/deploy.sh}"
STATE="$(git rev-parse --absolute-git-dir)"
FAILED_FILE="$STATE/autodeploy-failed"

log() { echo "[autodeploy] $*"; }
short() { git rev-parse --short "$1"; }

exec 9>"$STATE/autodeploy.lock"
flock -n 9 || exit 0

git fetch --quiet origin "$BRANCH" 2>/dev/null || {
  log "git fetch failed (offline?), next tick"
  exit 0
}
current="$(git rev-parse HEAD)"
target="$(git rev-parse FETCH_HEAD)"
[[ "$target" == "$current" ]] && exit 0
[[ "$(cat "$FAILED_FILE" 2>/dev/null)" == "$target" ]] && exit 0

# ── Is this checkout in a state to deploy? ───────────────────────────────────
branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "$BRANCH" ]]; then
  log "on branch '$branch', not '$BRANCH': not deploying $(short "$target")"
  exit 0
fi
if ! git diff --quiet || ! git diff --cached --quiet; then
  log "uncommitted changes to tracked files: not deploying $(short "$target")"
  exit 0
fi
if ! git merge-base --is-ancestor "$current" "$target"; then
  log "origin/$BRANCH ($(short "$target")) is not ahead of $(short "$current"): not deploying"
  exit 0
fi

# ── CI gate ──────────────────────────────────────────────────────────────────
repo="$(git remote get-url origin | sed -E 's#^(git@github\.com:|https://github\.com/)##; s#\.git$##')"
checks="$(gh api "repos/$repo/commits/$target/check-runs?per_page=100" \
  --jq '.check_runs[] | "\(.status) \(.conclusion) \(.name)"' 2>/dev/null)" || {
  log "GitHub unreachable, next tick"
  exit 0
}
if [[ -z "$checks" ]]; then
  log "$(short "$target"): no CI run yet"
  exit 0
fi
if grep -qv '^completed ' <<<"$checks"; then
  log "$(short "$target"): CI still running"
  exit 0
fi
bad="$(grep -Ev '^completed (success|skipped|neutral) ' <<<"$checks" || true)"
if [[ -n "$bad" ]]; then
  echo "$target" >"$FAILED_FILE"
  log "$(short "$target"): CI failed, not deploying:"
  log "$bad"
  exit 0
fi

# ── Deploy, and roll back if it fails ────────────────────────────────────────
# Best effort: a deploy must not depend on GitHub recording it.
deployment="$(gh api -X POST "repos/$repo/deployments" --input - --jq .id 2>/dev/null <<JSON || true
{"ref": "$target", "environment": "production", "auto_merge": false, "required_contexts": []}
JSON
)"
report() {
  [[ -n "$deployment" ]] || return 0
  gh api -X POST "repos/$repo/deployments/$deployment/statuses" \
    -f state="$1" -f description="$2" >/dev/null 2>&1 || true
}

log "deploying $(short "$current") → $(short "$target")"
report in_progress "deploying"
git merge --quiet --ff-only "$target"
if DEPLOY_NO_PULL=1 "$DEPLOY"; then
  rm -f "$FAILED_FILE"
  report success "deployed $(short "$target")"
  log "✓ deployed $(short "$target")"
  exit 0
fi

echo "$target" >"$FAILED_FILE"
log "✗ deploy of $(short "$target") failed, rolling back to $(short "$current")"
git reset --quiet --keep "$current"
if DEPLOY_NO_PULL=1 "$DEPLOY"; then
  report failure "deploy failed, rolled back to $(short "$current")"
  log "rolled back to $(short "$current")"
  exit 1
fi
report error "deploy failed, and so did the rollback to $(short "$current")"
log "✗ rollback to $(short "$current") failed too: the stack needs a hand"
exit 2
