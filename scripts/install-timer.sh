#!/usr/bin/env bash
# Installs one systemd user timer per source, using the schedule each source
# declares. Re-run it after adding a source: the timers are generated from the
# registry, so there is no list to keep in sync by hand.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

mkdir -p "$UNIT_DIR"
install -m 644 "$REPO/scripts/systemd/data-ingest@.service" "$UNIT_DIR/"

# Ask the engine itself what exists and when each source wants to run.
schedules=$(docker compose -f "$REPO/docker-compose.yml" run --rm --no-deps ingest list --porcelain)

while IFS=$'\t' read -r name schedule; do
  [ -n "$name" ] || continue
  cat > "$UNIT_DIR/data-ingest@${name}.timer" <<TIMER
[Unit]
Description=data-ingest schedule for ${name}

[Timer]
Unit=data-ingest@${name}.service
OnCalendar=${schedule}
Persistent=true
RandomizedDelaySec=300

[Install]
WantedBy=timers.target
TIMER
  echo "  ${name}: ${schedule}"
done <<< "$schedules"

systemctl --user daemon-reload
while IFS=$'\t' read -r name _; do
  [ -n "$name" ] || continue
  systemctl --user enable --now "data-ingest@${name}.timer"
done <<< "$schedules"

if ! loginctl show-user "$USER" --property=Linger | grep -q 'Linger=yes'; then
  echo
  echo "Lingering is off, so the timers will not fire while you are logged out."
  echo "Enable it with:  sudo loginctl enable-linger $USER"
fi

echo
systemctl --user list-timers 'data-ingest@*' --no-pager
