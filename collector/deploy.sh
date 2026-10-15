#!/usr/bin/env bash
# Install the collector from this checkout into a fixed directory and (re)start its
# systemd user units. Run by the prod workflow on the self-hosted runner; safe to rerun.
#
# The receiver is restarted only when its code or unit changed: every restart is a
# collection gap plus a full re-subscribe at Entur.
#
# One-time prerequisites on the VM (see collector/README.md):
#   sudo loginctl enable-linger "$USER"
#   ~/.config/dat535/collector.env
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="$HOME/dat535-collector"
UNITS="$HOME/.config/systemd/user"
ENV_FILE="$HOME/.config/dat535/collector.env"

# The runner is a system service, so it has no user session environment of its own.
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "missing $ENV_FILE (copy collector/systemd/collector.env.example)" >&2
    exit 1
fi
if [[ ! -S "$XDG_RUNTIME_DIR/bus" ]]; then
    echo "no user systemd at $XDG_RUNTIME_DIR (run: sudo loginctl enable-linger $USER)" >&2
    exit 1
fi

mkdir -p "$DEST" "$UNITS"
code_changes=$(rsync -rci --delete --exclude tests/ --exclude __pycache__/ "$SRC/" "$DEST/")
unit_changes=$(rsync -rci "$SRC"/systemd/dat535-* "$UNITS/")
[[ -n "$code_changes$unit_changes" ]] && printf '%s\n' "$code_changes" "$unit_changes" | sed '/^$/d'

systemctl --user daemon-reload
systemctl --user enable dat535-receiver.service dat535-status.service dat535-monitor.timer

changed() { grep -q -- "$1" <<<"$code_changes"$'\n'"$unit_changes"; }

if changed 'receiver.py\|dat535-receiver.service'; then
    echo "receiver changed: restarting"
    systemctl --user restart dat535-receiver.service
else
    systemctl --user start dat535-receiver.service
fi

if changed 'serve_status.py\|dat535-status.service'; then
    systemctl --user restart dat535-status.service
else
    systemctl --user start dat535-status.service
fi

# The monitor is a oneshot; the timer picks up new code on its next run.
systemctl --user start dat535-monitor.timer

sleep 2
systemctl --user --no-pager status dat535-receiver.service dat535-status.service dat535-monitor.timer | grep -E '^●|Active:' || true
systemctl --user is-active --quiet dat535-receiver.service dat535-status.service
