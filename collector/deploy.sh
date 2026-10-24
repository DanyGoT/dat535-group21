#!/usr/bin/env bash
# Install the collector from this checkout into a fixed directory and (re)start its
# systemd user units. Run by the prod workflow on the self-hosted runner; safe to rerun.
# Also installs pinned Prometheus and Grafana releases (checksum-verified) into ~/dat535-tools.
#
# Each service restarts only when its own code, config, unit or tool version changed.
# The receiver above all: every restart is a collection gap plus a full re-subscribe at Entur.
#
# One-time prerequisites on the VM (see collector/README.md):
#   sudo loginctl enable-linger "$USER"
#   ~/.config/dat535/collector.env
#   sudo apt install python3-websockets      (vehicles.py)
#   ~/.ssh/dat535-tunnel + TUNNEL_TARGET     (dat535-tunnel.service)
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="$HOME/dat535-collector"
UNITS="$HOME/.config/systemd/user"
ENV_FILE="$HOME/.config/dat535/collector.env"
TOOLS="$HOME/dat535-tools"

# Bump version and checksum together (checksums from the upstream release pages).
PROMETHEUS_VERSION=3.13.4
PROMETHEUS_SHA256=87f21a66f96c597a189cef8d640e8921b621fc17a9feff707c332c4f3b3ddd56
GRAFANA_VERSION=13.2.4
GRAFANA_SHA256=c48765988fd88572607e4ea670251af8a8e64ab3e360d0f3cf67a6f3587b92ab

# The runner is a system service, so it has no user session environment of its own.
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "missing $ENV_FILE (copy collector/systemd/collector.env.example)" >&2
    exit 1
fi
if ! /usr/bin/python3 -c 'import websockets' 2>/dev/null; then
    echo "python3 has no websockets module (run: sudo apt install python3-websockets)" >&2
    exit 1
fi
if [[ ! -S "$XDG_RUNTIME_DIR/bus" ]]; then
    echo "no user systemd at $XDG_RUNTIME_DIR (run: sudo loginctl enable-linger $USER)" >&2
    exit 1
fi

# install_tool NAME VERSION URL SHA256: unpack a release once into $TOOLS/NAME-VERSION and
# point $TOOLS/NAME at it. Adds NAME to tool_changes when the link moved.
tool_changes=""
install_tool() {
    local name=$1 version=$2 url=$3 sha=$4
    local dir="$TOOLS/$name-$version" tmp
    if [[ ! -d "$dir" ]]; then
        echo "installing $name $version"
        rm -rf "$TOOLS/.tmp-$name".*
        tmp=$(mktemp -d "$TOOLS/.tmp-$name.XXXXXX")
        curl -fsSL --retry 3 -o "$tmp/pkg.tar.gz" "$url"
        echo "$sha  $tmp/pkg.tar.gz" | sha256sum -c --quiet -
        mkdir "$tmp/pkg"
        tar -xzf "$tmp/pkg.tar.gz" -C "$tmp/pkg" --strip-components=1
        mv "$tmp/pkg" "$dir"
        rm -rf "$tmp"
    fi
    if [[ "$(readlink "$TOOLS/$name" || true)" != "$dir" ]]; then
        ln -sfn "$dir" "$TOOLS/$name"
        tool_changes+="$name"$'\n'
    fi
}

mkdir -p "$DEST" "$UNITS" "$TOOLS"
install_tool prometheus "$PROMETHEUS_VERSION" \
    "https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS_VERSION/prometheus-$PROMETHEUS_VERSION.linux-amd64.tar.gz" \
    "$PROMETHEUS_SHA256"
install_tool grafana "$GRAFANA_VERSION" \
    "https://dl.grafana.com/oss/release/grafana-$GRAFANA_VERSION.linux-amd64.tar.gz" \
    "$GRAFANA_SHA256"

code_changes=$(rsync -rci --delete --exclude tests/ --exclude __pycache__/ "$SRC/" "$DEST/")
unit_changes=$(rsync -rci "$SRC"/systemd/dat535-* "$UNITS/")
[[ -n "$code_changes$unit_changes" ]] && printf '%s\n' "$code_changes" "$unit_changes" | sed '/^$/d'

systemctl --user daemon-reload
SERVICES=(dat535-tunnel dat535-receiver dat535-vehicles dat535-status dat535-exporter dat535-prometheus dat535-grafana)
systemctl --user enable "${SERVICES[@]/%/.service}" dat535-monitor.timer

changed() { grep -q -- "$1" <<<"$code_changes"$'\n'"$unit_changes"$'\n'"$tool_changes"; }
restart_if() {  # restart_if UNIT PATTERN: restart when PATTERN matches a change, else just start
    if changed "$2"; then
        echo "$1 changed: restarting"
        systemctl --user restart "$1"
    else
        systemctl --user start "$1"
    fi
}

restart_if dat535-tunnel.service 'dat535-tunnel.service'
# The collectors share their landing writer, hourly.py.
restart_if dat535-receiver.service 'receiver.py\|hourly.py\|dat535-receiver.service'
restart_if dat535-vehicles.service 'vehicles.py\|hourly.py\|dat535-vehicles.service'
restart_if dat535-status.service 'serve_status.py\|dat535-status.service'
# exporter.py imports from monitor.py.
restart_if dat535-exporter.service 'exporter.py\|monitor.py\|dat535-exporter.service'
restart_if dat535-prometheus.service 'observability/prometheus.yml\|dat535-prometheus.service\|^prometheus$'
# Dashboard JSON is re-read by Grafana every 30 s; no restart needed for it.
restart_if dat535-grafana.service \
    'observability/grafana/grafana.ini\|observability/grafana/provisioning/\|dat535-grafana.service\|^grafana$'

# The monitor is a oneshot; the timer picks up new code on its next run.
systemctl --user start dat535-monitor.timer

sleep 2
systemctl --user --no-pager status "${SERVICES[@]/%/.service}" dat535-monitor.timer | grep -E '^●|Active:' || true
systemctl --user is-active --quiet "${SERVICES[@]/%/.service}"
