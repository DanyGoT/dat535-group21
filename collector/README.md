# Collector

Everything that runs all the time on the VM: the collectors, the monitor, metrics, the status
page and retention. How it fits together: [system/architecture.md](../docs/system/architecture.md). Capacity: [system/storage.md](../docs/system/storage.md).

Deployed by the prod workflow (`collector/deploy.sh`) to `~/dat535-collector` as systemd
**user** units. Nothing is cloned on the VM; edit here, push, approve the prod run.

## What runs

| Unit | Does | A restart costs |
| ---- | ---- | --------------- |
| `dat535-receiver` | SIRI ET/SX pub/sub receiver (`receiver.py`) | a gap and a re-subscribe at Entur, with a full resend |
| `dat535-vehicles` | vehicle positions subscription (`vehicles.py`) | a gap and a full snapshot |
| `dat535-datex` | DATEX poller (`datex.py`); idle without credentials | at most one poll per feed |
| `dat535-tunnel` | reverse SSH tunnel to the public host (below) | the receiver and status page go dark briefly |
| `dat535-status` | status page and `/grafana/` proxy (`serve_status.py`) | nothing |
| `dat535-exporter`, `-prometheus`, `-grafana` | metrics and dashboard | nothing |
| `dat535-monitor.timer` | checks every 2 min, status page, ntfy (`monitor.py`) | — |
| `dat535-retention.timer` | hourly storage retention (`retention.py`) | — |

`deploy.sh` restarts a unit only when its own files changed. The three collectors share
`hourly.py`: changing it restarts all three.

## One-time VM setup

As the user the runner runs as (`ubuntu`):

```sh
sudo loginctl enable-linger ubuntu          # user units run without a login session
mkdir -p ~/dat535-data/status ~/.config/dat535
cp collector/systemd/collector.env.example ~/.config/dat535/collector.env   # then edit it
chmod 600 ~/.config/dat535/collector.env    # holds NTFY_TOPIC and DATEX credentials
command -v rsync python3 flock              # needed by deploy.sh and the workflows
sudo apt install python3-websockets         # vehicles.py (10.4 on Ubuntu 24.04)
```

Then push to `main` and approve the prod run.

## Everyday commands

```sh
systemctl --user status 'dat535-*'
journalctl --user -u dat535-receiver -f              # or -vehicles, -datex, -retention
systemctl --user restart dat535-datex                # after editing collector.env
python3 ~/dat535-collector/peek.py et                # a few real messages (et, sx, vehicles)
python3 ~/dat535-collector/retention.py --dry-run    # what retention would delete now
```

`peek.py <feed>` prints random data messages from the hour being written, heartbeats skipped:
one ET journey or SX situation as indented XML, or a few vehicles as JSON. `--file
<hour=HH.jsonl.gz>` reads a closed hour instead.

Status page without the tunnel: `ssh -L 8751:127.0.0.1:8751 ubuntu@<vm>`, then open
`http://localhost:8751`.

## DATEX credentials

Without them `dat535-datex` runs idle and the status page shows `datex` as IDLE (not a
failure). To turn it on, add the account from NPRA's two emails to `collector.env`
(confidential: never in git):

```sh
DATEX_USERNAME=...
DATEX_PASSWORD=...
```

then check them and restart:

```sh
set -a; . ~/.config/dat535/collector.env; set +a
python3 ~/dat535-collector/datex.py --check     # one GET per feed: HTTP 200 and sizes, or why not
systemctl --user restart dat535-datex
```

The monitor then checks each feed (rows `datex-<feed>`, section POLLING · DATEX); Grafana has
a DATEX row. A rejected login pauses all feeds for 15 min and shows as FAILED.

## Retention

`retention.py` keeps `/data` from filling up: stale dev output first, then old prod Bronze
(rebuildable from landing), and only above 92 % the oldest ET landing days. Every deletion is
logged to `/data/dat535/retention-ledger.jsonl`. Policy and reasoning: [system/storage.md](../docs/system/storage.md).

## Tunnel

`dat535-tunnel.service` holds a reverse SSH tunnel from the VM to a public host, where Caddy
serves `siri-dat535.dany.no` -> `127.0.0.1:8750` and `status-dat535.dany.no` -> `:8751`. Never
expose anything else (landing data, Prometheus, the exporter).

The VM is shared, so the key is treated as readable by anyone on it: on the public host it
belongs to a `nologin` user that may only listen on those two loopback ports (no shell, no
`-L`, no other ports). Every login runs a forced command that ends the user's older sessions,
which the unit does before each start: sshd never notices a silently dead tunnel while its
ports see traffic, and the stale session would hold them (observed: 41 min).

One-time setup on the VM:

```sh
ssh-keygen -t ed25519 -N '' -C dat535-tunnel@vm -f ~/.ssh/dat535-tunnel
ssh-keyscan -t ed25519 <public host> > ~/.config/dat535/tunnel_known_hosts   # check the fingerprint
echo 'TUNNEL_TARGET=dat535-tunnel@<public host>' >> ~/.config/dat535/collector.env
```

On the public host: a system user `dat535-tunnel` (shell `/bin/sh`, needed to run the forced
command) with the key in `authorized_keys` as
`restrict,port-forwarding,permitlisten="127.0.0.1:8750",permitlisten="127.0.0.1:8751",command="/usr/local/bin/dat535-tunnel-kick" <key>`,
the kick script (ends the user's `sshd` processes other than its own ancestors), and an
sshd drop-in:

```
Match User dat535-tunnel
    AuthenticationMethods publickey
    AllowTcpForwarding remote
    GatewayPorts no
    PermitListen 127.0.0.1:8750 127.0.0.1:8751
    PermitOpen none
    AllowAgentForwarding no
    AllowStreamLocalForwarding no
    X11Forwarding no
    PermitTTY no
    PermitTunnel no
    ForceCommand /usr/local/bin/dat535-tunnel-kick
    ClientAliveInterval 30
    ClientAliveCountMax 3
Match all
```

## Tests

`python3 -m unittest discover -s collector/tests` (localhost only, never Entur or NPRA).
