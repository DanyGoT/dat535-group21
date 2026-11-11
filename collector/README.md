# Collector

Use this runbook to set up and operate collectors, monitoring services and the public tunnel.

Up: [project](../README.md) · Related: [architecture](../docs/system/architecture.md),
[storage](../docs/system/storage.md)

## What runs

The [production workflow](../.github/workflows/dat535-prod.yml) calls [deploy.sh](deploy.sh)
after approval and tests on pushes to `main`. It installs systemd **user** units under
`~/.config/systemd/user` and copies `collector/` to `~/dat535-collector`.

| Unit | Purpose | Restart cost |
| ---- | ------- | ------------ |
| `dat535-receiver` | [SIRI ET/SX receiver](receiver.py) | Gap, re-subscription and full resend |
| `dat535-vehicles` | [Vehicle subscription](vehicles.py) | Gap and full snapshot |
| `dat535-datex` | [DATEX poller](datex.py) | Interrupted polls and a collection gap |
| `dat535-tunnel` | Reverse SSH tunnel | Receiver and status temporarily unreachable |
| `dat535-status` | [Status server](serve_status.py) and `/grafana/` proxy | Brief page outage |
| `dat535-exporter`, `dat535-prometheus`, `dat535-grafana` | Metrics and dashboard | Brief monitoring outage |
| `dat535-monitor.timer` | [Checks](monitor.py), status files and ntfy every 2 min | — |
| `dat535-static.timer` | [NeTEx/NSR archive](static.py) daily at 04:15 UTC | — |

Service restarts follow dependency patterns in `deploy.sh`. Changing shared [hourly.py](hourly.py)
restarts all three streaming collectors. Restart manually after environment-file edits.

Deployment installs pinned, checksum-verified Prometheus and Grafana under `~/dat535-tools`.
It starts both timers and requests an archive, skipping existing files and downloads less than 20 h old.

**Never download NeTEx or NSR by hand from the VM.** Entur allows one download per file
per 24 h and may block the IP. Read `/data/dat535/landing/{netex,nsr}/`.
The code's 20-hour guard is shorter than this limit.

## One-time VM setup

Run from the checkout as the runner user, `ubuntu`:

```sh
sudo loginctl enable-linger ubuntu          # user units run without a login session
mkdir -p ~/dat535-data/status ~/.config/dat535
cp collector/systemd/collector.env.example ~/.config/dat535/collector.env   # then edit it
chmod 600 ~/.config/dat535/collector.env    # holds NTFY_TOPIC and DATEX credentials
command -v rsync python3 flock              # needed by deploy.sh and the workflows
sudo apt install python3-websockets         # vehicles.py (10.4 on Ubuntu 24.04)
```

Edit the copied [environment template](systemd/collector.env.example). Keep landing on `/data`.
Set `RECEIVER_PUBLIC_URL` after tunnel setup, or the receiver only listens.
Set a long random `NTFY_TOPIC` for notifications. Push to `main` and approve deployment.

## Everyday commands

On the VM:

```sh
systemctl --user status 'dat535-*'
journalctl --user -u dat535-receiver -f              # or -vehicles, -datex
systemctl --user restart dat535-datex                # after editing collector.env
systemctl --user list-timers 'dat535-*'
journalctl --user -u dat535-static
DAT535_LANDING_DIR=/data/dat535/landing python3 ~/dat535-collector/peek.py et
du -sh /data/dat535/{landing/*,dev,prod}
```

[peek.py](peek.py) skips heartbeats and prints an ET journey or SX situation as XML, or vehicles
as JSON. It supplements sparse open hours with recent closed files. `--file <hour=HH.jsonl.gz>`
reads from the start. Open-hour sampling uses random offsets. Set landing explicitly because
shells do not inherit unit environments.

For the status page without the public tunnel:

```sh
ssh -L 8751:127.0.0.1:8751 ubuntu@<vm>
```

Open `http://localhost:8751`. Substitute `dat535-ts` for `ubuntu@<vm>` off campus after
configuring [Tailscale](#off-campus).

## Off campus

SSH to the VM address requires the UiS network. Off campus, use Tailscale node `dat535-vm`
at `100.65.145.61` in one member's tailnet. `tailscaled` starts with
`--accept-dns=false --accept-routes=false`, leaving VM DNS and routes unchanged.
Collectors and the public tunnel do not depend on it.

Ask the tailnet owner to share the node, then add:

```sshconfig
# ~/.ssh/config
Host dat535-ts
    HostName 100.65.145.61
    User ubuntu
```

Remove Tailscale with `sudo systemctl disable --now tailscaled`.

## DATEX credentials

Without credentials, [datex.py](datex.py) stays IDLE and its feed checks are skipped.
The credentials have been on the VM since 2026-10-26, and collection began on 2026-10-28.
For setup, add the account from NPRA's two emails to `~/.config/dat535/collector.env`.
Credentials are confidential and must never enter git, docs or logs.

```sh
DATEX_USERNAME=...
DATEX_PASSWORD=...
```

Check once, then restart:

```sh
set -a; . ~/.config/dat535/collector.env; set +a
python3 ~/dat535-collector/datex.py --check     # one GET per feed, stops on rejected login
systemctl --user restart dat535-datex
```

The check prints status, size or failure reason. The monitor shows `datex-<feed>` rows and
POLLING · DATEX. Grafana has a DATEX row. Rejected logins pause all feeds for 15 min and show FAILED.

## Disk cleanup

The [storage guide](../docs/system/storage.md#when-it-fills-delete-by-hand-cheapest-loss-first)
owns cleanup order and commands. Its [monitoring section](../docs/system/storage.md#watching-it)
lists alert thresholds. Nothing removes stored days automatically.

## Tunnel

[dat535-tunnel.service](systemd/dat535-tunnel.service) opens a reverse SSH tunnel to a public
host. Caddy maps `siri-dat535.dany.no` to `127.0.0.1:8750` and `status-dat535.dany.no` to
`127.0.0.1:8751` there. Expose only these services. Landing, Prometheus and the exporter stay private.

Treat the key as readable by anyone on the shared VM. Restrict the public-host account to
those loopback listeners and a forced command, with no interactive shell or `-L` forwarding.

Before opening the tunnel, the unit runs the forced command to end older sessions.
Silently dead sessions retain listening ports while traffic reaches them. One held them for 41 min.

One-time setup on the VM:

```sh
ssh-keygen -t ed25519 -N '' -C dat535-tunnel@vm -f ~/.ssh/dat535-tunnel
ssh-keyscan -t ed25519 <public host> > ~/.config/dat535/tunnel_known_hosts   # check the fingerprint
echo 'TUNNEL_TARGET=dat535-tunnel@<public host>' >> ~/.config/dat535/collector.env
```

On the public host, create system user `dat535-tunnel` with shell `/bin/sh` so the forced
command can run. Put the key in `authorized_keys` with this prefix:

```text
restrict,port-forwarding,permitlisten="127.0.0.1:8750",permitlisten="127.0.0.1:8751",command="/usr/local/bin/dat535-tunnel-kick" <key>
```

Install `/usr/local/bin/dat535-tunnel-kick` to end the user's `sshd` processes except its own
ancestors. Configure this sshd drop-in exactly:

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

From a VM checkout with the system `websockets` package installed:

```sh
python3 -m unittest discover -s collector/tests
```

For a local environment pinned to the VM's version:

```sh
uv run --python 3.11 --with websockets==10.4 python -m unittest discover -s collector/tests
```

Both use localhost test servers and never contact Entur or NPRA.
