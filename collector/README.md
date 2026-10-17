# Collector

Receiver, vehicle positions collector (experiment), monitor and status page. Design:
`docs/architecture.md`; Entur subscription details: `docs/entur/realtime.md`.

Deployed by the prod workflow (`collector/deploy.sh`) to `~/dat535-collector` on the VM, as
systemd user units. Nothing is cloned on the VM.

## One-time VM setup

As the user the runner runs as (here `ubuntu`):

```sh
sudo loginctl enable-linger ubuntu          # user units run without a login session
mkdir -p ~/dat535-data/landing ~/dat535-data/status ~/.config/dat535
# create the env file from collector/systemd/collector.env.example, then:
chmod 600 ~/.config/dat535/collector.env    # set NTFY_TOPIC; RECEIVER_PUBLIC_URL when reachable
command -v rsync python3                    # both needed by deploy.sh
sudo apt install python3-websockets         # vehicles.py (10.4 on Ubuntu 24.04)
```

Then push to `main` and approve the prod run.

## Operating

```sh
systemctl --user status dat535-receiver dat535-vehicles dat535-status dat535-monitor.timer
journalctl --user -u dat535-receiver -f
systemctl --user restart dat535-receiver    # after editing the env file
```

Status page from a laptop without a tunnel: `ssh -L 8751:127.0.0.1:8751 ubuntu@<vm>`, open
`http://localhost:8751`.

`vehicles.py` subscribes to Entur's Vehicle Positions GraphQL API and lands every message in
`<landing>/vehicles/` (same line format and hourly files as the receiver). It is an
experiment: the monitor, exporter and Bronze do not cover it yet. Measured 2026-10-17
(Saturday, ~10:00 UTC): ~200 vehicle updates/s, ~1.4 MB/min gzipped (~2 GB/day).

A tunnel may expose port 8750 (receiver) and 8751 (status page) on separate, fixed
hostnames. Never expose the landing directory.

## Tunnel

`dat535-tunnel.service` holds a reverse SSH tunnel from the VM to a public host, where Caddy
serves `siri-dat535.dany.no` -> `127.0.0.1:8750` and `status-dat535.dany.no` -> `:8751`. The VM
is shared, so the key is treated as readable by anyone on it: on the public host it belongs
to a `nologin` user that may only listen on those two loopback ports (no shell, no `-L`, no
other ports). Every login runs a forced command that ends the user's older sessions, which
the unit does before each start: sshd never notices a silently dead tunnel while its
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

`python3 -m unittest discover -s collector/tests` (localhost only, never Entur).
