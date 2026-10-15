# Collector

Receiver, monitor and status page. Design: `docs/architecture.md`; Entur subscription
details: `docs/entur/realtime.md`.

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
```

Then push to `main` and approve the prod run.

## Operating

```sh
systemctl --user status dat535-receiver dat535-status dat535-monitor.timer
journalctl --user -u dat535-receiver -f
systemctl --user restart dat535-receiver    # after editing the env file
```

Status page from a laptop without a tunnel: `ssh -L 8751:127.0.0.1:8751 ubuntu@<vm>`, open
`http://localhost:8751`.

A tunnel may expose port 8750 (receiver) and 8751 (status page) on separate, fixed
hostnames. Never expose the landing directory.

## Tests

`python3 -m unittest discover -s collector/tests` (localhost only, never Entur).
