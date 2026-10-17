#!/usr/bin/env python3
"""Read-only collection monitor.

Checks the receiver and the landing zone (SIRI feeds and vehicle positions), writes a static status page
(index.html + status.json) and sends an ntfy push on state change only (OK<->FAIL).
Never writes to the landing zone. Exit code 1 plus a one-line reason on failure.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    RECEIVER_STATE_FILE     written by the receiver (default: ./run/receiver.json)
    MONITOR_FEEDS           feeds to check (default: the receiver's RECEIVER_FEEDS plus vehicles)
    RECEIVER_FEEDS          the receiver's SIRI feeds (default: et,sx)
    MONITOR_STATUS_DIR      output directory, the only thing ever served (default: ./status)
    MONITOR_MAX_PART_AGE_MIN  .part must be modified within this many minutes (default: 5)
    MONITOR_DISK_MAX_PCT    landing volume usage limit in percent (default: 90)
    NTFY_TOPIC              ntfy topic; unset = no notifications. Long and random: it is the secret.
    NTFY_SERVER             default: https://ntfy.sh
    STATUS_PUBLIC_URL       optional public address of the status page; tapping a notification opens it
"""

import html
import json
import os
import shutil
import socket
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("Europe/Oslo")
except Exception:
    LOCAL_TZ = timezone.utc

TITLE = "DAT535 COLLECTOR"

# Grafana panels embedded on the main page: (panel id in dashboards/collector.json, title, height px).
# serve_status.py proxies /grafana/ to Grafana.
DASHBOARD = "/grafana/d/dat535-collector/dat535-collector"
PANELS = [(8, "ingest rate", 240), (9, "stored on disk", 240), (10, "per day", 300)]
PANEL_URL = ("/grafana/d-solo/dat535-collector/dat535-collector?orgId=1&panelId={}"
             "&from=now-24h&to=now&refresh=1m&hideLogo=true")


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def check_receiver(state_file):
    try:
        state = json.loads(Path(state_file).read_text())
    except FileNotFoundError:
        return {"ok": False, "reason": "receiver not running (no state file)",
                "short": "receiver not running"}, None
    except (OSError, ValueError) as e:
        return {"ok": False, "reason": f"receiver state unreadable: {e}",
                "short": "receiver state unreadable"}, None
    if not pid_alive(int(state.get("pid", 0))):
        return {"ok": False, "reason": f"receiver pid {state.get('pid')} not alive",
                "short": "receiver not running"}, state
    return {"ok": True, "reason": None, "short": None}, state


VEHICLES = "vehicles"


def feeds_from_env():
    """Feeds to check or report: MONITOR_FEEDS, else the receiver's SIRI feeds plus vehicles."""
    raw = os.environ.get("MONITOR_FEEDS") or os.environ.get("RECEIVER_FEEDS", "et,sx") + "," + VEHICLES
    return [f.strip() for f in raw.split(",") if f.strip()]


def landing_dir(feed):
    """Landing directory of a feed: siri-<feed> from receiver.py, vehicles from vehicles.py."""
    return VEHICLES if feed == VEHICLES else f"siri-{feed}"


def current_part(landing, feed):
    parts = sorted((Path(landing) / landing_dir(feed)).glob("date=*/hour=*.jsonl.part"))
    return parts[-1] if parts else None


def check_feed(landing, feed, prev, now, max_age_min):
    part = current_part(landing, feed)
    if part is None:
        return {"ok": False, "reason": f"{feed}: no .part file", "short": f"{feed}: no data yet",
                "file": None, "size": None, "age_s": None}
    st = part.stat()
    rel = f"{part.parent.name}/{part.name}"
    age = now.timestamp() - st.st_mtime
    res = {"ok": True, "reason": None, "short": None, "file": rel, "size": st.st_size, "age_s": round(age)}
    if age > max_age_min * 60:
        res.update(ok=False, reason=f"{feed}: {rel} not modified for {fmt_age(age)}",
                   short=f"{feed}: no new data for {fmt_age(age)}")
    elif prev and prev.get("file") == rel and prev.get("size") is not None and st.st_size <= prev["size"]:
        res.update(ok=False, reason=f"{feed}: {rel} has not grown since last check",
                   short=f"{feed}: no new data since last check")
    return res


def check_disk(landing, max_pct):
    u = shutil.disk_usage(landing)
    pct = 100 * u.used / u.total
    res = {"ok": pct < max_pct, "reason": None, "short": None, "used_pct": round(pct, 1), "free_bytes": u.free}
    if not res["ok"]:
        res["reason"] = f"disk {pct:.1f}% used (limit {max_pct}%)"
        res["short"] = f"disk {pct:.0f}% full"
    return res


def fmt_age(seconds):
    s = int(seconds)
    if s < 120:
        return f"{s}s"
    if s < 7200:
        return f"{s // 60}m"
    if s < 172800:
        return f"{s // 3600}h {s % 3600 // 60}m"
    return f"{s // 86400}d {s % 86400 // 3600}h"


def fmt_bytes(n):
    for unit in ("B", "K", "M", "G", "T"):
        if n < 1024 or unit == "T":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def parse_iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def notify(server, topic, ok, body, click_url):
    headers = {"Title": f"Collector {'OK' if ok else 'FAIL'}",
               "Priority": "default" if ok else "high",
               "Tags": "white_check_mark" if ok else "rotating_light"}
    if click_url:
        headers["Click"] = click_url
    req = urllib.request.Request(f"{server.rstrip('/')}/{topic}", data=body.encode("utf-8"),
                                 method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


CSS_AND_HEAD = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<title>DAT535 Collector</title>
<style>
:root {
  --bg: #f2f1ec; --fg: #1b1b1a; --mute: #7a776f; --line: #d6d3ca;
  --run: #2e7a34; --fail: #b3261e; --busy: #9a6400;
}
@media (prefers-color-scheme: dark) {
  :root { --bg: #111110; --fg: #e4e1d8; --mute: #858279; --line: #2a2926;
          --run: #74c27a; --fail: #ff6f61; --busy: #e2a640; }
}
* { box-sizing: border-box; }
body {
  margin: 0; background: var(--bg); color: var(--fg);
  font: 14px/1.5 ui-monospace, "JetBrains Mono", "SF Mono", Menlo, Consolas, monospace;
  -webkit-text-size-adjust: 100%;
}
main { max-width: 760px; margin: 0 auto; padding: 28px 16px 64px; }
a { color: inherit; }
header { display: flex; flex-wrap: wrap; gap: 4px 18px; align-items: baseline; margin-bottom: 36px; }
h1 { font-size: 14px; letter-spacing: .18em; margin: 0; }
h2 { font-size: 12px; font-weight: 400; letter-spacing: .18em; color: var(--mute); margin: 0 0 10px; }
.links { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 0 32px; margin-bottom: 48px; }
.link { display: grid; grid-template-columns: 2ch 13ch 1fr; padding: 7px 0; text-decoration: none; }
.link b { font-weight: 700; }
.link:hover b { text-decoration: underline; }
.link i { font-style: normal; color: var(--run); }
.link.down i, .link.down .mute { color: var(--fail); }
.mute { color: var(--mute); }
.msg { margin: -20px 0 28px; color: var(--mute); }
.msg::before { content: "> "; }
.row { padding: 16px 0; border-top: 1px solid var(--line); }
.row:last-child { border-bottom: 1px solid var(--line); }
.top { display: grid; grid-template-columns: 11ch 1fr auto; gap: 12px; align-items: baseline; }
.name { font-weight: 700; text-decoration: none; overflow-wrap: anywhere; }
.nums { color: var(--mute); text-align: right; white-space: nowrap; }
.desc { color: var(--mute); margin: 2px 0 0 calc(11ch + 12px); }
.st { font-size: 12px; letter-spacing: .08em; color: var(--mute); }
.st::before { content: "■ "; }
.active .st { color: var(--run); }
.failed .st { color: var(--fail); }
.activating .st, .deactivating .st { color: var(--busy); }
button, .btn {
  font: inherit; font-size: 12px; letter-spacing: .06em; text-transform: uppercase;
  color: var(--fg); background: none; border: 0; padding: 6px 8px; cursor: pointer; text-decoration: none;
}
button:hover, .btn:hover { text-decoration: underline; }
pre { margin: 24px 0 0; font-size: 12px; line-height: 1.45; white-space: pre-wrap; overflow-wrap: anywhere; }
@media (max-width: 520px) {
  .top { grid-template-columns: 1fr auto; }
  .top .st { grid-column: 1 / -1; }
  .desc { margin-left: 0; }
}
.badge { font-weight: 700; margin-left: 1ch; color: var(--busy); text-decoration: none; }
.badge.error { color: var(--fail); }
.health h2 { margin-top: 32px; }
.kv { display: grid; grid-template-columns: max-content 1fr; gap: 4px 2ch; }
.ok { color: var(--run); }
.warn { color: var(--busy); }
.error { color: var(--fail); }
.zoom { margin-left: auto; white-space: nowrap; }
.zoom button { padding: 6px 10px; }
.panel { display: block; width: 100%; border: 0; margin: 0 0 12px; background: transparent; }
</style>
<script>
// Per-device page zoom; CSS zoom reflows the layout instead of panning.
var z = 1;
try { z = +localStorage.getItem("zoom") || 1; } catch (e) {}
document.documentElement.style.zoom = z;
function setZoom(v) {
  z = Math.round(Math.min(2, Math.max(0.6, v)) * 10) / 10;
  document.documentElement.style.zoom = z;
  try { localStorage.setItem("zoom", z); } catch (e) {}
  document.getElementById("zl").textContent = Math.round(z * 100) + "%";
}
// Every monitor run (2 min); the embedded panels refresh themselves every minute.
setTimeout(() => location.replace("/"), 120000);
</script>
</head><body><main>
"""


def row(state_word, ok, name, nums, desc):
    e = html.escape
    return (f'<section class="row {"active" if ok else "failed"}">\n'
            f'  <div class="top">\n'
            f'    <span class="st">{e(state_word)}</span>\n'
            f'    <span><span class="name">{e(name)}</span></span>\n'
            f'    <span class="nums">{e(nums)}</span>\n'
            f'  </div>\n'
            f'  <div class="desc">{e(desc)}</div>\n'
            f'</section>\n')


def render(status, host):
    e = html.escape
    now = parse_iso(status["checked_at"])
    checks = status["checks"]
    n_fail = sum(1 for c in checks.values() if not c["ok"])
    n_ok = len(checks) - n_fail
    counts = f"{n_ok} ok"
    if n_fail:
        counts += f' · <span style="color:var(--fail)">{n_fail} failed</span>'
    out = [CSS_AND_HEAD,
           "<header>\n"
           f"  <h1>{TITLE}</h1>\n"
           f'  <span class="mute">{e(host)} · {now.astimezone(LOCAL_TZ):%H:%M:%S}</span>\n'
           f'  <span class="mute">{counts}</span>\n'
           '  <span class="zoom"><button onclick="setZoom(z - 0.1)">−</button>'
           '<button id="zl" onclick="setZoom(1)" title="reset">100%</button>'
           '<button onclick="setZoom(z + 0.1)">+</button></span>\n'
           "  <script>setZoom(z)</script>\n"
           "</header>\n",
           "<h2>CHECKS</h2>\n"]

    rc = checks["receiver"]
    rs = status.get("receiver") or {}
    started = parse_iso(rs.get("started_at"))
    up = fmt_age((now - started).total_seconds()) if rc["ok"] and started else ""
    out.append(row("RUNNING" if rc["ok"] else "STOPPED", rc["ok"], "receiver", up,
                   rc["reason"] or f"pid {rs.get('pid')}"))

    for key, c in checks.items():
        if not key.startswith("feed:"):
            continue
        feed = key[5:]
        nums = (f"{fmt_bytes(c['size'])} · {fmt_age(c['age_s'])} ago" if c["file"] else "")
        out.append(row("RUNNING" if c["ok"] else "FAILED", c["ok"], landing_dir(feed), nums,
                       c["reason"] or c["file"]))

    d = checks["disk"]
    out.append(row("OK" if d["ok"] else "FAILED", d["ok"], "disk",
                   f"{d['used_pct']}% · {fmt_bytes(d['free_bytes'])} free",
                   d["reason"] or "landing volume"))

    out.append('<div class="health">\n')
    if n_fail:
        out.append('  <h2>PROBLEMS</h2>\n  <div class="kv">\n')
        for c in checks.values():
            if not c["ok"]:
                out.append(f'    <span class="error">error</span><span>{e(c["reason"])}</span>\n')
        out.append("  </div>\n")

    out.append(f'  <h2>LAST 24H · <a href="{DASHBOARD}">DASHBOARD</a></h2>\n')
    for panel_id, title, height in PANELS:
        out.append(f'  <iframe class="panel" title="{e(title)}" loading="lazy" style="height:{height}px"'
                   f' data-src="{e(PANEL_URL.format(panel_id))}"></iframe>\n')
    # Grafana's own theme follows the device, like the page colors.
    out.append('  <script>var t = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";\n'
               '  document.querySelectorAll("iframe[data-src]").forEach(f => f.src = f.dataset.src + "&theme=" + t);'
               '</script>\n')

    out.append('  <h2>SUBSCRIPTIONS</h2>\n  <div class="kv">\n')
    if not rs:
        out.append('    <span class="mute">state</span><span>unknown</span>\n')
    elif not rs.get("subscribing"):
        out.append('    <span class="mute">mode</span><span class="warn">receive only (no public url)</span>\n')
    for feed, s in (rs.get("feeds") or {}).items():
        last = parse_iso(s.get("last_push"))
        sub = parse_iso(s.get("subscribed_at"))
        exp = parse_iso(s.get("expires_at"))
        parts = [f"last push {fmt_age((now - last).total_seconds())} ago" if last else "no push yet"]
        if sub:
            parts.append(f"subscribed {fmt_age((now - sub).total_seconds())} ago")
        if exp:
            parts.append(f"expires {exp.astimezone(LOCAL_TZ):%Y-%m-%d %H:%M}")
        cls = ' class="warn"' if s.get("last_error") else ""
        out.append(f'    <span class="mute">{e(feed)}</span><span{cls}>{e(" · ".join(parts))}</span>\n')
        if s.get("last_error"):
            out.append(f'    <span class="mute"></span><span class="warn">{e(s["last_error"])}</span>\n')
    out.append("  </div>\n")

    out.append('  <h2>INFO</h2>\n  <div class="kv">\n'
               f'    <span class="mute">state</span><span class="{"ok" if status["ok"] else "error"}">'
               f'{"OK" if status["ok"] else "FAIL"}</span>\n'
               f'    <span class="mute">since</span><span>{e(status["since"])}</span>\n'
               f'    <span class="mute">checked</span><span>{e(status["checked_at"])}</span>\n'
               "  </div>\n</div>\n</main></body></html>\n")
    return "".join(out)


def write_atomic(path, text):
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def main():
    landing = Path(os.environ.get("DAT535_LANDING_DIR", "landing"))
    state_file = os.environ.get("RECEIVER_STATE_FILE", "run/receiver.json")
    feeds = feeds_from_env()
    status_dir = Path(os.environ.get("MONITOR_STATUS_DIR", "status"))
    max_age = float(os.environ.get("MONITOR_MAX_PART_AGE_MIN", "5"))
    disk_max = float(os.environ.get("MONITOR_DISK_MAX_PCT", "90"))
    topic = os.environ.get("NTFY_TOPIC")
    server = os.environ.get("NTFY_SERVER", "https://ntfy.sh")
    click_url = os.environ.get("STATUS_PUBLIC_URL")
    host = socket.gethostname()

    status_dir.mkdir(parents=True, exist_ok=True)
    status_file = status_dir / "status.json"
    try:
        prev = json.loads(status_file.read_text())
    except (FileNotFoundError, ValueError):
        prev = {}

    now = datetime.now(timezone.utc)
    checks = {}
    checks["receiver"], receiver_state = check_receiver(state_file)
    prev_checks = prev.get("checks", {})
    for f in feeds:
        checks[f"feed:{f}"] = check_feed(landing, f, prev_checks.get(f"feed:{f}"), now, max_age)
    checks["disk"] = check_disk(landing, disk_max)

    ok = all(c["ok"] for c in checks.values())
    reason = "; ".join(c["reason"] for c in checks.values() if not c["ok"])
    checked_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    since = prev.get("since") if prev.get("ok") == ok and prev.get("since") else checked_at

    # Notify on change of state relative to the last state we successfully notified about.
    # First run: only notify if failing.
    notified = prev.get("notified_ok", True)
    if topic and ok != notified:
        try:
            if ok:
                start = parse_iso(prev.get("since")) if prev.get("ok") is False else None
                body = f"recovered after {fmt_age((now - start).total_seconds())}" if start else "recovered"
            else:
                body = "\n".join(c["short"] for c in checks.values() if not c["ok"])
            notify(server, topic, ok, body, click_url)
            notified = ok
        except (urllib.error.URLError, OSError) as e:
            print(f"notify failed: {e}", file=sys.stderr)

    status = {"ok": ok, "reason": reason or None, "since": since, "checked_at": checked_at,
              "notified_ok": notified, "checks": checks,
              "receiver": {k: receiver_state.get(k) for k in ("pid", "started_at", "subscribing", "feeds")}
              if receiver_state else None}
    write_atomic(status_file, json.dumps(status, indent=1))
    write_atomic(status_dir / "index.html", render(status, host))

    if not ok:
        print(f"FAIL: {reason}")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
