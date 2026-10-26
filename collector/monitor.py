#!/usr/bin/env python3
"""Read-only collection monitor.

Checks the receiver, the DATEX poller and the landing zone (SIRI feeds, vehicle positions,
DATEX), writes a static status page (index.html + status.json) and sends an ntfy push on
state change only (OK<->FAIL).
Never writes to the landing zone. Exit code 1 plus a one-line reason on failure.

Environment:
    DAT535_LANDING_DIR      landing root (default: ./landing)
    RECEIVER_STATE_FILE     written by the receiver (default: ./run/receiver.json)
    DATEX_STATE_FILE        written by the DATEX poller (default: ./run/datex.json)
    MONITOR_FEEDS           feeds to check (default: the receiver's RECEIVER_FEEDS, vehicles and
                            datex-<feed> for each of the poller's DATEX_FEEDS)
    RECEIVER_FEEDS          the receiver's SIRI feeds (default: et,sx)
    DATEX_FEEDS             the DATEX poller's feeds (default: all of datex.FEEDS)
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

from datex import FEEDS as DATEX_FEEDS

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
SIRI_FEEDS = ("et", "sx")       # receiver.py's FEED_PATHS
DATEX_PREFIX = "datex-"


def feeds_from_env():
    """Feeds to check or report: MONITOR_FEEDS, else the receiver's SIRI feeds, vehicles and
    datex-<feed> for each DATEX feed."""
    raw = os.environ.get("MONITOR_FEEDS")
    if not raw:
        datex = os.environ.get("DATEX_FEEDS", ",".join(DATEX_FEEDS))
        raw = ",".join([os.environ.get("RECEIVER_FEEDS", "et,sx"), VEHICLES]
                       + [DATEX_PREFIX + f.strip() for f in datex.split(",") if f.strip()])
    return [f.strip() for f in raw.split(",") if f.strip()]


def landing_dir(feed):
    """Landing directory of a feed: siri-<feed> for the receiver's SIRI feeds; any other feed is
    its own directory (vehicles from vehicles.py, datex-<feed> from datex.py)."""
    return f"siri-{feed}" if feed in SIRI_FEEDS else feed


def newest_file(landing, feed):
    """Most recently written landing file of a feed: this hour's .part or, right after the hour
    turned and before the next push, the hour just closed. Only the last two days are looked at."""
    days = sorted((Path(landing) / landing_dir(feed)).glob("date=*"))[-2:]
    files = [p for d in days for p in d.glob("hour=*.jsonl*") if not p.name.endswith(".tmp")]
    return max(files, key=lambda p: p.stat().st_mtime, default=None)


def check_feed(landing, feed, prev, now, max_age_min):
    path = newest_file(landing, feed)
    if path is None:
        return {"ok": False, "reason": f"{feed}: nothing in the landing zone yet",
                "short": f"{feed}: no data yet", "file": None, "size": None, "age_s": None}
    st = path.stat()
    rel = f"{path.parent.name}/{path.name}"
    age = now.timestamp() - st.st_mtime
    res = {"ok": True, "reason": None, "short": None, "file": rel, "size": st.st_size, "age_s": round(age)}
    stale = age > max_age_min * 60
    if stale or (prev and prev.get("file") == rel and prev.get("size") is not None
                 and st.st_size <= prev["size"]):
        what = "pushes" if feed != VEHICLES else "positions"
        res.update(ok=False, reason=f"{feed}: no {what} received for {fmt_age(age)} (last write to {rel})",
                   short=f"{feed}: no data for {fmt_age(age)}")
    return res


def check_datex(state_file):
    """The DATEX poller: running, and polling with credentials NPRA accepts. Without credentials
    it is idle, which is a choice rather than a failure: no check fails, its feeds are skipped."""
    try:
        state = json.loads(Path(state_file).read_text())
    except FileNotFoundError:
        return {"ok": False, "reason": "datex poller not running (no state file)",
                "short": "datex not running"}, None
    except (OSError, ValueError) as e:
        return {"ok": False, "reason": f"datex state unreadable: {e}",
                "short": "datex state unreadable"}, None
    if not pid_alive(int(state.get("pid", 0))):
        return {"ok": False, "reason": f"datex pid {state.get('pid')} not alive",
                "short": "datex not running"}, state
    if not state.get("polling"):
        return {"ok": True, "reason": None, "short": None, "alive": True, "idle": state.get("error")}, state
    if state.get("error"):
        return {"ok": False, "reason": f"datex: {state['error']}", "short": "datex: login rejected",
                "alive": True}, state
    return {"ok": True, "reason": None, "short": None, "alive": True}, state


def check_datex_feed(landing, feed, s, started, now, max_age_min):
    """One polled DATEX feed. Every poll writes a line, so its last successful poll and the
    newest landing write must both be recent for its poll interval; and where NPRA promises a
    cadence (stale_after_s), the snapshot must keep changing."""
    res = {"ok": True, "reason": None, "short": None, "file": None, "size": None, "age_s": None}
    path = newest_file(landing, feed)
    if path:
        st = path.stat()
        res.update(file=f"{path.parent.name}/{path.name}", size=st.st_size,
                   age_s=round(now.timestamp() - st.st_mtime))
    limit = max(max_age_min * 60, 2 * (s.get("interval_s") or 60) + 60)    # one missed poll is fine
    last_ok, last_change = parse_iso(s.get("last_ok")), parse_iso(s.get("last_change"))
    if last_ok is None and not s.get("errors") and started and (now - started).total_seconds() < limit:
        return res                                                          # first poll pending
    ok_age = (now - last_ok).total_seconds() if last_ok else None
    if ok_age is None or ok_age > limit:
        why = f": {s['last_error']}" if s.get("errors") else ""
        since = f"for {fmt_age(ok_age)}" if ok_age is not None else "yet"
        res.update(ok=False, reason=f"{feed}: no successful poll {since}{why}",
                   short=f"{feed}: no data {since}")
    elif res["age_s"] is None or res["age_s"] > limit:
        res.update(ok=False, reason=f"{feed}: polls are not reaching the landing zone"
                                    + (f" (last write {fmt_age(res['age_s'])} ago)" if path else ""),
                   short=f"{feed}: not writing")
    elif s.get("stale_after_s") and last_change and (now - last_change).total_seconds() > s["stale_after_s"]:
        age = (now - last_change).total_seconds()
        res.update(ok=False, reason=f"{feed}: NPRA has published nothing new for {fmt_age(age)}"
                                    f" (every poll returns the same snapshot)", short=f"{feed}: source stale for {fmt_age(age)}")
    return res


def fail_body(checks):
    """Notification text: one line per failed check, plus a hint when every feed stopped at once."""
    lines = [c["short"] for c in checks.values() if not c["ok"]]
    feeds = [c for k, c in checks.items() if k.startswith("feed:")]
    if len(feeds) > 1 and checks["receiver"]["ok"] and not any(c["ok"] for c in feeds):
        lines.append("all feeds stopped together: likely the VM network or the tunnel, not Entur")
    return "\n".join(lines)


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


def row(state_word, ok, name, nums, desc, cls=None):
    e = html.escape
    return (f'<section class="row {cls or ("active" if ok else "failed")}">\n'
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

    if "datex" in checks:
        dc = checks["datex"]
        ds = status.get("datex") or {}
        started = parse_iso(ds.get("started_at"))
        up = fmt_age((now - started).total_seconds()) if dc.get("alive") and started else ""
        if dc.get("idle"):
            out.append(row("IDLE", True, "datex", up, f"{dc['idle']}: polling nothing", cls="idle"))
        else:
            word = "RUNNING" if dc["ok"] else "FAILED" if dc.get("alive") else "STOPPED"
            out.append(row(word, dc["ok"], "datex", up, dc["reason"] or f"pid {ds.get('pid')}"))

    for key, c in checks.items():
        if not key.startswith("feed:"):
            continue
        feed = key[5:]
        nums = (f"{fmt_bytes(c['size'])} · {fmt_age(c['age_s'])} ago" if c["file"] else "")
        out.append(row("RUNNING" if c["ok"] else "FAILED", c["ok"], landing_dir(feed), nums,
                       c["reason"] or c["file"] or "no landing file yet"))

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
        # last_error belongs to the last subscribe attempt; pushes after it mean the
        # subscription survived, so the error is history rather than a current problem.
        err = s.get("last_error")
        attempt = parse_iso(s.get("last_attempt"))
        current = err and not (last and attempt and last > attempt)
        cls = ' class="warn"' if current else ""
        out.append(f'    <span class="mute">{e(feed)}</span><span{cls}>{e(" · ".join(parts))}</span>\n')
        if current:
            out.append(f'    <span class="mute"></span><span class="warn">{e(err)}</span>\n')
        elif err:
            when = f"{attempt.astimezone(LOCAL_TZ):%H:%M} " if attempt else ""
            out.append(f'    <span class="mute"></span><span class="mute">resubscribe {e(when)}failed'
                       f' ({e(err)}); pushes resumed since</span>\n')
    out.append("  </div>\n")

    ds = status.get("datex") or {}
    if ds.get("polling"):
        out.append('  <h2>POLLING · DATEX</h2>\n  <div class="kv">\n')
        for feed, s in (ds.get("feeds") or {}).items():
            iv = s.get("interval_s") or 0
            every = f"{iv // 3600}h" if iv % 3600 == 0 else f"{iv // 60}m" if iv % 60 == 0 else f"{iv}s"
            last_ok, change = parse_iso(s.get("last_ok")), parse_iso(s.get("last_change"))
            parts = [f"every {every}",
                     f"last ok {fmt_age((now - last_ok).total_seconds())} ago" if last_ok else "no successful poll yet"]
            if change:
                parts.append(f"changed {fmt_age((now - change).total_seconds())} ago")
            if s.get("last_bytes"):
                parts.append(fmt_bytes(s["last_bytes"]))
            cls = ' class="warn"' if s.get("errors") else ""
            out.append(f'    <span class="mute">{e(feed)}</span><span{cls}>{e(" · ".join(parts))}</span>\n')
            err, at = s.get("last_error"), parse_iso(s.get("last_error_at"))
            if s.get("errors"):
                out.append(f'    <span class="mute"></span><span class="warn">{e(err)}'
                           f' ({s["errors"]} in a row)</span>\n')
            elif err:
                when = f"{at.astimezone(LOCAL_TZ):%H:%M} " if at else ""
                out.append(f'    <span class="mute"></span><span class="mute">failed {e(when)}({e(err)});'
                           f' polls succeed since</span>\n')
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
    datex_state_file = os.environ.get("DATEX_STATE_FILE", "run/datex.json")
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
    datex_state = None
    if any(f.startswith(DATEX_PREFIX) for f in feeds):
        checks["datex"], datex_state = check_datex(datex_state_file)
    prev_checks = prev.get("checks", {})
    for f in feeds:
        key = f"feed:{f}"
        if not f.startswith(DATEX_PREFIX):
            checks[key] = check_feed(landing, f, prev_checks.get(key), now, max_age)
        elif not checks["datex"].get("idle"):
            ds = datex_state or {}
            checks[key] = check_datex_feed(landing, f, (ds.get("feeds") or {}).get(f[len(DATEX_PREFIX):]) or {},
                                           parse_iso(ds.get("started_at")), now, max_age)
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
                body = fail_body(checks)
            notify(server, topic, ok, body, click_url)
            notified = ok
        except (urllib.error.URLError, OSError) as e:
            print(f"notify failed: {e}", file=sys.stderr)

    status = {"ok": ok, "reason": reason or None, "since": since, "checked_at": checked_at,
              "notified_ok": notified, "checks": checks,
              "receiver": {k: receiver_state.get(k) for k in ("pid", "started_at", "subscribing", "feeds")}
              if receiver_state else None,
              "datex": {k: datex_state.get(k) for k in ("pid", "started_at", "polling", "error", "feeds")}
              if datex_state else None}
    write_atomic(status_file, json.dumps(status, indent=1))
    write_atomic(status_dir / "index.html", render(status, host))

    if not ok:
        print(f"FAIL: {reason}")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
