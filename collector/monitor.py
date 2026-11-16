#!/usr/bin/env python3
"""Read-only collection monitor.

Checks the receiver, the DATEX poller, the landing zone and the disks; writes a static status
page (index.html + status.json); sends an ntfy push when the state flips (OK <-> FAIL).
Never writes to the landing zone. Exit code 1 plus a one-line reason on failure.

Environment (defaults in brackets):
    DAT535_LANDING_DIR [landing]   RECEIVER_STATE_FILE, DATEX_STATE_FILE [run/*.json]
    MONITOR_FEEDS [RECEIVER_FEEDS + vehicles + datex-<DATEX_FEEDS>]
    MONITOR_STATIC [netex,nsr]     MONITOR_STATIC_MAX_AGE_H [30]
    MONITOR_STATUS_DIR [status]    MONITOR_MAX_PART_AGE_MIN [5]   MONITOR_DISK_MAX_PCT [90]
    NTFY_TOPIC (unset: no pushes; long and random, it is the secret)   NTFY_SERVER [ntfy.sh]
    STATUS_PUBLIC_URL (opened when a notification is tapped)
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

# Grafana panels on the main page: (panel id in dashboards/collector.json, title, height px).
# serve_status.py proxies /grafana/ to Grafana.
DASHBOARD = "/grafana/d/dat535-collector/dat535-collector"
PANELS = [(8, "ingest rate", 240), (9, "stored on disk", 240), (10, "per day", 300)]
PANEL_URL = (
    "/grafana/d-solo/dat535-collector/dat535-collector?orgId=1&panelId={}"
    "&from=now-24h&to=now&refresh=1m&hideLogo=true"
)

VEHICLES = "vehicles"
SIRI_FEEDS = ("et", "sx")  # receiver.py's FEED_PATHS
DATEX_PREFIX = "datex-"


# --- Small helpers ---


def result(ok=True, reason=None, short=None, **extra):
    """One check's result. `reason` goes on the page, `short` into the notification."""
    return {"ok": ok, "reason": reason, "short": short, **extra}


def failed(reason, short, **extra):
    return result(False, reason, short, **extra)


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def parse_iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


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


# --- Which feeds and files ---


def feeds_from_env():
    """MONITOR_FEEDS, else the receiver's SIRI feeds, vehicles and datex-<feed> per DATEX feed."""
    raw = os.environ.get("MONITOR_FEEDS")
    if not raw:
        datex = os.environ.get("DATEX_FEEDS", ",".join(DATEX_FEEDS)).split(",")
        datex = [DATEX_PREFIX + f.strip() for f in datex if f.strip()]
        raw = ",".join([os.environ.get("RECEIVER_FEEDS", "et,sx"), VEHICLES] + datex)
    return [f.strip() for f in raw.split(",") if f.strip()]


def landing_dir(feed):
    """siri-<feed> for the receiver's feeds; any other feed is its own folder name."""
    return f"siri-{feed}" if feed in SIRI_FEEDS else feed


def newest_file(landing, feed):
    """A feed's most recently written landing file (this hour's .part, or the hour just closed)."""
    days = sorted((Path(landing) / landing_dir(feed)).glob("date=*"))[-2:]
    files = [p for d in days for p in d.glob("hour=*.jsonl*") if not p.name.endswith(".tmp")]
    return max(files, key=lambda p: p.stat().st_mtime, default=None)


def static_from_env():
    """Daily downloads of static.py to check: MONITOR_STATIC, else NeTEx and NSR."""
    raw = os.environ.get("MONITOR_STATIC", "netex,nsr")
    return [d.strip() for d in raw.split(",") if d.strip()]


def archived(path):
    """Is this file the download itself (not a .part, .tmp or the .meta.json beside it)?"""
    return not path.name.endswith((".part", ".json", ".tmp"))


def newest_static(landing, dataset):
    """Newest complete archived file of a daily download: <dataset>/date=*/<file>."""
    for day in sorted((Path(landing) / dataset).glob("date=*"), reverse=True):
        files = [p for p in day.iterdir() if p.is_file() and archived(p)]
        if files:
            return max(files, key=lambda p: p.stat().st_mtime)
    return None


# --- Checks ---


def read_state(state_file, name, short):
    """A collector's state file. Returns (failed result or None, state or None)."""
    try:
        state = json.loads(Path(state_file).read_text())
    except FileNotFoundError:
        return failed(f"{name} not running (no state file)", f"{short} not running"), None
    except (OSError, ValueError) as e:
        return failed(f"{short} state unreadable: {e}", f"{short} state unreadable"), None
    if not pid_alive(int(state.get("pid", 0))):
        return failed(f"{short} pid {state.get('pid')} not alive", f"{short} not running"), state
    return None, state


def check_receiver(state_file):
    problem, state = read_state(state_file, "receiver", "receiver")
    return problem or result(), state


def check_datex(state_file):
    """The poller runs and NPRA accepts its credentials. Idle (no credentials) is not a failure."""
    problem, state = read_state(state_file, "datex poller", "datex")
    if problem:
        return problem, state
    if not state.get("polling"):
        return result(alive=True, idle=state.get("error")), state
    if state.get("error"):
        return failed(f"datex: {state['error']}", "datex: login rejected", alive=True), state
    return result(alive=True), state


def file_info(path, now):
    st = path.stat()
    return dict(
        file=f"{path.parent.name}/{path.name}",
        size=st.st_size,
        age_s=round(now.timestamp() - st.st_mtime),
    )


def check_feed(landing, feed, prev, now, max_age_min):
    """A pushed feed (SIRI, vehicles): its newest file must have grown recently."""
    path = newest_file(landing, feed)
    if path is None:
        return failed(
            f"{feed}: nothing in the landing zone yet",
            f"{feed}: no data yet",
            file=None, size=None, age_s=None,
        )
    st = path.stat()
    rel = f"{path.parent.name}/{path.name}"
    age = now.timestamp() - st.st_mtime
    res = result(file=rel, size=st.st_size, age_s=round(age))

    # Too old, or no bigger than at the last run.
    stale = age > max_age_min * 60
    same = prev and prev.get("file") == rel and prev.get("size") is not None
    not_grown = same and st.st_size <= prev["size"]
    if stale or not_grown:
        what = "pushes" if feed != VEHICLES else "positions"
        res.update(
            ok=False,
            reason=f"{feed}: no {what} received for {fmt_age(age)} (last write to {rel})",
            short=f"{feed}: no data for {fmt_age(age)}",
        )
    return res


def check_datex_feed(landing, feed, s, started, now, max_age_min):
    """A polled DATEX feed: recent good polls, recent landing writes, and a changing snapshot."""
    res = result(file=None, size=None, age_s=None)
    path = newest_file(landing, feed)
    if path:
        res.update(file_info(path, now))

    # One missed poll is fine.
    limit = max(max_age_min * 60, 2 * (s.get("interval_s") or 60) + 60)
    last_ok, last_change = parse_iso(s.get("last_ok")), parse_iso(s.get("last_change"))
    just_started = started and (now - started).total_seconds() < limit
    if last_ok is None and not s.get("errors") and just_started:
        return res  # first poll still pending

    ok_age = (now - last_ok).total_seconds() if last_ok else None
    change_age = (now - last_change).total_seconds() if last_change else None
    stale_after = s.get("stale_after_s")

    if ok_age is None or ok_age > limit:
        why = f": {s['last_error']}" if s.get("errors") else ""
        since = f"for {fmt_age(ok_age)}" if ok_age is not None else "yet"
        res.update(
            ok=False,
            reason=f"{feed}: no successful poll {since}{why}",
            short=f"{feed}: no data {since}",
        )
    elif res["age_s"] is None or res["age_s"] > limit:
        last_write = f" (last write {fmt_age(res['age_s'])} ago)" if path else ""
        res.update(
            ok=False,
            reason=f"{feed}: polls are not reaching the landing zone{last_write}",
            short=f"{feed}: not writing",
        )
    # Where NPRA promises a cadence, the snapshot must keep changing.
    elif stale_after and change_age is not None and change_age > stale_after:
        res.update(
            ok=False,
            reason=(
                f"{feed}: NPRA has published nothing new for {fmt_age(change_age)}"
                " (every poll returns the same snapshot)"
            ),
            short=f"{feed}: source stale for {fmt_age(change_age)}",
        )
    return res


def check_static(landing, dataset, now, max_age_h):
    """A daily download (static.py): its newest file must be from the last max_age_h hours."""
    path = newest_static(landing, dataset)
    if path is None:
        return failed(
            f"{dataset}: nothing archived yet",
            f"{dataset}: not archived yet",
            file=None, size=None, age_s=None,
        )
    res = result(**file_info(path, now))
    age = now.timestamp() - path.stat().st_mtime
    if age > max_age_h * 3600:
        res.update(
            ok=False,
            reason=f"{dataset}: last archived {fmt_age(age)} ago ({res['file']})",
            short=f"{dataset}: not archived for {fmt_age(age)}",
        )
    return res


def check_disk(path, max_pct, name="disk"):
    u = shutil.disk_usage(path)
    pct = 100 * u.used / u.total
    res = result(pct < max_pct, used_pct=round(pct, 1), free_bytes=u.free)
    if not res["ok"]:
        res["reason"] = f"{name} {pct:.1f}% used (limit {max_pct}%)"
        res["short"] = f"{name} {pct:.0f}% full"
    return res


# --- Notifications ---


def fail_body(checks):
    """Notification text: one line per failed check, plus a hint when every feed stopped at once."""
    lines = [c["short"] for c in checks.values() if not c["ok"]]
    feeds = [c for k, c in checks.items() if k.startswith("feed:")]
    if len(feeds) > 1 and checks["receiver"]["ok"] and not any(c["ok"] for c in feeds):
        lines.append("all feeds stopped together: likely the VM network or the tunnel, not Entur")
    return "\n".join(lines)


def notify(server, topic, ok, body, click_url):
    headers = {
        "Title": f"Collector {'OK' if ok else 'FAIL'}",
        "Priority": "default" if ok else "high",
        "Tags": "white_check_mark" if ok else "rotating_light",
    }
    if click_url:
        headers["Click"] = click_url
    req = urllib.request.Request(
        f"{server.rstrip('/')}/{topic}",
        data=body.encode("utf-8"),
        method="POST",
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


# --- The status page ---

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
header {
  display: flex; flex-wrap: wrap; gap: 4px 18px; align-items: baseline; margin-bottom: 36px;
}
h1 { font-size: 14px; letter-spacing: .18em; margin: 0; }
h2 {
  font-size: 12px; font-weight: 400; letter-spacing: .18em; color: var(--mute); margin: 0 0 10px;
}
.mute { color: var(--mute); }
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
button {
  font: inherit; font-size: 12px; letter-spacing: .06em; text-transform: uppercase;
  color: var(--fg); background: none; border: 0; padding: 6px 8px; cursor: pointer;
  text-decoration: none;
}
button:hover { text-decoration: underline; }
@media (max-width: 520px) {
  .top { grid-template-columns: 1fr auto; }
  .top .st { grid-column: 1 / -1; }
  .desc { margin-left: 0; }
}
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
    cls = cls or ("active" if ok else "failed")
    return (
        f'<section class="row {cls}">\n'
        f'  <div class="top">\n'
        f'    <span class="st">{e(state_word)}</span>\n'
        f'    <span><span class="name">{e(name)}</span></span>\n'
        f'    <span class="nums">{e(nums)}</span>\n'
        f"  </div>\n"
        f'  <div class="desc">{e(desc)}</div>\n'
        f"</section>\n"
    )


def kv(key, value, cls=None):
    """One key/value line of a .kv grid. Both are HTML-escaped."""
    e = html.escape
    attr = f' class="{cls}"' if cls else ""
    return f'    <span class="mute">{e(key)}</span><span{attr}>{e(value)}</span>\n'


def ago(now, then):
    return fmt_age((now - then).total_seconds())


def render_header(host, now, n_ok, n_fail):
    e = html.escape
    counts = f"{n_ok} ok"
    if n_fail:
        counts += f' · <span style="color:var(--fail)">{n_fail} failed</span>'
    return (
        "<header>\n"
        f"  <h1>{TITLE}</h1>\n"
        f'  <span class="mute">{e(host)} · {now.astimezone(LOCAL_TZ):%H:%M:%S}</span>\n'
        f'  <span class="mute">{counts}</span>\n'
        '  <span class="zoom"><button onclick="setZoom(z - 0.1)">−</button>'
        '<button id="zl" onclick="setZoom(1)" title="reset">100%</button>'
        '<button onclick="setZoom(z + 0.1)">+</button></span>\n'
        "  <script>setZoom(z)</script>\n"
        "</header>\n"
    )


def render_checks(status, now):
    """One row per check: the processes, the feeds, the daily downloads and the disks."""
    checks = status["checks"]
    out = ["<h2>CHECKS</h2>\n"]

    rc = checks["receiver"]
    rs = status.get("receiver") or {}
    started = parse_iso(rs.get("started_at"))
    up = ago(now, started) if rc["ok"] and started else ""
    word = "RUNNING" if rc["ok"] else "STOPPED"
    out.append(row(word, rc["ok"], "receiver", up, rc["reason"] or f"pid {rs.get('pid')}"))

    if "datex" in checks:
        dc = checks["datex"]
        ds = status.get("datex") or {}
        started = parse_iso(ds.get("started_at"))
        up = ago(now, started) if dc.get("alive") and started else ""
        if dc.get("idle"):
            out.append(row("IDLE", True, "datex", up, f"{dc['idle']}: polling nothing", cls="idle"))
        else:
            word = "RUNNING" if dc["ok"] else "FAILED" if dc.get("alive") else "STOPPED"
            out.append(row(word, dc["ok"], "datex", up, dc["reason"] or f"pid {ds.get('pid')}"))

    def size_and_age(c):
        return f"{fmt_bytes(c['size'])} · {fmt_age(c['age_s'])} ago" if c["file"] else ""

    for key, c in checks.items():
        if key.startswith("feed:"):
            word = "RUNNING" if c["ok"] else "FAILED"
            desc = c["reason"] or c["file"] or "no landing file yet"
            out.append(row(word, c["ok"], landing_dir(key[5:]), size_and_age(c), desc))

    for key, c in checks.items():
        if key.startswith("static:"):
            word = "OK" if c["ok"] else "FAILED"
            desc = c["reason"] or f"daily archive, {c['file']}"
            out.append(row(word, c["ok"], key[7:], size_and_age(c), desc))

    disks = (
        ("disk", "data volume: landing, Bronze, Silver, Gold"),
        ("root-disk", "root volume: /tmp (Spark spill), Prometheus, the runner"),
    )
    for key, desc in disks:
        d = checks.get(key)
        if d:
            nums = f"{d['used_pct']}% · {fmt_bytes(d['free_bytes'])} free"
            out.append(row("OK" if d["ok"] else "FAILED", d["ok"], key, nums, d["reason"] or desc))
    return out


def render_panels():
    """The embedded Grafana panels."""
    e = html.escape
    out = [f'  <h2>LAST 24H · <a href="{DASHBOARD}">DASHBOARD</a></h2>\n']
    for panel_id, title, height in PANELS:
        out.append(
            f'  <iframe class="panel" title="{e(title)}" loading="lazy" style="height:{height}px"'
            f' data-src="{e(PANEL_URL.format(panel_id))}"></iframe>\n'
        )
    # Grafana's own theme follows the device, like the page colors.
    out.append(
        '  <script>var t = matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";\n'
        '  document.querySelectorAll("iframe[data-src]")'
        '.forEach(f => f.src = f.dataset.src + "&theme=" + t);'
        "</script>\n"
    )
    return out


def render_subscriptions(rs, now):
    """The receiver's Entur subscriptions: last push, age, expiry, last error."""
    out = ['  <h2>SUBSCRIPTIONS</h2>\n  <div class="kv">\n']
    if not rs:
        out.append(kv("state", "unknown"))
    elif not rs.get("subscribing"):
        out.append(kv("mode", "receive only (no public url)", "warn"))

    for feed, s in (rs.get("feeds") or {}).items():
        last = parse_iso(s.get("last_push"))
        sub = parse_iso(s.get("subscribed_at"))
        exp = parse_iso(s.get("expires_at"))
        parts = [f"last push {ago(now, last)} ago" if last else "no push yet"]
        if sub:
            parts.append(f"subscribed {ago(now, sub)} ago")
        if exp:
            parts.append(f"expires {exp.astimezone(LOCAL_TZ):%Y-%m-%d %H:%M}")

        # last_error is from the last subscribe attempt. Pushes since then mean the
        # subscription survived: the error is history, not a current problem.
        err = s.get("last_error")
        attempt = parse_iso(s.get("last_attempt"))
        current = err and not (last and attempt and last > attempt)
        out.append(kv(feed, " · ".join(parts), "warn" if current else None))
        if current:
            out.append(kv("", err, "warn"))
        elif err:
            when = f"{attempt.astimezone(LOCAL_TZ):%H:%M} " if attempt else ""
            out.append(kv("", f"resubscribe {when}failed ({err}); pushes resumed since", "mute"))
    out.append("  </div>\n")
    return out


def render_polling(ds, now):
    """The DATEX poller's feeds: interval, last good poll, last change, errors."""
    out = ['  <h2>POLLING · DATEX</h2>\n  <div class="kv">\n']
    for feed, s in (ds.get("feeds") or {}).items():
        iv = s.get("interval_s") or 0
        if iv % 3600 == 0:
            every = f"{iv // 3600}h"
        elif iv % 60 == 0:
            every = f"{iv // 60}m"
        else:
            every = f"{iv}s"
        last_ok, change = parse_iso(s.get("last_ok")), parse_iso(s.get("last_change"))
        parts = [f"every {every}"]
        parts.append(f"last ok {ago(now, last_ok)} ago" if last_ok else "no successful poll yet")
        if change:
            parts.append(f"changed {ago(now, change)} ago")
        if s.get("last_bytes"):
            parts.append(fmt_bytes(s["last_bytes"]))
        out.append(kv(feed, " · ".join(parts), "warn" if s.get("errors") else None))

        err, at = s.get("last_error"), parse_iso(s.get("last_error_at"))
        if s.get("errors"):
            out.append(kv("", f"{err} ({s['errors']} in a row)", "warn"))
        elif err:
            when = f"{at.astimezone(LOCAL_TZ):%H:%M} " if at else ""
            out.append(kv("", f"failed {when}({err}); polls succeed since", "mute"))
    out.append("  </div>\n")
    return out


def render(status, host):
    now = parse_iso(status["checked_at"])
    checks = status["checks"]
    n_fail = sum(1 for c in checks.values() if not c["ok"])
    out = [CSS_AND_HEAD, render_header(host, now, len(checks) - n_fail, n_fail)]
    out += render_checks(status, now)

    out.append('<div class="health">\n')
    if n_fail:
        out.append('  <h2>PROBLEMS</h2>\n  <div class="kv">\n')
        for c in checks.values():
            if not c["ok"]:
                reason = html.escape(c["reason"])
                out.append(f'    <span class="error">error</span><span>{reason}</span>\n')
        out.append("  </div>\n")
    out += render_panels()
    out += render_subscriptions(status.get("receiver") or {}, now)
    ds = status.get("datex") or {}
    if ds.get("polling"):
        out += render_polling(ds, now)

    state = "OK" if status["ok"] else "FAIL"
    out.append('  <h2>INFO</h2>\n  <div class="kv">\n')
    out.append(kv("state", state, "ok" if status["ok"] else "error"))
    out.append(kv("since", status["since"]))
    out.append(kv("checked", status["checked_at"]))
    out.append("  </div>\n</div>\n</main></body></html>\n")
    return "".join(out)


def write_atomic(path, text):
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# --- Main ---


def run_checks(landing, feeds, prev_checks, now, max_age, disk_max, state_file, datex_state_file):
    """Every check. Returns (checks, receiver state, datex state)."""
    checks = {}
    checks["receiver"], receiver_state = check_receiver(state_file)
    datex_state = None
    if any(f.startswith(DATEX_PREFIX) for f in feeds):
        checks["datex"], datex_state = check_datex(datex_state_file)

    for f in feeds:
        key = f"feed:{f}"
        if not f.startswith(DATEX_PREFIX):
            checks[key] = check_feed(landing, f, prev_checks.get(key), now, max_age)
        elif not checks["datex"].get("idle"):
            # An idle poller (no credentials) skips its feeds.
            ds = datex_state or {}
            feed_state = (ds.get("feeds") or {}).get(f[len(DATEX_PREFIX):]) or {}
            started = parse_iso(ds.get("started_at"))
            checks[key] = check_datex_feed(landing, f, feed_state, started, now, max_age)

    static_max_age = float(os.environ.get("MONITOR_STATIC_MAX_AGE_H", "30"))
    for d in static_from_env():
        checks[f"static:{d}"] = check_static(landing, d, now, static_max_age)

    checks["disk"] = check_disk(landing, disk_max)
    # Spark spills to /tmp on the small root volume; a full one failed Bronze on 2026-10-24.
    checks["root-disk"] = check_disk("/", disk_max, "root disk")
    return checks, receiver_state, datex_state


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
    checks, receiver_state, datex_state = run_checks(
        landing, feeds, prev.get("checks", {}), now, max_age, disk_max,
        state_file, datex_state_file,
    )

    ok = all(c["ok"] for c in checks.values())
    reason = "; ".join(c["reason"] for c in checks.values() if not c["ok"])
    checked_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    same_state = prev.get("ok") == ok and prev.get("since")
    since = prev.get("since") if same_state else checked_at

    # Notify when the state differs from the last one we notified about successfully.
    # (On the first run, only a failure is news.)
    notified = prev.get("notified_ok", True)
    if topic and ok != notified:
        try:
            if ok:
                start = parse_iso(prev.get("since")) if prev.get("ok") is False else None
                body = f"recovered after {ago(now, start)}" if start else "recovered"
            else:
                body = fail_body(checks)
            notify(server, topic, ok, body, click_url)
            notified = ok
        except (urllib.error.URLError, OSError) as e:
            print(f"notify failed: {e}", file=sys.stderr)

    def pick(state, keys):
        return {k: state.get(k) for k in keys} if state else None

    status = {
        "ok": ok,
        "reason": reason or None,
        "since": since,
        "checked_at": checked_at,
        "notified_ok": notified,
        "checks": checks,
        "receiver": pick(receiver_state, ("pid", "started_at", "subscribing", "feeds")),
        "datex": pick(datex_state, ("pid", "started_at", "polling", "error", "feeds")),
    }
    write_atomic(status_file, json.dumps(status, indent=1))
    write_atomic(status_dir / "index.html", render(status, host))

    if not ok:
        print(f"FAIL: {reason}")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
