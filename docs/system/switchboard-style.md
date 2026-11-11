# Switchboard style

This page defines visual conventions for the read-only collector status page. The renderer in
[monitor.py](../../collector/monitor.py) is the source of truth for HTML, CSS and scripts.

Up: [architecture](architecture.md) · Related: [runbook](../../collector/README.md)

## What the collector's page shows

| Section | Contents |
| ------- | -------- |
| CHECKS | Receiver, DATEX poller, enabled feeds, NeTEx/NSR archives, `disk` and `root-disk` |
| PROBLEMS | Failed checks and reasons. Hidden when all checks pass |
| LAST 24H | Ingest rate, stored-on-disk and per-day Grafana panels, plus a dashboard link |
| SUBSCRIPTIONS | SIRI push times, subscription age, expiry and errors |
| POLLING · DATEX | Poll intervals, last success, last change, sizes and errors. Shown while polling |
| INFO | Overall OK/FAIL state, time since transition and last check |

Default feed rows are `siri-et`, `siri-sx`, `vehicles` and the five `datex-<feed>` datasets.
DATEX without credentials is IDLE and omits its feed checks. Archive rows are `netex` and `nsr`.
The [architecture](architecture.md#monitoring) owns check thresholds and notification behaviour.

## Web

### Principles

- Server-rendered HTML, monospace text and no framework or asset build.
- Read-only checks and links. Page controls only adjust local zoom.
- Uppercase, letter-spaced titles, section headings, state words and zoom buttons.
  Plain text elsewhere.
- Meaningful colour and a `■` state glyph. No decorative copy.
- Same-origin Grafana iframes through `/grafana/`, with the device's light or dark theme.

[serve_status.py](../../collector/serve_status.py) serves the status directory using GET/HEAD
and rejects directory listings and dotfiles. The Grafana proxy also accepts POST for panel
queries. It binds to localhost by default and supplies no built-in authentication.
The tunnel must serve only the status directory and Grafana proxy.

### Head (CSS + zoom script)

Keep these tokens from `CSS_AND_HEAD` in [monitor.py](../../collector/monitor.py):

```css
:root {
  --bg: #f2f1ec; --fg: #1b1b1a; --mute: #7a776f; --line: #d6d3ca;
  --run: #2e7a34; --fail: #b3261e; --busy: #9a6400;
}
@media (prefers-color-scheme: dark) {
  :root { --bg: #111110; --fg: #e4e1d8; --mute: #858279; --line: #2a2926;
          --run: #74c27a; --fail: #ff6f61; --busy: #e2a640; }
}
```

Green means healthy, red failed, amber warning and muted grey secondary or idle.
Use the full `CSS_AND_HEAD` stylesheet from the renderer. Body text is 14 px with 1.5 line
height. Titles are 14 px, section headings and state words 12 px. The content width is
760 px, with 28 px top, 16 px side and 64 px bottom padding.
Rows have 16 px vertical padding and dividing borders. Their grid is `11ch 1fr auto` with
12 px gaps, and descriptions align under the name. Below 520 px, it becomes `1fr auto`,
state words span the row and descriptions lose their left indent.

Zoom uses CSS `zoom`, ranges from 60–200 % in 10 % steps and persists in local storage.
The page reloads after 120 s. Embedded panels refresh every minute.

### Header

Show `DAT535 COLLECTOR`, hostname, local check time and passed/failed check counts.
Failed counts are red. Place the minus, reset and plus zoom controls at the right.
The reset control displays the current percentage.

### Rows

Use `row()` in [monitor.py](../../collector/monitor.py). It escapes supplied text and emits:

- `.row` with `active`, `failed` or `idle` class.
- `.top` with state, bold `.name` and right-aligned muted `.nums`.
- `.desc` below for a file path, process ID or failure reason.

Rendered states are `RUNNING`, `STOPPED`, `FAILED`, `IDLE` and `OK`.
Key/value sections use `.kv`, with `.error` for failures and `.warn` for current warnings.
Recovered subscription and polling errors remain visible in muted text.
