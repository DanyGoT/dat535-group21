# Switchboard style

Look for the monitor's status page. Adapted from Switchboard; the page in this
project is **read-only**: no forms, no buttons that act, no POST endpoints.
Copy the CSS verbatim and follow the structure.

## Web

### Principles

- Plain server-rendered HTML. No framework, no JS build, no external assets (same-origin
  Grafana panels in iframes are the one exception on the collector's main page).
- Read-only. The only control is the client-side zoom.
- The page refreshes itself (`setTimeout(() => location.replace("/"), 15000)`). The
  collector's main page uses 120000 (one monitor run) because it embeds Grafana panels,
  which refresh themselves.
- All monospace. Uppercase letter-spaced words for titles, section headings,
  state words and buttons. Everything else is lowercase/plain.
- Color carries meaning only: green = ok/running, red = failed/destructive,
  amber = transitional/warning, muted grey = secondary info.
- Status is shown with a `■` glyph or a state word (`■ RUNNING`) in that color.
  Problems are shown as a bold `N!` badge (amber, or red if any is an error).
- No taglines, subtitles or decorative copy.
- Bind to localhost (or specific interfaces); no built-in auth. If exposed via
  tunnel, serve only the status page/directory.

### Head (CSS + zoom script)

```html
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<title>Switchboard</title>
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
</script>
</head><body><main>
```

### Header

Bold uppercase title, then muted host + time, then muted counts (failures in
red), zoom control pushed right.

```html
<header>
  <h1>SWITCHBOARD</h1>
  <span class="mute">nixos-desktop · 14:02:31</span>
  <span class="mute">7 up · <span style="color:var(--fail)">1 failed</span></span>
  <span class="zoom"><button onclick="setZoom(z - 0.1)">−</button><button id="zl" onclick="setZoom(1)" title="reset">100%</button><button onclick="setZoom(z + 0.1)">+</button></span>
  <script>setZoom(z)</script>
</header>
```

### Rows

Row class is the state (`active`, `failed`, `activating`, `deactivating`,
anything else = muted). Columns: state word, bold name (+ optional badge),
right-aligned muted numbers. Description under it.

```html
<h2>SERVICES</h2>
<section class="row active">
  <div class="top">
    <span class="st">RUNNING</span>
    <span><a class="name" href="/logs?u=hermes">hermes</a><a class="badge error" href="/logs?u=hermes" title="problems">2!</a></span>
    <span class="nums">3d 4h · 412M</span>
  </div>
  <div class="desc">Hermes agent (podman)</div>
</section>
```

State words: `RUNNING`, `DONE`, `FAILED`, `STARTING`, `RESTARTING`,
`STOPPING`, `STOPPED`.

### Detail page (key/value sections + logs)

Header becomes `<h1><a href="/" style="text-decoration:none">TITLE</a></h1>`
followed by `<span class="mute">/ logs</span>`. Then the item's row, then
`.kv` sections, then a `<pre>` of logs, auto-scrolled to the bottom.

```html
<div class="health">
  <h2>PROBLEMS</h2>
  <div class="kv">
    <span class="error">error</span>
    <span>discord gateway disconnected <span class="mute">· discord</span></span>
    <span class="warn">warn</span>
    <span>queue backlog 120 <span class="mute">· worker</span></span>
  </div>
  <h2>INFO</h2>
  <div class="kv">
    <span class="mute">model</span><span>claude-opus</span>
    <span class="mute">uptime</span><span>3d 4h</span>
  </div>
  <h2>LOGS</h2>
</div>
<pre>...</pre>
<script>scrollTo(0, document.body.scrollHeight)</script>
</main></body></html>
```
