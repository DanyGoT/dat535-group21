"use strict";
// Static viewer over dashboard/build.py output. Every rate is computed here from summed counts.

const G = window.GOLD;
if (!G) {
  document.body.textContent = "data.js is missing. Build it with dashboard/build.py (see dashboard/README.md).";
  throw new Error("no Gold data");
}
const $ = (selector) => document.querySelector(selector);
const nf = new Intl.NumberFormat("en-GB");
const pct = (x) => (x == null || !isFinite(x) ? "—" : `${Math.round(100 * x)}%`);
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function el(tag, attrs = {}, ...kids) {
  const node = tag.startsWith("svg:") ? document.createElementNS("http://www.w3.org/2000/svg", tag.slice(4))
    : document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (value === true) node.setAttribute(key, "");
    else if (value != null && value !== false) node.setAttribute(key, value);
  }
  for (const kid of kids.flat()) if (kid != null) node.append(kid.nodeType ? kid : String(kid));
  return node;
}

// ---------- tooltip ----------
const tipBox = $("#tip");
function tip(node, text) {
  node.append(el("svg:title", {}, text)); // also read by assistive technology
  node.addEventListener("pointerenter", () => { tipBox.textContent = text; tipBox.hidden = false; });
  node.addEventListener("pointermove", (e) => {
    tipBox.style.left = `${Math.min(e.clientX + 14, innerWidth - tipBox.offsetWidth - 8)}px`;
    tipBox.style.top = `${e.clientY + 14}px`;
  });
  node.addEventListener("pointerleave", () => { tipBox.hidden = true; });
}

// ---------- small SVG charts ----------
function topRounded(x, y, w, h, r) {
  r = Math.min(r, w / 2, h);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

// Vertical columns on a 0..max scale. data: [{label, value, title, thin, cls}]
function columns(data, { max = 1, height = 120, every = 1, format = pct } = {}) {
  const W = 380, left = 34, bottom = 18, plot = height - bottom, step = (W - left) / data.length;
  const svg = el("svg:svg", { class: "chart", viewBox: `0 0 ${W} ${height}`, role: "img" });
  for (const v of [0, max / 2, max]) {
    const y = plot - (v / max) * (plot - 6);
    svg.append(el("svg:line", { class: "axis", x1: left, x2: W, y1: y, y2: y }),
      el("svg:text", { x: left - 6, y: y + 4, "text-anchor": "end" }, format(v)));
  }
  data.forEach((d, i) => {
    const x = left + i * step;
    if (d.value != null) {
      const h = Math.max(1, (Math.min(d.value, max) / max) * (plot - 6));
      svg.append(el("svg:path", { class: `bar ${d.cls || ""} ${d.thin ? "thin" : ""}`,
        d: topRounded(x + 1, plot - h, Math.max(1, step - 2), h, 3) }));
    }
    if (i % every === 0) svg.append(el("svg:text", { x: x + step / 2, y: height - 4, "text-anchor": "middle" }, d.label));
    const hit = el("svg:rect", { class: "hit", x, y: 0, width: step, height: plot });
    tip(hit, d.title);
    svg.append(hit);
  });
  return svg;
}

// Chance of holding against spare minutes 0..10, with the chosen minute marked.
function curve(points, chosen) {
  const W = 380, H = 150, left = 34, bottom = 20, plot = H - bottom;
  const x = (m) => left + 8 + (m / 10) * (W - left - 16);
  const y = (v) => plot - v * (plot - 8);
  const svg = el("svg:svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img" });
  for (const v of [0, 0.5, 1]) {
    svg.append(el("svg:line", { class: "axis", x1: left, x2: W, y1: y(v), y2: y(v) }),
      el("svg:text", { x: left - 6, y: y(v) + 4, "text-anchor": "end" }, pct(v)));
  }
  for (let m = 0; m <= 10; m += 2) svg.append(el("svg:text", { x: x(m), y: H - 4, "text-anchor": "middle" }, `${m} min`));
  svg.append(el("svg:line", { class: "guide", x1: x(chosen), x2: x(chosen), y1: 4, y2: plot }));
  svg.append(el("svg:polyline", { class: "line", points: points.map((v, m) => `${x(m)},${y(v)}`).join(" ") }));
  points.forEach((v, m) => {
    const dot = el("svg:circle", { class: `dot ${m === chosen ? "on" : ""}`, cx: x(m), cy: y(v), r: m === chosen ? 6 : 4 });
    tip(dot, `${m} spare min: held ${pct(v)}`);
    svg.append(dot);
  });
  return svg;
}

// ---------- data ----------
const hubs = G.hubs.map((row) => Object.fromEntries(G.hub_columns.map((c, i) => [c, row[i]])));
hubs.forEach((h) => { h.name = typeof h.name === "string" && h.name ? h.name : h.id; }); // NSR names are nullable
const totals = G.days.reduce((t, d) => ({ scored: t.scored + d.totals.scored_departures,
  late: t.late + d.totals.late_departures, connections: t.connections + d.totals.transfer_connections }),
{ scored: 0, late: 0, connections: 0 });
const national = totals.late / totals.scored;
const shards = {}, waiting = {};
window.goldShard = (n, content) => { shards[n] = content; (waiting[n] || []).forEach((f) => f(content)); delete waiting[n]; };
function loadShard(n) {
  if (shards[n]) return Promise.resolve(shards[n]);
  return new Promise((resolve, reject) => {
    if (!waiting[n]) {
      waiting[n] = [];
      document.head.append(el("script", { src: `hubs/${String(n).padStart(2, "0")}.js`, onerror: reject }));
    }
    waiting[n].push(resolve);
  });
}
function lineName(id) {
  const line = G.lines[id];
  if (!line) return id.split(":").pop();
  const [code, name] = line;
  return !code ? name : !name || name.startsWith(code) ? name || code : `${code} ${name}`;
}
const lineCode = (id) => (G.lines[id] && G.lines[id][0]) || lineName(id);
const GAP_ZERO = -G.rules.gap_low_min; // index of "gap unchanged"
function expand(trimmed) {
  const counts = new Array(22).fill(0);
  trimmed.slice(1).forEach((v, i) => { counts[trimmed[0] + i] = v; });
  return counts;
}
const held = (counts, spare) => counts.slice(GAP_ZERO - spare).reduce((a, b) => a + b, 0);
const sum = (counts) => counts.reduce((a, b) => a + b, 0);

// ---------- header and footer ----------
const dates = G.days.map((d) => new Date(`${d.date}T12:00:00Z`));
const dayList = new Intl.ListFormat("en-GB").format(dates.map((d) => d.toLocaleDateString("en-GB", { day: "numeric", month: "short" })));
$("#coverage").textContent = `${G.days.length} operating days (${dayList} ${dates[0].getUTCFullYear()}): ` +
  `${nf.format(totals.scored)} timed departures and ${nf.format(totals.connections)} observed connections across Norway.`;
$("#foot").append(
  el("p", {}, `Late means leaving 3 minutes or more after the aimed time. ${pct(national)} of departures did, nationally. ` +
    `A connection is an arrival paired with the first departure of another line from the same stop place within ` +
    `${G.rules.max_transfer_slack_s / 60} minutes. Missing reports count as unknown, never as on time.`),
  el("p", {}, `Contains data under the Norwegian licence for public data (NLOD) from Entur AS and Statens vegvesen, ` +
    `aggregated by DAT535 group 21. Gold runs: ` +
    G.days.map((d) => `${d.date} ${d.gold_run_id}`).join(", ") + "."));

// ---------- views ----------
document.querySelectorAll(".views button").forEach((button) => button.addEventListener("click", () => {
  document.querySelectorAll(".views button").forEach((b) => b.setAttribute("aria-pressed", String(b === button)));
  $("#network").hidden = button.dataset.view !== "network";
  $("#roads").hidden = button.dataset.view !== "roads";
  if (button.dataset.view === "network") map.invalidateSize();
}));

// ---------- map ----------
const map = L.map("map", { preferCanvas: true, minZoom: 4 }).setView([64.5, 12], 5);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors', maxZoom: 19,
}).addTo(map);
new ResizeObserver(() => map.invalidateSize()).observe($("#map")); // the panel's height sets the map's
const bands = [[-0.12, "--better-2"], [-0.04, "--better-1"], [0.04, "--even"], [0.12, "--worse-1"], [Infinity, "--worse-2"]];
const colour = (share) => css(bands.find(([edge]) => share - national < edge)[1]);
const renderer = L.canvas({ padding: 0.4 });
const ring = L.circleMarker([0, 0], { radius: 0, color: css("--ink"), weight: 2.5, fill: false, interactive: false });
for (const hub of hubs) { // busiest first, so small stops are drawn on top
  const share = hub.late_departures / hub.scored_departures;
  hub.radius = Math.max(2.5, Math.min(13, Math.sqrt(hub.scored_departures) / 6));
  hub.marker = L.circleMarker([hub.lat, hub.lon], { renderer, radius: hub.radius, fillColor: colour(share),
    fillOpacity: 0.9, color: css("--surface"), weight: 0.6 })
    .bindTooltip(() => el("div", {}, el("strong", {}, hub.name), el("br"),
      `${pct(share)} late of ${nf.format(hub.scored_departures)} departures`))
    .on("click", () => select(hub, false))
    .addTo(map);
}
const legend = L.control({ position: "bottomleft" });
legend.onAdd = () => el("div", { class: "legend" }, el("b", {}, "Departures 3+ min late"),
  bands.map(([edge, token], i) => {
    const low = i ? national + bands[i - 1][0] : null;
    const label = low == null ? `under ${pct(national + edge)}` : edge === Infinity ? `${pct(low)} or more` : `${pct(low)} to ${pct(national + edge)}`;
    return el("div", {}, el("i", { style: `background:${css(token)}` }), label);
  }),
  el("div", {}, "Size: number of departures"));
legend.addTo(map);

// ---------- search ----------
const find = $("#find"), matches = $("#matches");
let found = [], active = 0;
function showMatches() {
  const q = find.value.trim().toLowerCase();
  found = q.length < 2 ? [] : hubs.filter((h) => h.name.toLowerCase().includes(q)).slice(0, 8);
  active = 0;
  matches.replaceChildren(...found.map((h, i) => el("li", { role: "option", "aria-selected": String(i === active),
    onmousedown: (e) => { e.preventDefault(); choose(h); } }, h.name, el("small", {}, `${nf.format(h.scored_departures)} dep.`))));
  matches.hidden = !found.length;
}
function choose(hub) { matches.hidden = true; find.value = hub.name; select(hub, true); }
find.addEventListener("input", showMatches);
find.addEventListener("blur", () => { matches.hidden = true; });
find.addEventListener("keydown", (e) => {
  if (!found.length) return;
  if (e.key === "ArrowDown" || e.key === "ArrowUp") {
    e.preventDefault();
    active = (active + (e.key === "ArrowDown" ? 1 : found.length - 1)) % found.length;
    [...matches.children].forEach((li, i) => li.setAttribute("aria-selected", String(i === active)));
  } else if (e.key === "Enter") choose(found[active]);
  else if (e.key === "Escape") matches.hidden = true;
});

// ---------- hub panel ----------
let current = null;
const state = { arrival: null, pair: null, spare: 2 };

function intro() {
  const busiest = hubs.slice(0, 3);
  $("#hub").replaceChildren(el("div", { class: "hint" },
    el("p", {}, "Pick a stop on the map or search for one. You will see when its departures run late and, where lines meet, how often a connection held."),
    el("p", { class: "section" }, "Busiest: ", busiest.flatMap((h, i) => [i ? ", " : "",
      el("a", { href: "#", onclick: (e) => { e.preventDefault(); choose(h); } }, h.name)]), ".")));
}

async function select(hub, move) {
  current = hub;
  Object.assign(state, { arrival: null, pair: null });
  ring.setLatLng([hub.lat, hub.lon]).setRadius(hub.radius + 4).addTo(map);
  if (move) map.setView([hub.lat, hub.lon], Math.max(map.getZoom(), 14));
  const share = hub.late_departures / hub.scored_departures;
  const box = el("div", { class: "hub" },
    el("h2", {}, hub.name),
    el("p", { class: "facts" }, `${nf.format(hub.scored_departures)} timed departures. ${pct(share)} left 3+ min late ` +
      `(national ${pct(national)}). ${nf.format(hub.cancelled_departures)} reported cancellations.`));
  $("#hub").replaceChildren(box);
  const bins = [["Early", "departures_early"], ["On time", "departures_on_time"], ["1–3", "departures_1_3_min"],
    ["3–5", "departures_3_5_min"], ["5–10", "departures_5_10_min"], ["10+", "departures_10_min_plus"]];
  const most = Math.max(...bins.map(([, c]) => hub[c])) / hub.scored_departures;
  box.append(el("div", { class: "section" }, el("h3", {}, "How late, in minutes"),
    columns(bins.map(([label, c], i) => ({ label, value: hub[c] / hub.scored_departures, cls: i >= 3 ? "late" : "",
      title: `${label}${i > 1 ? " min" : ""}: ${pct(hub[c] / hub.scored_departures)} (${nf.format(hub[c])})` })),
    { max: Math.min(1, Math.ceil(most * 10) / 10) }),
    el("p", { class: "note" }, "Early is more than a minute before the aimed time; on time is within a minute either side.")));
  let content;
  try { content = (await loadShard(hub.shard))[hub.id]; } catch {
    box.append(el("p", { class: "section empty" }, `Could not load hubs/${String(hub.shard).padStart(2, "0")}.js. Rebuild the dashboard.`));
    return;
  }
  if (current !== hub) return;
  const byHour = new Map(content.hours.map(([h, scored, late]) => [h, { scored, late }]));
  box.append(el("div", { class: "section" }, el("h3", {}, "Late departures by hour"),
    columns([...Array(24).keys()].map((h) => {
      const v = byHour.get(h);
      return { label: String(h).padStart(2, "0"), value: v ? v.late / v.scored : null, thin: !v || v.scored < 10, cls: "late",
        title: v ? `${String(h).padStart(2, "0")}:00–${String(h).padStart(2, "0")}:59: ${pct(v.late / v.scored)} late of ${nf.format(v.scored)}` : `${h}:00 no departures` };
    }), { every: 3 }),
    el("p", { class: "note" }, "Local time of the aimed departure. Faded bars rest on fewer than 10 departures.")));
  const transfers = el("div", { class: "section" }, el("h3", {}, "Transfers here"));
  box.append(transfers);
  renderTransfers(transfers, content.transfers);
}

function renderTransfers(box, pairs) {
  if (!pairs.length) {
    box.append(el("p", { class: "empty" },
      `No pair of lines here has ${G.rules.min_known_connections} observed connections yet. Try a terminal or station nearby.`));
    return;
  }
  const arrivals = new Map();
  for (const p of pairs) {
    p.counts = expand(p.gaps); p.known = sum(p.counts);
    const key = `${p.from}|${p.origin_id}`; // names are labels only: two origins may share one
    if (!arrivals.has(key)) arrivals.set(key, { key, from: p.from, origin: p.origin, known: 0, pairs: [] });
    const a = arrivals.get(key); a.known += p.known; a.pairs.push(p);
  }
  const options = [...arrivals.values()].sort((a, b) => b.known - a.known);
  options.forEach((a) => a.pairs.sort((x, y) => y.known - x.known));
  state.arrival = options[0];
  state.pair = options[0].pairs[0];
  const results = el("div");
  const value = el("output", { for: "spare" }, `${state.spare} min`);
  // Controls stay in place; only the list and the answer redraw, so the slider keeps its drag.
  box.append(
    el("p", { class: "note" }, "Spare minutes are the planned gap minus the time you need to walk between the two platforms. " +
      "A connection holds when the departure's delay minus the arrival's delay does not eat them."),
    el("div", { class: "controls" },
      el("label", { for: "arrival" }, "Arriving on", el("select", { id: "arrival",
        onchange: (e) => { state.arrival = options[e.target.selectedIndex]; state.pair = state.arrival.pairs[0]; update(); } },
        options.map((a) => el("option", { title: lineName(a.from) }, `${lineCode(a.from)} from ${a.origin || "?"} (${nf.format(a.known)})`)))),
      el("label", { for: "spare" }, "Spare minutes", el("div", { class: "buffer" },
        el("input", { id: "spare", type: "range", min: 0, max: 10, step: 1, value: state.spare,
          oninput: (e) => { state.spare = Number(e.target.value); value.textContent = `${state.spare} min`; update(); } }),
        value))),
    results);

  function update() {
    const { arrival, pair, spare } = state;
    const list = el("ul", { class: "pairs" }, arrival.pairs.slice(0, 40).map((p) => {
      const chance = held(p.counts, spare) / p.known;
      return el("li", {}, el("button", { type: "button", "aria-pressed": String(p === pair),
        title: lineName(p.to), onclick: () => { state.pair = p; update(); } },
        el("span", { class: "name" }, el("strong", {}, lineCode(p.to)), ` to ${p.destination || "?"}`),
        el("span", { class: "meter", "aria-hidden": "true" }, el("span", { style: `width:${(100 * chance).toFixed(1)}%` })),
        el("span", { class: "pct" }, pct(chance))));
    }));
    const hours = new Map(Object.entries(pair.hours).map(([h, t]) => [Number(h), expand(t)]));
    const hh = (h) => String(h).padStart(2, "0");
    results.replaceChildren(list, el("div", { class: "split" },
      el("div", { class: "answer" }, el("span", { class: "big" }, pct(held(pair.counts, spare) / pair.known)),
        el("p", {}, "of ", el("strong", {}, `${nf.format(pair.known)} observed connections`),
          ` from line ${lineCode(pair.from)} to line ${lineCode(pair.to)} towards ${pair.destination || "?"} held with ${spare} spare minute${spare === 1 ? "" : "s"}. ` +
          `${nf.format(pair.cancelled)} were lost to a cancellation, ${nf.format(pair.unknown)} lacked an actual time.`)),
      el("div", {}, el("h3", {}, "Chance against spare minutes"),
        curve([...Array(11).keys()].map((m) => held(pair.counts, m) / pair.known), spare)),
      el("div", {}, el("h3", {}, `Chance by hour of arrival, ${spare} spare min`),
        columns([...Array(24).keys()].map((h) => {
          const counts = hours.get(h), n = counts ? sum(counts) : 0;
          return { label: hh(h), value: n ? held(counts, spare) / n : null, thin: n < 5,
            title: n ? `${hh(h)}:00–${hh(h)}:59: held ${pct(held(counts, spare) / n)} of ${n}` : `${hh(h)}:00 none observed` };
        }), { every: 3 }),
        el("p", { class: "note" }, "Faded bars rest on fewer than 5 connections."))));
  }
  update();
}

// ---------- road conditions ----------
const FACTORS = [
  { key: "precipitation", title: "Precipitation", ref: "dry", levels: {
    dry: "Dry, under 0.1 mm/h", light: "Light, 0.1–1 mm/h", heavy: "Heavy, 1 mm/h or more", unknown: "No usable station or hour" } },
  { key: "road_surface", title: "Road surface temperature", ref: "above_zero", levels: {
    above_zero: "Above 0 °C", freezing: "At or below 0 °C", unknown: "No usable station or hour" } },
  { key: "road_situation", title: "Road situations within 500 m", ref: "none", levels: {
    none: "None active", roadworks: "Roadworks or traffic management", incident: "Incident: accident, obstruction or poor conditions",
    unknown: "Outside the DATEX day" } },
  { key: "traffic", title: "Traffic on sections within 500 m", ref: "free_flow", levels: {
    free_flow: "Free flow, under 1.2× free-flow time", slow: "Slow, 1.2–1.5×", congested: "Congested, 1.5× or more",
    unmeasured: "Section without a measurement", no_section: "No measured section nearby", unknown: "Outside the DATEX day" } },
];
const C = Object.fromEntries(G.condition_columns.map((c, i) => [c, i]));
const modes = new Map();
for (const r of G.conditions) modes.set(r[C.mode], (modes.get(r[C.mode]) || 0) + r[C.scored_departures]);
$("#mode").replaceChildren(el("option", { value: "" }, "All modes"),
  [...modes].sort((a, b) => b[1] - a[1]).map(([m, n]) => el("option", { value: m }, `${m} (${nf.format(n)})`)));
$("#mode").addEventListener("change", renderRoads);

function renderRoads() {
  const mode = $("#mode").value;
  const rows = G.conditions.filter((r) => !mode || r[C.mode] === mode);
  $("#factors").replaceChildren(...FACTORS.map((f) => {
    // Late share per level and per stratum (mode, hour), then each level against the reference in the same strata.
    const levels = {}, strata = {};
    for (const r of rows) {
      const level = r[C[f.key]], s = `${r[C.mode]}|${r[C.hour]}`, n = r[C.scored_departures], late = r[C.late_departures];
      const L = (levels[level] ||= { n: 0, late: 0, strata: {} });
      L.n += n; L.late += late;
      const S = (L.strata[s] ||= { n: 0, late: 0 }); S.n += n; S.late += late;
      if (level === f.ref) strata[s] = S;
    }
    const scale = Math.max(5, ...Object.entries(levels).filter(([level]) => level !== "unknown")
      .map(([level, L]) => Math.abs(matched(L, strata, level === f.ref).diff || 0)));
    const body = Object.keys(f.levels).filter((level) => levels[level]).map((level) => {
      const L = levels[level], m = matched(L, strata, level === f.ref);
      const bar = level === f.ref ? el("span", { class: "note" }, "reference")
        : level === "unknown" ? el("span", { class: "note" }, "a gap, not compared")
          : m.diff == null ? el("span", { class: "note" }, "no match") : diverging(m.diff, scale);
      return el("tr", {}, el("td", {}, f.levels[level]), el("td", {}, nf.format(L.n)), el("td", {}, pct(L.late / L.n)),
        el("td", { title: m.diff == null ? "" : `${(100 * m.covered).toFixed(0)}% of these departures had a reference in the same mode and hour` }, bar));
    });
    return el("section", { class: "factor" }, el("h3", {}, f.title),
      el("p", { class: "note" }, `Reference: ${f.levels[f.ref].toLowerCase()}.`),
      el("table", {}, el("thead", {}, el("tr", {}, el("th", {}, "Condition"), el("th", {}, "Departures"), el("th", {}, "Late"),
        el("th", {}, "vs reference"))), el("tbody", {}, body)));
  }));
}

// Weighted mean of (level - reference) late share over strata where the reference has 30+ departures.
function matched(L, strata, isRef) {
  if (isRef) return { diff: 0, covered: 1 };
  let weight = 0, total = 0;
  for (const [s, S] of Object.entries(L.strata)) {
    const R = strata[s];
    if (!R || R.n < 30) continue;
    weight += S.n; total += S.n * (S.late / S.n - R.late / R.n);
  }
  return weight ? { diff: (100 * total) / weight, covered: weight / L.n } : { diff: null };
}

function diverging(diff, scale) {
  const width = (50 * Math.min(Math.abs(diff), scale)) / scale;
  const box = el("span", { class: "delta" }, el("span", { class: "track", "aria-hidden": "true" },
    el("span", { class: diff > 0 ? "worse" : "better", style: `width:${width}%;${diff > 0 ? "left:50%" : `right:50%`}` })),
  el("b", {}, `${diff > 0 ? "+" : ""}${diff.toFixed(1)} pp`));
  box.title = `${diff > 0 ? "+" : ""}${diff.toFixed(1)} percentage points late compared with the reference in the same mode and hour`;
  return box;
}

intro();
renderRoads();
