"use strict";
// Static viewer over dashboard/build.py output. Every rate is computed here from summed counts.

const G = window.GOLD;
if (!G) {
  document.body.textContent =
    "data.js is missing. Build it with dashboard/build.py (see dashboard/README.md).";
  throw new Error("no Gold data");
}
const $ = (selector) => document.querySelector(selector);
const nf = new Intl.NumberFormat("en-GB");
const pct = (x) => (x == null || !isFinite(x) ? "—" : `${Math.round(100 * x)}%`);
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const pad2 = (n) => String(n).padStart(2, "0");

function el(tag, attrs = {}, ...kids) {
  const node = tag.startsWith("svg:")
    ? document.createElementNS("http://www.w3.org/2000/svg", tag.slice(4))
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
  node.addEventListener("pointerenter", () => {
    tipBox.textContent = text;
    tipBox.hidden = false;
  });
  node.addEventListener("pointermove", (e) => {
    tipBox.style.left = `${Math.min(e.clientX + 14, innerWidth - tipBox.offsetWidth - 8)}px`;
    tipBox.style.top = `${e.clientY + 14}px`;
  });
  node.addEventListener("pointerleave", () => {
    tipBox.hidden = true;
  });
}

// ---------- small SVG charts ----------
function topRounded(x, y, w, h, r) {
  r = Math.min(r, w / 2, h);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`;
}

// Vertical columns on a 0..max scale. data: [{label, value, title, thin, cls}]
function columns(data, { max = 1, height = 120, every = 1, format = pct } = {}) {
  const W = 380,
    left = 34,
    bottom = 18,
    plot = height - bottom,
    step = (W - left) / data.length;
  const svg = el("svg:svg", { class: "chart", viewBox: `0 0 ${W} ${height}`, role: "img" });
  for (const v of [0, max / 2, max]) {
    const y = plot - (v / max) * (plot - 6);
    svg.append(
      el("svg:line", { class: "axis", x1: left, x2: W, y1: y, y2: y }),
      el("svg:text", { x: left - 6, y: y + 4, "text-anchor": "end" }, format(v)),
    );
  }
  data.forEach((d, i) => {
    const x = left + i * step;
    if (d.value != null) {
      const h = Math.max(1, (Math.min(d.value, max) / max) * (plot - 6));
      svg.append(
        el("svg:path", {
          class: `bar ${d.cls || ""} ${d.thin ? "thin" : ""}`,
          d: topRounded(x + 1, plot - h, Math.max(1, step - 2), h, 3),
        }),
      );
    }
    if (i % every === 0)
      svg.append(
        el("svg:text", { x: x + step / 2, y: height - 4, "text-anchor": "middle" }, d.label),
      );
    const hit = el("svg:rect", { class: "hit", x, y: 0, width: step, height: plot });
    tip(hit, d.title);
    svg.append(hit);
  });
  return svg;
}

// Chance of holding against spare minutes 0..10, with the chosen minute marked.
function curve(points, chosen) {
  const W = 380,
    H = 150,
    left = 34,
    bottom = 20,
    plot = H - bottom;
  const x = (m) => left + 8 + (m / 10) * (W - left - 16);
  const y = (v) => plot - v * (plot - 8);
  const svg = el("svg:svg", { class: "chart", viewBox: `0 0 ${W} ${H}`, role: "img" });
  for (const v of [0, 0.5, 1]) {
    svg.append(
      el("svg:line", { class: "axis", x1: left, x2: W, y1: y(v), y2: y(v) }),
      el("svg:text", { x: left - 6, y: y(v) + 4, "text-anchor": "end" }, pct(v)),
    );
  }
  for (let m = 0; m <= 10; m += 2)
    svg.append(el("svg:text", { x: x(m), y: H - 4, "text-anchor": "middle" }, `${m} min`));
  svg.append(el("svg:line", { class: "guide", x1: x(chosen), x2: x(chosen), y1: 4, y2: plot }));
  svg.append(
    el("svg:polyline", {
      class: "line",
      points: points.map((v, m) => `${x(m)},${y(v)}`).join(" "),
    }),
  );
  points.forEach((v, m) => {
    const dot = el("svg:circle", {
      class: `dot ${m === chosen ? "on" : ""}`,
      cx: x(m),
      cy: y(v),
      r: m === chosen ? 6 : 4,
    });
    tip(dot, `${m} spare min: held ${pct(v)}`);
    svg.append(dot);
  });
  return svg;
}

// ---------- data ----------
const hubs = G.hubs.map((row) => Object.fromEntries(G.hub_columns.map((c, i) => [c, row[i]])));
hubs.forEach((h) => {
  h.name = typeof h.name === "string" && h.name ? h.name : h.id;
}); // NSR names are nullable
const totals = G.days.reduce(
  (t, d) => ({
    scored: t.scored + d.totals.scored_departures,
    late: t.late + d.totals.late_departures,
    connections: t.connections + d.totals.transfer_connections,
  }),
  { scored: 0, late: 0, connections: 0 },
);
const national = totals.late / totals.scored;
const shards = {},
  waiting = {};
window.goldShard = (n, content) => {
  shards[n] = content;
  (waiting[n] || []).forEach((f) => f(content));
  delete waiting[n];
};
function loadShard(n) {
  if (shards[n]) return Promise.resolve(shards[n]);
  return new Promise((resolve, reject) => {
    if (!waiting[n]) {
      waiting[n] = [];
      document.head.append(el("script", { src: `hubs/${pad2(n)}.js`, onerror: reject }));
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
function expand(trimmed, length = 22) {
  const counts = new Array(length).fill(0);
  trimmed.slice(1).forEach((v, i) => {
    counts[trimmed[0] + i] = v;
  });
  return counts;
}
const held = (counts, spare) => counts.slice(GAP_ZERO - spare).reduce((a, b) => a + b, 0);
const sum = (counts) => counts.reduce((a, b) => a + b, 0);

// ---------- header and footer ----------
const dates = G.days.map((d) => new Date(`${d.date}T12:00:00Z`));
const dayList = new Intl.ListFormat("en-GB").format(
  dates.map((d) => d.toLocaleDateString("en-GB", { day: "numeric", month: "short" })),
);
$("#coverage").textContent =
  `${G.days.length} operating days (${dayList} ${dates[0].getUTCFullYear()}): ` +
  `${nf.format(totals.scored)} timed departures and ${nf.format(totals.connections)} observed connections across Norway.`;
$("#foot").append(
  el(
    "p",
    {},
    `Late means leaving 3 minutes or more after the aimed time. ${pct(national)} of departures did, nationally. ` +
      `A connection is an arrival paired with the first departure of another line from the same stop place within ` +
      `${G.rules.max_transfer_slack_s / 60} minutes. Missing reports count as unknown, never as on time.`,
  ),
  el(
    "p",
    {},
    `Contains data under the Norwegian licence for public data (NLOD) from Entur AS and Statens vegvesen, ` +
      `aggregated by DAT535 group 21. Gold runs: ` +
      G.days.map((d) => `${d.date} ${d.gold_run_id}`).join(", ") +
      ".",
  ),
);

// ---------- views ----------
document.querySelectorAll(".views button").forEach((button) =>
  button.addEventListener("click", () => {
    document
      .querySelectorAll(".views button")
      .forEach((b) => b.setAttribute("aria-pressed", String(b === button)));
    for (const view of ["network", "segments", "roads"])
      $(`#${view}`).hidden = button.dataset.view !== view;
    if (button.dataset.view === "network") map.invalidateSize();
    if (button.dataset.view === "segments") showSegments();
  }),
);

// ---------- map ----------
function baseMap(id) {
  const m = L.map(id, { preferCanvas: true, minZoom: 4 }).setView([64.5, 12], 5);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(m);
  new ResizeObserver(() => m.invalidateSize()).observe($(`#${id}`)); // the panel's height sets the map's
  return m;
}
const map = baseMap("map");
const bands = [
  [-0.12, "--better-2"],
  [-0.04, "--better-1"],
  [0.04, "--even"],
  [0.12, "--worse-1"],
  [Infinity, "--worse-2"],
];
const colour = (share) => css(bands.find(([edge]) => share - national < edge)[1]);
const renderer = L.canvas({ padding: 0.4 });
const rings = [null, "4 3"].map((dashArray) =>
  L.circleMarker([0, 0], {
    renderer,
    radius: 0,
    color: css("--ink"),
    weight: 2.5,
    dashArray,
    fill: false,
    interactive: false,
  }),
);
function mark(list) {
  rings.forEach((r, i) =>
    list[i]
      ? r
          .setLatLng([list[i].lat, list[i].lon])
          .setRadius(list[i].radius + 4)
          .addTo(map)
      : r.remove(),
  );
}
for (const hub of hubs) {
  // busiest first, so small stops are drawn on top
  const share = hub.late_departures / hub.scored_departures;
  hub.radius = Math.max(2.5, Math.min(13, Math.sqrt(hub.scored_departures) / 6));
  hub.marker = L.circleMarker([hub.lat, hub.lon], {
    renderer,
    radius: hub.radius,
    fillColor: colour(share),
    fillOpacity: 0.9,
    color: css("--surface"),
    weight: 0.6,
  })
    .bindTooltip(() =>
      el(
        "div",
        {},
        el("strong", {}, hub.name),
        el("br"),
        `${pct(share)} late of ${nf.format(hub.scored_departures)} departures`,
      ),
    )
    .on("click", (e) => pick(hub, e.originalEvent.shiftKey, false))
    .addTo(map);
}
const legend = L.control({ position: "bottomleft" });
legend.onAdd = () =>
  el(
    "div",
    { class: "legend" },
    el("b", {}, "Departures 3+ min late"),
    bands.map(([edge, token], i) => {
      const low = i ? national + bands[i - 1][0] : null;
      const label =
        low == null
          ? `under ${pct(national + edge)}`
          : edge === Infinity
            ? `${pct(low)} or more`
            : `${pct(low)} to ${pct(national + edge)}`;
      return el("div", {}, el("i", { style: `background:${css(token)}` }), label);
    }),
    el("div", {}, "Size: number of departures"),
  );
legend.addTo(map);

// ---------- search ----------
const find = $("#find"),
  matches = $("#matches");
let found = [],
  active = 0;
function showMatches() {
  const q = find.value.trim().toLowerCase();
  found = q.length < 2 ? [] : hubs.filter((h) => h.name.toLowerCase().includes(q)).slice(0, 8);
  active = 0;
  matches.replaceChildren(
    ...found.map((h, i) =>
      el(
        "li",
        {
          role: "option",
          "aria-selected": String(i === active),
          onmousedown: (e) => {
            e.preventDefault();
            choose(h);
          },
        },
        h.name,
        el("small", {}, `${nf.format(h.scored_departures)} dep.`),
      ),
    ),
  );
  matches.hidden = !found.length;
}
function choose(hub) {
  matches.hidden = true;
  find.value = hub.name;
  pick(hub, false, true);
}
find.addEventListener("input", showMatches);
find.addEventListener("blur", () => {
  matches.hidden = true;
});
find.addEventListener("keydown", (e) => {
  if (!found.length) return;
  if (e.key === "ArrowDown" || e.key === "ArrowUp") {
    e.preventDefault();
    active = (active + (e.key === "ArrowDown" ? 1 : found.length - 1)) % found.length;
    [...matches.children].forEach((li, i) =>
      li.setAttribute("aria-selected", String(i === active)),
    );
  } else if (e.key === "Enter") choose(found[active]);
  else if (e.key === "Escape") matches.hidden = true;
});

// ---------- hub panel ----------
let shown = []; // the one or two stops in the panel
let adding = false; // the next pick becomes the second stop
const state = { arrival: null, pair: null, spare: 2 };

function intro() {
  const busiest = hubs.slice(0, 3);
  $("#hub").replaceChildren(
    el(
      "div",
      { class: "hint" },
      el(
        "p",
        {},
        "Pick a stop on the map or search for one. You will see when its departures run late and, where lines meet, how often a connection held.",
      ),
      el(
        "p",
        { class: "section" },
        "Shift-click a second stop to estimate a transfer between two nearby stops, such as a bus stop and a station.",
      ),
      el(
        "p",
        { class: "section" },
        "Busiest: ",
        busiest.flatMap((h, i) => [
          i ? ", " : "",
          el(
            "a",
            {
              href: "#",
              onclick: (e) => {
                e.preventDefault();
                choose(h);
              },
            },
            h.name,
          ),
        ]),
        ".",
      ),
    ),
  );
}

function pick(hub, add, move) {
  if ((add || adding) && shown.length && shown[0] !== hub) compare(shown[0], hub);
  else select(hub, move);
}

const BINS = [
  ["Early", "departures_early"],
  ["On time", "departures_on_time"],
  ["1–3", "departures_1_3_min"],
  ["3–5", "departures_3_5_min"],
  ["5–10", "departures_5_10_min"],
  ["10+", "departures_10_min_plus"],
];
const facts = (hub) =>
  `${nf.format(hub.scored_departures)} timed departures. ` +
  `${pct(hub.late_departures / hub.scored_departures)} left 3+ min late (national ${pct(national)}). ` +
  `${nf.format(hub.cancelled_departures)} reported cancellations.`;

function delays(hub) {
  const most = Math.max(...BINS.map(([, c]) => hub[c])) / hub.scored_departures;
  return el(
    "div",
    { class: "section" },
    el("h3", {}, "How late, in minutes"),
    columns(
      BINS.map(([label, c], i) => {
        const part = hub[c] / hub.scored_departures;
        return {
          label,
          value: part,
          cls: i >= 3 ? "late" : "",
          title: `${label}${i > 1 ? " min" : ""}: ${pct(part)} (${nf.format(hub[c])})`,
        };
      }),
      { max: Math.min(1, Math.ceil(most * 10) / 10) },
    ),
    el(
      "p",
      { class: "note" },
      "Early is more than a minute before the aimed time; on time is within a minute either side.",
    ),
  );
}

async function select(hub, move) {
  const token = (shown = [hub]);
  adding = false;
  Object.assign(state, { arrival: null, pair: null });
  mark(shown);
  if (move) map.setView([hub.lat, hub.lon], Math.max(map.getZoom(), 14));
  const add = el(
    "button",
    {
      type: "button",
      class: "link",
      onclick: () => {
        adding = true;
        add.replaceWith(
          el("p", { class: "note" }, "Click a second stop on the map or search for it."),
        );
      },
    },
    "Compare with a nearby stop",
  );
  const box = el(
    "div",
    { class: "hub" },
    el("h2", {}, hub.name),
    el("p", { class: "facts" }, facts(hub)),
    add,
  );
  $("#hub").replaceChildren(box);
  box.append(delays(hub));
  let content;
  try {
    content = (await loadShard(hub.shard))[hub.id];
  } catch {
    box.append(
      el(
        "p",
        { class: "section empty" },
        `Could not load hubs/${pad2(hub.shard)}.js. Rebuild the dashboard.`,
      ),
    );
    return;
  }
  if (shown !== token) return;
  const byHour = new Map(content.hours.map(([h, scored, late]) => [h, { scored, late }]));
  box.append(
    el(
      "div",
      { class: "section" },
      el("h3", {}, "Late departures by hour"),
      columns(
        [...Array(24).keys()].map((h) => {
          const v = byHour.get(h);
          const share = v ? v.late / v.scored : null;
          return {
            label: pad2(h),
            value: share,
            thin: !v || v.scored < 10,
            cls: "late",
            title: v
              ? `${pad2(h)}:00–${pad2(h)}:59: ${pct(share)} late of ${nf.format(v.scored)}`
              : `${h}:00 no departures`,
          };
        }),
        { every: 3 },
      ),
      el(
        "p",
        { class: "note" },
        "Local time of the aimed departure. Faded bars rest on fewer than 10 departures.",
      ),
    ),
  );
  const transfers = el("div", { class: "section" }, el("h3", {}, "Transfers here"));
  box.append(transfers);
  renderTransfers(transfers, content.transfers);
}

function renderTransfers(box, pairs) {
  if (!pairs.length) {
    box.append(
      el(
        "p",
        { class: "empty" },
        `No pair of lines here has ${G.rules.min_known_connections} observed connections yet. Try a terminal or station nearby.`,
      ),
    );
    return;
  }
  const arrivals = new Map();
  for (const p of pairs) {
    p.counts = expand(p.gaps);
    p.known = sum(p.counts);
    const key = `${p.from}|${p.origin_id}`; // names are labels only: two origins may share one
    if (!arrivals.has(key))
      arrivals.set(key, { key, from: p.from, origin: p.origin, known: 0, pairs: [] });
    const a = arrivals.get(key);
    a.known += p.known;
    a.pairs.push(p);
  }
  const options = [...arrivals.values()].sort((a, b) => b.known - a.known);
  options.forEach((a) => a.pairs.sort((x, y) => y.known - x.known));
  state.arrival = options[0];
  state.pair = options[0].pairs[0];
  const results = el("div");
  const value = el("output", { for: "spare" }, `${state.spare} min`);
  // Controls stay in place; only the list and the answer redraw, so the slider keeps its drag.
  box.append(
    el(
      "p",
      { class: "note" },
      "Spare minutes are the planned gap minus the time you need to walk between the two platforms. " +
        "A connection holds when the departure's delay minus the arrival's delay does not eat them.",
    ),
    el(
      "div",
      { class: "controls" },
      el(
        "label",
        { for: "arrival" },
        "Arriving on",
        el(
          "select",
          {
            id: "arrival",
            onchange: (e) => {
              state.arrival = options[e.target.selectedIndex];
              state.pair = state.arrival.pairs[0];
              update();
            },
          },
          options.map((a) =>
            el(
              "option",
              { title: lineName(a.from) },
              `${lineCode(a.from)} from ${a.origin || "?"} (${nf.format(a.known)})`,
            ),
          ),
        ),
      ),
      el(
        "label",
        { for: "spare" },
        "Spare minutes",
        el(
          "div",
          { class: "buffer" },
          el("input", {
            id: "spare",
            type: "range",
            min: 0,
            max: 10,
            step: 1,
            value: state.spare,
            oninput: (e) => {
              state.spare = Number(e.target.value);
              value.textContent = `${state.spare} min`;
              update();
            },
          }),
          value,
        ),
      ),
    ),
    results,
  );

  function update() {
    const { arrival, pair, spare } = state;
    const list = el(
      "ul",
      { class: "pairs" },
      arrival.pairs.slice(0, 40).map((p) => {
        const chance = held(p.counts, spare) / p.known;
        return el(
          "li",
          {},
          el(
            "button",
            {
              type: "button",
              "aria-pressed": String(p === pair),
              title: lineName(p.to),
              onclick: () => {
                state.pair = p;
                update();
              },
            },
            el(
              "span",
              { class: "name" },
              el("strong", {}, lineCode(p.to)),
              ` to ${p.destination || "?"}`,
            ),
            el(
              "span",
              { class: "meter", "aria-hidden": "true" },
              el("span", { style: `width:${(100 * chance).toFixed(1)}%` }),
            ),
            el("span", { class: "pct" }, pct(chance)),
          ),
        );
      }),
    );
    const hours = new Map(Object.entries(pair.hours).map(([h, t]) => [Number(h), expand(t)]));
    results.replaceChildren(
      list,
      el(
        "div",
        { class: "split" },
        el(
          "div",
          { class: "answer" },
          el("span", { class: "big" }, pct(held(pair.counts, spare) / pair.known)),
          el(
            "p",
            {},
            "of ",
            el("strong", {}, `${nf.format(pair.known)} observed connections`),
            ` from line ${lineCode(pair.from)} to line ${lineCode(pair.to)} towards ${pair.destination || "?"} held with ${spare} spare minute${spare === 1 ? "" : "s"}. ` +
              `${nf.format(pair.cancelled)} were lost to a cancellation, ${nf.format(pair.unknown)} lacked an actual time.`,
          ),
        ),
        el(
          "div",
          {},
          el("h3", {}, "Chance against spare minutes"),
          curve(
            [...Array(11).keys()].map((m) => held(pair.counts, m) / pair.known),
            spare,
          ),
        ),
        el(
          "div",
          {},
          el("h3", {}, `Chance by hour of arrival, ${spare} spare min`),
          columns(
            [...Array(24).keys()].map((h) => {
              const counts = hours.get(h),
                n = counts ? sum(counts) : 0;
              return {
                label: pad2(h),
                value: n ? held(counts, spare) / n : null,
                thin: n < 5,
                title: n
                  ? `${pad2(h)}:00–${pad2(h)}:59: held ${pct(held(counts, spare) / n)} of ${n}`
                  : `${pad2(h)}:00 none observed`,
              };
            }),
            { every: 3 },
          ),
          el("p", { class: "note" }, "Faded bars rest on fewer than 5 connections."),
        ),
      ),
    );
  }
  update();
}

// ---------- two stops ----------
// Gold pairs calls only within one hub, so a transfer between two hubs is estimated, not observed:
// the arriving line's arrival delays at one stop against the leaving line's departure delays at the
// other, in the same time of day, taken as independent.
const BANDS = G.rules.bands.map(([name, start], i, all) => {
  const hh = (h) => String(h).padStart(2, "0"), end = i + 1 < all.length ? all[i + 1][1] : 24;
  return { name, hours: `${hh(start)}–${hh(end)}` };
});
function histogram(entry, band) { // minute counts from rules.delay_low_min, one band or all (null)
  const counts = new Array(G.rules.delay_bins).fill(0);
  entry.bands.forEach((t, i) => { if (band == null || band === i) expand(t, counts.length).forEach((v, m) => { counts[m] += v; }); });
  return counts;
}
// Each delay is spread evenly over its floored minute, so k whole minutes of slack left after
// both delays hold for k >= 1, half the time for k = 0 and never below.
function estimate(arrive, leave, spare) {
  let p = 0;
  arrive.forEach((ca, a) => { if (ca) leave.forEach((cl, d) => { const k = d - a + spare; if (k >= 0) p += ca * cl * (k ? 1 : 0.5); }); });
  return p / (sum(arrive) * sum(leave));
}
function metres(a, b) {
  const rad = Math.PI / 180, x = (b.lon - a.lon) * rad * Math.cos(((a.lat + b.lat) / 2) * rad), y = (b.lat - a.lat) * rad;
  return 6371000 * Math.hypot(x, y);
}

async function compare(a, b) {
  const token = shown = [a, b];
  adding = false;
  mark(shown);
  const m = metres(a, b);
  const stops = [a, b].map((hub) => el("div", { class: "section stop" },
    el("h3", {}, hub.name, " ", el("button", { type: "button", class: "link", onclick: () => select(hub, false) }, "show alone")),
    el("p", { class: "facts" }, facts(hub)), delays(hub)));
  const box = el("div", { class: "hub" }, el("h2", {}, "Transfer between two stops"),
    el("p", { class: "facts" }, `${nf.format(Math.round(m / 10) * 10)} m apart in a straight line, ` +
      `about ${Math.max(1, Math.round(m / 80))} min on foot.`));
  $("#hub").replaceChildren(box);
  let content;
  try { content = await Promise.all([a, b].map((hub) => loadShard(hub.shard).then((s) => s[hub.id]))); } catch {
    box.append(el("p", { class: "section empty" }, "Could not load the stops' line delays. Rebuild the dashboard."), ...stops);
    return;
  }
  if (shown !== token) return;
  for (const c of content) for (const list of Object.values(c.delays)) list.sort((x, y) => (y.line === "") - (x.line === "") || y.calls - x.calls);

  const pick = { flip: 0, arrive: 0, leave: 0, band: null };
  const sides = () => pick.flip ? [b, a, content[1].delays.arrival, content[0].delays.departure]
    : [a, b, content[0].delays.arrival, content[1].delays.departure];
  const label = (e, way) => e.line ? `${lineCode(e.line)} ${way} ${e.end || "?"}` : "Any line";
  const lines = el("div", { class: "controls" }), results = el("div");
  const value = el("output", { for: "spare" }, `${state.spare} min`);
  function chooseLines() {
    const [from, to, arrivals, departures] = sides();
    const list = (title, entries, way, key) => el("label", {}, title, el("select", {
      onchange: (e) => { pick[key] = e.target.selectedIndex; update(); } },
    entries.map((e, i) => el("option", { title: e.line ? lineName(e.line) : "", selected: i === pick[key] },
      `${label(e, way)} (${nf.format(sum(histogram(e)))})`))));
    lines.replaceChildren(list(`Arriving at ${from.name} on`, arrivals, "from", "arrive"),
      list(`Leaving ${to.name} on`, departures, "to", "leave"));
  }
  box.append(
    el("div", { class: "controls" },
      el("label", {}, "Direction", el("select", { onchange: (e) => {
        Object.assign(pick, { flip: e.target.selectedIndex, arrive: 0, leave: 0 }); chooseLines(); update(); } },
      el("option", {}, `Arrive at ${a.name}, leave from ${b.name}`), el("option", {}, `Arrive at ${b.name}, leave from ${a.name}`))),
      lines,
      el("label", {}, "Time of day", el("select", { onchange: (e) => { pick.band = e.target.selectedIndex ? e.target.selectedIndex - 1 : null; update(); } },
        el("option", {}, "All day"), BANDS.map((band) => el("option", {}, `${band.name}, ${band.hours}`)))),
      el("label", { for: "spare" }, "Spare minutes", el("div", { class: "buffer" },
        el("input", { id: "spare", type: "range", min: 0, max: 10, step: 1, value: state.spare,
          oninput: (e) => { state.spare = Number(e.target.value); value.textContent = `${state.spare} min`; update(); } }),
        value))),
    el("p", { class: "note" }, "Spare minutes are the planned gap minus the walk."),
    results, ...stops);

  function update() {
    const [from, to, arrivals, departures] = sides(), spare = state.spare;
    const arriving = arrivals[pick.arrive], leaving = departures[pick.leave];
    if (!arriving || !leaving) {
      results.replaceChildren(el("p", { class: "empty" }, `Too few timed calls at ${arriving ? to.name : from.name} to estimate a transfer.`));
      return;
    }
    const counts = (band) => [histogram(arriving, band), histogram(leaving, band)];
    const [arrive, leave] = counts(pick.band), n = [sum(arrive), sum(leave)];
    const enough = Math.min(...n) >= G.rules.min_known_connections;
    const when = pick.band == null ? "over the whole day" : `in the ${BANDS[pick.band].name.toLowerCase()} (${BANDS[pick.band].hours})`;
    if (!Math.min(...n)) {
      results.replaceChildren(el("p", { class: "empty" }, `No timed calls ${when} for this pair of lines.`));
      return;
    }
    const points = [...Array(11).keys()].map((s) => estimate(arrive, leave, s));
    results.replaceChildren(el("div", { class: "split" },
      el("div", { class: `answer ${enough ? "" : "thin"}` }, el("span", { class: "big" }, pct(points[spare])),
        el("p", {}, "estimated chance the connection holds with ", el("strong", {}, `${spare} spare minute${spare === 1 ? "" : "s"}`),
          `, arriving at ${from.name} on ${label(arriving, "from").replace("Any", "any")} and leaving ${to.name} ` +
          `on ${label(leaving, "to").replace("Any", "any")}, ${when}. ` +
          `From ${nf.format(n[0])} timed arrivals and ${nf.format(n[1])} timed departures` +
          (leaving.cancelled ? `; ${nf.format(leaving.cancelled)} cancelled departure${leaving.cancelled === 1 ? "" : "s"} over the day.` : ".") +
          (enough ? "" : ` Fewer than ${G.rules.min_known_connections} on one side: a rough guide only.`))),
      el("div", {}, el("h3", {}, "Chance against spare minutes"), curve(points, spare)),
      el("div", {}, el("h3", {}, `Chance by time of day, ${spare} spare min`),
        columns(BANDS.map((band, i) => {
          const [x, y] = counts(i), k = Math.min(sum(x), sum(y));
          return { label: band.hours, value: k ? estimate(x, y, spare) : null, thin: k < G.rules.min_known_connections,
            title: k ? `${band.name}: ${pct(estimate(x, y, spare))} from ${nf.format(sum(x))} arrivals and ${nf.format(sum(y))} departures`
              : `${band.name}: no timed calls on one side` };
        })),
        el("p", { class: "note" }, `Faded bars rest on fewer than ${G.rules.min_known_connections} calls on one side.`)),
      el("p", { class: "note" }, "An estimate, not observed connections: Gold pairs arrivals and departures only within one stop place. " +
        "It takes the arriving line's arrival delays and the leaving line's departure delays at these stops in the same time of day, " +
        "assumes the two are independent and spreads each delay evenly over its minute.")));
  }
  chooseLines();
  update();
}

// ---------- road conditions ----------
const FACTORS = [
  {
    key: "precipitation",
    title: "Precipitation",
    ref: "dry",
    levels: {
      dry: "Dry, under 0.1 mm/h",
      light: "Light, 0.1–1 mm/h",
      heavy: "Heavy, 1 mm/h or more",
      unknown: "No usable station or hour",
    },
  },
  {
    key: "road_surface",
    title: "Road surface temperature",
    ref: "above_zero",
    levels: {
      above_zero: "Above 0 °C",
      freezing: "At or below 0 °C",
      unknown: "No usable station or hour",
    },
  },
  {
    key: "road_situation",
    title: "Road situations within 500 m",
    ref: "none",
    levels: {
      none: "None active",
      roadworks: "Roadworks or traffic management",
      incident: "Incident: accident, obstruction or poor conditions",
      unknown: "Outside the DATEX day",
    },
  },
  {
    key: "traffic",
    title: "Traffic on sections within 500 m",
    ref: "free_flow",
    levels: {
      free_flow: "Free flow, under 1.2× free-flow time",
      slow: "Slow, 1.2–1.5×",
      congested: "Congested, 1.5× or more",
      unmeasured: "Section without a measurement",
      no_section: "No measured section nearby",
      unknown: "Outside the DATEX day",
    },
  },
];
const C = Object.fromEntries(G.condition_columns.map((c, i) => [c, i]));
const modes = new Map();
for (const r of G.conditions)
  modes.set(r[C.mode], (modes.get(r[C.mode]) || 0) + r[C.scored_departures]);
$("#mode").replaceChildren(
  el("option", { value: "" }, "All modes"),
  [...modes]
    .sort((a, b) => b[1] - a[1])
    .map(([m, n]) => el("option", { value: m }, `${m} (${nf.format(n)})`)),
);
$("#mode").addEventListener("change", renderRoads);

function renderRoads() {
  const mode = $("#mode").value;
  const rows = G.conditions.filter((r) => !mode || r[C.mode] === mode);
  $("#factors").replaceChildren(
    ...FACTORS.map((f) => {
      // Late share per level and per stratum (mode, hour). Each level is then compared with
      // the reference level within the same strata.
      const levels = {},
        strata = {};
      for (const r of rows) {
        const level = r[C[f.key]],
          s = `${r[C.mode]}|${r[C.hour]}`,
          n = r[C.scored_departures],
          late = r[C.late_departures];
        const L = (levels[level] ||= { n: 0, late: 0, strata: {} });
        L.n += n;
        L.late += late;
        const S = (L.strata[s] ||= { n: 0, late: 0 });
        S.n += n;
        S.late += late;
        if (level === f.ref) strata[s] = S;
      }
      const scale = Math.max(
        5,
        ...Object.entries(levels)
          .filter(([level]) => level !== "unknown")
          .map(([level, L]) => Math.abs(matched(L, strata, level === f.ref).diff || 0)),
      );
      const body = Object.keys(f.levels)
        .filter((level) => levels[level])
        .map((level) => {
          const L = levels[level],
            m = matched(L, strata, level === f.ref);
          const bar =
            level === f.ref
              ? el("span", { class: "note" }, "reference")
              : level === "unknown"
                ? el("span", { class: "note" }, "a gap, not compared")
                : m.diff == null
                  ? el("span", { class: "note" }, "no match")
                  : diverging(m.diff, scale);
          return el(
            "tr",
            {},
            el("td", {}, f.levels[level]),
            el("td", {}, nf.format(L.n)),
            el("td", {}, pct(L.late / L.n)),
            el(
              "td",
              {
                title:
                  m.diff == null
                    ? ""
                    : `${(100 * m.covered).toFixed(0)}% of these departures had a reference in the same mode and hour`,
              },
              bar,
            ),
          );
        });
      return el(
        "section",
        { class: "factor" },
        el("h3", {}, f.title),
        el("p", { class: "note" }, `Reference: ${f.levels[f.ref].toLowerCase()}.`),
        el(
          "table",
          {},
          el(
            "thead",
            {},
            el(
              "tr",
              {},
              el("th", {}, "Condition"),
              el("th", {}, "Departures"),
              el("th", {}, "Late"),
              el("th", {}, "vs reference"),
            ),
          ),
          el("tbody", {}, body),
        ),
      );
    }),
  );
}

// Weighted mean of (level - reference) late share, over the strata where the reference
// has 30+ departures.
function matched(L, strata, isRef) {
  if (isRef) return { diff: 0, covered: 1 };
  let weight = 0,
    total = 0;
  for (const [s, S] of Object.entries(L.strata)) {
    const R = strata[s];
    if (!R || R.n < 30) continue;
    weight += S.n;
    total += S.n * (S.late / S.n - R.late / R.n);
  }
  return weight ? { diff: (100 * total) / weight, covered: weight / L.n } : { diff: null };
}

function diverging(diff, scale) {
  const signed = `${diff > 0 ? "+" : ""}${diff.toFixed(1)}`;
  const width = (50 * Math.min(Math.abs(diff), scale)) / scale;
  const box = el(
    "span",
    { class: "delta" },
    el(
      "span",
      { class: "track", "aria-hidden": "true" },
      el("span", {
        class: diff > 0 ? "worse" : "better",
        style: `width:${width}%;${diff > 0 ? "left:50%" : `right:50%`}`,
      }),
    ),
    el("b", {}, `${signed} pp`),
  );
  box.title = `${signed} percentage points late compared with the reference in the same mode and hour`;
  return box;
}

// ---------- delay between stops ----------
// Loaded on first view: segments.js holds one row per hub pair (see dashboard/build.py).
const SEG_BANDS = [[-60, "--better-2", "recovers 1+ min"], [-15, "--better-1", "recovers 15–60 s"],
  [15, "--even", "within 15 s"], [60, "--worse-1", "gains 15–60 s"], [Infinity, "--worse-2", "gains 1+ min"]];
const signed = (x, text) => `${x > 0 ? "+" : x < 0 ? "−" : "±"}${text}`;
const perRun = (s) => signed(s.mean, `${Math.abs(s.mean / 60).toFixed(1)} min`);
const total = (s) => signed(s.change, `${nf.format(Math.round(Math.abs(s.change) / 3600))} h`);
let segMap = null, segs = [];

function showSegments() {
  if (segMap) { segMap.invalidateSize(); return; }
  segMap = baseMap("segmap");
  window.goldSegments = drawSegments;
  document.head.append(el("script", { src: "segments.js", onerror: () => $("#seglists").replaceChildren(
    el("p", { class: "empty" }, "Could not load segments.js. Rebuild the dashboard.")) }));
}

// Each direction sits 3 px to the right of travel, so A→B and B→A are drawn side by side.
function rightOfTravel(s) {
  const a = segMap.latLngToLayerPoint(s.a), b = segMap.latLngToLayerPoint(s.b);
  const d = b.subtract(a), len = Math.hypot(d.x, d.y) || 1, n = L.point((-3 * d.y) / len, (3 * d.x) / len);
  return [segMap.layerPointToLatLng(a.add(n)), segMap.layerPointToLatLng(b.add(n))];
}

function segmentText(s) {
  return el("div", {}, el("strong", {}, `${s.from} → ${s.to}`), el("br"),
    `${perRun(s)} per run over ${nf.format(s.runs)} runs (${total(s)} in all)`, el("br"),
    `Gained 1+ min on ${pct(s.gained / s.runs)}, recovered 1+ min on ${pct(s.recovered / s.runs)}`);
}

function drawSegments(rows) {
  segs = rows.map(([from, to, fy, fx, ty, tx, runs, change, gained, recovered]) => ({ from: from || "?", to: to || "?",
    a: L.latLng(fy, fx), b: L.latLng(ty, tx), runs, change, gained, recovered, mean: change / runs }));
  const renderer = L.canvas({ padding: 0.4 });
  // Strongest last, so they are drawn on top of the neutral ones.
  for (const s of [...segs].sort((x, y) => Math.abs(x.mean) - Math.abs(y.mean))) {
    s.line = L.polyline(rightOfTravel(s), { renderer, color: css(SEG_BANDS.find(([edge]) => s.mean < edge)[1]),
      weight: Math.max(1.5, Math.min(6, Math.sqrt(s.runs) / 8)), opacity: 0.9 })
      .bindTooltip(() => segmentText(s), { sticky: true }).on("click", () => focusSegment(s)).addTo(segMap);
  }
  segMap.on("zoomend", () => segs.forEach((s) => s.line.setLatLngs(rightOfTravel(s))));
  const legend = L.control({ position: "bottomleft" });
  legend.onAdd = () => el("div", { class: "legend" }, el("b", {}, "Delay per run to the next stop"),
    SEG_BANDS.map(([, token, label]) => el("div", {}, el("i", { style: `background:${css(token)}` }), label)),
    el("div", {}, `Width: number of runs, at least ${G.rules.min_segments}`));
  legend.addTo(segMap);
  renderSegmentLists();
}

function focusSegment(s) {
  segMap.fitBounds(L.latLngBounds(s.a, s.b), { maxZoom: 15, padding: [60, 60] });
  s.line.bringToFront().openTooltip(L.latLngBounds(s.a, s.b).getCenter());
}

function renderSegmentLists() {
  const byTotal = $("#rank").value === "total", key = byTotal ? (s) => s.change : (s) => s.mean;
  const sorted = [...segs].sort((x, y) => key(y) - key(x));
  const list = (items) => el("ol", { class: "pairs segs" }, items.map((s) => el("li", {},
    el("button", { type: "button", onclick: () => focusSegment(s), title: `${nf.format(s.runs)} runs` },
      el("span", { class: "name" }, `${s.from} → ${s.to}`),
      el("span", { class: `pct ${s.mean > 0 ? "gain" : "loss"}` }, byTotal ? total(s) : perRun(s))))));
  $("#seglists").replaceChildren(
    el("div", { class: "section" }, el("h3", {}, "Gains the most delay"), list(sorted.filter((s) => s.mean > 0).slice(0, 15))),
    el("div", { class: "section" }, el("h3", {}, "Recovers the most delay"),
      list(sorted.filter((s) => s.mean < 0).slice(-15).reverse())),
    el("p", { class: "section note" }, `${nf.format(segs.length)} stop pairs with at least ${G.rules.min_segments} timed runs. ` +
      `Each run's change is capped at ±${G.rules.max_segment_change_s / 60} min. Operators that copy aimed into actual ` +
      "times show no change. Where the timetable has slack, recovery is planned, not a sign of fast running."));
}
$("#rank").addEventListener("change", () => { if (segs.length) renderSegmentLists(); });

intro();
renderRoads();
