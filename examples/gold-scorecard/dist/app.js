"use strict";

(() => {
  const data = window.GOLD_SCORECARD;
  const $ = (id) => document.getElementById(id);
  const integer = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });
  const decimal = new Intl.NumberFormat("en-GB", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const dateFormat = new Intl.DateTimeFormat("en-GB", { day: "numeric", month: "long", year: "numeric", timeZone: "UTC" });
  const isNumber = (value) => typeof value === "number" && Number.isFinite(value);
  const count = (value) => isNumber(value) ? integer.format(value) : "—";
  const numberOrZero = (value) => isNumber(value) ? value : 0;
  const lineCode = (line) => line.public_code || String(line.line_id || "?").split(":").pop();
  const lineName = (line) => line.line_name || "Unnamed line";
  const plural = (value, singular, multiple) => value === 1 ? singular : multiple;
  const isDataset = (value) => value && value.meta && Array.isArray(value.lines);
  const isReady = (meta) => meta.ready === true && meta.status === "complete";
  const LIST_LIMIT = 50;
  const COMPARISON_LIMIT = 25;

  function formattedDate(value) {
    const date = /^\d{4}-\d{2}-\d{2}$/.test(value || "") ? new Date(`${value}T12:00:00Z`) : null;
    return date && !Number.isNaN(date.getTime()) ? dateFormat.format(date) : "Date unavailable";
  }

  const days = new Map();
  if (Array.isArray(data?.days)) {
    data.days.filter(isDataset).forEach((day) => {
      if (/^\d{4}-\d{2}-\d{2}$/.test(day.meta.operating_date || "")) days.set(day.meta.operating_date, day);
    });
    // Keep the latest top-level dataset available if the optional collection
    // contains only the preceding days.
    if (isDataset(data) && /^\d{4}-\d{2}-\d{2}$/.test(data.meta.operating_date || "") && !days.has(data.meta.operating_date)) {
      days.set(data.meta.operating_date, { meta: data.meta, lines: data.lines });
    }
  }
  const availableDates = Array.from(days.keys()).sort().reverse();
  const initial = isDataset(data) ? data : days.get(availableDates[0]);
  if (!initial) {
    $("sample-notice").hidden = true;
    $("load-error").hidden = false;
    return;
  }

  let meta;
  let lines = [];
  let selectedId = null;

  function setMetric(id, value, unit) {
    const element = $(id);
    element.replaceChildren();
    element.classList.toggle("is-unmeasured", !isNumber(value));
    if (!isNumber(value)) {
      element.textContent = "Not measured";
      return;
    }
    element.append(document.createTextNode(decimal.format(value)));
    const suffix = document.createElement("span");
    suffix.className = "metric-unit";
    suffix.textContent = unit;
    element.append(suffix);
  }

  function renderDetail(line) {
    $("detail-empty").hidden = Boolean(line);
    $("selected-results").hidden = !line;
    if (!line) {
      $("selection-announcement").textContent = `No matching lines for ${formattedDate(meta.operating_date)}.`;
      return;
    }

    const measured = isNumber(line.scored_departures) && line.scored_departures > 0;
    $("selected-code").textContent = lineCode(line);
    $("selected-code").title = line.public_code ? "" : line.line_id || "";
    $("selected-name").textContent = lineName(line);
    $("selected-operator").textContent = [line.operator_name || "Operator not available", line.mode].filter(Boolean).join(" · ");
    $("journey-count").textContent = count(line.reported_journeys);
    $("journey-label").textContent = plural(line.reported_journeys, "journey observed", "journeys observed");
    $("departure-count").textContent = count(line.scored_departures);
    $("departure-label").textContent = plural(line.scored_departures, "stop departure timed", "stop departures timed");
    $("coverage-note").textContent = isNumber(line.reported_planned_journeys) && isNumber(line.planned_journeys)
      ? `${count(line.reported_planned_journeys)} of ${count(line.planned_journeys)} planned journeys have a report. Missing reports remain unknown.`
      : "Reporting coverage is unavailable. Missing reports remain unknown.";
    setMetric("median-delay", measured && isNumber(line.median_departure_delay_s) ? line.median_departure_delay_s / 60 : null, "min");
    setMetric("p90-delay", measured && isNumber(line.p90_departure_delay_s) ? line.p90_departure_delay_s / 60 : null, "min");
    setMetric("late-share", measured ? line.late_departure_pct : null, "%");
    $("late-count").textContent = measured
      ? `${count(line.late_departures)} of ${count(line.scored_departures)} timed stop departures.`
      : "No usable actual departure times for this day.";

    const early = line.early_departures;
    const late = line.late_departures;
    const distributionKnown = measured && isNumber(early) && isNumber(late) && early >= 0 && late >= 0 && early + late <= line.scored_departures;
    $("distribution-content").hidden = !distributionKnown;
    $("measurement-empty").hidden = distributionKnown;
    $("measurement-empty").textContent = measured
      ? "The delay split is unavailable for these observations."
      : "No usable actual departure times are available for this line on this day.";
    if (distributionKnown) {
      const within = line.scored_departures - early - late;
      [["early", early], ["within", within], ["late", late]].forEach(([key, value]) => {
        $(`segment-${key}`).style.width = `${value / line.scored_departures * 100}%`;
        $(`count-${key}`).textContent = count(value);
      });
      $("distribution-bar").setAttribute("aria-label", `${count(early)} departures early, ${count(within)} from zero to less than three minutes late, and ${count(late)} at least three minutes late, out of ${count(line.scored_departures)} timed stop departures.`);
    }

    $("reading-note").textContent = isReady(meta)
      ? "A journey visits many stops. Delay measurements describe departures from those individual stops on the selected day."
      : "A journey visits many stops. Results use only the available observations for this day; missing reports remain unknown.";
    $("selection-announcement").textContent = `${formattedDate(meta.operating_date)}. Line ${lineCode(line)} selected. ${count(line.reported_journeys)} ${plural(line.reported_journeys, "journey", "journeys")} observed and ${count(line.scored_departures)} stop departures timed.`;
  }

  function renderList(filtered) {
    const previousScroll = $("line-list").scrollTop;
    $("line-list").replaceChildren();
    $("line-count").textContent = count(filtered.length);
    $("search-empty").hidden = filtered.length > 0;
    $("line-list-note").textContent = filtered.length > LIST_LIMIT
      ? `Showing ${LIST_LIMIT} of ${count(filtered.length)} matches. Search to narrow the list.`
      : `${count(filtered.length)} ${plural(filtered.length, "matching line", "matching lines")}.`;
    const visible = filtered.slice(0, LIST_LIMIT);
    // Keep a selected search result visible when the search is broadened again.
    const selected = filtered.find((line) => line.line_id === selectedId);
    if (selected && !visible.some((line) => line.line_id === selectedId)) visible[visible.length - 1] = selected;
    visible.forEach((line) => {
      const item = document.createElement("li");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "line-button";
      button.dataset.lineId = line.line_id;
      button.setAttribute("aria-pressed", String(line.line_id === selectedId));
      button.setAttribute("aria-controls", "selected-results");
      button.setAttribute("aria-label", `Line ${lineCode(line)}, ${lineName(line)}, ${count(line.scored_departures)} timed stop departures`);
      const code = document.createElement("span");
      code.className = "mini-route";
      code.textContent = lineCode(line);
      if (!line.public_code) code.title = line.line_id || "";
      const copy = document.createElement("span");
      copy.className = "line-button-copy";
      const name = document.createElement("span");
      name.className = "line-button-name";
      name.textContent = lineName(line);
      const observations = document.createElement("span");
      observations.className = "line-button-count";
      observations.textContent = `${count(line.scored_departures)} timed stop ${plural(line.scored_departures, "departure", "departures")}`;
      copy.append(name, observations);
      button.append(code, copy);
      item.append(button);
      $("line-list").append(item);
    });
    $("line-list").scrollTop = previousScroll;
  }

  function filteredLines() {
    const term = $("line-search").value.trim().toLocaleLowerCase();
    return term ? lines.filter((line) => [lineCode(line), line.line_id || "", lineName(line), line.operator_name || ""]
      .some((value) => String(value).toLocaleLowerCase().includes(term))) : lines;
  }

  function selectLine(id, focusButton = false) {
    selectedId = id;
    renderViews(filteredLines());
    if (focusButton) {
      const selected = Array.from($("line-list").querySelectorAll("button"))
        .find((button) => button.dataset.lineId === id);
      if (selected) selected.focus({ preventScroll: true });
    }
  }

  function renderComparison(filtered) {
    $("comparison-rows").replaceChildren();
    $("comparison").hidden = filtered.length === 0;
    $("comparison-limit-note").textContent = filtered.length > COMPARISON_LIMIT
      ? `Showing ${COMPARISON_LIMIT} of ${count(filtered.length)} matching lines. Search to compare another line.`
      : `${count(filtered.length)} ${plural(filtered.length, "matching line", "matching lines")} shown for this day.`;
    filtered.slice(0, COMPARISON_LIMIT).forEach((line) => {
      const row = document.createElement("tr");
      const label = document.createElement("td");
      const button = document.createElement("button");
      button.className = "table-line-button";
      button.type = "button";
      button.dataset.lineId = line.line_id;
      button.textContent = lineCode(line);
      button.setAttribute("aria-label", `View line ${lineCode(line)}, ${lineName(line)}`);
      const name = document.createElement("span");
      name.className = "table-line-name";
      name.textContent = lineName(line);
      label.append(button, name);
      row.append(label);
      const measured = isNumber(line.scored_departures) && line.scored_departures > 0;
      [count(line.reported_journeys), count(line.scored_departures),
        measured && isNumber(line.median_departure_delay_s) ? `${decimal.format(line.median_departure_delay_s / 60)} min` : "—",
        measured && isNumber(line.late_departure_pct) ? `${decimal.format(line.late_departure_pct)}%` : "—"
      ].forEach((value) => {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      });
      $("comparison-rows").append(row);
    });
  }

  function renderViews(filtered) {
    renderList(filtered);
    renderDetail(filtered.find((line) => line.line_id === selectedId));
    renderComparison(filtered);
  }

  function showDay(dataset) {
    meta = dataset.meta;
    // Each dataset is already a daily aggregate. Never combine its percentiles
    // with another day: the selector switches datasets rather than averaging.
    lines = dataset.lines.slice().sort((a, b) =>
      numberOrZero(b.scored_departures) - numberOrZero(a.scored_departures) ||
      String(lineCode(a)).localeCompare(String(lineCode(b)), "en", { numeric: true })
    );
    selectedId = lines.length ? lines[0].line_id : null;
    $("line-search").value = "";
    $("line-list").scrollTop = 0;
    $("operating-date").dateTime = meta.operating_date || "";
    $("operating-date").textContent = formattedDate(meta.operating_date);
    if (days.has(meta.operating_date)) $("day-select").value = meta.operating_date;
    const ready = isReady(meta);
    $("sample-notice").classList.toggle("is-complete", ready);
    $("sample-tag").textContent = ready ? "Daily results" : days.size ? "Partial data" : "Small sample";
    $("sample-description").textContent = ready
      ? `${count(meta.scored_departures)} timed stop departures across ${count(meta.reported_journeys)} observed journeys. Missing reports remain unknown.`
      : days.size
        ? `Incomplete input for this day: ${count(meta.reported_journeys)} journeys reported and ${count(meta.scored_departures)} stop departures timed. Missing reports remain unknown.`
        : `${count(meta.reported_journeys)} journeys sampled · ${count(meta.scored_departures)} stop departures timed. These observations cover only a small part of the day’s services.`;
    $("picker-caption").textContent = days.size ? "Search all available lines for this day" : "Lines with reports in this sample";
    $("comparison-note").textContent = ready
      ? "Use the observation counts when comparing lines. Each day's delay statistics describe that day separately."
      : "Use the observation counts when comparing lines. Incomplete observations cannot describe a line’s usual reliability.";
    renderViews(lines);
  }

  if (days.size) {
    availableDates.forEach((day) => {
      const option = document.createElement("option");
      option.value = day;
      option.textContent = formattedDate(day);
      $("day-select").append(option);
    });
    $("day-field").hidden = false;
    $("operating-date").hidden = true;
    $("page-description").textContent = "Choose a day and line to explore observed departure delays.";
    $("day-select").addEventListener("change", () => {
      const dataset = days.get($("day-select").value);
      if (dataset) showDay(dataset);
    });
  }
  $("download-label").textContent = days.size > 1 ? "Download period CSV" : "Download CSV";
  $("workspace").hidden = false;
  showDay(days.get(initial.meta.operating_date) || (days.size ? days.get(availableDates[0]) : initial));

  $("line-search").addEventListener("input", () => {
    const filtered = filteredLines();
    if (!filtered.some((line) => line.line_id === selectedId)) selectedId = filtered.length ? filtered[0].line_id : null;
    renderViews(filtered);
  });
  $("clear-search").addEventListener("click", () => {
    $("line-search").value = "";
    selectLine(lines.length ? lines[0].line_id : null);
    $("line-search").focus();
  });
  $("line-list").addEventListener("click", (event) => {
    const button = event.target.closest("button[data-line-id]");
    if (button) selectLine(button.dataset.lineId, true);
  });
  $("comparison-rows").addEventListener("click", (event) => {
    const button = event.target.closest("button[data-line-id]");
    if (!button) return;
    selectLine(button.dataset.lineId);
    $("line-detail").focus({ preventScroll: true });
    $("line-detail").scrollIntoView({ behavior: "auto", block: "start" });
  });
})();
