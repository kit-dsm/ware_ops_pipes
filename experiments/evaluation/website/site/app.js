let results = [];

const METRIC_INFO = {
  total_distance: { label: "Total distance", direction: "min", unit: "" },
  total_cpu_time: { label: "CPU time", direction: "min", unit: "s" },
  makespan: { label: "Makespan", direction: "min", unit: "" },
  on_time_rate: { label: "On-time rate", direction: "max", unit: "%" },
  max_tardiness: { label: "Max. tardiness", direction: "min", unit: "" },
};

const STAGES = ["item_assignment", "batching", "routing", "scheduling"];

async function loadData() {
  results = await fetch("data/results.json?v=" + Date.now()).then(r => r.json());

  initControls();
  render();
}

function unique(xs) {
  return [...new Set(xs.filter(x => x !== null && x !== undefined && x !== ""))].sort();
}

function initControls() {
  const instanceSets = ["All", ...unique(results.map(r => r.instance_set))];

  document.getElementById("instanceSet").innerHTML =
    instanceSets.map(x => `<option value="${x}">${x}</option>`).join("");

  document.getElementById("instanceSet").addEventListener("change", render);
  document.getElementById("metric").addEventListener("change", render);
  document.getElementById("onlyComparable").addEventListener("change", render);
}

function selectedInstanceSet() {
  return document.getElementById("instanceSet").value;
}

function selectedMetric() {
  return document.getElementById("metric").value;
}

function metricInfo(metric) {
  return METRIC_INFO[metric] || { label: metric, direction: "min", unit: "" };
}

function lowerIsBetter(metric) {
  return metricInfo(metric).direction === "min";
}

function filteredByInstanceSet() {
  const instanceSet = selectedInstanceSet();

  return results.filter(r => {
    return instanceSet === "All" || r.instance_set === instanceSet;
  });
}

function filteredMetricResults() {
  const metric = selectedMetric();

  return filteredByInstanceSet().filter(r => {
    return r[metric] !== null && r[metric] !== undefined && r[metric] !== "";
  });
}

function expectedInstances(rows) {
  return unique(rows.map(r => r.instance_name)).length;
}

function mean(xs) {
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

function short(value, n = 8) {
  if (value === null || value === undefined || value === "") return "";
  return String(value).slice(0, n);
}

function strategyGroups(rows, metric) {
  const expected = expectedInstances(rows);
  const grouped = new Map();

  for (const r of rows) {
    const strategy = r.strategy || r.strategy_versioned || "unknown";

    if (!grouped.has(strategy)) {
      grouped.set(strategy, {
        strategy,
        values: [],
        instances: new Set(),
        strategyVersions: new Set(),
        pipelineVersions: new Set(),
      });
    }

    const g = grouped.get(strategy);
    g.values.push(Number(r[metric]));
    g.instances.add(r.instance_name);
    g.strategyVersions.add(r.strategy_versioned || "");
    g.pipelineVersions.add(r.pipeline_chain_fingerprint || "");
  }

  return [...grouped.values()].map(g => ({
    strategy: g.strategy,
    mean: mean(g.values),
    n_results: g.values.length,
    n_instances: g.instances.size,
    expected_instances: expected,
    n_strategy_versions: [...g.strategyVersions].filter(x => x !== "").length,
    n_pipeline_versions: [...g.pipelineVersions].filter(x => x !== "").length,
    versions: [...g.strategyVersions].filter(x => x !== ""),
  }));
}

function comparableStrategyRows(rows, metric) {
  const onlyComparable = document.getElementById("onlyComparable").checked;
  let groups = strategyGroups(rows, metric);

  if (onlyComparable) {
    groups = groups.filter(g => g.n_instances === g.expected_instances);
  }

  return groups.sort((a, b) => {
    return lowerIsBetter(metric) ? a.mean - b.mean : b.mean - a.mean;
  });
}

function renderPerformanceChart() {
  const rows = filteredMetricResults();
  const metric = selectedMetric();
  const info = metricInfo(metric);
  const chartRows = comparableStrategyRows(rows, metric);

  Plotly.newPlot("performanceChart", [{
    type: "bar",
    orientation: "h",
    x: chartRows.map(r => r.mean),
    y: chartRows.map(r => r.strategy),
    text: chartRows.map(r => `${r.n_instances}/${r.expected_instances} instances`),
    textposition: "auto",
    customdata: chartRows.map(r => [
      r.n_results,
      r.n_strategy_versions,
      r.n_pipeline_versions,
      r.versions.join("<br>"),
    ]),
    hovertemplate:
      "<b>%{y}</b><br>" +
      `${info.label}: %{x}<br>` +
      "coverage: %{text}<br>" +
      "result rows: %{customdata[0]}<br>" +
      "strategy versions: %{customdata[1]}<br>" +
      "pipeline versions: %{customdata[2]}<br>" +
      "<br><b>Versioned strategy</b><br>%{customdata[3]}" +
      "<extra></extra>",
  }], {
    title: {
      text: `Mean ${info.label} by strategy (${lowerIsBetter(metric) ? "lower is better" : "higher is better"})`,
      x: 0,
      xanchor: "left",
      font: { size: 14 },
    },
    margin: { l: 320, r: 30, t: 45, b: 45 },
    xaxis: { title: info.unit ? `${info.label} [${info.unit}]` : info.label },
    yaxis: { automargin: true, autorange: "reversed" },
  }, { displayModeBar: false });
}

function className(path) {
  return String(path).split(".").pop();
}

function configValueText(value) {
  if (value && typeof value === "object" && value.class) {
    return `${className(value.class)}@${short(value.fingerprint)}`;
  }

  if (value && typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function configText(config) {
  if (!config) return "—";

  if (typeof config === "string") {
    config = JSON.parse(config);
  }

  return Object.entries(config)
    .map(([key, value]) => `${key}: ${configValueText(value)}`)
    .join(", ");
}

function componentRows() {
  const rows = filteredByInstanceSet();
  const grouped = new Map();

  for (const r of rows) {
    for (const stage of STAGES) {
      const component = r[`${stage}_algo`];
      if (!component) continue;

      const config = r[`${stage}_config`] || null;
      const algoFp = r[`${stage}_algo_fingerprint`] || "";
      const ownFp = r[`${stage}_own_fingerprint`] || "";
      const key = `${stage}|${component}|${ownFp}|${JSON.stringify(config)}`;

      if (!grouped.has(key)) {
        grouped.set(key, {
          stage,
          component,
          config,
          algoFp,
          ownFp,
          n_results: 0,
        });
      }

      grouped.get(key).n_results += 1;
    }
  }

  return [...grouped.values()].sort((a, b) => {
    return (
      a.stage.localeCompare(b.stage) ||
      a.component.localeCompare(b.component) ||
      b.n_results - a.n_results
    );
  });
}

function renderComponentTable() {
  const rows = componentRows();

  const header = `
    <tr>
      <th>Stage</th>
      <th>Component</th>
      <th>Configuration</th>
      <th>Implementation version</th>
      <th>Component version</th>
      <th>Result rows</th>
    </tr>
  `;

  const body = rows.map(r => `
    <tr>
      <td>${r.stage}</td>
      <td>${r.component}</td>
      <td>${configText(r.config)}</td>
      <td>${short(r.algoFp)}</td>
      <td>${short(r.ownFp)}</td>
      <td>${r.n_results}</td>
    </tr>
  `).join("");

  document.getElementById("componentTable").innerHTML = header + body;
}

function render() {
  renderPerformanceChart();
  renderComponentTable();
}

loadData();