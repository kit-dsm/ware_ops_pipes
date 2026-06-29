let results = [];
let overview = {};
let versionOverview = [];

async function loadData() {
  results = await fetch("data/results.json").then(r => r.json());
  overview = await fetch("data/overview.json").then(r => r.json());
  versionOverview = await fetch("data/version_overview.json").then(r => r.json());

  initControls();
  render();
}

function unique(xs) {
  return [...new Set(xs.filter(x => x !== null && x !== undefined && x !== ""))].sort();
}

function initControls() {
  const instanceSets = ["All", ...unique(results.map(r => r.instance_set))];
  const select = document.getElementById("instanceSet");
  select.innerHTML = instanceSets.map(x => `<option value="${x}">${x}</option>`).join("");

  document.getElementById("instanceSet").addEventListener("change", render);
  document.getElementById("metric").addEventListener("change", render);
}

function filteredResults() {
  const instanceSet = document.getElementById("instanceSet").value;
  const metric = document.getElementById("metric").value;

  return results.filter(r => {
    if (instanceSet !== "All" && r.instance_set !== instanceSet) return false;
    if (r[metric] === null || r[metric] === undefined || r[metric] === "") return false;
    return true;
  });
}

function renderCards() {
  document.getElementById("cards").innerHTML = `
    <div class="card"><b>${overview.n_results ?? 0}</b><span>results</span></div>
    <div class="card"><b>${overview.n_instances ?? 0}</b><span>instances</span></div>
    <div class="card"><b>${overview.n_strategies ?? 0}</b><span>strategies</span></div>
    <div class="card"><b>${overview.n_strategy_versions ?? 0}</b><span>strategy versions</span></div>
  `;
}

function renderPerformanceChart() {
  const rows = filteredResults();
  const metric = document.getElementById("metric").value;

  const grouped = {};
  for (const r of rows) {
    const key = r.strategy_versioned;
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(Number(r[metric]));
  }

  const chartRows = Object.entries(grouped)
    .map(([strategy, values]) => ({
      strategy,
      mean: values.reduce((a, b) => a + b, 0) / values.length,
      n: values.length
    }))
    .sort((a, b) => a.mean - b.mean)
    .slice(0, 25);

  Plotly.newPlot("performanceChart", [{
    type: "bar",
    x: chartRows.map(r => r.mean),
    y: chartRows.map(r => r.strategy),
    orientation: "h",
    text: chartRows.map(r => `n=${r.n}`),
  }], {
    margin: {l: 260, r: 20, t: 20, b: 40},
    xaxis: {title: metric},
    yaxis: {automargin: true},
  });
}

function renderVersionChart() {
  const labels = versionOverview.map(r => `${r.stage}: ${r.algo}@${r.algo_fp_short}`);
  const values = versionOverview.map(r => r.n_results);

  Plotly.newPlot("versionChart", [{
    type: "bar",
    x: values,
    y: labels,
    orientation: "h",
  }], {
    margin: {l: 280, r: 20, t: 20, b: 40},
    xaxis: {title: "number of results"},
    yaxis: {automargin: true},
  });
}

function renderTable() {
  const rows = filteredResults().slice(0, 200);
  const metric = document.getElementById("metric").value;

  const cols = [
    "instance_set",
    "instance_name",
    "strategy",
    "strategy_versioned",
    metric,
    "pipeline_fp_short",
  ];

  const header = `<tr>${cols.map(c => `<th>${c}</th>`).join("")}</tr>`;
  const body = rows.map(r => `
    <tr>
      ${cols.map(c => `<td>${r[c] ?? ""}</td>`).join("")}
    </tr>
  `).join("");

  document.getElementById("resultsTable").innerHTML = header + body;
}

function render() {
  renderCards();
  renderPerformanceChart();
  renderVersionChart();
  renderTable();
}

loadData();