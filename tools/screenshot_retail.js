const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'docs/screenshots');
const AF = 'http://localhost:8081';
const DAG = 'retail_daily_pipeline';
fs.mkdirSync(OUT, { recursive: true });

const wait = ms => new Promise(r => setTimeout(r, ms));
async function shot(page, name, fullPage = true) {
  const file = path.join(OUT, `${name}.png`);
  await page.screenshot({ path: file, fullPage });
  console.log(`saved: ${file}`);
}

function reportHtml() {
  const rep = JSON.parse(fs.readFileSync(path.join(ROOT, 'data/retail/quality/report.json'), 'utf8'));
  const monthly = fs.readFileSync(path.join(ROOT, 'data/retail/mart/monthly_sales.csv'), 'utf8').trim().split('\n').map(l => l.split(','));
  const flagged = fs.readFileSync(path.join(ROOT, 'data/retail/quality/flagged.jsonl'), 'utf8').trim().split('\n').slice(0, 6).map(JSON.parse);
  const n = x => Number(x).toLocaleString('en-US');
  const kpi = (label, value) => `<div class="k"><div class="l">${label}</div><div class="v">${value}</div></div>`;
  const max = Math.max(...monthly.slice(1).map(r => +r[2]));
  return `<!doctype html><meta charset="utf-8"><style>
    body{font-family:'Segoe UI','Malgun Gothic',sans-serif;margin:32px;color:#1f2328;background:#fff;width:1100px}
    h1{font-size:22px;margin:0 0 4px} .sub{color:#656d76;margin-bottom:20px}
    .kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px}
    .k{border:1px solid #d0d7de;border-radius:8px;padding:12px} .l{font-size:12px;color:#656d76} .v{font-size:22px;font-weight:600;margin-top:4px}
    h2{font-size:16px;margin:24px 0 8px} table{border-collapse:collapse;width:100%;font-size:13px}
    th,td{border-bottom:1px solid #eaeef2;padding:6px 8px;text-align:right} th:first-child,td:first-child{text-align:left}
    th{color:#656d76;font-weight:600} .bar{background:#4c78a8;height:10px;border-radius:2px}
    .warn{background:#fff8c5;border:1px solid #d4a72c;border-radius:8px;padding:10px 14px;margin:6px 0;font-size:14px}
    .cols{display:grid;grid-template-columns:1.3fr 1fr;gap:24px}
  </style>
  <h1>Retail Data Quality Report</h1>
  <div class="sub">retail_daily_pipeline · run ${rep.run_at} · ${rep.date_range[0]} ~ ${rep.date_range[1]}</div>
  <div class="kpis">
    ${kpi('Kafka 적재 라인', n(rep.raw_lines))}${kpi('재전달 중복 제거', n(rep.redelivered_dropped))}
    ${kpi('정제 통과', n(rep.clean))}${kpi('규칙 위반', n(rep.rejected))}${kpi('단가 이상치', `${rep.flagged_price_outliers} (${(rep.flagged_rate*100).toFixed(1)}%)`)}
  </div>
  <h2>품질 경고</h2>${rep.warnings.map(w => `<div class="warn">⚠ ${w}</div>`).join('')}
  <div class="cols"><div>
    <h2>월별 매출 (mart/monthly_sales.csv)</h2>
    <table><tr><th>월</th><th>거래</th><th>매출액</th><th>수량</th><th style="width:30%"></th></tr>
    ${monthly.slice(1).map(r => `<tr><td>${r[0]}</td><td>${n(r[1])}</td><td>${n(Math.round(r[2]))}</td><td>${n(r[3])}</td><td><div class="bar" style="width:${(r[2]/max*100).toFixed(0)}%"></div></td></tr>`).join('')}
    </table></div><div>
    <h2>단가 이상치 예시 (quality/flagged.jsonl)</h2>
    <table><tr><th>날짜</th><th>매출액</th><th>수량</th><th>단가</th><th>z</th></tr>
    ${flagged.map(f => `<tr><td>${f.date}</td><td>${f.sales_amount}</td><td>${f.units_sold}</td><td>${f.unit_price.toFixed(2)}</td><td>${f.price_z}</td></tr>`).join('')}
    </table><div class="sub" style="margin-top:8px">중앙 단가 ${rep.median_unit_price} · 기준 |z| &gt; 3.5 (log 단가, median/MAD)</div>
  </div></div>`;
}

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  await page.goto(`${AF}/login`, { waitUntil: 'networkidle2' });
  await page.type('#username', 'admin');
  await page.type('#password', 'admin');
  await Promise.all([page.waitForNavigation({ waitUntil: 'networkidle2' }), page.click('[type=submit]')]);

  await page.goto(`${AF}/home`, { waitUntil: 'networkidle2' });
  await wait(1500);
  await shot(page, '06_airflow_dags_both', false);

  await page.goto(`${AF}/dags/${DAG}/grid`, { waitUntil: 'networkidle2' });
  await wait(2500);
  await shot(page, '07_retail_dag_grid', false);

  await page.goto(`${AF}/dags/${DAG}/grid?tab=graph`, { waitUntil: 'networkidle2' });
  await wait(3000);
  await shot(page, '08_retail_dag_graph', false);

  const runs = await page.evaluate(async (dag) => (await (await fetch(`/api/v1/dags/${dag}/dagRuns?order_by=-execution_date&limit=1`)).json()), DAG);
  const runId = runs.dag_runs && runs.dag_runs[0] && runs.dag_runs[0].dag_run_id;
  if (runId) {
    await page.goto(`${AF}/dags/${DAG}/grid?dag_run_id=${encodeURIComponent(runId)}&task_id=quality_gate&tab=logs`, { waitUntil: 'networkidle2' });
    await wait(3500);
    await shot(page, '09_retail_quality_gate_log', false);
  } else {
    console.log('no dag run found, skipping log screenshot', JSON.stringify(runs).slice(0, 200));
  }

  const rp = await browser.newPage();
  await rp.setViewport({ width: 1170, height: 800 });
  await rp.setContent(reportHtml(), { waitUntil: 'load' });
  await shot(rp, '10_retail_quality_report');

  await browser.close();
  console.log('done');
})();
