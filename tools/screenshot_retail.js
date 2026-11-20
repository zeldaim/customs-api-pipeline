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
  await rp.setViewport({ width: 1170, height: 800, deviceScaleFactor: 1 });
  // DAG 의 build_report 태스크가 만든 실제 산출물을 그대로 연다
  const reportFile = path.join(ROOT, 'data/retail/quality/report.html');
  await rp.goto(require('url').pathToFileURL(reportFile).href, { waitUntil: 'load' });
  await shot(rp, '10_retail_quality_report');

  await browser.close();
  console.log('done');
})();
