const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const ROOT = path.join(__dirname, '..');
const OUT = path.join(ROOT, 'docs/screenshots');
// 호스트 Chrome이 Docker 포트포워딩과 안 맞으면 컨테이너 안에서 AIRFLOW_URL=http://airflow:8080 으로 실행
const AF = process.env.AIRFLOW_URL || 'http://localhost:8081';
const DAG = 'retail_daily_pipeline';
// 기본: 정상 실행 화면(06~10). `node tools/screenshot_retail.js fail` : 불량 주입 후 게이트 실패 화면(11~13)
const FAIL = process.argv[2] === 'fail';
const NAMES = FAIL
  ? { grid: '11_retail_gate_failed_grid', log: '12_retail_gate_failed_log', report: '13_retail_report_gate_failed' }
  : { grid: '07_retail_dag_grid', log: '09_retail_quality_gate_log', report: '10_retail_quality_report' };
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

  await page.goto(`${AF}/login/`, { waitUntil: 'load' });
  await page.waitForSelector('#username', { timeout: 30000 });
  await page.type('#username', 'admin');
  await page.type('#password', 'admin');
  await Promise.all([page.waitForNavigation({ waitUntil: 'load' }), page.click('[type=submit]')]);

  if (!FAIL) {
    await page.goto(`${AF}/home`, { waitUntil: 'load' });
    await wait(1500);
    await shot(page, '06_airflow_dags_both', false);
  }

  await page.goto(`${AF}/dags/${DAG}/grid`, { waitUntil: 'load' });
  await wait(2500);
  await shot(page, NAMES.grid, false);

  if (!FAIL) {
    await page.goto(`${AF}/dags/${DAG}/grid?tab=graph`, { waitUntil: 'load' });
    await wait(3000);
    await shot(page, '08_retail_dag_graph', false);
  }

  const runs = await page.evaluate(async (dag) => (await (await fetch(`/api/v1/dags/${dag}/dagRuns?order_by=-execution_date&limit=1`)).json()), DAG);
  const runId = runs.dag_runs && runs.dag_runs[0] && runs.dag_runs[0].dag_run_id;
  if (runId) {
    await page.goto(`${AF}/dags/${DAG}/grid?dag_run_id=${encodeURIComponent(runId)}&task_id=quality_gate&tab=logs`, { waitUntil: 'load' });
    await wait(3500);
    await shot(page, NAMES.log, false);
  } else {
    console.log('no dag run found, skipping log screenshot', JSON.stringify(runs).slice(0, 200));
  }

  const rp = await browser.newPage();
  await rp.setViewport({ width: 1170, height: 800, deviceScaleFactor: 1 });
  // DAG 의 build_report 태스크가 만든 실제 산출물을 그대로 연다
  const reportFile = path.join(ROOT, 'data/retail/quality/report.html');
  await rp.goto(require('url').pathToFileURL(reportFile).href, { waitUntil: 'load' });
  await shot(rp, NAMES.report);

  await browser.close();
  console.log('done');
})();
