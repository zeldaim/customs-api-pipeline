const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

// 호스트 Chrome이 Docker 포트포워딩과 안 맞으면 컨테이너 안에서
// AIRFLOW_URL=http://airflow:8080 API_URL=http://customs-api:8080 으로 실행
const AF = process.env.AIRFLOW_URL || 'http://localhost:8081';
const API = process.env.API_URL || 'http://localhost:8080';
const DAG = 'customs_api_pipeline';
// API 화면(04, 05)은 `--api` 일 때만 찍는다. 찍는 요청도 access log 에 남아 평가 데이터에 섞이기 때문.
const WITH_API = process.argv.includes('--api');

const SCREENSHOTS_DIR = path.join(__dirname, '../docs/screenshots');
fs.mkdirSync(SCREENSHOTS_DIR, { recursive: true });
const wait = ms => new Promise(r => setTimeout(r, ms));

async function shot(page, name, fullPage = false) {
  const file = path.join(SCREENSHOTS_DIR, `${name}.png`);
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

  await page.goto(`${AF}/dags/${DAG}/grid`, { waitUntil: 'load' });
  await wait(2500);
  await shot(page, '02_dag_grid');

  await page.goto(`${AF}/dags/${DAG}/grid?tab=graph`, { waitUntil: 'load' });
  await wait(3000);
  await shot(page, '03_dag_graph');

  // 최신 실행의 탐지 태스크 로그 (방법별 precision / recall)
  const runs = await page.evaluate(async (dag) =>
    (await (await fetch(`/api/v1/dags/${dag}/dagRuns?order_by=-execution_date&limit=1`)).json()), DAG);
  const runId = runs.dag_runs && runs.dag_runs[0] && runs.dag_runs[0].dag_run_id;
  if (runId) {
    await page.goto(`${AF}/dags/${DAG}/grid?dag_run_id=${encodeURIComponent(runId)}&task_id=run_anomaly_detection&tab=logs`,
      { waitUntil: 'load' });
    await wait(3500);
    await shot(page, '14_customs_detection_eval_log');
  }

  if (WITH_API) {
    await page.goto(`${API}/api/v1/health`, { waitUntil: 'load' });
    await shot(page, '04_customs_api_health', true);
    await page.goto(`${API}/api/v1/hs-code/8471.30`, { waitUntil: 'load' });
    await shot(page, '05_hs_code_lookup', true);
  }

  await browser.close();
  console.log('done');
})();
