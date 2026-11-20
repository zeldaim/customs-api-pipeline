const puppeteer = require('puppeteer');
const path = require('path');
const fs = require('fs');

const SCREENSHOTS_DIR = path.join(__dirname, '../docs/screenshots');
fs.mkdirSync(SCREENSHOTS_DIR, { recursive: true });

async function shot(page, name) {
  const file = path.join(SCREENSHOTS_DIR, `${name}.png`);
  await page.screenshot({ path: file, fullPage: true });
  console.log(`saved: ${file}`);
}

(async () => {
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.setViewport({ width: 1400, height: 900 });

  // 1. Airflow 로그인
  await page.goto('http://localhost:8081/login', { waitUntil: 'networkidle2' });
  await page.type('#username', 'admin');
  await page.type('#password', 'admin');
  await page.click('[type=submit]');
  await page.waitForNavigation({ waitUntil: 'networkidle2' });
  await shot(page, '01_airflow_home');

  // 2. DAG 목록
  await page.goto('http://localhost:8081/dags/customs_api_pipeline/grid', { waitUntil: 'networkidle2' });
  await new Promise(r => setTimeout(r, 2000));
  await shot(page, '02_dag_grid');

  // 3. DAG 그래프
  await page.goto('http://localhost:8081/dags/customs_api_pipeline/grid?tab=graph', { waitUntil: 'networkidle2' });
  await new Promise(r => setTimeout(r, 2000));
  await shot(page, '03_dag_graph');

  // 4. 관세 API health
  await page.goto('http://localhost:8080/api/v1/health', { waitUntil: 'networkidle2' });
  await shot(page, '04_customs_api_health');

  // 5. 관세 API HS코드 조회
  await page.goto('http://localhost:8080/api/v1/hs-code/8471.30', { waitUntil: 'networkidle2' });
  await shot(page, '05_hs_code_lookup');

  await browser.close();
  console.log('All screenshots done.');
})();
