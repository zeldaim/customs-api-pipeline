"""
Retail 품질 리포트 HTML 생성 - DAG 마지막 태스크.

입력 : quality/report.json, quality/flagged.jsonl, quality/ml_summary.json, quality/ml_anomalies.jsonl,
       mart/monthly_sales.csv
출력 : quality/report.html   (외부 리소스 없는 단일 파일, 브라우저로 바로 열림)
"""
import csv
import html
import json
import os

BASE = os.getenv("RETAIL_DATA_DIR", "/opt/airflow/data/retail")


def _read_json(*p):
    path = os.path.join(BASE, *p)
    return json.load(open(path, encoding="utf-8")) if os.path.exists(path) else None


def _read_jsonl(*p, limit=8):
    path = os.path.join(BASE, *p)
    if not os.path.exists(path):
        return []
    out = []
    for line in open(path, encoding="utf-8"):
        out.append(json.loads(line))
        if len(out) >= limit:
            break
    return out


def n(x):
    return f"{x:,}" if isinstance(x, int) else f"{x:,.2f}"


def kpi(label, value):
    return f'<div class="k"><div class="l">{label}</div><div class="v">{value}</div></div>'


CSS = """
:root{--fg:#1f2328;--muted:#656d76;--line:#d0d7de;--soft:#eaeef2;--bar:#4c78a8;--warnbg:#fff8c5;--warnline:#d4a72c;--bg:#fff}
@media (prefers-color-scheme: dark){:root{--fg:#e6edf3;--muted:#9198a1;--line:#3d444d;--soft:#2a3038;--bar:#6c9bd2;--warnbg:#3b3212;--warnline:#9e7a1f;--bg:#0d1117}}
body{font-family:'Segoe UI','Malgun Gothic',sans-serif;margin:32px;color:var(--fg);background:var(--bg);max-width:1100px}
h1{font-size:22px;margin:0 0 4px}.sub{color:var(--muted);margin-bottom:20px;font-size:14px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:20px}
.k{border:1px solid var(--line);border-radius:8px;padding:12px}.l{font-size:12px;color:var(--muted)}.v{font-size:22px;font-weight:600;margin-top:4px}
h2{font-size:16px;margin:28px 0 8px}table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border-bottom:1px solid var(--soft);padding:6px 8px;text-align:right}th:first-child,td:first-child{text-align:left}
th{color:var(--muted);font-weight:600}.bar{background:var(--bar);height:10px;border-radius:2px}
.warn{background:var(--warnbg);border:1px solid var(--warnline);border-radius:8px;padding:10px 14px;margin:6px 0;font-size:14px}
.cols{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:24px}
.yes{font-weight:600}.no{color:var(--muted)}
"""


def build():
    rep = _read_json("quality", "report.json")
    if rep is None:
        raise FileNotFoundError("quality/report.json 없음 - retail_etl 먼저 실행")
    ml = _read_json("quality", "ml_summary.json")
    flagged = _read_jsonl("quality", "flagged.jsonl", limit=6)
    ml_rows = _read_jsonl("quality", "ml_anomalies.jsonl", limit=8)
    monthly = list(csv.DictReader(open(os.path.join(BASE, "mart", "monthly_sales.csv"), encoding="utf-8")))
    peak = max(float(m["sales_amount"]) for m in monthly)

    parts = [f"""<!doctype html><html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Retail Data Quality Report</title>
<style>{CSS}</style></head><body>
<h1>Retail Data Quality Report</h1>
<div class="sub">retail_daily_pipeline · run {html.escape(rep['run_at'])} · {rep['date_range'][0]} ~ {rep['date_range'][1]}</div>
<div class="kpis">
{kpi('Kafka 적재 라인', n(rep['raw_lines']))}{kpi('중복 이벤트 제거', n(rep['duplicate_events_dropped']))}
{kpi('정제 통과', n(rep['clean']))}{kpi('규칙 위반', n(rep['rejected']))}
{kpi('단가 이상치 (룰)', f"{rep['flagged_price_outliers']} ({rep['flagged_rate']:.1%})")}
{kpi('ML 다수결 이상', n(ml['majority']) if ml else '-')}
</div>"""]

    if rep["warnings"]:
        parts.append("<h2>품질 경고</h2>" + "".join(f'<div class="warn">⚠ {html.escape(w)}</div>' for w in rep["warnings"]))

    parts.append('<div class="cols"><div><h2>월별 매출 (mart/monthly_sales.csv)</h2>'
                 '<table><tr><th>월</th><th>거래</th><th>매출액</th><th>수량</th><th style="width:30%"></th></tr>')
    for m in monthly:
        w = float(m["sales_amount"]) / peak * 100
        parts.append(f'<tr><td>{m["period"]}</td><td>{n(int(m["transactions"]))}</td>'
                     f'<td>{n(round(float(m["sales_amount"])))}</td><td>{n(int(m["units_sold"]))}</td>'
                     f'<td><div class="bar" style="width:{w:.0f}%"></div></td></tr>')
    parts.append('</table></div><div><h2>단가 이상치 예시 (룰, quality/flagged.jsonl)</h2>'
                 '<table><tr><th>날짜</th><th>매출액</th><th>수량</th><th>단가</th><th>z</th></tr>')
    for f in flagged:
        parts.append(f'<tr><td>{f["date"]}</td><td>{f["sales_amount"]}</td><td>{f["units_sold"]}</td>'
                     f'<td>{f["unit_price"]:.2f}</td><td>{f["price_z"]}</td></tr>')
    parts.append(f'</table><div class="sub" style="margin-top:8px">중앙 단가 {rep["median_unit_price"]} · '
                 f'기준 |z| &gt; 3.5 (log 단가, median/MAD)</div></div></div>')

    if ml:
        pm = ml["per_model"]
        parts.append(f"""<h2>ML 이상탐지 (IF · LOF · Autoencoder, 2개 이상 동의)</h2>
<div class="kpis">
{kpi('Isolation Forest', n(pm['iforest']))}{kpi('LOF', n(pm['lof']))}{kpi('Autoencoder', n(pm['autoencoder']))}
{kpi('3개 모두', n(ml['all_three']))}{kpi('룰 이상치 중 ML도 탐지', f"{ml['overlap_majority_and_rule']} / {ml['rule_flagged']}")}
</div>
<div class="sub">피처: {', '.join(ml['features'])} · contamination {ml['contamination']} (모델별 상위 {ml['contamination']:.0%}를 이상으로 표시)</div>
<table><tr><th>날짜</th><th>매출액</th><th>수량</th><th>단가</th><th>IF</th><th>LOF</th><th>AE</th><th>룰</th></tr>""")
        yn = lambda b: '<span class="yes">●</span>' if b else '<span class="no">·</span>'
        for r in ml_rows:
            parts.append(f'<tr><td>{r["date"]}</td><td>{r["sales_amount"]}</td><td>{r["units_sold"]}</td>'
                         f'<td>{r["unit_price"]:.2f}</td><td>{yn(r["iforest"])}</td><td>{yn(r["lof"])}</td>'
                         f'<td>{yn(r["autoencoder"])}</td><td>{yn(r["rule_flagged"])}</td></tr>')
        parts.append("</table>")

    parts.append("</body></html>")
    out = os.path.join(BASE, "quality", "report.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    print(f"[RETAIL-REPORT] saved {out}")
    return out


if __name__ == "__main__":
    build()
