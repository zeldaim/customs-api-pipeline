"""
Retail ETL - Kafka가 적재한 raw 이벤트를 검증 → 정제 → 집계한다.

입력 : data/retail/raw/sales_events.jsonl   (retail_consumer 가 적재)
출력 : data/retail/clean/sales_clean.csv     검증 통과 거래
       data/retail/quality/rejected.jsonl    규칙 위반 거래 + 사유
       data/retail/quality/flagged.jsonl     통과했지만 의심스러운 거래 (단가 이상치)
       data/retail/quality/report.json       품질 리포트 (건수/비율)
       data/retail/mart/daily_sales.csv      일별 매출 집계
       data/retail/mart/monthly_sales.csv    월별 매출 집계

외부 라이브러리 없이 표준 라이브러리만 사용 → Airflow 컨테이너/로컬 어디서나 실행 가능.
매 실행마다 raw 전체를 다시 계산하므로 몇 번을 돌려도 결과가 같다 (idempotent).
"""
import csv
import json
import math
import os
import statistics
from collections import defaultdict
from datetime import date, datetime, timezone

BASE = os.getenv("RETAIL_DATA_DIR", "/opt/airflow/data/retail")
RAW = os.path.join(BASE, "raw", "sales_events.jsonl")

# 단가(sales_amount / units_sold) 이상치 기준: log 단가의 robust z-score
PRICE_Z_THRESHOLD = float(os.getenv("PRICE_Z_THRESHOLD", "3.5"))
MAX_UNITS = 10_000          # 한 거래에 이 이상이면 입력 오류로 간주
MAX_AMOUNT = 1_000_000


def _path(*parts):
    p = os.path.join(BASE, *parts)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    return p


def load_raw():
    """raw JSONL 로드. 같은 event_seq 가 여러 번 들어온 중복(producer 재실행, Kafka 재전달)은 제거."""
    events, seen, redelivered, broken = [], set(), 0, 0
    if not os.path.exists(RAW):
        raise FileNotFoundError(f"raw 파일 없음: {RAW} (retail-producer/consumer 실행 확인)")
    with open(RAW, encoding="utf-8") as f:
        for line in f:
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                broken += 1
                continue
            key = e.get("event_seq")
            if key in seen:
                redelivered += 1
                continue
            seen.add(key)
            events.append(e)
    return events, redelivered, broken


def validate(e):
    """규칙 위반 사유 리스트와 파싱된 레코드를 돌려준다."""
    errors = []
    for col in ("date", "store_id", "product_id", "sales_amount", "units_sold"):
        if str(e.get(col, "")).strip() == "":
            errors.append(f"missing_{col}")
    if errors:
        return errors, None

    try:
        d = date.fromisoformat(e["date"])
    except ValueError:
        errors.append("bad_date")
        d = None
    try:
        amount = float(e["sales_amount"])
        if not math.isfinite(amount) or amount <= 0:
            errors.append("non_positive_amount")
        elif amount > MAX_AMOUNT:
            errors.append("amount_too_large")
    except ValueError:
        errors.append("bad_amount")
        amount = None
    try:
        units_f = float(e["units_sold"])
        units = int(units_f)
        if units != units_f:
            errors.append("fractional_units")
        elif units <= 0:
            errors.append("non_positive_units")
        elif units > MAX_UNITS:
            errors.append("units_too_large")
    except ValueError:
        errors.append("bad_units")
        units = None
    if d and d > date.today():
        errors.append("future_date")

    if errors:
        return errors, None
    return [], {
        "date": d.isoformat(),
        "store_id": e["store_id"].strip(),
        "product_id": e["product_id"].strip(),
        "sales_amount": round(amount, 2),
        "units_sold": units,
        "unit_price": round(amount / units, 4),
    }


def flag_price_outliers(rows):
    """log 단가의 median/MAD 기반 robust z-score. 분포가 치우쳐도 안정적."""
    logs = [math.log(r["unit_price"]) for r in rows]
    med = statistics.median(logs)
    mad = statistics.median(abs(x - med) for x in logs) or 1e-9
    flagged = []
    for r, x in zip(rows, logs):
        z = 0.6745 * (x - med) / mad
        r["price_z"] = round(z, 2)
        if abs(z) > PRICE_Z_THRESHOLD:
            flagged.append({**r, "reason": "unit_price_outlier"})
    return flagged, math.exp(med)


def aggregate(rows, keyfn, path):
    agg = defaultdict(lambda: {"tx": 0, "sales": 0.0, "units": 0, "stores": set()})
    for r in rows:
        a = agg[keyfn(r)]
        a["tx"] += 1
        a["sales"] += r["sales_amount"]
        a["units"] += r["units_sold"]
        a["stores"].add(r["store_id"])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["period", "transactions", "sales_amount", "units_sold",
                    "active_stores", "avg_ticket"])
        for k in sorted(agg):
            a = agg[k]
            w.writerow([k, a["tx"], round(a["sales"], 2), a["units"],
                        len(a["stores"]), round(a["sales"] / a["tx"], 2)])
    return len(agg)


def run():
    events, redelivered, broken = load_raw()

    clean, rejected, seen_biz = [], [], set()
    for e in events:
        errors, rec = validate(e)
        if rec:
            biz_key = (rec["date"], rec["store_id"], rec["product_id"],
                       rec["sales_amount"], rec["units_sold"])
            if biz_key in seen_biz:
                errors = ["duplicate_transaction"]
                rec = None
            else:
                seen_biz.add(biz_key)
        if errors:
            rejected.append({**e, "errors": errors})
        else:
            clean.append(rec)

    if not clean:
        raise ValueError("검증 통과 레코드가 0건 - 원본 데이터 확인 필요")

    flagged, median_price = flag_price_outliers(clean)

    # ── 출력 ───────────────────────────────────────────
    with open(_path("clean", "sales_clean.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(clean[0]))
        w.writeheader()
        w.writerows(sorted(clean, key=lambda r: r["date"]))
    for name, items in (("rejected.jsonl", rejected), ("flagged.jsonl", flagged)):
        with open(_path("quality", name), "w", encoding="utf-8") as f:
            for it in items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")

    n_days = aggregate(clean, lambda r: r["date"], _path("mart", "daily_sales.csv"))
    n_months = aggregate(clean, lambda r: r["date"][:7], _path("mart", "monthly_sales.csv"))

    reasons = defaultdict(int)
    for r in rejected:
        for err in r["errors"]:
            reasons[err] += 1

    amounts = [r["sales_amount"] for r in clean]
    units = [r["units_sold"] for r in clean]
    total = len(events)
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_lines": total + redelivered + broken,
        "unique_events": total,
        "duplicate_events_dropped": redelivered,
        "broken_lines": broken,
        "clean": len(clean),
        "rejected": len(rejected),
        "rejected_rate": round(len(rejected) / total, 4) if total else 0,
        "rejected_by_reason": dict(reasons),
        "flagged_price_outliers": len(flagged),
        "flagged_rate": round(len(flagged) / len(clean), 4),
        "median_unit_price": round(median_price, 2),
        "date_range": [min(r["date"] for r in clean), max(r["date"] for r in clean)],
        "days": n_days,
        "months": n_months,
        "unique_stores": len({r["store_id"] for r in clean}),
        "unique_products": len({r["product_id"] for r in clean}),
        # 매출액과 판매수량은 보통 강한 양의 상관. 0 근처면 원천 데이터 의심 신호.
        "corr_amount_units": round(statistics.correlation(amounts, units), 3),
    }
    report["warnings"] = quality_warnings(report)
    with open(_path("quality", "report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def quality_warnings(rep):
    w = []
    if rep["rejected_rate"] > 0.05:
        w.append(f"규칙 위반 비율 {rep['rejected_rate']:.1%} - 원천 시스템 점검 필요")
    if rep["flagged_rate"] > 0.02:
        w.append(f"단가 이상치 비율 {rep['flagged_rate']:.1%} - 가격/수량 입력 검토 필요")
    if abs(rep["corr_amount_units"]) < 0.2:
        w.append(f"매출액-수량 상관 {rep['corr_amount_units']} - 금액이 가공(익명화/노이즈)됐을 가능성")
    if rep["unique_stores"] > rep["clean"] * 0.5:
        w.append("매장당 거래가 대부분 1건 - 샘플링된 데이터일 가능성")
    return w


if __name__ == "__main__":
    run()
