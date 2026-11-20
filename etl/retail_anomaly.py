"""
Retail 거래 이상탐지 - 관세 파이프라인과 같은 detector/models.py 의 IF · LOF · Autoencoder 를 재사용.

입력 : data/retail/clean/sales_clean.csv        (retail_etl 출력)
       data/retail/quality/flagged.jsonl         (룰 기반 단가 이상치, 비교용)
출력 : data/retail/quality/ml_anomalies.jsonl    2개 이상 모델이 이상으로 본 거래
       data/retail/quality/ml_summary.json       모델별 건수, 다수결 건수, 룰과의 겹침

피처 : log(매출액), log(판매수량), log(단가)
       금액·수량은 오른쪽으로 긴 분포라 log 로 펴서 거리 기반 모델(LOF, AE)이 한쪽에 끌려가지 않게 한다.
판정 : 3개 중 2개 이상 동의 (다수결). 한 모델만 튀는 경우를 줄인다.
"""
import argparse
import csv
import json
import math
import os
import sys

import numpy as np

BASE = os.getenv("RETAIL_DATA_DIR", "/opt/airflow/data/retail")
DETECTOR_DIR = os.getenv("DETECTOR_DIR", "/opt/airflow/detector")
CONTAMINATION = float(os.getenv("RETAIL_CONTAMINATION", "0.02"))
METHODS = ["iforest", "lof", "autoencoder"]
FEATURES = ["log_amount", "log_units", "log_unit_price"]


def load():
    path = os.path.join(BASE, "clean", "sales_clean.csv")
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    if len(rows) < 20:
        raise ValueError(f"거래가 {len(rows)}건뿐 - 이상탐지에 부족")
    X = np.array([[math.log(float(r["sales_amount"])),
                   math.log(float(r["units_sold"])),
                   math.log(float(r["unit_price"]))] for r in rows])
    return rows, X


def rule_flagged_keys():
    path = os.path.join(BASE, "quality", "flagged.jsonl")
    if not os.path.exists(path):
        return set()
    keys = set()
    for line in open(path, encoding="utf-8"):
        f = json.loads(line)
        keys.add((f["date"], f["store_id"], f["product_id"]))
    return keys


def run():
    sys.path.insert(0, DETECTOR_DIR)
    from models import run as run_model

    rows, X = load()
    args = argparse.Namespace(contamination=CONTAMINATION, nu=CONTAMINATION, z=3.0,
                              rule_rpm=None, rule_level=10)

    flags, scores = {}, {}
    for m in METHODS:
        s, f = run_model(m, X, X, args, train_is_all=True)
        scores[m], flags[m] = np.asarray(s), np.asarray(f, dtype=bool)
        print(f"[RETAIL-ML] {m:<11} {int(flags[m].sum())} anomalies")

    votes = sum(flags[m].astype(int) for m in METHODS)
    majority = votes >= 2

    rule_keys = rule_flagged_keys()
    is_rule = np.array([(r["date"], r["store_id"], r["product_id"]) in rule_keys for r in rows])

    out_path = os.path.join(BASE, "quality", "ml_anomalies.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        order = np.argsort(-scores["iforest"])
        for i in order:
            if not majority[i]:
                continue
            r = rows[i]
            f.write(json.dumps({
                "date": r["date"], "store_id": r["store_id"], "product_id": r["product_id"],
                "sales_amount": float(r["sales_amount"]), "units_sold": int(r["units_sold"]),
                "unit_price": float(r["unit_price"]),
                "votes": int(votes[i]),
                "iforest": bool(flags["iforest"][i]), "lof": bool(flags["lof"][i]),
                "autoencoder": bool(flags["autoencoder"][i]),
                "rule_flagged": bool(is_rule[i]),
            }, ensure_ascii=False) + "\n")

    both = int((majority & is_rule).sum())
    summary = {
        "transactions": len(rows),
        "features": FEATURES,
        "contamination": CONTAMINATION,
        "per_model": {m: int(flags[m].sum()) for m in METHODS},
        "all_three": int((votes == 3).sum()),
        "majority": int(majority.sum()),
        "rule_flagged": int(is_rule.sum()),
        "overlap_majority_and_rule": both,
        # 룰이 잡은 단가 이상치 중 ML 다수결도 잡은 비율 / ML 다수결 중 룰에도 걸린 비율
        "rule_recall_by_ml": round(both / is_rule.sum(), 3) if is_rule.sum() else None,
        "ml_share_explained_by_rule": round(both / majority.sum(), 3) if majority.sum() else None,
    }
    with open(os.path.join(BASE, "quality", "ml_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


if __name__ == "__main__":
    run()
