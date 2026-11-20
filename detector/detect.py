"""
이상탐지 — api_logs.jsonl → 피처 추출 → ml_detection models.py 앙상블 (IF | LOF | Autoencoder)
Airflow DAG에서 호출
"""
import json
import os
import csv
import sys
import io
from datetime import datetime

import pandas as pd
import numpy as np

LOG_FILE = "/opt/airflow/data/logs/api_logs.jsonl"
FEATURE_FILE = "/opt/airflow/data/logs/features.csv"
RESULT_FILE = "/opt/airflow/data/results/detection_results.jsonl"

os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)
os.makedirs(os.path.dirname(FEATURE_FILE), exist_ok=True)

METHODS = ["iforest", "lof", "autoencoder"]


def load_logs():
    if not os.path.exists(LOG_FILE):
        print("[DETECTOR] No log file found.")
        return pd.DataFrame()

    rows = []
    with open(LOG_FILE) as f:
        for line in f:
            try:
                rows.append(json.loads(line.strip()))
            except:
                continue
    return pd.DataFrame(rows)


def extract_features(df):
    """
    IP × 5분 윈도우 피처 추출
    models.py 호환 컬럼: window_start, src_ip, req_count, uniq_paths,
                         err_ratio, write_ratio, endpoint_entropy,
                         shadow_count, bola_count
    """
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["window_start"] = df["timestamp"].dt.floor("5min").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def entropy(series):
        counts = series.value_counts(normalize=True)
        return float(-(counts * np.log2(counts + 1e-9)).sum())

    features = df.groupby(["src_ip", "window_start"]).agg(
        req_count=("path", "count"),
        uniq_paths=("path", "nunique"),
        err_ratio=("status_code", lambda x: (x >= 400).mean()),
        write_ratio=("method", lambda x: (x == "POST").mean()),
        endpoint_entropy=("path", entropy),
        shadow_count=("actor_type", lambda x: (x == "shadow_probe").sum()),
        bola_count=("actor_type", lambda x: (x == "bola").sum()),
    ).reset_index()

    return features


def save_features_csv(features):
    features.to_csv(FEATURE_FILE, index=False)


def run_ensemble(features):
    """
    ml_detection models.py 앙상블 실행 (IF | LOF | Autoencoder)
    하나라도 이상이면 anomaly로 판정
    """
    # models.py가 같은 디렉토리에 있음
    sys.path.insert(0, os.path.dirname(__file__))
    import types
    import argparse

    # models.py의 run() 함수 직접 사용
    from models import run

    feature_cols = ["req_count", "uniq_paths", "err_ratio", "write_ratio",
                    "endpoint_entropy", "shadow_count", "bola_count"]
    X = features[feature_cols].fillna(0).values

    if len(X) < 5:
        print("[DETECTOR] Not enough windows for detection.")
        return pd.DataFrame()

    # argparse namespace로 파라미터 전달
    args = argparse.Namespace(contamination=0.1, nu=0.1, z=3.0,
                               rule_rpm=None, rule_level=10)

    results = {}
    for method in METHODS:
        try:
            scores, is_anomaly = run(method, X, X, args, train_is_all=True)
            results[method] = is_anomaly
            print(f"[DETECTOR] {method}: {is_anomaly.sum()} anomalies")
        except Exception as e:
            print(f"[DETECTOR] {method} failed: {e}")

    if not results:
        return pd.DataFrame()

    # OR 앙상블: 하나라도 이상이면 anomaly
    combined = np.zeros(len(X), dtype=bool)
    for is_anomaly in results.values():
        combined |= is_anomaly

    features = features.copy()
    features["anomaly_if"] = results.get("iforest", np.zeros(len(X), dtype=bool))
    features["anomaly_lof"] = results.get("lof", np.zeros(len(X), dtype=bool))
    features["anomaly_ae"] = results.get("autoencoder", np.zeros(len(X), dtype=bool))
    features["is_anomaly"] = combined

    return features[features["is_anomaly"]]


def run_detection():
    df = load_logs()
    if df.empty:
        print("[DETECTOR] No data.")
        return

    features = extract_features(df)
    save_features_csv(features)
    print(f"[DETECTOR] {len(features)} windows extracted")

    anomalies = run_ensemble(features)
    if anomalies.empty:
        print("[DETECTOR] No anomalies detected.")
        return

    with open(RESULT_FILE, "a") as f:
        for _, row in anomalies.iterrows():
            result = {
                "detected_at": datetime.utcnow().isoformat(),
                "src_ip": row["src_ip"],
                "window_start": row["window_start"],
                "req_count": int(row["req_count"]),
                "uniq_paths": int(row["uniq_paths"]),
                "shadow_count": int(row["shadow_count"]),
                "bola_count": int(row["bola_count"]),
                "anomaly_if": bool(row["anomaly_if"]),
                "anomaly_lof": bool(row["anomaly_lof"]),
                "anomaly_ae": bool(row["anomaly_ae"]),
            }
            f.write(json.dumps(result) + "\n")
            print(f"[DETECTOR] ANOMALY: {result['src_ip']} "
                  f"IF={result['anomaly_if']} LOF={result['anomaly_lof']} AE={result['anomaly_ae']}")

    print(f"[DETECTOR] {len(anomalies)} anomalies saved.")


if __name__ == "__main__":
    run_detection()
