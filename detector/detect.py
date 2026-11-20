"""
이상탐지 — api_logs.jsonl 읽어서 IsolationForest로 이상 IP 탐지
Airflow DAG에서 호출
"""
import json
import os
import pandas as pd
from sklearn.ensemble import IsolationForest
from datetime import datetime

LOG_FILE = "/opt/airflow/data/logs/api_logs.jsonl"
RESULT_FILE = "/opt/airflow/data/results/detection_results.jsonl"

os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)


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
    """IP별 5분 윈도우 피처 추출"""
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["window"] = df["timestamp"].dt.floor("5min")

    features = df.groupby(["src_ip", "window"]).agg(
        req_count=("path", "count"),
        unique_paths=("path", "nunique"),
        error_rate=("status_code", lambda x: (x >= 400).mean()),
        shadow_count=("actor_type", lambda x: (x == "shadow_probe").sum()),
        bola_count=("actor_type", lambda x: (x == "bola").sum()),
    ).reset_index()

    return features


def run_detection():
    df = load_logs()
    if df.empty:
        print("[DETECTOR] No data to process.")
        return

    features = extract_features(df)
    if len(features) < 5:
        print("[DETECTOR] Not enough data yet.")
        return

    feature_cols = ["req_count", "unique_paths", "error_rate", "shadow_count", "bola_count"]
    X = features[feature_cols].fillna(0)

    model = IsolationForest(contamination=0.1, random_state=42)
    features["anomaly"] = model.fit_predict(X)
    features["score"] = model.score_samples(X)
    features["is_anomaly"] = features["anomaly"] == -1

    anomalies = features[features["is_anomaly"]]
    print(f"[DETECTOR] {len(anomalies)} anomalies detected out of {len(features)} windows")

    # 결과 저장
    with open(RESULT_FILE, "a") as f:
        for _, row in anomalies.iterrows():
            result = {
                "detected_at": datetime.utcnow().isoformat(),
                "src_ip": row["src_ip"],
                "window": str(row["window"]),
                "req_count": int(row["req_count"]),
                "unique_paths": int(row["unique_paths"]),
                "shadow_count": int(row["shadow_count"]),
                "bola_count": int(row["bola_count"]),
                "anomaly_score": round(row["score"], 4),
            }
            f.write(json.dumps(result) + "\n")
            print(f"[DETECTOR] ANOMALY: {result['src_ip']} score={result['anomaly_score']}")


if __name__ == "__main__":
    run_detection()
