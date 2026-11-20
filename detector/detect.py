"""
이상탐지 — API 서버 access log → IP × 5분 윈도우 피처 → models.py (zscore · IF · LOF · Autoencoder) → 평가

입력 : data/logs/api_logs.jsonl          API 서버가 직접 남긴 access log (Kafka consumer 가 적재)
       data/labels/ground_truth.jsonl    트래픽 생성기가 남긴 정답 라벨 — 평가에만 쓰고 피처에는 절대 쓰지 않는다
출력 : data/results/features.csv         윈도우별 피처 (+ 평가용 label 열)
       data/results/detection_results.jsonl   이상 윈도우 (매 실행 전체 재계산, 덮어씀)
       data/results/evaluation.json      방법별 TP/FP/FN, precision/recall, 공격 유형별 탐지율

피처는 서버가 관찰할 수 있는 것만: 요청 수, 고유 경로 수, 4xx/5xx 비율, POST 비율, 경로 엔트로피,
같은 엔드포인트에서 서로 다른 객체 ID 수. (예전 버전은 생성기가 붙인 정답(actor_type)을 피처로 써서 누수가 있었다)
"""
import argparse
import json
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

DATA = os.getenv("CUSTOMS_DATA_DIR", "/opt/airflow/data")
LOG_FILE = os.path.join(DATA, "logs", "api_logs.jsonl")
LABEL_FILE = os.path.join(DATA, "labels", "ground_truth.jsonl")
FEATURE_FILE = os.path.join(DATA, "results", "features.csv")
RESULT_FILE = os.path.join(DATA, "results", "detection_results.jsonl")
EVAL_FILE = os.path.join(DATA, "results", "evaluation.json")

WINDOW = "5min"
FEATURES = ["req_count", "uniq_paths", "err_ratio", "write_ratio", "endpoint_entropy", "uniq_object_ids"]
ML_METHODS = ["iforest", "lof", "autoencoder"]
BASELINE = "zscore"          # ML 이 아닌 비교군: 피처별 robust z (median/MAD) 중 최대 |z| ≥ 3
CONTAMINATION = float(os.getenv("CUSTOMS_CONTAMINATION", "0.1"))


def read_jsonl(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(rows)


def entropy(series):
    p = series.value_counts(normalize=True)
    return float(-(p * np.log2(p)).sum())


def extract_features(logs):
    df = logs.copy()
    df["ts"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["window_start"] = df["ts"].dt.floor(WINDOW)
    # 아직 끝나지 않은 윈도우는 뺀다. 5분 윈도우 중 1분만 지난 시점이면 IP당 요청이 1~2건뿐이라
    # 모든 모델이 '요청이 적은 IP'를 이상으로 잡는다 (실측: 오탐 19건 중 18건이 진행 중 윈도우).
    df = df[df["window_start"] + pd.Timedelta(WINDOW) <= df["ts"].max()]
    # /api/v1/import-stats/user_3 → 엔드포인트 /api/v1/import-stats/{id}, 객체 user_3
    parts = df["path"].str.rsplit("/", n=1)
    df["endpoint"] = np.where(df["path"].str.startswith("/api/v1/import-stats/"),
                              "/api/v1/import-stats/{id}", df["path"])
    df["object_id"] = np.where(df["endpoint"] == "/api/v1/import-stats/{id}", parts.str[-1], None)

    return df.groupby(["src_ip", "window_start"]).agg(
        req_count=("path", "size"),
        uniq_paths=("path", "nunique"),
        err_ratio=("status_code", lambda s: float((s >= 400).mean())),
        write_ratio=("method", lambda s: float((s == "POST").mean())),
        endpoint_entropy=("endpoint", entropy),
        uniq_object_ids=("object_id", "nunique"),
    ).reset_index()


def attach_labels(features, labels):
    """(IP, 윈도우)에 공격 라벨이 하나라도 있으면 그 공격 유형, 아니면 normal.
    생성기 기록 시각과 서버 로그 시각은 수 ms 차이가 나므로, 경계에 걸린 요청은 ±2초 양쪽 윈도우에 붙인다."""
    features = features.copy()
    features["label"] = "normal"
    if labels.empty:
        return features
    atk = labels[labels["actor_type"] != "normal"].copy()
    atk["ts"] = pd.to_datetime(atk["timestamp"], utc=True, format="ISO8601")
    keyed = {}
    for shift in ("-2s", "0s", "2s"):
        w = (atk["ts"] + pd.Timedelta(shift)).dt.floor(WINDOW)
        for ip, ws, a in zip(atk["src_ip"], w, atk["actor_type"]):
            keyed[(ip, ws)] = a
    features["label"] = [keyed.get((ip, ws), "normal")
                         for ip, ws in zip(features["src_ip"], features["window_start"])]
    return features


def detect(features):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from models import run

    X = features[FEATURES].fillna(0).to_numpy(dtype=float)
    args = argparse.Namespace(contamination=CONTAMINATION, nu=CONTAMINATION, z=3.0,
                              rule_rpm=None, rule_level=10)
    flags = {}
    for m in [BASELINE] + ML_METHODS:
        _, is_anom = run(m, X, X, args, train_is_all=True)
        flags[m] = np.asarray(is_anom, dtype=bool)
    ml_votes = sum(flags[m].astype(int) for m in ML_METHODS)
    flags["ml_any"] = ml_votes >= 1
    flags["ml_majority"] = ml_votes >= 2
    return flags


def evaluate(features, flags):
    y = (features["label"] != "normal").to_numpy()
    out = {}
    for name, pred in flags.items():
        tp, fp = int((pred & y).sum()), int((pred & ~y).sum())
        fn, tn = int((~pred & y).sum()), int((~pred & ~y).sum())
        by_type = {}
        for t in sorted(set(features["label"]) - {"normal"}):
            mask = (features["label"] == t).to_numpy()
            by_type[t] = f"{int((pred & mask).sum())}/{int(mask.sum())}"
        out[name] = {
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(tp / (tp + fp), 3) if tp + fp else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else None,
            "detected_by_type": by_type,
        }
    return out


def drop_incomplete_windows(features):
    """수집 공백 검사: 윈도우 전체 요청 수가 전 윈도우 중앙값의 절반 미만이면 수집이 끊긴 구간으로 보고 탐지에서 뺀다.
    (실측: 호스트가 5분 멈춘 윈도우에서 IP당 요청이 2~9건으로 줄어 오탐 8건이 났다. 수집 공백은 공격이 아니라
    파이프라인 문제로 따로 알려야 한다.) CUSTOMS_SKIP_INCOMPLETE=0 이면 검사를 끈다."""
    if os.getenv("CUSTOMS_SKIP_INCOMPLETE", "1") == "0":
        return features, []
    totals = features.groupby("window_start")["req_count"].sum()
    sparse = totals[totals < 0.5 * totals.median()]
    skipped = [{"window_start": ws.strftime("%Y-%m-%dT%H:%M:%SZ"), "requests": int(n),
                "median_requests": int(totals.median())} for ws, n in sparse.items()]
    for s in skipped:
        print(f"[DETECTOR] skip incomplete window {s['window_start']}: "
              f"{s['requests']} requests (median {s['median_requests']}) - collection gap, not scored")
    return features[~features["window_start"].isin(sparse.index)].reset_index(drop=True), skipped


def run_detection():
    logs = read_jsonl(LOG_FILE)
    if logs.empty:
        print("[DETECTOR] No log data.")
        return None

    features = attach_labels(extract_features(logs), read_jsonl(LABEL_FILE))
    features, skipped = drop_incomplete_windows(features)
    os.makedirs(os.path.dirname(FEATURE_FILE), exist_ok=True)
    features.assign(window_start=features["window_start"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")) \
            .to_csv(FEATURE_FILE, index=False)
    n_attack = int((features["label"] != "normal").sum())
    print(f"[DETECTOR] {len(logs)} requests -> {len(features)} windows ({n_attack} attack windows by ground truth)")

    if len(features) < 10:
        print("[DETECTOR] Not enough windows for detection.")
        return None

    flags = detect(features)
    evaluation = {
        "evaluated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "requests": int(len(logs)),
        "windows": int(len(features)),
        "attack_windows": n_attack,
        "skipped_incomplete_windows": skipped,
        "features": FEATURES,
        "contamination": CONTAMINATION,
        "methods": evaluate(features, flags),
    }

    os.makedirs(os.path.dirname(RESULT_FILE), exist_ok=True)
    flagged = flags["ml_majority"] | flags[BASELINE]
    with open(RESULT_FILE, "w", encoding="utf-8") as f:  # 매 실행 전체 재계산 → 덮어쓰기 (중복 누적 없음)
        for i in np.flatnonzero(flagged):
            r = features.iloc[i]
            f.write(json.dumps({
                "src_ip": r["src_ip"],
                "window_start": r["window_start"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                **{c: (int(r[c]) if c in ("req_count", "uniq_paths", "uniq_object_ids") else round(float(r[c]), 3))
                   for c in FEATURES},
                **{m: bool(flags[m][i]) for m in [BASELINE] + ML_METHODS},
                "label": r["label"],  # 평가용 정답 (탐지에는 쓰이지 않음)
            }) + "\n")
    with open(EVAL_FILE, "w", encoding="utf-8") as f:
        json.dump(evaluation, f, ensure_ascii=False, indent=2)

    for name, m in evaluation["methods"].items():
        print(f"[DETECTOR] {name:<12} P={m['precision']} R={m['recall']} "
              f"TP={m['tp']} FP={m['fp']} FN={m['fn']} {m['detected_by_type']}")
    return evaluation


if __name__ == "__main__":
    run_detection()
