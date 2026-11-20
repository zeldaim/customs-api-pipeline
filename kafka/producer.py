"""
트래픽 생성기 — 관세 API 에 정상 + 공격 트래픽을 보낸다.

이 스크립트는 로그를 만들지 않는다. access log 는 API 서버가 직접 Kafka 로 보낸다 (api/app.py).
대신 "어떤 IP 가 언제 무엇을 했는지" 정답 라벨을 data/labels/ground_truth.jsonl 에 따로 남긴다.
라벨은 탐지 모델의 입력이 아니라 평가(precision/recall)에만 쓴다.

클라이언트 IP 는 X-Forwarded-For 로 전달한다 (API 가 리버스 프록시 뒤에 있다고 가정).
User-Agent 는 모두 같게 해서 UA 로 공격자를 구분할 수 없게 한다.
"""
import json
import os
import random
import time
from datetime import datetime, timezone

import requests

API_URL = os.getenv("API_URL", "http://customs-api:8080")
LABEL_FILE = os.getenv("LABEL_FILE", "/app/data/labels/ground_truth.jsonl")
UA = "customs-client/1.0"

# 정상 사용자 20명, 사용자마다 고정 IP
NORMAL_USERS = {f"user_{i}": f"192.168.1.{10 + i}" for i in range(1, 21)}
SHADOW_IP, BOLA_IP = "192.168.99.100", "10.0.0.55"

HS_CODES = ["8471.30", "6203.42", "0901.11", "8703.23", "2204.21"]
SHADOW_PATHS = ["/api/v1/admin", "/api/v1/users", "/api/v1/internal/config",
                "/api/v2/export-stats", "/admin/dashboard"]

os.makedirs(os.path.dirname(LABEL_FILE), exist_ok=True)
labels = open(LABEL_FILE, "a", encoding="utf-8")


def call(ip, actor_type, method, path, **kw):
    """요청을 보내고 정답 라벨을 기록한다. 라벨은 요청과 같은 시각·IP 로 남겨 평가 때 윈도우에 붙인다."""
    labels.write(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "src_ip": ip, "actor_type": actor_type, "method": method, "path": path,
    }) + "\n")
    labels.flush()
    headers = {"X-Forwarded-For": ip, "User-Agent": UA}
    try:
        r = requests.request(method, f"{API_URL}{path}", headers=headers, timeout=3, **kw)
        status = r.status_code
    except requests.RequestException:
        status = "ERR"
    print(f"[TRAFFIC] {actor_type:12s} {ip:15s} {method} {path} -> {status}", flush=True)


def normal_traffic():
    """정상 사용자: HS 코드 조회, 본인 수입 통계 조회, 수입 신고"""
    user, ip = random.choice(list(NORMAL_USERS.items()))
    action = random.choices(["hs_lookup", "import_stats", "declaration"], weights=[5, 3, 2])[0]
    if action == "hs_lookup":
        # 가끔 오타 난 코드도 조회한다 (정상 사용자도 404 를 낸다)
        code = random.choice(HS_CODES) if random.random() > 0.1 else "9999.99"
        call(ip, "normal", "GET", f"/api/v1/hs-code/{code}")
    elif action == "import_stats":
        call(ip, "normal", "GET", f"/api/v1/import-stats/{user}")
    else:
        call(ip, "normal", "POST", "/api/v1/declaration",
             json={"hs_code": random.choice(HS_CODES), "value": random.randint(100, 10000)})


def shadow_probe():
    """Shadow API 탐색 — 명세 외 경로 스캔"""
    for path in random.sample(SHADOW_PATHS, k=3):
        call(SHADOW_IP, "shadow_probe", "GET", path)
        time.sleep(0.3)


def bola_attack():
    """BOLA — 다른 사용자의 수입 통계를 ID 를 바꿔가며 조회"""
    for uid in range(1, 10):
        call(BOLA_IP, "bola", "GET", f"/api/v1/import-stats/user_{uid}")
        time.sleep(0.5)


def main():
    print("[TRAFFIC] Starting traffic generation...", flush=True)
    time.sleep(10)  # API·Kafka 준비 대기

    tick = 0
    while True:
        for _ in range(random.randint(3, 7)):
            normal_traffic()
            time.sleep(0.2)

        # 공격 트래픽 (30tick마다 shadow, 50tick마다 bola)
        if tick % 30 == 0 and tick > 0:
            shadow_probe()
        if tick % 50 == 0 and tick > 0:
            bola_attack()

        tick += 1
        time.sleep(2)


if __name__ == "__main__":
    main()
