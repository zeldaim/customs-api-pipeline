"""
Kafka Producer — 관세 API에 트래픽을 보내고 로그를 Kafka 토픽에 전송
정상 트래픽 + 공격 트래픽 (Shadow API 탐색, BOLA) 혼합
"""
import json
import time
import random
import requests
from kafka import KafkaProducer
from datetime import datetime
import os

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
API_URL = os.getenv("API_URL", "http://customs-api:8080")
TOPIC = "customs-api-logs"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

NORMAL_USERS = [f"user_{i}" for i in range(1, 21)]
ATTACK_IPS = ["192.168.99.100", "10.0.0.55"]

HS_CODES = ["8471.30", "6203.42", "0901.11", "8703.23", "2204.21"]
SHADOW_PATHS = ["/api/v1/admin", "/api/v1/users", "/api/v1/internal/config",
                "/api/v2/export-stats", "/admin/dashboard"]


def send_log(ip, method, path, status, user_id=None, actor_type="normal"):
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "src_ip": ip,
        "method": method,
        "path": path,
        "status_code": status,
        "user_id": user_id,
        "actor_type": actor_type,  # normal / shadow_probe / bola
    }
    producer.send(TOPIC, log)
    print(f"[PRODUCER] {actor_type:12s} {method} {path} → {status}")


def normal_traffic():
    """정상 사용자 트래픽"""
    user = random.choice(NORMAL_USERS)
    ip = f"192.168.1.{random.randint(10, 100)}"
    action = random.choice(["hs_lookup", "import_stats", "declaration"])

    if action == "hs_lookup":
        code = random.choice(HS_CODES)
        r = requests.get(f"{API_URL}/api/v1/hs-code/{code}")
        send_log(ip, "GET", f"/api/v1/hs-code/{code}", r.status_code, user, "normal")

    elif action == "import_stats":
        # 본인 통계만 조회
        uid = user.split("_")[1] if "_" in user else "1"
        r = requests.get(f"{API_URL}/api/v1/import-stats/user_{uid}")
        send_log(ip, "GET", f"/api/v1/import-stats/user_{uid}", r.status_code, user, "normal")

    elif action == "declaration":
        code = random.choice(HS_CODES)
        r = requests.post(f"{API_URL}/api/v1/declaration", json={"hs_code": code, "value": random.randint(100, 10000)})
        send_log(ip, "POST", "/api/v1/declaration", r.status_code, user, "normal")


def shadow_probe():
    """Shadow API 탐색 공격 — 명세 외 경로 스캔"""
    ip = ATTACK_IPS[0]
    for path in random.sample(SHADOW_PATHS, k=3):
        try:
            r = requests.get(f"{API_URL}{path}", timeout=2)
            status = r.status_code
        except:
            status = 404
        send_log(ip, "GET", path, status, actor_type="shadow_probe")
        time.sleep(0.3)


def bola_attack():
    """BOLA 공격 — 타 사용자 수입 통계 무작위 조회"""
    ip = ATTACK_IPS[1]
    for uid in range(1, 10):
        r = requests.get(f"{API_URL}/api/v1/import-stats/user_{uid}")
        send_log(ip, "GET", f"/api/v1/import-stats/user_{uid}", r.status_code, actor_type="bola")
        time.sleep(0.5)


def main():
    print("[PRODUCER] Starting traffic generation...")
    time.sleep(10)  # Kafka 준비 대기

    tick = 0
    while True:
        # 정상 트래픽 (매 tick)
        for _ in range(random.randint(3, 7)):
            normal_traffic()
            time.sleep(0.2)

        # 공격 트래픽 (30tick마다 shadow, 50tick마다 bola)
        if tick % 30 == 0 and tick > 0:
            print("[PRODUCER] >> Shadow probe attack started")
            shadow_probe()

        if tick % 50 == 0 and tick > 0:
            print("[PRODUCER] >> BOLA attack started")
            bola_attack()

        tick += 1
        time.sleep(2)


if __name__ == "__main__":
    main()
