"""
모의 관세 API 서버
- 엔드포인트: HS코드 조회, 수입 통계 조회(BOLA 취약), 수입 신고
- 모든 요청의 access log 를 서버가 직접 Kafka(customs-api-logs)로 보낸다.
  로그에는 서버가 관찰할 수 있는 것만 들어간다 (시각, IP, 메서드, 경로, 상태코드, 응답 크기, UA).
  공격 여부 같은 정답 라벨은 들어가지 않는다 - 그건 트래픽 생성기가 따로 기록한다 (평가 전용).
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

from flask import Flask, g, jsonify, request

app = Flask(__name__)

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
LOG_TOPIC = os.getenv("LOG_TOPIC", "customs-api-logs")
_producer = None


def log_producer():
    """Kafka 가 늦게 뜰 수 있으므로 첫 로그 전송 때 연결한다. 실패하면 다음 요청에서 다시 시도."""
    global _producer
    if _producer is None:
        from kafka import KafkaProducer
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _producer


# ── 샘플 데이터 ───────────────────────────────────────
HS_CODES = {
    "8471.30": {"desc": "노트북 컴퓨터", "tariff_rate": 0.0},
    "6203.42": {"desc": "남성용 면바지", "tariff_rate": 13.0},
    "0901.11": {"desc": "커피(볶지 않은 것)", "tariff_rate": 2.0},
    "8703.23": {"desc": "승용차 1500~3000cc", "tariff_rate": 8.0},
    "2204.21": {"desc": "포도주 2L 이하", "tariff_rate": 15.0},
}

# 정상 사용자 user_1 ~ user_20 모두 존재 (없는 사용자를 조회하면 404)
IMPORT_STATS = {f"user_{i}": {"count": i % 9 + 1, "total_value": 10000 * (i % 7 + 1)} for i in range(1, 21)}


# ── access log ───────────────────────────────────────
@app.before_request
def _start():
    g.t0 = time.perf_counter()


@app.after_request
def _access_log(resp):
    # 리버스 프록시 뒤에 있다고 가정: 클라이언트 IP 는 X-Forwarded-For 의 첫 번째 값
    xff = request.headers.get("X-Forwarded-For", "")
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "src_ip": xff.split(",")[0].strip() or request.remote_addr,
        "method": request.method,
        "path": request.path,
        "status_code": resp.status_code,
        "resp_bytes": resp.calculate_content_length() or 0,
        "latency_ms": round((time.perf_counter() - g.t0) * 1000, 2),
        "user_agent": request.headers.get("User-Agent", ""),
    }
    try:
        log_producer().send(LOG_TOPIC, log)
    except Exception as e:  # 로그 전송 실패가 API 응답을 막으면 안 된다
        print(f"[ACCESS-LOG] kafka send failed: {e}", file=sys.stderr, flush=True)
    return resp


# ── 엔드포인트 ────────────────────────────────────────
@app.route("/api/v1/hs-code/<code>", methods=["GET"])
def get_hs_code(code):
    """HS 코드 조회 (정상 엔드포인트)"""
    if code in HS_CODES:
        return jsonify({"code": code, **HS_CODES[code]})
    return jsonify({"error": "HS code not found"}), 404


@app.route("/api/v1/import-stats/<user_id>", methods=["GET"])
def get_import_stats(user_id):
    """수입 통계 조회 (BOLA 취약점 — 인증 없이 타 사용자 조회 가능)"""
    if user_id in IMPORT_STATS:
        return jsonify({"user_id": user_id, **IMPORT_STATS[user_id]})
    return jsonify({"error": "User not found"}), 404


@app.route("/api/v1/declaration", methods=["POST"])
def submit_declaration():
    """수출입 신고 (정상 엔드포인트)"""
    data = request.get_json(silent=True)
    if not data or "hs_code" not in data:
        return jsonify({"error": "hs_code required"}), 400
    return jsonify({"status": "accepted", "declaration_id": f"DEC-{int(time.time())}"})


@app.route("/api/v1/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
