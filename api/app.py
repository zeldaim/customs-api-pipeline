"""
가짜 관세 API 서버
- 정상 트래픽: HS코드 조회, 수출입 통계
- 공격 트래픽: Shadow API 탐색, 비정상 대량 조회 (BOLA)
"""
from flask import Flask, request, jsonify
import logging
import json
import time

app = Flask(__name__)

# 액세스 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s'
)

# ── 샘플 데이터 ───────────────────────────────────────
HS_CODES = {
    "8471.30": {"desc": "노트북 컴퓨터", "tariff_rate": 0.0},
    "6203.42": {"desc": "남성용 면바지", "tariff_rate": 13.0},
    "0901.11": {"desc": "커피(볶지 않은 것)", "tariff_rate": 2.0},
    "8703.23": {"desc": "승용차 1500~3000cc", "tariff_rate": 8.0},
    "2204.21": {"desc": "포도주 2L 이하", "tariff_rate": 15.0},
}

IMPORT_STATS = {
    "user_1": {"count": 5, "total_value": 150000},
    "user_2": {"count": 2, "total_value": 30000},
    "user_3": {"count": 8, "total_value": 420000},
}

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
    data = request.get_json()
    if not data or "hs_code" not in data:
        return jsonify({"error": "hs_code required"}), 400
    return jsonify({"status": "accepted", "declaration_id": f"DEC-{int(time.time())}"})


@app.route("/api/v1/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
