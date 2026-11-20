"""
Kafka Consumer — Kafka 토픽에서 로그를 꺼내 JSONL 파일에 저장
"""
import json
import os
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "customs-api-logs"
OUTPUT_FILE = "/app/data/logs/api_logs.jsonl"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="customs-consumer-group",
)

print(f"[CONSUMER] Listening on topic: {TOPIC}")

with open(OUTPUT_FILE, "a") as f:
    for message in consumer:
        log = message.value
        f.write(json.dumps(log) + "\n")
        f.flush()
        print(f"[CONSUMER] Saved: {log['src_ip']:15s} {log['method']} {log['path']} {log['status_code']}")
