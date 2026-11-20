"""
Retail Consumer — retail-sales 토픽을 raw JSONL로 적재 (landing zone)
"""
import json
import os

from kafka import KafkaConsumer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("RETAIL_TOPIC", "retail-sales")
OUTPUT_FILE = "/app/data/retail/raw/sales_events.jsonl"

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
# 컨테이너는 root, Airflow 는 uid 50000 → Airflow 가 clean/quality/mart 를 만들 수 있게 열어둔다
os.chmod("/app/data/retail", 0o777)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="retail-consumer-group",
)

print(f"[RETAIL-CONSUMER] Listening on topic: {TOPIC}")

with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
    for n, message in enumerate(consumer, 1):
        f.write(json.dumps(message.value) + "\n")
        f.flush()
        if n % 500 == 0:
            print(f"[RETAIL-CONSUMER] saved {n} events")
