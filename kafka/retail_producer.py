"""
Retail Producer — 매출 CSV를 날짜순으로 Kafka에 재생(replay)
실제 거래가 실시간으로 들어오는 상황을 시뮬레이션한다.
"""
import csv
import json
import os
import time

from kafka import KafkaProducer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CSV_PATH = os.getenv("RETAIL_CSV", "/app/data/raw/retail_sales_performance.csv")
TOPIC = os.getenv("RETAIL_TOPIC", "retail-sales")
RATE = float(os.getenv("RETAIL_RATE", "50"))  # 초당 전송 건수 (0 = 최대 속도)


def main():
    time.sleep(10)  # Kafka 준비 대기
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    with open(CSV_PATH, encoding="utf-8") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r["date"])

    print(f"[RETAIL-PRODUCER] {len(rows)} rows -> topic '{TOPIC}' ({RATE}/s)")
    for i, row in enumerate(rows, 1):
        # 원본 값을 그대로 보낸다. 검증/정제는 Airflow ETL 단계의 책임.
        event = {**row, "event_seq": i}
        producer.send(TOPIC, key=row["store_id"], value=event)
        if i % 500 == 0:
            print(f"[RETAIL-PRODUCER] sent {i}/{len(rows)} (event date {row['date']})")
        if RATE > 0:
            time.sleep(1 / RATE)

    producer.flush()
    print("[RETAIL-PRODUCER] replay finished")


if __name__ == "__main__":
    main()
