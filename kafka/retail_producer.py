"""
Retail Producer - 매출 CSV를 날짜순으로 Kafka에 재생(replay)
실제 거래가 실시간으로 들어오는 상황을 시뮬레이션한다.

체크포인트: 보낸 마지막 event_seq 를 파일에 기록하고, 재시작하면 그 다음부터 보낸다.
            (체크포인트가 없던 초기 버전은 컨테이너가 다시 뜰 때마다 5,000건을 처음부터 다시 보냈다)
            처음부터 다시 보내려면 RETAIL_REPLAY_FROM_START=1
"""
import csv
import json
import os
import time

from kafka import KafkaProducer

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CSV_PATH = os.getenv("RETAIL_CSV", "/app/data/raw/retail_sales_performance.csv")
CHECKPOINT = os.getenv("RETAIL_CHECKPOINT", "/app/data/retail/producer_checkpoint.json")
TOPIC = os.getenv("RETAIL_TOPIC", "retail-sales")
RATE = float(os.getenv("RETAIL_RATE", "50"))  # 초당 전송 건수 (0 = 최대 속도)
CHECKPOINT_EVERY = 100


def load_checkpoint():
    if os.getenv("RETAIL_REPLAY_FROM_START") == "1" or not os.path.exists(CHECKPOINT):
        return 0
    with open(CHECKPOINT, encoding="utf-8") as f:
        return int(json.load(f)["last_seq"])


def save_checkpoint(seq):
    os.makedirs(os.path.dirname(CHECKPOINT), exist_ok=True)
    tmp = CHECKPOINT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_seq": seq, "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, f)
    os.replace(tmp, CHECKPOINT)  # 쓰는 도중 죽어도 이전 체크포인트가 깨지지 않게


def main():
    with open(CSV_PATH, encoding="utf-8") as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r["date"])

    start = load_checkpoint()
    if start >= len(rows):
        print(f"[RETAIL-PRODUCER] checkpoint {start}/{len(rows)} - already replayed, nothing to send", flush=True)
        return

    time.sleep(10)  # Kafka 준비 대기
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    print(f"[RETAIL-PRODUCER] {len(rows)} rows, resume after seq {start} -> topic '{TOPIC}' ({RATE}/s)", flush=True)
    for i, row in enumerate(rows, 1):
        if i <= start:
            continue
        # 원본 값을 그대로 보낸다. 검증/정제는 Airflow ETL 단계의 책임.
        producer.send(TOPIC, key=row["store_id"], value={**row, "event_seq": i})
        if i % CHECKPOINT_EVERY == 0 or i == len(rows):
            producer.flush()          # 브로커가 받은 것만 체크포인트에 기록
            save_checkpoint(i)
        if i % 500 == 0:
            print(f"[RETAIL-PRODUCER] sent {i}/{len(rows)} (event date {row['date']})", flush=True)
        if RATE > 0:
            time.sleep(1 / RATE)

    producer.flush()
    print("[RETAIL-PRODUCER] replay finished", flush=True)


if __name__ == "__main__":
    main()
