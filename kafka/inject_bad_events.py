"""
품질 게이트 시연용 - 규칙 위반 이벤트를 retail-sales 토픽에 섞어 보낸다.

정상 경로(Kafka → retail-consumer → raw JSONL)를 그대로 타므로 ETL 입장에서는 실제 불량 데이터와 같다.
400건을 보내면 5,000 + 400 중 rejected 비율이 7.4%가 되어 게이트 기준(5%)을 넘는다.

  주입 : docker compose run --rm retail-producer python inject_bad_events.py
  정리 : docker compose stop retail-consumer
         docker compose run --rm retail-producer python inject_bad_events.py --cleanup
         docker compose start retail-consumer

주입 이벤트는 "injected": true 와 event_seq 1,000,001부터의 번호를 달아 원본(1~5,000)과 구분한다.
"""
import csv
import json
import os
import sys

CSV_PATH = os.getenv("RETAIL_CSV", "/app/data/raw/retail_sales_performance.csv")
RAW = "/app/data/retail/raw/sales_events.jsonl"
TOPIC = os.getenv("RETAIL_TOPIC", "retail-sales")
SEQ_BASE = 1_000_000

# (규칙, 건수, 원본 행 → 불량 행)
CASES = [
    ("missing_store_id",      60, lambda r: {**r, "store_id": ""}),
    ("bad_date",              60, lambda r: {**r, "date": "2023-13-45"}),
    ("future_date",           60, lambda r: {**r, "date": "2099-01-01"}),
    ("non_positive_amount",   60, lambda r: {**r, "sales_amount": "-" + r["sales_amount"]}),
    ("fractional_units",      60, lambda r: {**r, "units_sold": "2.5"}),
    ("amount_too_large",      50, lambda r: {**r, "sales_amount": "5000000"}),
    ("duplicate_transaction", 50, lambda r: dict(r)),  # 같은 거래를 새 event_seq 로 한 번 더
]


def inject():
    from kafka import KafkaProducer

    with open(CSV_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    seq, i = SEQ_BASE, 0
    for rule, n, make in CASES:
        for _ in range(n):
            seq += 1
            bad = {**make(rows[i % len(rows)]), "event_seq": seq, "injected": True}
            producer.send(TOPIC, key=bad["store_id"] or "none", value=bad)
            i += 1
        print(f"[INJECT] {rule:<22} {n}")
    producer.flush()
    print(f"[INJECT] sent {seq - SEQ_BASE} bad events -> '{TOPIC}'")


def cleanup():
    """raw JSONL 에서 주입 이벤트만 걷어낸다. consumer 를 멈춘 상태에서 실행할 것 (파일을 바꿔치기함)."""
    with open(RAW, encoding="utf-8") as f:
        lines = f.readlines()
    keep = [ln for ln in lines if '"injected": true' not in ln]
    tmp = RAW + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.writelines(keep)
    os.replace(tmp, RAW)
    print(f"[INJECT] removed {len(lines) - len(keep)} injected lines, {len(keep)} kept")


if __name__ == "__main__":
    cleanup() if "--cleanup" in sys.argv else inject()
