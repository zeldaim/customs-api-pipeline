"""
Airflow DAG - 리테일 매출 ETL (매일 01:00)
1. raw 적재 확인 (Kafka retail-consumer 가 쌓은 JSONL)
2. 검증/정제/집계 (etl/retail_etl.py)
3. 품질 게이트: 규칙 위반 비율이 임계치를 넘으면 실패 처리
4. ML 이상탐지: 관세 파이프라인과 같은 IF · LOF · Autoencoder (etl/retail_anomaly.py)
5. 리포트: quality/report.html 생성 (etl/retail_report.py)
"""
import json
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/etl")

REPORT = "/opt/airflow/data/retail/quality/report.json"
MAX_REJECTED_RATE = 0.05

default_args = {
    "owner": "zeldaim",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_etl():
    import retail_etl
    return retail_etl.run()["clean"]


def detect_anomalies():
    import retail_anomaly
    return retail_anomaly.run()["majority"]


def build_report():
    import retail_report
    return retail_report.build()


def quality_gate():
    with open(REPORT, encoding="utf-8") as f:
        rep = json.load(f)
    for w in rep["warnings"]:
        print(f"[WARN] {w}")
    if rep["rejected_rate"] > MAX_REJECTED_RATE:
        raise AirflowFailException(
            f"rejected_rate {rep['rejected_rate']:.2%} > {MAX_REJECTED_RATE:.0%}")
    print(f"[OK] clean={rep['clean']} rejected={rep['rejected']} "
          f"flagged={rep['flagged_price_outliers']}")


with DAG(
    dag_id="retail_daily_pipeline",
    description="리테일 매출 Kafka 적재분 → 검증/정제 → 일·월 집계 마트",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl", "data-quality", "kafka", "ml"],
) as dag:

    check_raw = BashOperator(
        task_id="check_raw_events",
        bash_command="test -s /opt/airflow/data/retail/raw/sales_events.jsonl "
                     "&& wc -l /opt/airflow/data/retail/raw/sales_events.jsonl",
    )

    etl = PythonOperator(task_id="validate_clean_aggregate", python_callable=run_etl)

    gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate)

    detect = PythonOperator(task_id="detect_anomalies", python_callable=detect_anomalies)

    report = PythonOperator(task_id="build_report", python_callable=build_report)

    check_raw >> etl >> [gate, detect] >> report
