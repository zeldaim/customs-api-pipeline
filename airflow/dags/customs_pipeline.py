"""
Airflow DAG — 5분마다 실행
1. 로그 수집 확인
2. 이상탐지 모델 실행
3. 결과 리포트
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/opt/airflow/detector")

default_args = {
    "owner": "zeldaim",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="customs_api_pipeline",
    description="관세 API 로그 수집 → 이상탐지 파이프라인",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["customs", "security", "kafka"],
) as dag:

    check_logs = BashOperator(
        task_id="check_log_file",
        bash_command="wc -l /opt/airflow/data/logs/api_logs.jsonl 2>/dev/null || echo '0 lines'",
    )

    run_detection = PythonOperator(
        task_id="run_anomaly_detection",
        python_callable=lambda: __import__("detect").run_detection(),
    )

    report = BashOperator(
        task_id="print_results",
        bash_command="tail -5 /opt/airflow/data/results/detection_results.jsonl 2>/dev/null || echo 'No anomalies yet'",
    )

    check_logs >> run_detection >> report
