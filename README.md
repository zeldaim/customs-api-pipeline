# customs-api-pipeline

관세 API 보안 로그 파이프라인 — Kafka 실시간 스트리밍 + Airflow 스케줄링 + ML 이상탐지

## 아키텍처

```
가짜 관세 API (Flask)
       │ 정상 트래픽 + Shadow API 탐색 + BOLA 공격
       ▼
   Kafka Producer ──▶ Topic: customs-api-logs
                              │
                    Kafka Consumer
                              │ JSONL 저장
                              ▼
                    data/logs/api_logs.jsonl
                              │
                    Airflow DAG (5분마다)
                              │
                    IsolationForest 이상탐지
                              │
                    data/results/detection_results.jsonl
```

## 탐지 대상

| 공격 유형 | 설명 |
|-----------|------|
| Shadow API 탐색 | 명세 외 경로 스캔 (`/admin`, `/internal` 등) |
| BOLA | 타 사용자 수입 통계 무단 조회 |

## 빠른 시작

```bash
git clone https://github.com/zeldaim/customs-api-pipeline.git
cd customs-api-pipeline

docker compose up -d --build

# Airflow UI: http://localhost:8081 (admin / admin)
# 관세 API:   http://localhost:8080/api/v1/health
```

## 파일 구조

```
customs-api-pipeline/
├── api/                  # 가짜 관세 API (Flask)
├── kafka/                # Producer / Consumer
├── airflow/dags/         # Airflow DAG
├── detector/             # IsolationForest 이상탐지
├── data/
│   ├── logs/             # Kafka Consumer 저장 로그
│   └── results/          # 탐지 결과
├── docs/screenshots/     # 과정 스크린샷
└── docker-compose.yml
```

## 스택

- **Kafka** 7.6.0 (Confluent)
- **Airflow** 2.9.1
- **Python** 3.11 — kafka-python, scikit-learn, pandas, flask
- **Docker Compose**
