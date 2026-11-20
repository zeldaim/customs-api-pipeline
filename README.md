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

## 파이프라인 2 — 리테일 매출 ETL + 데이터 품질

실제 매출 거래 데이터(5,000건, 2022-01 ~ 2023-04)를 Kafka로 실시간 재생하고, Airflow가 매일 검증·정제·집계한다.

```
retail_sales_performance.csv
       │ 날짜순 replay (초당 50건, key=store_id)
       ▼
Retail Producer ──▶ Topic: retail-sales ──▶ Retail Consumer
                                                  │ raw 적재 (landing zone)
                                                  ▼
                                data/retail/raw/sales_events.jsonl
                                                  │
                              Airflow DAG retail_daily_pipeline (매일 01:00)
                                                  │
              check_raw_events ─▶ validate_clean_aggregate ─▶ quality_gate
                                                  │
              clean/ (정제 거래) · quality/ (위반·의심 거래, 리포트) · mart/ (일·월 집계)
```

**품질 규칙** (`etl/retail_etl.py`)

| 단계 | 규칙 | 처리 |
|------|------|------|
| 전달 중복 | 같은 `event_seq` 재수신 (Kafka at-least-once) | 제거 |
| 스키마 | 필수 컬럼 누락, 날짜/숫자 파싱 실패, 미래 날짜 | rejected |
| 값 범위 | 매출액·수량 ≤ 0, 소수 수량, 상한 초과 | rejected |
| 업무 중복 | 날짜·매장·상품·금액·수량이 동일한 거래 | rejected |
| 단가 이상치 | log(매출액/수량)의 robust z-score > 3.5 (median/MAD) | flagged (통과하되 표시) |
| 품질 게이트 | rejected 비율 > 5% | DAG 실패 처리 |

**실행 결과**

| 지표 | 값 |
|------|----|
| 적재 라인 / 고유 이벤트 | 5,687 / 5,000 |
| Kafka 재전달 중복 제거 | 687건 (consumer 재시작 시 미커밋 오프셋 재수신) |
| 규칙 위반 | 0건 |
| 단가 이상치 | 69건 (1.4%) — 예: 1개에 984.22, 29개에 13.11 |
| 집계 | 484일 / 16개월 |

품질 리포트는 원천 데이터 자체의 특성도 경고로 남긴다: 매출액-수량 상관이 −0.016으로 거의 없고(금액 익명화·노이즈 가능성), 3,834개 매장 중 대부분이 거래 1건(샘플링 가능성). 그래서 매장·상품 단위 분석보다 **파이프라인과 데이터 품질 검증**에 초점을 맞췄다.

> 원본 CSV(`data/raw/`)와 생성 결과(`data/retail/`)는 저장소에 올리지 않는다.

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
├── kafka/                # Producer / Consumer (관세 + 리테일)
├── etl/                  # 리테일 검증·정제·집계
├── airflow/dags/         # Airflow DAG (customs, retail)
├── detector/             # IsolationForest 이상탐지
├── data/
│   ├── raw/              # 리테일 원본 CSV (git 제외)
│   ├── retail/           # 리테일 raw/clean/quality/mart (git 제외)
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
