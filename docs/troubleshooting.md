# 트러블슈팅 기록

구축 과정에서 발생한 오류와 해결 방법을 기록한다.

---

## 오류 1 — LocalExecutor + SQLite 호환 불가

**발생 위치:** Airflow 컨테이너 시작 시

**오류 메시지:**
```
airflow.exceptions.AirflowConfigException: error: cannot use SQLite with the LocalExecutor
```

**원인:**
`LocalExecutor`는 병렬 처리를 위해 SQLite 대신 PostgreSQL/MySQL 같은 DB가 필요하다.
초기 설정에서 `AIRFLOW__CORE__EXECUTOR: LocalExecutor` + `sqlite` 조합을 사용해 충돌 발생.

**해결:**
```yaml
# docker-compose.yml
AIRFLOW__CORE__EXECUTOR: SequentialExecutor  # LocalExecutor → SequentialExecutor
```

SequentialExecutor는 SQLite와 호환되며 단일 태스크 순차 실행 방식이다.
병렬 처리가 필요하면 추후 PostgreSQL + LocalExecutor로 전환 필요.

---

## 오류 2 — Airflow users create 명령어 파싱 실패

**발생 위치:** docker-compose.yml command 블록

**오류 메시지:**
```
/bin/bash: line 3: --firstname: command not found
/bin/bash: line 4: --role: command not found
```

**원인:**
YAML의 `>` 블록 스칼라로 멀티라인 명령어를 작성했을 때 bash가 `--firstname`, `--role` 등의 플래그를 별도 명령으로 인식.

**해결:**
`airflow users create` 명령어 대신 환경변수로 초기 유저를 자동 생성하는 방식으로 변경.
```yaml
_AIRFLOW_WWW_USER_CREATE: "true"
_AIRFLOW_WWW_USER_USERNAME: admin
_AIRFLOW_WWW_USER_PASSWORD: admin
```

---

## 오류 3 — ModuleNotFoundError: No module named 'sklearn'

**발생 위치:** Airflow DAG `run_anomaly_detection` 태스크

**오류 메시지:**
```
ModuleNotFoundError: No module named 'sklearn'
```

**원인:**
Airflow 공식 이미지(`apache/airflow:2.9.1`)에는 scikit-learn이 기본 포함되지 않는다.
`detector/detect.py`에서 `from sklearn.ensemble import IsolationForest`를 호출할 때 실패.

**해결:**
docker-compose.yml의 Airflow command에 pip 설치 추가:
```yaml
command: bash -c "pip install scikit-learn pandas -q && airflow db migrate && airflow webserver & airflow scheduler"
```

설치 확인: `sklearn 1.9.1`

---

## 현재 상태 (2026-11-20)

| 컨테이너 | 상태 |
|----------|------|
| customs-api | 정상 |
| zookeeper | 정상 |
| kafka | 정상 |
| kafka-producer | 정상 (트래픽 생성 중) |
| kafka-consumer | 정상 (로그 저장 중) |
| airflow | 정상 (http://localhost:8081) |

---

## 오류 — Airflow가 리테일 출력 폴더에 쓰지 못함 (PermissionError)

**발생 위치:** `retail_daily_pipeline` → `validate_clean_aggregate`

**오류 메시지:**
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/data/retail/clean'
```

**원인:** `data/retail/`을 retail-consumer 컨테이너(root)가 먼저 만들어서, Airflow 컨테이너 사용자(uid 50000)가 하위 폴더를 만들 수 없었다.

**해결:** consumer가 폴더를 만든 직후 권한을 연다 (`os.chmod("/app/data/retail", 0o777)`). 운영 환경이라면 컨테이너 uid를 맞추거나 출력 위치를 분리한다.

---

## 오류 — 리테일 이벤트가 두 배로 적재됨

**증상:** 품질 리포트의 적재 라인이 10,217로 실제 거래 5,000건의 두 배 이상.

**원인:** `docker compose up -d --build retail-consumer` 실행 시 이미 종료된 retail-producer도 다시 시작되어 CSV 전체를 처음부터 다시 보냈다. producer에 "어디까지 보냈는지" 기록이 없었다.

**영향:** 없음. ETL이 `event_seq` 기준으로 중복을 제거하므로 정제 결과·집계는 5,000건 그대로였다.

**해결:** producer가 브로커에 flush한 마지막 `event_seq`를 `data/retail/producer_checkpoint.json`에 원자적으로(tmp 파일 → rename) 기록하고, 재시작 시 그다음부터 보낸다. 처음부터 다시 보내려면 `RETAIL_REPLAY_FROM_START=1`.

