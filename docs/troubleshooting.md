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
