# supply-chain — API-BOM · SBOM · 빌드 재현성 → 보고서

이 레포의 API 와 이미지를 표준 형식(CycloneDX 1.6)으로 적고, 취약점·빌드 재현성을 검사해 보고서 한 장으로 만든다.
CI(`.github/workflows/supply-chain.yml`)와 로컬이 같은 스크립트(`run.sh`)를 쓴다.

형식과 절차는 [zeldaim/api-security-supply-chain](https://github.com/zeldaim/api-security-supply-chain)의
`standardize/api-bom/source_to_baseline.py`·자동 일일 보고서를 이 앱(Flask)에 맞게 옮긴 것이다.

```
api/app.py ──(ast 로 라우트 추출)──▶ baseline.csv ◀── 사람이 decision 승인 (PR)
                                        │ check: 소스와 다르면 CI 실패
                                        ▼
이미지 3개 ──syft──▶ SBOM ──BOM-Link──▶ API-BOM (CycloneDX 1.6) ──▶ 스키마 검증 ──▶ OpenAPI 허용 목록
     │                                    │
     └─grype──▶ 취약점                     └─ observe: 샘플 access log 와 대조 (documented / probe / shadow)
Dockerfile·compose ──build_check──▶ 빌드 재현성·EOL
                                  ──────────────▶ report.py ──▶ out/report.md
```

## 파일

| 파일 | 하는 일 |
|---|---|
| `api_bom.py` | `extract`(소스→기준선 후보) · `check`(게이트) · `cyclonedx` · `validate` · `openapi` · `observe` |
| `baseline.csv` | **승인된 API 기준선** — 사람이 고치는 유일한 파일 |
| `build_check.py` | Dockerfile·compose 정적 검사 (B1~B4, S3) |
| `report.py` | `out/` 결과 → Markdown 보고서 |
| `run.sh` | 전체 실행 (로컬·CI 공통) |
| `fixtures/access_sample.jsonl` | 관측 대조용 샘플 로그 556줄 — API 서버가 남긴 실제 로그에서 공격 IP 2개의 요청 전부 + 정상 400건 (모의 트래픽, 사설 IP) |
| `reports/` | 로컬 실행 보고서 기록 |

## 판정 기준

| 코드 | 심각도 | 기준 | 출처 |
|---|---|---|---|
| S1 | 높음 | 부품에 Critical/High 취약점 | grype |
| S3 | 높음 | 지원 종료(EOL) 런타임 | `build_check.py` EOL 표 (근거 URL 포함) |
| G3 | 낮음 | 헬스 경로 외부 노출 | API-BOM |
| G4 | 높음 | 인증 없는 고객 데이터 조회 | API-BOM (`auth=none` + 분류 `customer-transaction`) |
| B1 | 중간 | 버전 고정 없는 `pip install` | `build_check.py` |
| B2 | 중간 | 기반 이미지 digest 미고정 | `build_check.py` |
| B3 | 높음 | 컨테이너 시작 시 패키지 설치 — 실행 코드가 이미지 SBOM 밖 | `build_check.py` + SBOM 교차 확인 |
| B4 | 중간 | 의존성 목록 파일 없음 — 소스 SBOM 이 의존성을 못 봄 | `build_check.py` |

S·G 코드는 api-security-supply-chain 의 기준(S1 부품 취약점, S3 지원 종료, G3·G4 API)과 같은 뜻으로 쓴다.
B 코드는 이 레포에서 새로 정의했다.

## 사람이 하는 일

API 를 추가·삭제하면 CI 의 `check` 가 실패한다. 그때:

```bash
python supply-chain/api_bom.py extract api/app.py --prev supply-chain/baseline.csv > /tmp/new.csv
# 새 줄의 decision(review)을 active / deprecated / exclude 로 정하고 auth·owner·note 를 채워 baseline.csv 로 커밋
```

## 실행

```bash
bash supply-chain/run.sh supply-chain/out    # Docker 필요, 레포 루트에서
# 예상 출력 마지막 줄: == 결과: API-BOM 게이트 통과
# 보고서: supply-chain/out/report.md
```

도구는 버전과 digest 를 고정한 컨테이너로 돌린다: syft v1.56.0, grype v0.121.1 (둘 다 2026-11-02 릴리스 —
갓 나온 릴리스는 쓰지 않는다). GitHub Actions 도 커밋 SHA 로 고정했다.
