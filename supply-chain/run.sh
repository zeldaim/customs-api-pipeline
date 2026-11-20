#!/usr/bin/env bash
# 공급망 파이프라인 — 로컬과 CI(.github/workflows/supply-chain.yml)가 같은 스크립트를 쓴다.
#
#   bash supply-chain/run.sh [출력 디렉터리]        (기본 supply-chain/out, 레포 루트에서 실행)
#
# 1) API-BOM : 소스 ↔ 승인 기준선 검사(게이트) → CycloneDX 1.6 → 스키마 검증 → OpenAPI 허용 목록 → 샘플 로그 대조
# 2) SBOM    : 이미지 3개 빌드·받기 → syft(CycloneDX 1.6) → grype 취약점
# 3) 빌드 재현성·EOL 검사 → 4) 보고서 out/report.md
#
# 실패 기준: API-BOM 검사·스키마 검증이 실패하면 종료코드 1 (보고서는 그래도 만든다).
#            취약점·빌드 재현성 발견은 보고서로만 남기고 실패시키지 않는다.
# 도구는 버전을 고정한 컨테이너로 돌린다 (릴리스 후 2주 이상 지난 버전, digest 고정).
set -uo pipefail

OUT=${1:-supply-chain/out}
SYFT_IMAGE="anchore/syft:v1.56.0@sha256:697ae864af1299cfa43c54b161cea3dd65c6d54de82e3dba66ea3dccb3acc313"
GRYPE_IMAGE="anchore/grype:v0.121.1@sha256:6b06708b036f74b2cd70e94b706b4586af96d1932e6ac46255940b2e39dd44a9"
AIRFLOW_IMAGE="apache/airflow:2.9.1"   # docker-compose.yml 과 같은 값
SC=supply-chain
mkdir -p "$OUT"
export MSYS_NO_PATHCONV=1            # Git Bash(Windows)에서 컨테이너 경로가 바뀌지 않게
HOST_PWD=$(pwd -W 2>/dev/null || pwd)
GATE=0

echo "== 1) API-BOM"
# 같은 소스면 같은 BOM: 시각을 app.py 마지막 커밋 시각으로 고정
export SOURCE_DATE_EPOCH=$(git log -1 --format=%ct -- api/app.py 2>/dev/null || date +%s)
python $SC/api_bom.py check api/app.py $SC/baseline.csv > "$OUT/check.txt" 2>&1 || GATE=1
cat "$OUT/check.txt"

echo "== 2) 이미지 빌드·받기"
docker build -q -t customs-api:sc api >/dev/null && echo "built customs-api:sc"
docker build -q -t kafka-clients:sc kafka >/dev/null && echo "built kafka-clients:sc"
docker pull -q "$AIRFLOW_IMAGE" >/dev/null && echo "pulled $AIRFLOW_IMAGE"

echo "== 3) SBOM (syft)"
syft() {  # syft <이미지> <파일 이름>
  docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$HOST_PWD:/work" -w /work \
    "$SYFT_IMAGE" "docker:$1" -o "cyclonedx-json@1.6=$OUT/$2" -q
}
syft customs-api:sc        sbom-customs-api.cdx.json
syft kafka-clients:sc      sbom-kafka-clients.cdx.json
syft "$AIRFLOW_IMAGE"      sbom-airflow.cdx.json
docker run --rm -v "$HOST_PWD:/work" -w /work "$SYFT_IMAGE" dir:. \
  --exclude './data/**' --exclude './airflow/logs/**' --exclude "./$OUT/**" \
  -o "cyclonedx-json@1.6=$OUT/sbom-source.cdx.json" -q
ls -1 "$OUT"/sbom-*.cdx.json

echo "== 4) API-BOM 생성·검증 (이미지 SBOM 과 BOM-Link 로 연결)"
python $SC/api_bom.py cyclonedx $SC/baseline.csv api/app.py --sbom "$OUT/sbom-customs-api.cdx.json" > "$OUT/api-bom.cdx.json"
python $SC/api_bom.py validate "$OUT/api-bom.cdx.json" > "$OUT/validate.txt" 2>&1 || GATE=1
cat "$OUT/validate.txt"
python $SC/api_bom.py openapi "$OUT/api-bom.cdx.json" > "$OUT/openapi.from-bom.json"
python $SC/api_bom.py observe $SC/baseline.csv $SC/fixtures/access_sample.jsonl --json "$OUT/observe.json"

echo "== 5) 취약점 (grype)"
for s in customs-api kafka-clients airflow; do
  docker run --rm -v "$HOST_PWD:/work" -w /work -v grype-db:/root/.cache/grype \
    "$GRYPE_IMAGE" "sbom:$OUT/sbom-$s.cdx.json" -o json -q > "$OUT/grype-$s.json"
  python -c "import json,collections,sys;c=collections.Counter(m['vulnerability']['severity'] for m in json.load(open(sys.argv[1]))['matches']);print(sys.argv[2],dict(c))" "$OUT/grype-$s.json" "$s"
done

echo "== 6) 빌드 재현성·EOL"
python $SC/build_check.py . --json "$OUT/build_findings.json"

echo "== 7) 보고서"
python - "$OUT" "$SYFT_IMAGE" "$GRYPE_IMAGE" <<'PY'
import json, sys
out, syft, grype = sys.argv[1:4]
g = json.load(open(f"{out}/grype-customs-api.json", encoding="utf-8"))
db = (g.get("descriptor", {}).get("db", {}) or {}).get("status", {}) or g.get("descriptor", {}).get("db", {})
built = db.get("built") or db.get("Built") or "?"
json.dump({"syft": syft.split(":")[1].split("@")[0], "grype": grype.split(":")[1].split("@")[0],
           "grype_db": str(built)[:10]}, open(f"{out}/tools.json", "w"))
PY
python $SC/report.py "$OUT" --commit "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)" > "$OUT/report.md"
echo "report: $OUT/report.md ($(wc -l < "$OUT/report.md") lines)"

[ $GATE -eq 0 ] && echo "== 결과: API-BOM 게이트 통과" || echo "== 결과: API-BOM 게이트 실패"
exit $GATE
