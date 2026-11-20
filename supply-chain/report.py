"""공급망 보안 보고서 — run.sh 결과(out/)를 모아 Markdown 보고서 한 장을 만든다.

    python report.py <out 디렉터리> [--commit <커밋>] > report.md

양식은 zeldaim/api-security-supply-chain 의 자동 일일 보고서(automate/daily-report)를 따른다:
요약 → 보안 발견사항(ID·심각도·기준·대상·근거) → API 자산 → 이미지별 SBOM → 권고 → 재현 방법.
기준 코드: S1 부품 Critical/High 취약점 · S3 지원 종료 런타임 · G3 헬스·관리 경로 · G4 인증 없는 고객 데이터 조회
          · B1~B4 빌드 재현성 (build_check.py 참고)
입력 파일이 없으면 그 절은 '(없음)'으로 남기고 나머지는 만든다. 표준 라이브러리만 사용.
"""
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
SEV_ORDER = {"높음": 0, "중간": 1, "낮음": 2}
IMAGES = {  # sbom 파일 이름 → (표시 이름, 설명)
    "customs-api": ("customs-api", "api/Dockerfile — 관세 API (Flask)"),
    "kafka-clients": ("kafka-clients", "kafka/Dockerfile — producer·consumer·트래픽 생성기"),
    "airflow": ("apache/airflow:2.9.1", "docker-compose.yml — 탐지·ETL 실행"),
}
S1_TOP = 8   # 이미지별로 보고서 표에 올리는 위험 부품 수 (나머지는 합계로)


def load(p):
    p = Path(p)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def prop(svc, name):
    return next((p["value"] for p in svc.get("properties", []) if p["name"] == name), None)


def source_name(artifact):
    """deb 바이너리 패키지 → 원본(소스) 패키지 이름. libssl3t64·openssl-provider-legacy → openssl.
    같은 CVE 가 바이너리 패키지마다 따로 잡혀 건수가 부풀므로 원본 기준으로 묶는다."""
    ups = artifact.get("upstreams") or []
    return ups[0]["name"] if ups else artifact["name"]


def grype_cves(g, image, groups):
    """Grype JSON → 이미지의 고유 CVE(심각도별). groups 에 원본 패키지별로 쌓는다."""
    per_image = defaultdict(set)
    for m in g.get("matches", []):
        v, a = m["vulnerability"], m["artifact"]
        s = v.get("severity", "Unknown")
        per_image[s].add(v["id"])
        if s not in ("Critical", "High"):
            continue
        grp = groups[(source_name(a), a.get("type", ""))]
        grp[s].add(v["id"])
        grp["pkgs"].add(a["name"])
        grp["images"].add(image)
        grp["versions"].add(a.get("version", ""))
        if v.get("fix", {}).get("state") == "fixed":
            grp["fixed"].add(v["id"])
    return per_image


def main():
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    out = Path(sys.argv[1])
    commit = sys.argv[sys.argv.index("--commit") + 1] if "--commit" in sys.argv else "unknown"
    now = datetime.now(KST)

    bom = load(out / "api-bom.cdx.json")
    observe = load(out / "observe.json") or {"endpoints": [], "unseen_active": [], "logs": []}
    build = load(out / "build_findings.json") or []
    tools = load(out / "tools.json") or {}
    check_txt = (out / "check.txt").read_text(encoding="utf-8").strip() if (out / "check.txt").exists() else "(없음)"
    validate_txt = (out / "validate.txt").read_text(encoding="utf-8").strip() if (out / "validate.txt").exists() else "(없음)"

    findings = []   # (심각도, 기준, 대상, 근거)

    # ── API 자산 (API-BOM) → G3·G4 ──────────────────────────────────────────
    ops = []
    obs_by_key = {e["endpoint_key"]: e for e in observe["endpoints"]}
    if bom:
        for grp in bom["services"]:
            for svc in grp["services"]:
                ops.append(svc)
                key = svc["name"]
                cls = svc["data"][0]["classification"]
                if svc.get("authenticated") is False and cls == "customer-transaction" and key.startswith("GET "):
                    o = obs_by_key.get(key, {}).get("max_distinct_paths_per_ip")
                    seen = (f" · 샘플 로그에서 IP {o['ip']} 가 서로 다른 객체 {o['paths']}개 조회" if o and o["paths"] > 1
                            else "")
                    findings.append(("높음", "G4", key, f"G4: 인증 없음 · 데이터 분류 {cls} — 다른 사용자 ID 로 조회 가능(BOLA){seen}"))
                if any(h in prop(svc, "sesac:api:path") for h in ("/health", "/status", "/healthz")):
                    findings.append(("낮음", "G3", key, "G3: 헬스 경로 외부 노출 — 내부망·모니터링 대역으로 제한 권고"))

    # ── 빌드 재현성·EOL (build_check) ───────────────────────────────────────
    for f in build:
        findings.append((f["severity"], f["criterion"], f["target"], f"{f['criterion']}: {f['evidence']}"))

    # ── 이미지 SBOM + Grype → S1, B3 실측 ────────────────────────────────────
    # S1 은 원본 패키지 기준으로 묶고 CVE 는 고유 ID 로 센다 (같은 CVE 가 바이너리 패키지·이미지마다 반복되므로)
    image_rows, all_cves = [], defaultdict(set)
    groups = defaultdict(lambda: {"Critical": set(), "High": set(), "fixed": set(),
                                  "pkgs": set(), "images": set(), "versions": set()})
    for stem, (label, desc) in IMAGES.items():
        sbom = load(out / f"sbom-{stem}.cdx.json")
        grype = load(out / f"grype-{stem}.json")
        if not sbom:
            image_rows.append((label, desc, "-", "-", "(SBOM 없음)", ""))
            continue
        comps = [c for c in sbom.get("components", []) if c.get("type") != "file"]
        py = {c["name"].lower() for c in comps if (c.get("purl") or "").startswith("pkg:pypi/")}
        cves = grype_cves(grype, label, groups) if grype else {}
        for s, ids in cves.items():
            all_cves[s] |= ids
        note = ""
        if stem == "airflow":
            missing = [p for p in ("scikit-learn", "pandas") if p not in py]
            # B3 교차 확인: compose 가 시작할 때 설치하는 패키지가 이미지 SBOM 에 있는가
            note = ("B3 실측: 이미지 SBOM 에 " + ", ".join(missing) + " 없음 → 실행 중인 코드가 SBOM 밖") if missing \
                else "B3 확인: scikit-learn·pandas 가 이미지에 이미 있음"
        image_rows.append((label, desc, len(comps), len(py),
                           " · ".join(f"{s} {len(cves.get(s, ()))}" for s in ("Critical", "High", "Medium")), note))
    ranked = sorted(groups.items(), key=lambda kv: (-len(kv[1]["Critical"]), -len(kv[1]["High"]), kv[0]))
    for (src, typ), p in ranked[:S1_TOP]:
        n_fix = len(p["fixed"] & (p["Critical"] | p["High"]))
        findings.append(("높음", "S1", f"sbom:{src}",
                         f"S1: Critical {len(p['Critical'])} · High {len(p['High'])} (고유 CVE, 수정 버전 있음 {n_fix}) · "
                         f"{typ} {', '.join(sorted(p['versions']))} · 부품 {', '.join(sorted(p['pkgs']))} · "
                         f"이미지 {', '.join(sorted(p['images']))}"))
    rest = ranked[S1_TOP:]
    if rest:
        rc = set().union(*(p["Critical"] for _, p in rest))
        rh = set().union(*(p["High"] for _, p in rest))
        findings.append(("높음", "S1", f"sbom:(그 외 {len(rest)}개 원본 패키지)",
                         f"S1: Critical {len(rc)} · High {len(rh)} (고유 CVE) — 전체 목록은 grype-*.json"))

    findings.sort(key=lambda f: (SEV_ORDER[f[0]], f[1], "그 외" in f[2], f[2]))
    n_sev = Counter(f[0] for f in findings)
    n_crit = Counter(f[1] for f in findings)

    # ── 출력 ─────────────────────────────────────────────────────────────────
    P = print
    P(f"# 공급망 보안 보고서 (자동) — {now:%Y-%m-%d}\n")
    P(f"**대상**: customs-api-pipeline (커밋 `{commit}`) · **생성**: `supply-chain/run.sh` → `report.py` · "
      f"**생성 시각**: {now:%Y-%m-%d %H:%M} KST")
    if tools:
        P(f"**도구**: syft {tools.get('syft', '?')} · grype {tools.get('grype', '?')} (취약점 DB {tools.get('grype_db', '?')}) · "
          f"CycloneDX {bom['specVersion'] if bom else '?'}")
    P("\n---\n\n## 요약\n")
    P("| 구분 | 내용 |\n|---|---|")
    active = sum(prop(s, "sesac:baseline:decision") == "active" for s in ops)
    P(f"| API 자산 (API-BOM) | 기준선 {len(ops)}개 (승인 {active} · 승인 대기 {len(ops) - active}) · 소스와 일치 검사: "
      f"{'통과' if '통과' in check_txt else '실패'} · 스키마 검증: {'통과' if '통과' in validate_txt else '실패'} |")
    labels = Counter(e["label"] for e in observe["endpoints"])
    P(f"| 관측 대조 (샘플 로그) | 엔드포인트 {len(observe['endpoints'])}개 — "
      + " · ".join(f"{k} {v}" for k, v in sorted(labels.items())) + " |")
    P(f"| 기준 위반 | 높음 {n_sev['높음']} · 중간 {n_sev['중간']} · 낮음 {n_sev['낮음']} ("
      + " · ".join(f"{k} {v}" for k, v in sorted(n_crit.items())) + ") |")
    P(f"| 부품 취약점 (Grype, 이미지 3개, 고유 CVE) | Critical {len(all_cves['Critical'])} · High {len(all_cves['High'])} · "
      f"Medium {len(all_cves['Medium'])} · Low {len(all_cves['Low'])} — 가장 많은 원본 패키지: "
      + ", ".join(f"{src}" for (src, _), _p in ranked[:3]) + " |")
    roots = [f[3].split(" — ", 1)[-1] for f in findings if f[1] == "S3"]
    P(f"| 근본 원인 | " + (" / ".join(roots) if roots else "-") +
      " · 의존성 버전 미고정·시작 시 설치로 SBOM 이 실제 실행 상태를 보장하지 못함 |")
    P("| 사람 결정 필요 | Airflow 3.x 이전 계획 · 의존성 고정 방식(requirements.txt + 해시 / lock) · "
      "`import-stats` 인증 추가 여부(실습용 취약점이면 예외로 문서화) |")

    P("\n---\n\n## 보안 발견사항 (자동 판정)\n")
    P("| ID | 심각도 | 기준 | 대상 | 근거 |\n|---|---|---|---|---|")
    for i, (sev, crit, target, ev) in enumerate(findings, 1):
        P(f"| AUTO-{i:03d} | {sev} | {crit} | `{target}` | {ev.replace('|', '/')} |")

    P("\n---\n\n## API 자산 (CycloneDX API-BOM)\n")
    if bom:
        app = bom["metadata"]["component"]
        link = (app.get("externalReferences") or [{}])[0].get("url", "-")
        P(f"BOM `{bom['serialNumber']}` · 애플리케이션 `{app['name']}` 버전 `{app['version']}` · "
          f"이미지 SBOM 연결(BOM-Link) `{link}`\n")
        obs = {e["endpoint_key"]: e for e in observe["endpoints"]}
        P("| API | 승인 | 인증 | 데이터 분류 | 소스 | 관측(샘플) |\n|---|---|---|---|---|---|")
        for s in ops:
            o = obs.get(s["name"])
            seen = f"{o['count']}건 · IP {len(o['ips'])}개 · " + " ".join(f"{k}:{v}" for k, v in sorted(o["status"].items())) if o else "0건"
            auth = {True: "필요", False: "없음"}.get(s.get("authenticated"), "미상")
            P(f"| `{s['name']}` | {prop(s, 'sesac:baseline:decision')} | {auth} | {s['data'][0]['classification']} | "
              f"{prop(s, 'sesac:source:location')} | {seen} |")
    else:
        P("(없음)")
    outside = [e for e in observe["endpoints"] if e["label"] != "documented"]
    if outside:
        P(f"\n**기준선 밖 호출** ({', '.join(observe['logs'])}) — 전부 4xx 면 '탐색 시도'(앱이 응답하지 않음), "
          "2xx·5xx 가 있으면 숨은 API(Shadow)\n")
        P("| 분류 | API | 건수 | 출발 IP |\n|---|---|---|---|")
        for e in outside:
            P(f"| {e['label']} | `{e['endpoint_key']}` | {e['count']} | {', '.join(e['ips'])} |")
    P(f"\n```\n{check_txt}\n{validate_txt}\n```")

    P("\n---\n\n## 이미지별 SBOM (CycloneDX 1.6, syft)\n")
    P("| 이미지 | 출처 | 부품 | 파이썬 패키지 | 취약점 (Grype) | 비고 |\n|---|---|---|---|---|---|")
    for row in image_rows:
        P("| " + " | ".join(str(c) for c in row) + " |")
    P("\n> 범위: 이 레포가 빌드하는 이미지 2개와 탐지·ETL 이 실행되는 Airflow 이미지. "
      "Kafka·ZooKeeper(confluentinc 7.6.0)는 인프라 이미지라 이번 범위에서 뺐다(B2 digest 검사만 적용).")

    P("\n---\n\n## 권고\n")
    P("| 기준 | 할 일 |\n|---|---|")
    advice = {
        "S3": "Airflow 3.x 로 이전. 당장 못 하면 2.x 최종 패치로 올리고 예외(만료일 포함)로 기록",
        "B3": "compose 의 시작 시 `pip install` 제거 → `FROM apache/airflow:<버전>` + `pip install -r requirements.txt` 로 이미지 빌드, 그 이미지로 SBOM",
        "B1": "`requirements.txt` 에 `==` 로 고정(가능하면 `--require-hashes`), Dependabot 으로 갱신",
        "B2": "`이미지:태그@sha256:<digest>` 로 고정, Dependabot docker 생태계로 갱신",
        "B4": "서비스별 `requirements.txt` 추가 → 소스 SBOM·Dependabot 이 의존성을 볼 수 있음",
        "S1": "수정 버전이 있는 부품부터 기반 이미지 갱신(slim 최신 패치)으로 해소, 남는 것은 도달성 판단 후 VEX 로 기록",
        "G4": "`import-stats` 에 인증과 객체 소유권 검사 추가 — 실습용으로 남길 거면 예외로 문서화",
        "G3": "헬스 경로는 내부 대역에서만 허용",
    }
    for crit in sorted(n_crit, key=lambda c: min(SEV_ORDER[f[0]] for f in findings if f[1] == c)):
        P(f"| {crit} | {advice.get(crit, '-')} |")

    P("\n---\n\n## 재현\n")
    P("```bash\nbash supply-chain/run.sh supply-chain/out     # Docker 필요. 결과: out/ 와 out/report.md\n```")
    P("CI: `.github/workflows/supply-chain.yml` — push·PR·매주 월요일·수동 실행. API-BOM 검사(소스↔승인 기준선, 스키마)가 "
      "실패하면 CI 실패, SBOM·취약점·빌드 재현성은 보고서로만 남긴다(차단하지 않음).")


if __name__ == "__main__":
    main()
