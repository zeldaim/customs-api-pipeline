"""관세 API 소스(Flask) → API 기준선 → CycloneDX 1.6 API-BOM → OpenAPI 허용 목록 → 관측 로그 대조.

zeldaim/api-security-supply-chain 의 standardize/api-bom/source_to_baseline.py 를 Flask 앱에 맞게 옮긴 것.
형식은 그대로 둔다: baseline.csv 열·decision 값, CycloneDX 1.6 services 구조, sesac:* 속성 이름.
달라진 점: 라우트 추출(PHP 라우터 정규식 → Flask 데코레이터를 ast 로), 관측 로그(Apache → API 서버 JSON 로그),
         check(소스와 승인된 기준선이 어긋나면 CI 실패) 추가.

    python api_bom.py extract   <app.py> [--prev baseline.csv]           > baseline.csv
    python api_bom.py check     <app.py> <baseline.csv>                    # 승인 안 된 라우트·사라진 라우트 → 종료코드 1
    python api_bom.py cyclonedx <baseline.csv> <app.py> [--sbom image.cdx.json] > api-bom.cdx.json
    python api_bom.py validate  <api-bom.cdx.json>
    python api_bom.py openapi   <api-bom.cdx.json>                          > openapi.from-bom.json
    python api_bom.py observe   <baseline.csv> <access.jsonl> [--json out.json]

decision: active(승인) / deprecated(폐기 예정 → 호출되면 zombie) / exclude·review(미승인 → 호출되면 shadow)
표준 라이브러리만 사용 (validate 만 cyclonedx-python-lib[json-validation]).
"""
import ast
import csv
import json
import os
import re
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

CSV_FIELDS = ["endpoint_key", "kind", "auth", "frontend", "in_spec", "source", "decision", "owner", "note"]
DECISIONS = {"active", "deprecated", "exclude", "review"}
SPEC_VERSION = "1.6"
TOOL_VERSION = "1.0"
APP_NAME = "customs-api"
OBSERVED_AS = {"active": "documented", "deprecated": "zombie", "review": "shadow", "exclude": "shadow"}

# 경로 → 데이터 분류 (위에서부터 먼저 맞는 것)
DATA_CLASS = [
    (r"/import-stats\b", "customer-transaction"),   # 사용자별 수입 실적 — BOLA 대상
    (r"/declaration\b", "customer-transaction"),
    (r"/health\b", "system"),
    (r".*", "public"),
]


def normalize(path):
    """Flask <code>, <int:id> 와 OpenAPI {x} 를 모두 {id} 로 — 관측 로그 정규화와 같은 규칙."""
    path = path.split("?", 1)[0]
    path = re.sub(r"<(?:[^:>]+:)?[^>]+>", "{id}", path)
    path = re.sub(r"\{[^}]+\}", "{id}", path)
    path = re.sub(r"/(user_\d+|\d+(?:\.\d+)?)(?=/|$)", "/{id}", path)   # user_3, 8471.30
    return path.rstrip("/") if len(path) > 1 else path


# ── 1) extract ───────────────────────────────────────────────────────────────
def routes_from_flask(app_py):
    """@app.route('/x', methods=[...]) 를 구문 트리로 읽는다 (주석·문자열 속 가짜 라우트에 속지 않음)."""
    tree = ast.parse(Path(app_py).read_text(encoding="utf-8"))
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for deco in node.decorator_list:
            if not (isinstance(deco, ast.Call) and isinstance(deco.func, ast.Attribute)
                    and deco.func.attr in ("route", "get", "post", "put", "patch", "delete")):
                continue
            if not deco.args or not isinstance(deco.args[0], ast.Constant):
                continue
            path = deco.args[0].value
            if deco.func.attr == "route":
                methods = ["GET"]
                for kw in deco.keywords:
                    if kw.arg == "methods" and isinstance(kw.value, (ast.List, ast.Tuple)):
                        methods = [e.value.upper() for e in kw.value.elts if isinstance(e, ast.Constant)]
            else:
                methods = [deco.func.attr.upper()]
            for m in methods:
                key = f"{m} {normalize(path)}"
                found[key] = {"source": f"{Path(app_py).name}:{deco.lineno}", "func": node.name,
                              "doc": (ast.get_docstring(node) or "").splitlines()[0] if ast.get_docstring(node) else ""}
    return found


def read_baseline(path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        if r["decision"] not in DECISIONS:
            sys.exit(f"decision 값이 잘못됨: {r['endpoint_key']} → '{r['decision']}' ({'/'.join(sorted(DECISIONS))})")
    return rows


def cmd_extract(args):
    app_py = args[0]
    prev = {r["endpoint_key"]: r for r in read_baseline(args[args.index("--prev") + 1])} if "--prev" in args else {}
    w = csv.DictWriter(sys.stdout, fieldnames=CSV_FIELDS, lineterminator="\n")
    w.writeheader()
    for key, info in sorted(routes_from_flask(app_py).items(), key=lambda kv: kv[0].split(" ", 1)[1]):
        p = prev.get(key)
        w.writerow({
            "endpoint_key": key, "kind": "api",
            "auth": p["auth"] if p else "unknown", "frontend": "n", "in_spec": "",
            "source": info["source"],
            # 이전에 사람이 정한 결정은 이어받고, 새 라우트는 review (승인 전에는 shadow 취급)
            "decision": p["decision"] if p else "review",
            "owner": p["owner"] if p else "",
            "note": p["note"] if p else f"새 라우트 — 담당자 승인 필요 ({info['func']}: {info['doc']})",
        })


def cmd_check(args):
    """소스의 라우트와 승인된 기준선을 비교. CI 게이트 — 승인 없이 API 가 늘거나 기준선에만 남은 API 가 있으면 실패."""
    code = routes_from_flask(args[0])
    base = {r["endpoint_key"]: r for r in read_baseline(args[1])}
    new = sorted(set(code) - set(base))
    gone = sorted(k for k in set(base) - set(code) if base[k]["decision"] != "exclude")
    pending = sorted(k for k, r in base.items() if r["decision"] == "review")
    print(f"[check] 소스 라우트 {len(code)}개 · 기준선 {len(base)}개")
    for k in new:
        print(f"  [새 API] {k} ({code[k]['source']}) — baseline.csv 에 추가하고 decision 을 정할 것")
    for k in gone:
        print(f"  [사라진 API] {k} — 소스에 없음. 지웠다면 baseline.csv 에서 exclude 로 바꿀 것")
    for k in pending:
        print(f"  [승인 대기] {k} — decision=review")
    if new or gone or pending:
        sys.exit(1)
    print("[check] 통과 — 모든 라우트가 승인된 기준선과 일치")


# ── 2) observe ───────────────────────────────────────────────────────────────
def cmd_observe(args):
    rows = read_baseline(args[0])
    out_json = args[args.index("--json") + 1] if "--json" in args else None
    logs = [a for a in args[1:] if a != "--json" and a != out_json]
    status_of = {r["endpoint_key"]: r["decision"] for r in rows}
    hits, ips = defaultdict(Counter), defaultdict(set)
    raw = defaultdict(lambda: defaultdict(set))   # endpoint_key → IP → 실제 경로 (객체 ID 다양성 = BOLA 신호)
    for log in logs:
        with open(log, encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = f"{e['method'].upper()} {normalize(e['path'])}"
                hits[key][f"{str(e['status_code'])[0]}xx"] += 1
                ips[key].add(e.get("src_ip", ""))
                raw[key][e.get("src_ip", "")].add(e["path"])

    def label(key):
        d = status_of.get(key)
        if d == "active":
            return "documented"
        if d == "deprecated":
            return "ZOMBIE"
        if d in ("review", "exclude"):
            return "SHADOW(코드에 있음)"
        # 코드에도 없는 경로: 앱이 응답(2xx/3xx/5xx)했다면 숨은 경로, 전부 4xx 면 스캔·탐색 시도
        return "SHADOW(코드 밖)" if sum(v for k, v in hits[key].items() if k != "4xx") else "probe(4xx만)"

    order = {"ZOMBIE": 0, "SHADOW(코드 밖)": 1, "SHADOW(코드에 있음)": 2, "probe(4xx만)": 3, "documented": 4}
    result = []
    print(f"{'분류':<18} {'건수':>6} {'IP':>4}  상태코드            endpoint_key")
    for key in sorted(hits, key=lambda k: (order[label(k)], -sum(hits[k].values()))):
        codes = " ".join(f"{k}:{v}" for k, v in sorted(hits[key].items()))
        print(f"{label(key):<18} {sum(hits[key].values()):>6} {len(ips[key]):>4}  {codes:<19} {key}")
        top_ip, top_paths = max(raw[key].items(), key=lambda kv: len(kv[1]))
        result.append({"endpoint_key": key, "label": label(key), "count": sum(hits[key].values()),
                       "ips": sorted(ips[key]), "status": dict(hits[key]),
                       "max_distinct_paths_per_ip": {"ip": top_ip, "paths": len(top_paths)}})
    unseen = [r["endpoint_key"] for r in rows if r["decision"] == "active" and r["endpoint_key"] not in hits]
    if unseen:
        print(f"\n[참고] 기준선(active)인데 관측 0건 {len(unseen)}개 — 미사용 후보: " + ", ".join(unseen))
    if out_json:
        Path(out_json).write_text(json.dumps({"logs": [Path(l).name for l in logs], "endpoints": result,
                                              "unseen_active": unseen}, ensure_ascii=False, indent=2),
                                  encoding="utf-8")


# ── 3) cyclonedx ─────────────────────────────────────────────────────────────
def operation_service(row, base_url, owner):
    method, path = row["endpoint_key"].split(" ", 1)
    svc = {
        "bom-ref": f"op:{row['endpoint_key']}",
        "provider": {"name": row["owner"] or owner},
        "name": row["endpoint_key"],
        "description": row["note"],
        "endpoints": [base_url + quote(path, safe="/")],
        "x-trust-boundary": True,
        "trustZone": "dmz",
        "data": [{"flow": "outbound" if method == "GET" else "bi-directional",
                  "classification": next(c for rx, c in DATA_CLASS if re.search(rx, path))}],
    }
    if row["auth"] in ("required", "none"):
        svc["authenticated"] = row["auth"] == "required"
    props = [("sesac:api:endpoint_key", row["endpoint_key"]), ("sesac:api:method", method),
             ("sesac:api:path", path), ("sesac:api:kind", row["kind"]),
             ("sesac:baseline:decision", row["decision"]),
             ("sesac:baseline:observed-as", OBSERVED_AS[row["decision"]]),
             ("sesac:source:location", row["source"])]
    svc["properties"] = [{"name": n, "value": v} for n, v in props]
    return svc


def git_version(src):
    if os.environ.get("API_BOM_SRC_VERSION"):
        return os.environ["API_BOM_SRC_VERSION"]
    try:
        return subprocess.run(["git", "-C", str(Path(src).parent), "log", "-1", "--format=%h", "--", Path(src).name],
                              capture_output=True, text=True, check=True).stdout.strip() or "unknown"
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def cmd_cyclonedx(args):
    rows = read_baseline(args[0])
    src = args[1]
    opt = lambda name, default: args[args.index(name) + 1] if name in args else default
    base_url = opt("--base-url", "http://customs-api:8080").rstrip("/")
    owner = opt("--owner", "관세API팀")

    app = {"type": "application", "bom-ref": f"app:{APP_NAME}", "name": APP_NAME, "version": git_version(src),
           "description": "모의 관세 API (Flask, api/app.py)"}
    sbom_path = opt("--sbom", None)
    if sbom_path:
        # BOM-Link: 이 API 를 실행하는 이미지 SBOM 을 가리킨다 (API-BOM ↔ SBOM 연결)
        sbom = json.loads(Path(sbom_path).read_text(encoding="utf-8"))
        link = f"urn:cdx:{sbom['serialNumber'].removeprefix('urn:uuid:')}/{sbom.get('version', 1)}"
        ref = (sbom.get("metadata", {}).get("component") or {}).get("bom-ref")
        app["externalReferences"] = [{"type": "bom", "url": link + (f"#{ref}" if ref else ""),
                                      "comment": f"이 서비스를 실행하는 이미지의 SBOM ({Path(sbom_path).name})"}]

    group = {"bom-ref": f"svc:{APP_NAME}-v1", "provider": {"name": owner}, "group": APP_NAME,
             "name": f"{APP_NAME}-v1", "endpoints": [base_url + "/api/v1"], "x-trust-boundary": True,
             "trustZone": "dmz", "services": [operation_service(r, base_url, owner) for r in rows]}
    pending = sum(r["decision"] == "review" for r in rows)
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    built_at = datetime.fromtimestamp(int(epoch), timezone.utc) if epoch else datetime.now(timezone.utc)
    bom = {
        "$schema": f"http://cyclonedx.org/schema/bom-{SPEC_VERSION}.schema.json",
        "bomFormat": "CycloneDX", "specVersion": SPEC_VERSION, "serialNumber": None, "version": 1,
        "metadata": {
            "timestamp": built_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "lifecycles": [{"phase": "pre-build"}],
            "tools": {"components": [{"type": "application", "name": "api_bom.py", "version": TOOL_VERSION}]},
            "component": app,
            "properties": [{"name": "sesac:baseline:csv", "value": Path(args[0]).name},
                           {"name": "sesac:baseline:operations", "value": str(len(rows))},
                           {"name": "sesac:baseline:review-pending", "value": str(pending)}],
        },
        "services": [group],
        "dependencies": [{"ref": app["bom-ref"]}, {"ref": group["bom-ref"], "dependsOn": [app["bom-ref"]]}],
    }
    # SOURCE_DATE_EPOCH 가 있으면 시각·serialNumber 를 내용에서 결정 → 같은 입력이면 같은 BOM (재현 가능)
    serial = uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(bom, ensure_ascii=False, sort_keys=True)) if epoch else uuid.uuid4()
    bom["serialNumber"] = f"urn:uuid:{serial}"
    print(json.dumps(bom, ensure_ascii=False, indent=2))


# ── 4) validate ──────────────────────────────────────────────────────────────
def cmd_validate(args):
    try:
        from cyclonedx.schema import SchemaVersion
        from cyclonedx.validation.json import JsonStrictValidator
    except ImportError:
        sys.exit('검증 라이브러리가 없습니다: python -m pip install "cyclonedx-python-lib[json-validation]"')
    text = Path(args[0]).read_text(encoding="utf-8")
    version = json.loads(text).get("specVersion", SPEC_VERSION)
    error = JsonStrictValidator(SchemaVersion.from_version(version)).validate_str(text)
    if error:
        detail = getattr(error.data, "message", str(error).splitlines()[0])
        where = "/".join(map(str, getattr(error.data, "absolute_path", [])))
        sys.exit(f"[실패] CycloneDX {version} 스키마 위반: {detail} (위치: {where or '-'})")
    print(f"[통과] CycloneDX {version} 공식 JSON 스키마(strict) 검증 통과: {args[0]}")


# ── 5) openapi ───────────────────────────────────────────────────────────────
def cmd_openapi(args):
    """BOM → OpenAPI 3.0 허용 목록 (decision=active 만). Shadow 룰 생성기·인벤토리의 입력."""
    bom = json.loads(Path(args[0]).read_text(encoding="utf-8"))
    prop = lambda svc, name: next((p["value"] for p in svc.get("properties", []) if p["name"] == name), None)
    paths, skipped = {}, Counter()
    for group in bom.get("services", []):
        for svc in group.get("services", []):
            if prop(svc, "sesac:baseline:decision") != "active":
                skipped[prop(svc, "sesac:baseline:decision")] += 1
                continue
            op = {"summary": svc["name"], "x-bom-ref": svc["bom-ref"],
                  "responses": {"default": {"description": "see CycloneDX API-BOM"}}}
            if svc.get("authenticated"):
                op["security"] = [{"bearerAuth": []}]
            paths.setdefault(prop(svc, "sesac:api:path"), {})[prop(svc, "sesac:api:method").lower()] = op
    app = bom["metadata"]["component"]
    print(json.dumps({
        "openapi": "3.0.3",
        "info": {"title": f"{app['name']} — CycloneDX API-BOM 에서 생성한 허용 목록", "version": app.get("version", "unknown"),
                 "description": f"원본 BOM {bom['serialNumber']}. 직접 고치지 말고 baseline.csv 를 고칠 것."},
        "servers": [{"url": "/"}], "paths": dict(sorted(paths.items())),
        "components": {"securitySchemes": {"bearerAuth": {"type": "http", "scheme": "bearer"}}},
    }, ensure_ascii=False, indent=2))
    print(f"[openapi] 허용 {sum(len(v) for v in paths.values())}개 operation · 제외 "
          + (", ".join(f"{k} {v}" for k, v in sorted(skipped.items())) or "0"), file=sys.stderr)


def main():
    sys.stdout.reconfigure(encoding="utf-8", newline="\n")
    sys.stderr.reconfigure(encoding="utf-8")
    commands = {"extract": cmd_extract, "check": cmd_check, "observe": cmd_observe,
                "cyclonedx": cmd_cyclonedx, "validate": cmd_validate, "openapi": cmd_openapi}
    if len(sys.argv) < 3 or sys.argv[1] not in commands:
        sys.exit(__doc__)
    commands[sys.argv[1]](sys.argv[2:])


if __name__ == "__main__":
    main()
