"""빌드 재현성 · 런타임 지원 종료 검사 — Dockerfile·docker-compose.yml 을 읽어 판정한다 (빌드하지 않음).

    python build_check.py <레포 루트> [--json out.json]

SBOM 은 "이미지에 무엇이 들어 있나"를 적는다. 그런데 빌드할 때마다 내용이 바뀌거나 컨테이너가 뜬 뒤에 패키지를
설치하면, 오늘 만든 SBOM 이 내일 도는 것과 다르다. 그래서 SBOM 과 함께 아래를 검사한다.

  B1 (중간) 버전 고정 없는 pip install        — 빌드할 때마다 다른 버전이 들어올 수 있음
  B2 (중간) 기반 이미지를 digest 없이 태그로만 지정 — 같은 태그가 다른 이미지로 바뀔 수 있음
  B3 (높음) 컨테이너 시작 시 패키지 설치          — 실제로 도는 코드가 이미지 SBOM 에 없음
  B4 (중간) 의존성 목록 파일 없음                 — 소스 SBOM 이 파이썬 의존성을 하나도 못 봄
  S3 (높음) 지원 종료(EOL) 런타임                — 보안 패치가 더 나오지 않음 (기준은 아래 EOL 표, 근거 URL 포함)
표준 라이브러리만 사용.
"""
import json
import re
import sys
from datetime import date
from pathlib import Path

# (이미지 이름 정규식, 버전 정규식, 지원 종료일, 설명, 근거)
EOL = [
    (r"^apache/airflow$", r"^2\.", date(2026, 4, 22), "Apache Airflow 2.x",
     "https://airflow.apache.org/docs/apache-airflow/stable/installation/supported-versions.html"),
    (r"^python$", r"^3\.9\b", date(2025, 10, 31), "Python 3.9", "https://devguide.python.org/versions/"),
    (r"^python$", r"^3\.10\b", date(2026, 10, 31), "Python 3.10", "https://devguide.python.org/versions/"),
]
SEVERITY = {"B1": "중간", "B2": "중간", "B3": "높음", "B4": "중간", "S3": "높음"}
NAME = {"B1": "버전 고정 없는 pip install", "B2": "기반 이미지 digest 미고정", "B3": "컨테이너 시작 시 패키지 설치",
        "B4": "의존성 목록 파일 없음", "S3": "지원 종료 런타임"}
PIP = re.compile(r"pip3? install\s+(.+?)(?:&&|;|$|\")")
MANIFESTS = ["requirements*.txt", "pyproject.toml", "Pipfile.lock", "poetry.lock", "uv.lock", "setup.py"]


def unpinned(pkgs_text):
    out = []
    for tok in pkgs_text.split():
        if tok.startswith("-"):
            continue
        tok = tok.strip("'\"")
        if not tok or "==" in tok or tok.startswith((".", "/")):
            continue
        out.append(tok)
    return out


def image_ref(ref):
    """'apache/airflow:2.9.1' → ('apache/airflow', '2.9.1', digest 여부)"""
    has_digest = "@sha256:" in ref
    ref = ref.split("@", 1)[0]
    name, _, tag = ref.rpartition(":") if ":" in ref.split("/")[-1] else (ref, "", "latest")
    return name, tag, has_digest


def check(root):
    root = Path(root)
    findings = []
    add = lambda c, target, evidence: findings.append(
        {"criterion": c, "severity": SEVERITY[c], "name": NAME[c], "target": target, "evidence": evidence})
    images = []   # (이름, 태그, digest, 위치)

    for df in sorted(root.glob("**/Dockerfile")):
        if ".git" in df.parts:
            continue
        rel = df.relative_to(root).as_posix()
        for no, line in enumerate(df.read_text(encoding="utf-8").splitlines(), 1):
            if m := re.match(r"\s*FROM\s+(\S+)", line, re.I):
                images.append((*image_ref(m.group(1)), f"{rel}:{no}"))
            if re.match(r"\s*RUN\b", line, re.I):
                for pm in PIP.finditer(line):
                    if pk := unpinned(pm.group(1)):
                        add("B1", rel, f"{rel}:{no} `{line.strip()}` — 버전 없음: {', '.join(pk)}")

    compose = root / "docker-compose.yml"
    if compose.exists():
        service = None
        for no, line in enumerate(compose.read_text(encoding="utf-8").splitlines(), 1):
            if m := re.match(r"^  ([\w-]+):\s*$", line):
                service = m.group(1)
            if m := re.match(r"\s+image:\s*(\S+)", line):
                images.append((*image_ref(m.group(1)), f"docker-compose.yml:{no} ({service})"))
            if re.match(r"\s+command:", line) and "pip install" in line:
                pkgs = [p for pm in PIP.finditer(line) for p in unpinned(pm.group(1))]
                add("B3", f"compose:{service}", f"docker-compose.yml:{no} 시작할 때마다 `pip install` — "
                    f"이미지 SBOM 에 없는 패키지가 실행됨 ({', '.join(pkgs) or '고정됨'})")
                if pkgs:
                    add("B1", f"compose:{service}", f"docker-compose.yml:{no} — 버전 없음: {', '.join(pkgs)}")

    for name, tag, has_digest, where in images:
        if not has_digest:
            add("B2", f"image:{name}:{tag}", f"{where} — `{name}:{tag}` digest(@sha256) 없음")
        for rx_name, rx_ver, eol, label, src in EOL:
            if re.search(rx_name, name) and re.search(rx_ver, tag) and eol <= date.today():
                add("S3", f"image:{name}:{tag}", f"{where} — {label} 지원 종료 {eol.isoformat()} · 근거 {src}")

    found = [p for pat in MANIFESTS for p in root.glob(f"**/{pat}") if ".git" not in p.parts]
    if not found:
        add("B4", "repo", "requirements.txt·pyproject.toml·lock 파일 없음 — 파이썬 의존성이 Dockerfile·compose 안에만 있음")

    # 같은 이미지가 여러 서비스에서 쓰이면 한 번만 (근거는 합침)
    merged = {}
    for f in findings:
        k = (f["criterion"], f["target"])
        if k in merged:
            merged[k]["evidence"] += " / " + f["evidence"]
        else:
            merged[k] = f
    order = {"높음": 0, "중간": 1, "낮음": 2}
    return sorted(merged.values(), key=lambda f: (order[f["severity"]], f["criterion"], f["target"]))


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    findings = check(sys.argv[1])
    for f in findings:
        print(f"[{f['severity']}] {f['criterion']} {f['target']}: {f['evidence']}")
    print(f"[build_check] {len(findings)}건 (높음 {sum(f['severity'] == '높음' for f in findings)})")
    if "--json" in sys.argv:
        Path(sys.argv[sys.argv.index("--json") + 1]).write_text(
            json.dumps(findings, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
