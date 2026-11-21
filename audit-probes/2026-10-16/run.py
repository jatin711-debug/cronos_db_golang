"""Run the 2026-10-16 audit reproductions without modifying package sources."""

import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    here = Path(__file__).resolve().parent
    root = here.parents[2]
    target = root / "internal/api/audit_20261016_test.go"
    if target.exists():
        raise SystemExit(f"Refusing to overlay an existing file: {target}")
    env = dict(os.environ, GOARCH="amd64", CGO_ENABLED="0")
    with tempfile.TemporaryDirectory(prefix="cronos-audit-20261016-") as tmp:
        overlay = Path(tmp) / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(target): str(here / "api.go.txt")}}), encoding="utf-8")
        command = ["go", "test", "-overlay", str(overlay), "-count=1", "-timeout=90s", "-v", "-run", "^TestAudit20261016", "./internal/api"]
        print("These probes assert safety invariants and are expected to FAIL before the findings are fixed.", flush=True)
        result = subprocess.run(command, cwd=root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        evidence = here / "evidence.txt"
        evidence.write_text(result.stdout, encoding="utf-8")
        print(result.stdout, end="")
        print(f"Evidence: {evidence}")
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
