"""Run isolated production-audit probes without modifying repository Go files."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race", action="store_true", help="Run the delivery ownership race probe (requires cgo).")
    args = parser.parse_args()
    probe_dir = Path(__file__).resolve().parent
    root = probe_dir.parent.parent
    replacements = {}
    packages = []
    for source in sorted(probe_dir.glob("*.go.txt")):
        name = source.name.removesuffix(".go.txt")
        package = Path("pkg/client") if name == "client" else Path("internal") / name
        target = root / package / "production_audit_test.go"
        if not target.exists():
            replacements[str(target)] = str(source)
        packages.append("./" + package.as_posix())

    env = os.environ.copy()
    env["GOARCH"] = "amd64"
    env["CGO_ENABLED"] = "1" if args.race else "0"
    with tempfile.TemporaryDirectory(prefix="cronos-production-audit-") as temp:
        overlay = Path(temp) / "overlay.json"
        overlay.write_text(json.dumps({"Replace": replacements}), encoding="utf-8")
        command = ["go", "test", "-overlay", str(overlay), "-count=1", "-timeout=30s"]
        if args.race:
            command += ["-race", "-run", "^TestAuditWorkerOwnsDispatchSlice$", "./internal/delivery"]
        else:
            # The race probe has no functional assertion; run it separately with -race.
            pattern = "^TestAudit(Snapshot|Promotion|Hydrator|Delayed|Zero|Quorum|Replay|Negative|DLQ|Unordered|Wrong|Deployment|Consumer|Cannot|Prepare)"
            command += ["-run", pattern, *packages]
        print("These probes assert desired production behavior. Failures are expected at the audited commit.", flush=True)
        return subprocess.run(command, cwd=root, env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
