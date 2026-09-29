"""Compare accepted-event throughput in matching Go benchmark logs."""
import argparse
import re
import statistics
from pathlib import Path


def read_samples(path):
    text = Path(path).read_text(encoding="utf-8-sig")
    if not re.search(r"^PASS\s*$", text, re.MULTILINE) or re.search(r"^FAIL|^--- FAIL", text, re.MULTILINE):
        raise ValueError(f"{path}: benchmarks did not pass")
    samples = {}
    pattern = r"^(BenchmarkPublishBatch_EndToEnd_Matrix/\S+)\s+\d+\s+\S+\s+ns/op.*?\s(\S+)\s+events/s"
    for name, throughput in re.findall(pattern, text, re.MULTILINE):
        samples.setdefault(name, []).append(float(throughput))
    if not samples or any(len(values) < 3 for values in samples.values()):
        raise ValueError(f"{path}: need at least three successful samples per configuration")
    return samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before")
    parser.add_argument("after")
    parser.add_argument("--max-regression-percent", type=float, default=10)
    args = parser.parse_args()
    before, after = read_samples(args.before), read_samples(args.after)
    if before.keys() != after.keys():
        raise ValueError("before and after must contain exactly the same benchmark configurations")
    failed = False
    for name in sorted(before):
        old, new = statistics.median(before[name]), statistics.median(after[name])
        change = (new / old - 1) * 100
        failed |= change < -args.max_regression_percent
        print(f"{name}: {old:,.0f} -> {new:,.0f} events/s ({change:+.2f}%; medians of {len(before[name])}/{len(after[name])} samples)")
    print(f"{'FAIL' if failed else 'PASS'}: maximum allowed median regression {args.max_regression_percent:g}%")
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
