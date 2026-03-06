import json
import sys
from pathlib import Path

import yaml

CONFIG_PATH = Path("config.yaml")
DQ_PATH = Path("reports/data_quality.json")


def load_thresholds() -> dict:
    if not CONFIG_PATH.exists():
        raise SystemExit(f"Missing config file: {CONFIG_PATH}")

    cfg = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    dq = cfg.get("dq_gate") or {}

    # defaults (safe)
    return {
        "mode": str(dq.get("mode", "fail")).strip().lower(),  # fail | warn
        "duplicate_primary_keys_max": int(dq.get("duplicate_primary_keys_max", 0)),
        "RMA_DATE_null_rate_max": float(dq.get("RMA_DATE_null_rate_max", 0.05)),
    }


def fail_or_warn(mode: str, msg: str) -> int:
    mode = (mode or "fail").lower()
    if mode == "warn":
        print(f"WARNING: {msg}")
        return 0
    print(f"FAIL: {msg}")
    return 1


def main() -> None:
    t = load_thresholds()
    mode = t["mode"]
    if mode not in ("fail", "warn"):
        raise SystemExit(f"Invalid dq_gate.mode={mode!r}. Use 'fail' or 'warn' in config.yaml")

    if not DQ_PATH.exists():
        print("DQ report missing.")
        sys.exit(1)

    dq = json.loads(DQ_PATH.read_text(encoding="utf-8"))

    exit_code = 0

    # 1) Duplicate PK threshold
    dup = int(dq.get("duplicate_primary_keys", 0))
    if dup > t["duplicate_primary_keys_max"]:
        exit_code = max(exit_code, fail_or_warn(mode, f"duplicate_primary_keys = {dup} (max {t['duplicate_primary_keys_max']})"))

    # 2) RMA_DATE null-rate threshold (based on DQ report)
    total = int(dq.get("total_rows", 1)) or 1
    rma_nulls = int(dq.get("critical_nulls", {}).get("RMA_DATE", 0))
    null_rate = rma_nulls / total

    if null_rate > t["RMA_DATE_null_rate_max"]:
        exit_code = max(exit_code, fail_or_warn(mode, f"RMA_DATE null_rate = {null_rate:.4f} (max {t['RMA_DATE_null_rate_max']:.4f})"))

    if exit_code == 0:
        print("DQ GATE PASSED")
    else:
        print("DQ GATE FAILED")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
