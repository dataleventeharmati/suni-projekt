from __future__ import annotations

import json
from pathlib import Path

import yaml
import pyarrow.parquet as pq

def fmt_bytes(n: int) -> str:
    for unit in ["B","KB","MB","GB"]:
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"

def parquet_shape(path: Path) -> tuple[int,int]:
    t = pq.read_table(path)
    return t.num_rows, t.num_columns

def main() -> None:
    cfg = yaml.safe_load(Path("config.yaml").read_text(encoding="utf-8")) or {}
    dq_gate = cfg.get("dq_gate", {})

    bronze_xlsx = Path("data/bronze/TCH_activity_report_2026-02-15_03-13-30.xlsx")
    bronze_pq = Path("data/bronze/tch_repairs_raw.parquet")
    silver_pq = Path("data/silver/tch_repairs_cleaned.parquet")
    gold_pq = Path("data/gold/kpi_monthly.parquet")
    dq_json = Path("reports/data_quality.json")

    print("=== ARTIFACTS SUMMARY ===")

    for p in [bronze_xlsx, bronze_pq, silver_pq, gold_pq, dq_json]:
        if p.exists():
            size = fmt_bytes(p.stat().st_size)
            extra = ""
            if p.suffix == ".parquet":
                r, c = parquet_shape(p)
                extra = f" | shape={r}x{c}"
            print(f"- {p.as_posix()} | {size}{extra}")
        else:
            print(f"- {p.as_posix()} | MISSING")

    if dq_json.exists():
        dq = json.loads(dq_json.read_text(encoding="utf-8"))
        print("\n=== DQ REPORT (high-level) ===")
        print(f"- total_rows: {dq.get('total_rows')}")
        print(f"- duplicate_primary_keys: {dq.get('duplicate_primary_keys')}")
        print(f"- dedupe_dropped_rows: {dq.get('dedupe_dropped_rows')}")
        cn = dq.get("critical_nulls", {}) or {}
        print(f"- critical_nulls.RMA_DATE: {cn.get('RMA_DATE')}")
        print(f"- critical_nulls.REPAIR_END_DATE: {cn.get('REPAIR_END_DATE')}")
        print(f"- date_nulls_after_parse.INVOICE_DATE: {(dq.get('date_nulls_after_parse') or {}).get('INVOICE_DATE')}")

    print("\n=== DQ GATE CONFIG ===")
    print(f"- mode: {dq_gate.get('mode')}")
    print(f"- duplicate_primary_keys_max: {dq_gate.get('duplicate_primary_keys_max')}")
    print(f"- RMA_DATE_null_rate_max: {dq_gate.get('RMA_DATE_null_rate_max')}")

if __name__ == "__main__":


    # ---- write summary file ----
    import io, sys
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    main()
    sys.stdout = old_stdout

    Path("reports").mkdir(exist_ok=True)
    Path("reports/artifacts_summary.txt").write_text(buf.getvalue())

