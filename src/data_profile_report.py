from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

INFILE = Path("data/silver/tch_repairs_cleaned.parquet")
OUTFILE = Path("reports/data_profile.md")

def main() -> None:
    OUTFILE.parent.mkdir(parents=True, exist_ok=True)

    if not INFILE.exists():
        raise SystemExit(f"Missing input file: {INFILE}")

    df = pq.read_table(INFILE).to_pandas()

    row_count, col_count = df.shape
    dtypes = df.dtypes.astype(str).reset_index()
    dtypes.columns = ["column_name", "dtype"]

    nulls = df.isna().sum().sort_values(ascending=False).reset_index()
    nulls.columns = ["column_name", "null_count"]
    nulls = nulls[nulls["null_count"] > 0].head(20)

    lines = []
    lines.append("# Data Profile Report")
    lines.append("")
    lines.append(f"- Source: `{INFILE.as_posix()}`")
    lines.append(f"- Rows: **{row_count}**")
    lines.append(f"- Columns: **{col_count}**")
    lines.append("")

    lines.append("## Columns and Types")
    lines.append("")
    lines.append("| column_name | dtype |")
    lines.append("|---|---|")
    for _, row in dtypes.iterrows():
        lines.append(f"| {row['column_name']} | {row['dtype']} |")
    lines.append("")

    lines.append("## Top Null Columns")
    lines.append("")
    lines.append("| column_name | null_count |")
    lines.append("|---|---:|")
    if len(nulls) == 0:
        lines.append("| none | 0 |")
    else:
        for _, row in nulls.iterrows():
            lines.append(f"| {row['column_name']} | {int(row['null_count'])} |")

    OUTFILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: wrote {OUTFILE}")

if __name__ == "__main__":
    main()
