from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BRONZE_DIR = Path("data/bronze")
INPUT_XLSX = next(BRONZE_DIR.glob("*.xlsx"))
OUT_PARQUET = BRONZE_DIR / "tch_repairs_raw.parquet"

def main():
    print(f"Reading Excel: {INPUT_XLSX}")
    df = pd.read_excel(INPUT_XLSX, engine="openpyxl")

    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    table = pa.Table.from_pandas(df)
    pq.write_table(table, OUT_PARQUET)

    print(f"OK: wrote {OUT_PARQUET}")

if __name__ == "__main__":
    main()
