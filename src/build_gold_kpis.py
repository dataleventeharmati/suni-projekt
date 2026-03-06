from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

SILVER = Path("data/silver/tch_repairs_cleaned.parquet")
GOLD_DIR = Path("data/gold")
OUT = GOLD_DIR / "kpi_monthly.parquet"
OUT_CSV = GOLD_DIR / "kpi_monthly.csv"

DATE_COL = "RECEIVED_DATE"

def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    df = pq.read_table(SILVER).to_pandas()

    # Parse date
    if DATE_COL in df.columns:
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")
    else:
        raise SystemExit(f"Missing {DATE_COL}")

    df["year_month"] = df[DATE_COL].dt.to_period("M").astype(str)

    # TAT
    if "REPAIR_END_DATE" in df.columns:
        df["REPAIR_END_DATE"] = pd.to_datetime(df["REPAIR_END_DATE"], errors="coerce")
        df["TAT_days"] = (df["REPAIR_END_DATE"] - df[DATE_COL]).dt.days
    else:
        df["TAT_days"] = pd.NA

    # Invoice + flags (no groupby.apply; faster and warning-free)
    df["_inv_amt"] = pd.to_numeric(df.get("INVOICE_AMOUNT"), errors="coerce")
    df["_inv_present"] = df["_inv_amt"].notna().astype(int)

    tat_num = pd.to_numeric(df["TAT_days"], errors="coerce")
    df["_tat_60plus"] = (tat_num > 60).astype(int)

    g = df.groupby("year_month", dropna=False)

    out = pd.DataFrame({
        "repairs": g.size(),
        "tat_p50": g["TAT_days"].median(),
        "tat_p90": g["TAT_days"].quantile(0.90),
        "tat_60plus": g["_tat_60plus"].sum(),
        "invoice_count": g["_inv_present"].sum(),
        "invoice_sum": g["_inv_amt"].sum(min_count=1),
    }).reset_index()

    df.drop(columns=["_inv_amt","_inv_present","_tat_60plus"], inplace=True, errors="ignore")

    out.to_parquet(OUT, index=False)
    out.to_csv(OUT_CSV, index=False)
    print(f"OK: wrote {OUT} rows={len(out)} cols={len(out.columns)}")

if __name__ == "__main__":
    main()
