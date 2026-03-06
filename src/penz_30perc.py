from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

INP = Path("data/processed/tch_repairs_cleaned.parquet")
OUT_DIR = Path("reports/grafikonok")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def to_num(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    x = x.str.replace("\u00a0", "", regex=False)
    x = x.str.replace(" ", "", regex=False)

    has_comma = x.str.contains(",", na=False)
    has_dot = x.str.contains(r"\.", na=False)
    only_comma = has_comma & (~has_dot)

    x = x.where(~only_comma, x.str.replace(",", ".", regex=False))
    both = has_comma & has_dot
    x = x.where(~both, x.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))

    return pd.to_numeric(x, errors="coerce")

def main():
    print("Adatok betöltése...")
    df = pq.read_table(INP).to_pandas()

    df["INVOICE_DATE"] = pd.to_datetime(df["INVOICE_DATE"], errors="coerce")

    for c in ["INVOICE_AMOUNT","LABOR_CHARGE","PARTS_CHARGE","FREIGHT_CHARGE","TAX"]:
        if c in df.columns:
            df[c + "_NUM"] = to_num(df[c])

    inv = df["INVOICE_AMOUNT_NUM"]

    print("\nGyors stat:")
    print("Hiányzó:", int(inv.isna().sum()))
    inv_ok = inv.dropna()
    if len(inv_ok):
        print("p50:", float(inv_ok.median()))
        print("p90:", float(inv_ok.quantile(0.90)))
        print("max:", float(inv_ok.max()))

    m = df.dropna(subset=["INVOICE_DATE"]).copy()
    m["year_month"] = m["INVOICE_DATE"].dt.to_period("M").astype(str)

    monthly_sum = m.groupby("year_month")["INVOICE_AMOUNT_NUM"].sum().sort_index()

    plt.figure()
    monthly_sum.plot(kind="bar")
    plt.gca().set_xticks(range(0, len(monthly_sum), 2))
    plt.title("Havi INVOICE_AMOUNT összeg")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "invoice_amount_monthly_sum.png")

    print("Kész pénzügyi grafikon.")

if __name__ == "__main__":
    main()
