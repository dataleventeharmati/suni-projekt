from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

INP = Path("data/processed/tch_repairs_cleaned.parquet")
OUT_DIR = Path("reports/grafikonok")
OUT_DIR.mkdir(parents=True, exist_ok=True)

MONEY_COLS = ["LABOR_CHARGE", "PARTS_CHARGE", "FREIGHT_CHARGE", "TAX", "INVOICE_AMOUNT"]
DATE_COL = "INVOICE_DATE"

def to_num(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "None": None, "nan": None})

    # Minden nem szám / pont / vessző / mínusz karakter törlése
    s = s.str.replace(r"[^\d,.\-]", "", regex=True)

    # Ha van vessző és nincs pont → vessző = tizedes
    mask_decimal_comma = s.str.contains(",", na=False) & (~s.str.contains(r"\.", na=False))
    s.loc[mask_decimal_comma] = s.loc[mask_decimal_comma].str.replace(",", ".", regex=False)

    # Ha pont és vessző is van → vessző ezres elválasztó
    mask_both = s.str.contains(",", na=False) & s.str.contains(r"\.", na=False)
    s.loc[mask_both] = s.loc[mask_both].str.replace(",", "", regex=False)

    return pd.to_numeric(s, errors="coerce")

def main():
    print("Adatok betöltése...")
    cols = [DATE_COL] + MONEY_COLS
    df = pq.read_table(INP, columns=cols).to_pandas()

    df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce")

    for c in MONEY_COLS:
        if c in df.columns:
            df[c + "_NUM"] = to_num(df[c])

    inv = df["INVOICE_AMOUNT_NUM"].dropna()

    # 1) Eloszlás
    if len(inv) > 0:
        cap = inv.quantile(0.99)
        inv_clip = inv.clip(upper=cap)

        plt.figure()
        plt.hist(inv_clip, bins=50)
        plt.title("INVOICE_AMOUNT eloszlás (99% vágás)")
        plt.xlabel("Összeg")
        plt.ylabel("Darabszám")
        plt.tight_layout()
        plt.savefig(OUT_DIR / "invoice_distribution.png")

    # 2) Havi összeg
    m = df.dropna(subset=[DATE_COL]).copy()
    m["year_month"] = m[DATE_COL].dt.to_period("M").astype(str)

    monthly_sum = m.groupby("year_month")["INVOICE_AMOUNT_NUM"].sum().sort_index()
    monthly_cnt = m.groupby("year_month")["INVOICE_AMOUNT_NUM"].count().sort_index()

    plt.figure()
    monthly_sum.plot(kind="bar")
    plt.title("Havi INVOICE_AMOUNT összeg")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "invoice_monthly_sum.png")

    plt.figure()
    monthly_cnt.plot(kind="bar")
    plt.title("Havi számlázott darabszám")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "invoice_monthly_count.png")

    print("\nPénzügyi gyors stat:")
    print("INVOICE_DATE hiányzó:", int(df[DATE_COL].isna().sum()))
    print("INVOICE_AMOUNT hiányzó:", int(df["INVOICE_AMOUNT_NUM"].isna().sum()))

    if len(inv) > 0:
        print("p50:", float(inv.median()))
        print("p90:", float(inv.quantile(0.90)))
        print("max:", float(inv.max()))

if __name__ == "__main__":
    main()
