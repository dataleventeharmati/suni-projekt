import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
from pathlib import Path

CLEAN = Path("data/processed/tch_repairs_cleaned.parquet")
OUT_DIR = Path("reports")
OUT_DIR.mkdir(exist_ok=True)

print("Adatok betöltése...")
df = pq.read_table(CLEAN).to_pandas()

# -------------------------------
# 1. Kritikus hiányok
# -------------------------------
critical_cols = [
    "TCH_RMA_NUMBER",
    "RMA_DATE",
    "REPAIR_END_DATE",
    "INVOICE_DATE"
]

missing = {c: df[c].isna().sum() for c in critical_cols}

plt.figure()
plt.bar(missing.keys(), missing.values())
plt.xticks(rotation=45)
plt.title("Kritikus hiányzó mezők")
plt.tight_layout()
plt.savefig(OUT_DIR / "kpi_missing.png")
plt.close()

# -------------------------------
# 2. Repair idő (TAT)
# -------------------------------
if "RECEIVED_DATE" in df.columns and "REPAIR_END_DATE" in df.columns:
    df["TAT_days"] = (df["REPAIR_END_DATE"] - df["RECEIVED_DATE"]).dt.days
    tat = df["TAT_days"].dropna()

    plt.figure()
    tat.hist(bins=50)
    plt.title("Repair idő eloszlás (nap)")
    plt.xlabel("Nap")
    plt.ylabel("Darab")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "kpi_tat_distribution.png")
    plt.close()

    print("Átlag repair idő:", round(tat.mean(), 2), "nap")
    print("Medián repair idő:", round(tat.median(), 2), "nap")

# -------------------------------
# 3. Havi volumen
# -------------------------------
if "RMA_DATE" in df.columns:
    monthly = df["RMA_DATE"].dt.to_period("M").value_counts().sort_index()

    plt.figure()
    monthly.plot()
    plt.title("Havi RMA volumen")
    plt.ylabel("Darab")
    plt.tight_layout()
    plt.savefig(OUT_DIR / "kpi_monthly_volume.png")
    plt.close()

# -------------------------------
# 4. Top Failure Code
# -------------------------------
if "FAILURE_CODE_1" in df.columns:
    top_fail = df["FAILURE_CODE_1"].value_counts().head(10)

    plt.figure()
    top_fail.plot(kind="bar")
    plt.title("Top 10 Failure Code")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "kpi_top_failure.png")
    plt.close()

print("\nRiport elkészült a 'reports' mappában.")
