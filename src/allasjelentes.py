import json
from pathlib import Path
import pyarrow.parquet as pq

REPORT_JSON = Path("reports/data_quality.json")
CLEAN_PARQUET = Path("data/processed/tch_repairs_cleaned.parquet")

def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def main():
    if not REPORT_JSON.exists():
        raise SystemExit("Hiányzik a riport: reports/data_quality.json (futtasd: python3 src/validate_and_clean.py)")
    if not CLEAN_PARQUET.exists():
        raise SystemExit("Hiányzik a cleaned parquet: data/processed/tch_repairs_cleaned.parquet (futtasd: python3 src/validate_and_clean.py)")

    report = json.loads(REPORT_JSON.read_text(encoding="utf-8"))

    total = int(report.get("total_rows", 0))
    dup = int(report.get("duplicate_primary_keys", 0))
    critical = report.get("critical_nulls", {}) or {}
    date_nulls = report.get("date_nulls_after_parse", {}) or {}

    # TOP üzemek
    t = pq.read_table(CLEAN_PARQUET.as_posix(), columns=["REPAIR_PLANT_ID"])
    df = t.to_pandas()
    top = df["REPAIR_PLANT_ID"].dropna().astype(str).value_counts().head(5).to_dict()

    print("=" * 52)
    print("         TCH – Állapotjelentés (futtatás után)")
    print("=" * 52)
    print(f"Összes rekord:            {fmt(total)}")
    print(f"Duplikált elsődleges kulcs: {fmt(dup)}")
    print("")
    print("Kritikus hiányok:")
    for k, v in critical.items():
        print(f"  - {k}: {fmt(int(v))}")
    print("")
    print("Dátum mezők – üres értékek (parse után):")
    for k, v in date_nulls.items():
        print(f"  - {k}: {fmt(int(v))}")
    print("")
    print("TOP 5 üzem (legtöbb rekord):")
    i = 0
    for plant, cnt in top.items():
        i += 1
        print(f"  {i}. {plant}: {fmt(int(cnt))}")
    print("=" * 52)

if __name__ == "__main__":
    main()
