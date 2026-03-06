import json
import re
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import pyarrow.parquet as pq
import yaml

CONFIG = Path("config.yaml")
RAW_PARQUET = Path("data/processed/tch_repairs_raw.parquet")
CLEAN_PARQUET = Path("data/processed/tch_repairs_cleaned.parquet")
REPORT_JSON = Path("reports/data_quality.json")

# sharedStrings: már nálad ott van /tmp-ben (korábban kiexportáltuk)
SHARED_XML = Path("/tmp/tch_sharedStrings.xml")
SENT = "__S__"


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def normalize_dates(series: pd.Series) -> pd.Series:
    excel_origin = pd.Timestamp("1899-12-30")

    def _excel(n: int) -> str:
        return (excel_origin + pd.to_timedelta(n, unit="D")).date().isoformat()

    def _norm(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None

        if isinstance(x, (int, float)) and not pd.isna(x):
            n = int(x)
            if 20000 <= n <= 80000:
                return _excel(n)
            return str(n)

        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return None

            if re.fullmatch(r"\d{5}", x):
                n = int(x)
                if 20000 <= n <= 80000:
                    return _excel(n)
                return x

            if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", x):
                y, m, d = x.split("/")
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

            return x

        return x

    return series.map(_norm)


def main():
    if not RAW_PARQUET.exists():
        raise SystemExit("Hiányzik: data/processed/tch_repairs_raw.parquet (előbb: python3 src/extract_to_parquet.py)")
    if not SHARED_XML.exists():
        raise SystemExit("Hiányzik: /tmp/tch_sharedStrings.xml (előbb exportálni kell az xlsx-ből)")

    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    print("[1/5] Raw parquet beolvasás…")
    df = pq.read_table(RAW_PARQUET.as_posix()).to_pandas()
    total_rows = len(df)
    print("  sorok:", fmt(total_rows), "oszlopok:", len(df.columns))

    print("[2/5] __S__ indexek összegyűjtése… (ez lehet pár perc)")
    needed = set()
    per_col = {}
    for col in df.columns:
        s = df[col]
        if s.dtype != "object":
            continue
        ss = s.astype(str)
        m = ss.str.startswith(SENT)
        cnt = int(m.sum())
        if cnt:
            per_col[col] = cnt
            idx = ss[m].str.slice(len(SENT)).astype("int64", errors="ignore")
            # idx lehet object ha valami furcsa -> szűrjük
            for v in idx.values:
                try:
                    needed.add(int(v))
                except Exception:
                    pass

    total_placeholders = sum(per_col.values())
    print("  placeholder összesen:", fmt(total_placeholders))
    print("  egyedi indexek:", fmt(len(needed)))

    print("[3/5] SharedStrings feloldás streamelve…")
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    want = set(needed)
    mapping = {}
    i = -1
    for event, elem in ET.iterparse(SHARED_XML.as_posix(), events=("end",)):
        if elem.tag.endswith("}si"):
            i += 1
            if i in want:
                txt = "".join((t.text or "") for t in elem.findall(".//m:t", ns)).strip()
                mapping[i] = txt
                if len(mapping) % 50000 == 0:
                    print("  feloldva:", fmt(len(mapping)))
                if len(mapping) == len(want):
                    break
            elem.clear()

    print("  mapping méret:", fmt(len(mapping)))
    missing = len(want - set(mapping.keys()))
    print("  hiányzó indexek:", fmt(missing))

    print("[4/5] Placeholder csere oszloponként…")
    resolved_count = 0
    unresolved_count = 0

    for col, cnt in sorted(per_col.items(), key=lambda x: x[1], reverse=True):
        s = df[col].astype(str)
        m = s.str.startswith(SENT)
        if not m.any():
            continue

        idx = pd.to_numeric(s[m].str.slice(len(SENT)), errors="coerce").astype("Int64")
        repl = idx.map(mapping.get)

        # számlálás
        resolved_here = int(repl.notna().sum())
        unresolved_here = int(repl.isna().sum())
        resolved_count += resolved_here
        unresolved_count += unresolved_here

        df.loc[m, col] = repl

    # basic clean: trim strings
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)

    print("  feloldott cellák:", fmt(resolved_count))
    print("  feloldatlan cellák:", fmt(unresolved_count))

    # --- validations (same logic as before) ---
    report = {
        "total_rows": int(total_rows),
        "resolved_shared_string_values": int(resolved_count),
        "unresolved_shared_string_values": int(unresolved_count),
        "critical_nulls": {},
        "duplicate_primary_keys": 0,
        "date_parse_errors": {},
        "date_normalized_to_null": {},
        "date_nulls_after_parse": {},
    }

    # Critical nulls
    for col in config["critical_columns"]:
        report["critical_nulls"][col] = int(df[col].isna().sum())

    # Duplicate PK
    pk = config["primary_key"]
    report["duplicate_primary_keys"] = int(df.duplicated(subset=pk).sum())

    # Dates: normalize + 2-pass parse
    for col in config["date_columns"]:
        raw0 = df[col]
        raw_norm = normalize_dates(raw0)

        normalized_to_null = raw_norm.isna() & raw0.notna()
        report["date_normalized_to_null"][col] = int(normalized_to_null.sum())

        parsed = pd.to_datetime(raw_norm, errors="coerce", format="%Y-%m-%d")
        mask = parsed.isna() & raw_norm.notna()
        if mask.any():
            parsed2 = pd.to_datetime(raw_norm[mask], errors="coerce", dayfirst=False)
            parsed.loc[mask] = parsed2

        true_parse_error = parsed.isna() & raw_norm.notna()
        report["date_parse_errors"][col] = int(true_parse_error.sum())

        df[col] = parsed
        report["date_nulls_after_parse"][col] = int(df[col].isna().sum())

    print("[5/5] Mentés…")
    CLEAN_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CLEAN_PARQUET, index=False)
    REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n=== KÉSZ (magyar összegzés) ===")
    print("Összes rekord:", fmt(total_rows))
    print("Feloldott értékek:", fmt(resolved_count))
    print("Feloldatlan értékek:", fmt(unresolved_count))
    print("PK duplikáció:", fmt(report["duplicate_primary_keys"]))
    print("Mentve:", CLEAN_PARQUET.as_posix())
    print("Riport:", REPORT_JSON.as_posix())


if __name__ == "__main__":
    main()
