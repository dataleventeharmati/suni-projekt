import re
import json
from pathlib import Path
import yaml
import pandas as pd
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET

CONFIG = Path("config.yaml")
INPUT = Path("data/bronze/tch_repairs_raw.parquet")
OUT_PARQUET = Path("data/silver/tch_repairs_cleaned.parquet")
OUT_REPORT = Path("reports/data_quality.json")

SHARED_XML = Path("/tmp/tch_sharedStrings.xml")
SENTINEL_PREFIX = "__S__"

def extract_indexes(series: pd.Series) -> set[int]:
    idxs = set()
    for v in series.dropna().astype(str).values:
        if v.startswith(SENTINEL_PREFIX):
            try:
                idxs.add(int(v[len(SENTINEL_PREFIX):]))
            except ValueError:
                pass
    return idxs

def resolve_shared_strings(needed: set[int]) -> dict[int, str]:
    if not needed:
        return {}
    wanted = set(needed)
    max_needed = max(wanted)

    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    mapping: dict[int, str] = {}
    i = -1

    for event, elem in ET.iterparse(SHARED_XML, events=("end",)):
        if elem.tag.endswith("}si"):
            i += 1
            if i in wanted:
                txt = "".join((t.text or "") for t in elem.findall(".//m:t", ns)).strip()
                mapping[i] = txt
                if len(mapping) == len(wanted):
                    break
            elem.clear()
            if i >= max_needed and len(mapping) == len(wanted):
                break

    return mapping

def apply_resolution(series: pd.Series, mapping: dict[int, str]) -> pd.Series:
    def _one(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        s = str(v)
        if s.startswith(SENTINEL_PREFIX):
            try:
                idx = int(s[len(SENTINEL_PREFIX):])
            except ValueError:
                return None
            return mapping.get(idx)
        return s
    return series.map(_one)

def normalize_dates(series: pd.Series) -> pd.Series:
    """
    Normalize:
    - strip whitespace
    - empty string -> None
    - YYYY/MM/DD (or YYYY/M/D) -> YYYY-MM-DD
    - Excel serial date (e.g. 45734) -> ISO date, origin 1899-12-30
    """
    excel_origin = pd.Timestamp("1899-12-30")

    def _excel_serial_to_iso(n: int) -> str:
        return (excel_origin + pd.to_timedelta(n, unit="D")).date().isoformat()

    def _norm(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None

        if isinstance(x, (int, float)) and not pd.isna(x):
            n = int(x)
            if 20000 <= n <= 80000:
                return _excel_serial_to_iso(n)
            return str(n)

        if isinstance(x, str):
            x = x.strip()
            if x == "":
                return None

            if re.fullmatch(r"\d{5}", x):
                n = int(x)
                if 20000 <= n <= 80000:
                    return _excel_serial_to_iso(n)
                return x

            if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", x):
                y, m, d = x.split("/")
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

            return x

        return x

    return series.map(_norm)

def main():
    config = yaml.safe_load(CONFIG.read_text())
    df = pq.read_table(INPUT).to_pandas()
    total_rows = len(df)

    cols_to_resolve = (
        set(config.get("critical_columns", []))
        | set(config.get("date_columns", []))
        | set(config.get("financial_columns", []))
        | set(config.get("primary_key", []))
    )
    cols_to_resolve = [c for c in cols_to_resolve if c in df.columns]

    needed = set()
    for c in cols_to_resolve:
        needed |= extract_indexes(df[c])

    mapping = resolve_shared_strings(needed)

    for c in cols_to_resolve:
        df[c] = apply_resolution(df[c], mapping)

    report = {
        "total_rows": total_rows,
        "resolved_shared_string_values": len(mapping),
        "critical_nulls": {},
        "duplicate_primary_keys": 0,
        "date_parse_errors": {},            # truly unparseable AFTER normalization
        "date_normalized_to_null": {},      # raw present but normalized to None
        "date_nulls_after_parse": {},       # final NaT count
    }

    for col in config["critical_columns"]:
        report["critical_nulls"][col] = int(df[col].isna().sum())

    pk = config["primary_key"]
    report["duplicate_primary_keys"] = int(df.duplicated(subset=pk).sum())

    for col in config["date_columns"]:
        raw0 = df[col]
        raw_norm = normalize_dates(raw0)

        # normalized-to-null: had something originally, but normalization turned it into None
        normalized_to_null = raw_norm.isna() & raw0.notna()
        report["date_normalized_to_null"][col] = int(normalized_to_null.sum())

        # 2-pass parsing for stability:
        # 1) strict ISO (YYYY-MM-DD)
        parsed = pd.to_datetime(raw_norm, errors="coerce", format="%Y-%m-%d")
        # 2) fallback (handles e.g. MM/DD/YYYY, YYYY/MM/DD, etc.)
        mask = parsed.isna() & raw_norm.notna()
        if mask.any():
            parsed2 = pd.to_datetime(raw_norm[mask], errors="coerce", dayfirst=False)
            parsed.loc[mask] = parsed2

        # true parse errors: raw_norm has value, but parsed is NaT
        true_parse_error = parsed.isna() & raw_norm.notna()
        report["date_parse_errors"][col] = int(true_parse_error.sum())

        df[col] = parsed
        report["date_nulls_after_parse"][col] = int(df[col].isna().sum())

    for col in config.get("financial_columns", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(lambda x: x.strip() if isinstance(x, str) else x)

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    
    # ---------- Deterministic PK de-duplication (medior policy) ----------

    # Avoid pandas fragmentation warnings when adding helper rank columns
    df = df.copy()  # defragment for safe column inserts

    pk = config.get("primary_key", [])
    dropped = 0

    if pk and all(c in df.columns for c in pk):
        # ranking: keep "most complete / billable / finished"
        rank_cols = []

        if "INVOICE_DATE" in df.columns:
            df["_rank_invoice_date"] = df["INVOICE_DATE"].notna().astype(int)
            rank_cols.append("_rank_invoice_date")

        if "INVOICE_NUMBER" in df.columns:
            df["_rank_invoice_number"] = df["INVOICE_NUMBER"].notna().astype(int)
            rank_cols.append("_rank_invoice_number")

        if "REPAIR_END_DATE" in df.columns:
            df["_rank_repair_end"] = df["REPAIR_END_DATE"].notna().astype(int)
            rank_cols.append("_rank_repair_end")

        if "NB_OF_TOTAL_PARTS_REPLACED" in df.columns:
            df["_rank_parts"] = pd.to_numeric(df["NB_OF_TOTAL_PARTS_REPLACED"], errors="coerce").fillna(0)
            rank_cols.append("_rank_parts")

        for c in ("REPAIR_START_DATE", "RECEIVED_DATE"):
            if c in df.columns:
                df[f"_rank_{c.lower()}"] = pd.to_datetime(df[c], errors="coerce")
                rank_cols.append(f"_rank_{c.lower()}")

        if rank_cols:
            df = df.sort_values(pk + rank_cols)

        before = len(df)
        df = df.drop_duplicates(subset=pk, keep="last").copy()
        dropped = before - len(df)

        # cleanup helper cols
        for c in list(df.columns):
            if c.startswith("_rank_"):
                df.drop(columns=[c], inplace=True, errors="ignore")

    # persist dedupe stats in report
    report["dedupe_dropped_rows"] = int(dropped)
    # ---------- /Deterministic PK de-duplication ----------
    # Recompute duplicate PK metric AFTER dedupe (report must reflect final silver state)
    pk2 = config.get("primary_key", [])
    if pk2 and all(c in df.columns for c in pk2):
        report["duplicate_primary_keys"] = int(df.duplicated(subset=pk2, keep=False).sum() / 2)


    df.to_parquet(OUT_PARQUET, index=False)
    OUT_REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("Validation complete.")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
