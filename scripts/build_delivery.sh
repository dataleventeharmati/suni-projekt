#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DELIVERY_NAME="DE_delivery_medior_v2"
OUT_ZIP="${DELIVERY_NAME}.zip"

STAGING_DIR="delivery/${DELIVERY_NAME}"
rm -rf "delivery"
mkdir -p "${STAGING_DIR}"

echo "==> 1) Run pipeline (make run)"
make run

echo "==> 2) Validate expected artifacts exist"
required_files=(
  "data/bronze/tch_repairs_raw.parquet"
  "data/silver/tch_repairs_cleaned.parquet"
  "data/gold/kpi_monthly.parquet"
  "data/gold/kpi_monthly.csv"
  "reports/data_quality.json"
  "reports/artifacts_summary.txt"
)

missing=0
for f in "${required_files[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "MISSING: $f"
    missing=1
  fi
done

if [[ "$missing" -ne 0 ]]; then
  echo "ERROR: Missing required artifacts. Aborting delivery build."
  exit 2
fi

echo "==> 3) Copy artifacts into staging folder"
mkdir -p "${STAGING_DIR}/data/bronze" "${STAGING_DIR}/data/silver" "${STAGING_DIR}/data/gold" "${STAGING_DIR}/reports"

cp -v data/bronze/tch_repairs_raw.parquet "${STAGING_DIR}/data/bronze/"
cp -v data/silver/tch_repairs_cleaned.parquet "${STAGING_DIR}/data/silver/"
cp -v data/gold/kpi_monthly.parquet "${STAGING_DIR}/data/gold/"
cp -v data/gold/kpi_monthly.csv "${STAGING_DIR}/data/gold/"
cp -v reports/data_quality.json "${STAGING_DIR}/reports/"
cp -v reports/artifacts_summary.txt "${STAGING_DIR}/reports/"
cp -v reports/kpi_report.html "${STAGING_DIR}/reports/"

echo "==> 4) Add metadata"
cat > "${STAGING_DIR}/DELIVERY_INFO.txt" <<'TXT'
Project: suni-projekt
Architecture: Bronze → Silver → Data Quality Gate → Gold → Artifacts Summary
Stack: Python, pandas, pyarrow, openpyxl, Makefile
Run: make run
Delivery: automated package
TXT

echo "==> 5) Create zip"
rm -f "${OUT_ZIP}"
( cd "delivery" && zip -r "../${OUT_ZIP}" "${DELIVERY_NAME}" >/dev/null )

echo "==> 6) Checksums"
shasum -a 256 "${OUT_ZIP}" > "${OUT_ZIP}.sha256"

echo "DONE:"
ls -lh "${OUT_ZIP}" "${OUT_ZIP}.sha256"
