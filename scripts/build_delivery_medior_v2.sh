#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[1/4] Running pipeline (reproducible)..."
make run

echo "[2/4] Creating delivery folder..."
rm -rf DE_delivery_medior_v2
mkdir -p DE_delivery_medior_v2/{bronze,silver,gold,reports,logs,config,docs}

# Bronze
cp data/bronze/TCH_activity_report_2026-02-15_03-13-30.xlsx DE_delivery_medior_v2/bronze/
cp data/bronze/tch_repairs_raw.parquet DE_delivery_medior_v2/bronze/

# Silver
cp data/silver/tch_repairs_cleaned.parquet DE_delivery_medior_v2/silver/

# Gold
cp data/gold/kpi_monthly.parquet DE_delivery_medior_v2/gold/
cp data/gold/kpi_monthly.csv DE_delivery_medior_v2/gold/

# Reports + logs
cp reports/data_quality.json DE_delivery_medior_v2/reports/
cp reports/artifacts_summary.txt DE_delivery_medior_v2/reports/
cp pipeline_build.log DE_delivery_medior_v2/logs/ 2>/dev/null || true

# Config + docs
cp config.yaml DE_delivery_medior_v2/config/
cp -R docs/* DE_delivery_medior_v2/docs/ 2>/dev/null || true

# Minimal README for the delivery pack
cat > DE_delivery_medior_v2/README.txt <<'TXT'
DE Delivery Pack (Medior v2)

FOLDERS
- bronze/  : original Excel + extracted raw parquet
- silver/  : cleaned parquet (post-dedupe)
- gold/    : KPI mart (parquet + CSV)
- reports/ : data quality report + artifacts summary
- logs/    : pipeline build log
- config/  : config.yaml (PK, critical columns, DQ gate thresholds)
- docs/    : runbook + KPI defs + DQ contract + data dictionary

ENTRYPOINT (repo)
- make run   # builds bronze->silver->dq->gold->summary

TXT

echo "[3/4] Zipping delivery..."
rm -f DE_delivery_medior_v2.zip
zip -r DE_delivery_medior_v2.zip DE_delivery_medior_v2 -x "*.DS_Store" "__MACOSX/*" >/dev/null

echo "[4/4] Done."
ls -lh DE_delivery_medior_v2.zip
