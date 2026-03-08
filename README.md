# Suni Data Pipeline

Production-style Data Engineering pipeline built in Python.

This project demonstrates a small but realistic end-to-end data pipeline with validation, profiling, KPI aggregation and automated reporting.

The goal is to show how a raw Excel dataset can be transformed into clean analytics outputs using reproducible pipeline steps.

---

# Architecture

flowchart LR
    A[Excel Source] --> B[Bronze Ingest]
    B --> C[Silver Clean & Validate]
    C --> D[Data Profiling]
    D --> E[Data Quality Gate]
    E --> F[Gold KPI Mart]
    F --> G[Reports]
    G --> H[KPI CSV]
    G --> I[HTML Report]
    G --> J[Data Quality JSON]
    F --> K[Delivery Package]

---

# Pipeline Layers

## Bronze
Raw Excel ingestion.

## Silver
Data cleaning and normalization.

Includes:
- schema normalization
- date parsing
- duplicate removal
- null validation

## Data Profiling
Automatic dataset overview report.

Generated file:
reports/data_profile.md

Contains:
- row / column counts
- column data types
- null analysis

## Data Quality Gate
Validation rules applied before analytics.

Examples:
- duplicate primary keys
- null thresholds
- date parsing errors

If validation fails the pipeline stops.

## Gold Layer
Aggregated analytics dataset.

Example metrics:
- monthly repair volume
- repair turnaround time
- invoice metrics

Output:
data/gold/kpi_monthly.parquet
data/gold/kpi_monthly.csv

---

# Reports Generated

reports/data_profile.md  
reports/kpi_report.html  
reports/data_quality.json  
reports/artifacts_summary.txt  

---

# Versioned Runs

Example:
runs/2026-03-08_14-00-36/reports

---

# Run Pipeline

git clone https://github.com/dataleventeharmati/suni-projekt.git  
cd suni-projekt  

make run

---

# Build Delivery Package

make deliver

Output:
DE_delivery_medior_v2.zip

---

# Stack

- Python
- pandas
- pyarrow
- openpyxl
- Makefile
- GitHub Actions

---

# Notes

This repository intentionally does not include the original dataset.

The pipeline structure and code are provided for demonstration purposes.
