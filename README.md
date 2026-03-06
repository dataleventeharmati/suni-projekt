# Suni Data Pipeline

Production-style Data Engineering pipeline built in Python.

## Architecture

flowchart LR
  A[Excel Source] --> B[Bronze Layer]
  B --> C[Silver Clean & Validate]
  C --> D[Data Quality Gate]
  D --> E[Gold KPI Mart]
  E --> F[Reports]
  F --> G[KPI CSV]
  F --> H[HTML Report]
  F --> I[Data Quality JSON]
  E --> J[Automated Delivery Package]

## Pipeline Layers

Bronze
- Raw Excel ingestion
- Stored as parquet

Silver
- Data cleaning
- Validation
- Schema normalization

Data Quality Gate
- Null rate validation
- Duplicate detection

Gold
- Monthly KPI aggregation
- Repair metrics
- Invoice metrics

Reports
- KPI CSV
- HTML report
- Data quality summary

## Run Pipeline

make run

## Build Delivery Package

make deliver

Outputs

DE_delivery_medior_v2.zip

## Stack

Python
pandas
pyarrow
openpyxl
PyYAML
Makefile
