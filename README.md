# Real-Time ML Feature Delivery Pipeline (AWS & GCP)

## Overview
A production-grade multi-cloud ETL pipeline that engineers and delivers ML-ready feature data across AWS and GCP simultaneously, enabling fully automated daily model predictions.

## Problem It Solves
Analysts were manually preparing data for ML forecasting every day — slow, error-prone, and a bottleneck for the data science team. This pipeline eliminated 100% of that manual effort and reduced end-to-end data delivery latency significantly.

## Architecture
Salesforce CRM + RevOps Sources → Apache Airflow → PySpark (Databricks) → AWS Redshift + GCP BigQuery → dbt Feature Tables → Scikit-learn Forecasting Model

## Key Features
- Multi-cloud delivery: writes to AWS Redshift and GCP BigQuery simultaneously with zero schema drift
- dbt models for auditable, version-controlled feature transformation logic
- Full lineage from raw CRM ingestion to ML-ready feature tables
- Automated daily predictions via Scikit-learn forecasting model
- Eliminated 100% of manual analyst data preparation

## Tech Stack
| Layer | Tools |
|-------|-------|
| Language | Python, SQL |
| Processing | PySpark, Databricks |
| Orchestration | Apache Airflow |
| Transformation | dbt (models, lineage, tests) |
| Data Warehouses | AWS Redshift, GCP BigQuery, Snowflake |
| CRM Source | Salesforce, RevOps feeds |
| ML | Scikit-learn forecasting model |
| Infrastructure | GitHub Actions CI/CD, Docker |

## Results
- Eliminated 100% of manual data preparation effort
- Reduced end-to-end data delivery latency for daily model runs
- Zero schema drift across multi-cloud delivery
- Fully automated daily ML predictions replacing manual analyst workflows

## Project Structure
dags/                  # Airflow DAG definitions
spark/
  ingestion/           # Salesforce + RevOps source readers
  transforms/          # PySpark feature engineering jobs
dbt/
  models/
    staging/           # Raw source cleaning
    intermediate/      # Business logic
    mart/              # Final ML-ready feature tables
  tests/               # dbt data quality tests
ml/
  forecasting_model.py # Scikit-learn model consuming feature tables
