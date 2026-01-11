App User Visits ETL to ClickHouse
Overview

This project is designed to extract, transform, and load (ETL) app user visit data from a Spark DataFrame into a ClickHouse database. It ensures proper handling of nullable columns, timestamps, and boolean flags, and maintains a checkpoint table for incremental data loads.

The main components are:

Main fact table (app_user_visits_fact) – stores user visit events.

ETL checkpoint table (etl_checkpoint) – tracks the last successfully processed batch and ensures incremental loading.

Tables
1. app_user_visits_fact

2. etl_checkpoint

Python ETL Script
Purpose

The Python script:

Converts Spark DataFrames to Pandas.

Normalizes DateTime64 columns (created_at, updated_at, expires_at) to Python datetime.

Normalizes UInt8 boolean flag columns (seen, expired, is_deleted, is_fraud).

Handles nullable columns (NaN → None) for ClickHouse compatibility.

Inserts data into app_user_visits_fact.

Updates etl_checkpoint with the max updated_at timestamp and batch info.

Key Functions
def clean_datetime(series):
    """Convert pd.Timestamp to datetime, handle NaT/None safely."""
    
def normalize_uint8(series, default=0):
    """Convert values to 0/1 for UInt8 flags, default for None/NaN."""
    
def insert_to_clickhouse(df_spark, job_name="app_user_visits_fact"):
    """Insert Spark DataFrame into ClickHouse fact table and update checkpoint."""

Usage

Prepare Spark DataFrame df_final with all required columns matching CLICKHOUSE_COLUMNS.

Run ETL insert function:

insert_to_clickhouse(df_final, job_name="app_user_visits_fact")


Check ClickHouse:

SELECT * FROM app_user_visits_fact ORDER BY created_at DESC LIMIT 10;
SELECT * FROM etl_checkpoint ORDER BY updated_at DESC LIMIT 5;

Requirements

Python 3.9+

Packages:

pip install pandas numpy clickhouse-connect pyspark


Access to ClickHouse cluster (ch_client) initialized using clickhouse-connect:

from clickhouse_connect import Client
ch_client = Client(host='your_clickhouse_host', username='default', password='your_password', database='default')

Notes

_ingested_at is automatically populated by ClickHouse; do not include in data insert.

Ensure all DateTime64 columns are either valid datetime or None.

UInt8 flags must be 0 or 1; None values are automatically normalized to 0.

The checkpoint prevents duplicate processing in incremental ETL.
