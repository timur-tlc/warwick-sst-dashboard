"""
Lambda handler for weekly Athena → BigQuery export.

Invoked by Step Functions with {"table": "sessions|events|items"}.
Queries Athena SAL views, downloads CSV from S3, loads to BigQuery.
"""

import json
import os
import sys
import time
import tempfile
from urllib.parse import urlparse

import boto3
import pandas as pd
from google.cloud import bigquery

# --- Config ---
ATHENA_DATABASE = "warwick_weave_sst_events"
ATHENA_OUTPUT = "s3://warwick-com-au-athena-results/"
BQ_PROJECT = "376132452327"
BQ_DATASET = "sst_events"
GCP_SECRET_NAME = "warwick/gcp-service-account"

# Columns that look numeric but must stay as strings
STRING_DTYPES = {"ga_session_id": str, "transaction_id": str}

SESSIONS_QUERY = """
SELECT
    ga_session_id,
    user_pseudo_id,
    MIN(timestamp) AS session_start,
    MAX(timestamp) AS session_end,
    ARBITRARY(device_category) AS device_category,
    ARBITRARY(device_browser) AS device_browser,
    ARBITRARY(device_operating_system) AS device_operating_system,
    ARBITRARY(device_brand) AS device_brand,
    ARBITRARY(geo_country) AS geo_country,
    ARBITRARY(geo_country_code) AS geo_country_code,
    ARBITRARY(geo_region) AS geo_region,
    ARBITRARY(site) AS site,
    COALESCE(MIN_BY(session_default_channel_group_event, timestamp), 'Direct') AS session_default_channel_group,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS pageviews,
    COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases,
    MAX(CASE WHEN event_name = 'purchase' THEN ecommerce_value END) AS purchase_value,
    ARBITRARY(year) AS year,
    ARBITRARY(month) AS month,
    ARBITRARY(day) AS day
FROM warwick_weave_sst_events.sst_events_transformed
WHERE NOT is_synthetic_event
  AND NOT is_fallback_event
  AND is_likely_human
  AND ga_session_id IS NOT NULL
GROUP BY ga_session_id, user_pseudo_id
"""

EVENTS_QUERY = """
SELECT
    timestamp,
    event_name,
    measurement_id,
    user_pseudo_id,
    ga_session_id,
    device_category,
    device_operating_system,
    device_browser,
    device_brand,
    geo_country_code,
    geo_country,
    geo_region,
    geo_city,
    page_location,
    page_title,
    page_referrer,
    language,
    screen_resolution,
    site,
    transaction_id,
    ecommerce_value,
    ecommerce_currency,
    engagement_time_msec,
    link_text,
    link_url,
    search_term,
    session_default_channel_group_event,
    is_likely_human,
    is_bot,
    is_synthetic_event,
    is_fallback_event,
    ip_address,
    user_agent,
    year,
    month,
    day
FROM warwick_weave_sst_events.sst_events_transformed
WHERE NOT is_synthetic_event
  AND NOT is_fallback_event
  AND is_likely_human
  AND ga_session_id IS NOT NULL
"""

ITEMS_QUERY = """
SELECT
    CAST(from_iso8601_timestamp(timestamp) AS DATE) AS date,
    event_name,
    ga_session_id,
    user_pseudo_id,
    transaction_id,
    ecommerce_value,
    ecommerce_currency,
    item_id,
    item_name,
    item_brand,
    item_category,
    item_category2,
    item_category3,
    item_variant,
    price,
    quantity
FROM warwick_weave_sst_events.sst_ecommerce_items
WHERE event_name = 'purchase'
"""

QUERIES = {
    "sessions": SESSIONS_QUERY,
    "events": EVENTS_QUERY,
    "items": ITEMS_QUERY,
}


def get_gcp_credentials():
    """Retrieve GCP service account key from Secrets Manager and write to temp file."""
    sm = boto3.client("secretsmanager")
    secret = sm.get_secret_value(SecretId=GCP_SECRET_NAME)
    key_json = secret["SecretString"]

    creds_path = "/tmp/gcp_sa.json"
    with open(creds_path, "w") as f:
        f.write(key_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return creds_path


def run_athena_query(athena, query, label):
    """Submit Athena query and wait for completion. Returns S3 output path."""
    print(f"[{label}] Submitting query...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup="primary",
    )
    query_id = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            s3_path = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
            stats = status["QueryExecution"].get("Statistics", {})
            scanned_mb = stats.get("DataScannedInBytes", 0) / 1024 / 1024
            runtime_s = stats.get("EngineExecutionTimeInMillis", 0) / 1000
            print(f"[{label}] Query completed in {runtime_s:.1f}s ({scanned_mb:.1f} MB scanned)")
            return s3_path
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)


def download_csv(s3, s3_path, label, use_tempfile=False):
    """Download Athena CSV results from S3 into a DataFrame."""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if use_tempfile:
        # For large files (events), stream to /tmp first
        tmp_path = "/tmp/athena_result.csv"
        print(f"[{label}] Downloading to {tmp_path}...")
        s3.download_file(bucket, key, tmp_path)
        file_size_mb = os.path.getsize(tmp_path) / 1024 / 1024
        print(f"[{label}] Downloaded {file_size_mb:.1f} MB")
        # Return path instead of DataFrame for chunked processing
        return tmp_path
    else:
        print(f"[{label}] Downloading to memory...")
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"], dtype=STRING_DTYPES, low_memory=False)
        print(f"[{label}] Loaded {len(df):,} rows")
        return df


def process_sessions(df):
    """Add date and new_user columns to sessions DataFrame."""
    df["date"] = pd.to_datetime(
        df[["year", "month", "day"]].rename(
            columns={"year": "year", "month": "month", "day": "day"}
        )
    ).dt.date

    df["session_start_ts"] = pd.to_datetime(df["session_start"])
    df["new_user"] = 0
    first_sessions = df.groupby("user_pseudo_id")["session_start_ts"].idxmin()
    df.loc[first_sessions, "new_user"] = 1
    df.drop(columns=["session_start_ts"], inplace=True)
    print(f"[sessions] new_user=1 for {df['new_user'].sum():,} / {len(df):,} sessions")
    return df


def process_events(df):
    """Add date column to events DataFrame."""
    df["date"] = pd.to_datetime(
        df[["year", "month", "day"]].rename(
            columns={"year": "year", "month": "month", "day": "day"}
        )
    ).dt.date
    return df


def process_items(df):
    """Convert date column in items DataFrame."""
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def load_to_bigquery(bq, df, table_name, label, write_disposition="WRITE_TRUNCATE"):
    """Load DataFrame to BigQuery."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    print(f"[{label}] Loading {len(df):,} rows to {table_id} ({write_disposition})...")
    job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = bq.get_table(table_id)
    print(f"[{label}] Table now has: {table.num_rows:,} rows")
    return table.num_rows


def load_events_chunked(bq, csv_path, label):
    """Load large events CSV to BigQuery in chunks to avoid OOM."""
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.events"
    chunk_size = 500_000
    total_rows = 0
    first_chunk = True

    print(f"[{label}] Loading in chunks of {chunk_size:,}...")
    for chunk in pd.read_csv(csv_path, dtype=STRING_DTYPES, low_memory=False, chunksize=chunk_size):
        chunk = process_events(chunk)
        disposition = "WRITE_TRUNCATE" if first_chunk else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(write_disposition=disposition)
        job = bq.load_table_from_dataframe(chunk, table_id, job_config=job_config)
        job.result()
        total_rows += len(chunk)
        print(f"[{label}] Loaded chunk: {len(chunk):,} rows (total so far: {total_rows:,})")
        first_chunk = False
        del chunk  # Free memory

    os.remove(csv_path)
    table = bq.get_table(table_id)
    print(f"[{label}] Final table: {table.num_rows:,} rows")
    return table.num_rows


def handler(event, context):
    """Lambda entry point. Expects {"table": "sessions|events|items"}."""
    table = event.get("table")
    if table not in QUERIES:
        raise ValueError(f"Invalid table: {table}. Must be one of: {list(QUERIES.keys())}")

    print(f"=== Exporting {table} ===")

    # Set up GCP credentials
    get_gcp_credentials()

    # AWS clients (Lambda role provides credentials automatically)
    athena = boto3.client("athena", region_name="ap-southeast-2")
    s3 = boto3.client("s3", region_name="ap-southeast-2")
    bq = bigquery.Client(project=BQ_PROJECT)

    # Run Athena query
    s3_path = run_athena_query(athena, QUERIES[table], table)

    if table == "events":
        # Events table is too large to fit in memory — use chunked loading
        csv_path = download_csv(s3, s3_path, table, use_tempfile=True)
        row_count = load_events_chunked(bq, csv_path, table)
    else:
        # Sessions and items fit in memory
        df = download_csv(s3, s3_path, table, use_tempfile=False)
        if table == "sessions":
            df = process_sessions(df)
        elif table == "items":
            df = process_items(df)
        row_count = load_to_bigquery(bq, df, table, table)

    result = {"table": table, "rows": row_count}
    print(f"=== {table} complete: {row_count:,} rows ===")
    return result
