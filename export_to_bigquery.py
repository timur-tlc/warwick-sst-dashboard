"""
Export SST data from Athena to BigQuery.

Queries the SAL views (sst_sessions, sst_ecommerce_items) in Athena,
downloads results from S3, and loads them into BigQuery tables that
Looker Studio reads from. The _ga4 views auto-update.

Usage:
    aws sso login --profile warwick
    python export_to_bigquery.py
"""

import sys
import time
import io
from urllib.parse import urlparse

import boto3
import pandas as pd
from google.cloud import bigquery

# --- Config ---
AWS_PROFILE = "warwick"
AWS_REGION = "ap-southeast-2"
ATHENA_DATABASE = "warwick_weave_sst_events"
ATHENA_OUTPUT = "s3://warwick-com-au-athena-results/"
BQ_PROJECT = "376132452327"
BQ_DATASET = "sst_events"

# Sessions query: sst_sessions view + geo_region + date column
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

# Items query: purchase items with date column
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

# Events query: event-level data for events_ga4 view
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

# Columns that look numeric but must stay as strings
STRING_DTYPES = {"ga_session_id": str, "transaction_id": str}


def run_athena_query(athena_client, query, label="query"):
    """Submit an Athena query and wait for it to complete. Returns the S3 output path."""
    print(f"  Submitting {label}...")
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup="primary",
    )
    query_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            s3_path = status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
            stats = status["QueryExecution"].get("Statistics", {})
            scanned_mb = stats.get("DataScannedInBytes", 0) / 1024 / 1024
            runtime_ms = stats.get("EngineExecutionTimeInMillis", 0)
            print(f"  {label} completed in {runtime_ms/1000:.1f}s ({scanned_mb:.1f} MB scanned)")
            return s3_path
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            print(f"  {label} {state}: {reason}", file=sys.stderr)
            sys.exit(1)
        time.sleep(2)


def download_athena_csv(s3_client, s3_path):
    """Download Athena CSV results from S3 into a pandas DataFrame."""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    print(f"  Downloading s3://{bucket}/{key}...")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"], dtype=STRING_DTYPES, low_memory=False)
    return df


def load_to_bigquery(bq_client, df, table_id, label="table"):
    """Load a DataFrame into BigQuery, replacing existing data."""
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    print(f"  Loading {len(df):,} rows to {table_id}...")
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for completion
    table = bq_client.get_table(table_id)
    print(f"  {label} loaded: {table.num_rows:,} rows")
    return table.num_rows


def main():
    print("=== SST Athena → BigQuery Export ===\n")

    # Authenticate
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    athena = session.client("athena")
    s3 = session.client("s3")
    bq = bigquery.Client(project=BQ_PROJECT)

    # --- Sessions ---
    print("[1/3] Sessions")
    s3_path = run_athena_query(athena, SESSIONS_QUERY, label="sessions query")
    sessions_df = download_athena_csv(s3, s3_path)

    # Derive date from year/month/day columns (views need both date AND y/m/d)
    sessions_df["date"] = pd.to_datetime(
        sessions_df[["year", "month", "day"]].rename(columns={"year": "year", "month": "month", "day": "day"})
    ).dt.date

    # Compute new_user: 1 for the earliest session per user_pseudo_id
    sessions_df["session_start_ts"] = pd.to_datetime(sessions_df["session_start"])
    sessions_df["new_user"] = 0
    first_sessions = sessions_df.groupby("user_pseudo_id")["session_start_ts"].idxmin()
    sessions_df.loc[first_sessions, "new_user"] = 1
    sessions_df.drop(columns=["session_start_ts"], inplace=True)
    print(f"  new_user=1 for {sessions_df['new_user'].sum():,} / {len(sessions_df):,} sessions")

    sessions_table = f"{BQ_PROJECT}.{BQ_DATASET}.sessions"
    load_to_bigquery(bq, sessions_df, sessions_table, label="Sessions")

    # --- Events ---
    print("\n[2/3] Events")
    s3_path = run_athena_query(athena, EVENTS_QUERY, label="events query")
    events_df = download_athena_csv(s3, s3_path)

    # Derive date column from year/month/day
    events_df["date"] = pd.to_datetime(
        events_df[["year", "month", "day"]].rename(columns={"year": "year", "month": "month", "day": "day"})
    ).dt.date

    events_table = f"{BQ_PROJECT}.{BQ_DATASET}.events"
    load_to_bigquery(bq, events_df, events_table, label="Events")

    # --- Items ---
    print("\n[3/3] Items")
    s3_path = run_athena_query(athena, ITEMS_QUERY, label="items query")
    items_df = download_athena_csv(s3, s3_path)

    # Convert date column
    if "date" in items_df.columns:
        items_df["date"] = pd.to_datetime(items_df["date"]).dt.date

    items_table = f"{BQ_PROJECT}.{BQ_DATASET}.items"
    load_to_bigquery(bq, items_df, items_table, label="Items")

    # --- Summary ---
    print(f"\n=== Export Complete ===")
    print(f"  Sessions: {len(sessions_df):,} rows")
    print(f"  Events:   {len(events_df):,} rows")
    print(f"  Items:    {len(items_df):,} rows")

    # Spot-check device_brand
    if "device_brand" in sessions_df.columns:
        brand_counts = sessions_df["device_brand"].value_counts().head(10)
        not_set = (sessions_df["device_brand"].isna() | (sessions_df["device_brand"] == "(not set)")).mean() * 100
        print(f"\n  device_brand top 10:")
        for brand, count in brand_counts.items():
            print(f"    {brand}: {count:,}")
        print(f"  (not set) rate: {not_set:.1f}%")

    print("\nNext: In Looker Studio, Resource → Manage → Refresh Fields on all data sources")


if __name__ == "__main__":
    main()
