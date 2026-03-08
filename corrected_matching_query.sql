-- ============================================================================
-- CORRECTED SESSION CATEGORIZATION QUERY
-- ============================================================================
-- Purpose: Correctly categorize sessions using timestamp+attribute matching
-- instead of ga_session_id matching which fails due to timing differences.
--
-- Key insight: Same user session can have different ga_session_ids in SST
-- and Direct due to sub-second timing differences in event arrival.
--
-- Matching logic:
-- - Timestamp within ±5 minutes (300 seconds)
-- - Device category matches
-- - Country matches
-- ============================================================================

-- Step 1: Get SST sessions with corrected category
WITH sst_sessions_categorized AS (
  SELECT
    s.ga_session_id,
    s.user_pseudo_id,
    s.session_start,
    s.device_category,
    s.device_operating_system,
    s.device_browser,
    s.geo_country,
    s.purchases,
    -- Check if there's a matching Direct session
    CASE
      WHEN d.ga_session_id IS NOT NULL THEN 'Both'
      ELSE 'SST-only'
    END as session_category
  FROM warwick_weave_sst_events.sst_sessions s
  LEFT JOIN (
    -- Subquery to get Direct sessions from BigQuery
    -- NOTE: This would need to be replaced with actual BigQuery data
    -- For now, using a placeholder structure
    SELECT
      ga_session_id,
      session_start_ts,
      device_category,
      geo_country
    FROM bigquery_sessions  -- Placeholder: needs real BigQuery connection
  ) d
  ON ABS(CAST(to_unixtime(from_iso8601_timestamp(s.session_start)) AS BIGINT) * 1000000 - d.session_start_ts) <= 300000000
  AND s.device_category = d.device_category
  AND s.geo_country = d.geo_country
),

-- Step 2: Get Direct sessions with corrected category
direct_sessions_categorized AS (
  SELECT
    d.ga_session_id,
    d.user_pseudo_id,
    d.session_start_ts,
    d.device_category,
    d.device_operating_system,
    d.device_browser,
    d.geo_country,
    d.purchases,
    -- Check if there's a matching SST session
    CASE
      WHEN s.ga_session_id IS NOT NULL THEN 'Both'
      ELSE 'Direct-only'
    END as session_category
  FROM bigquery_sessions d  -- Placeholder
  LEFT JOIN warwick_weave_sst_events.sst_sessions s
  ON ABS(d.session_start_ts - CAST(to_unixtime(from_iso8601_timestamp(s.session_start)) AS BIGINT) * 1000000) <= 300000000
  AND d.device_category = s.device_category
  AND d.geo_country = s.geo_country
)

-- Step 3: Combine and aggregate
SELECT
  session_category,
  COUNT(*) as session_count,
  SUM(purchases) as total_purchases,
  SUM(purchases) * 100.0 / COUNT(*) as purchase_rate
FROM (
  SELECT session_category, purchases FROM sst_sessions_categorized
  UNION ALL
  SELECT session_category, purchases FROM direct_sessions_categorized
)
WHERE session_category != 'Both'  -- Avoid double-counting 'Both'
  OR session_category IS NULL
GROUP BY session_category;

-- ============================================================================
-- ISSUE: BigQuery Integration
-- ============================================================================
-- The above query requires joining Athena (SST) with BigQuery (Direct).
-- Athena cannot directly query BigQuery.
--
-- Solutions:
-- 1. Export BigQuery data to S3, query from Athena
-- 2. Use federated query (AWS Athena Federated Query)
-- 3. Pre-compute matches in Python, store results in Athena table
-- 4. Keep matching logic in Python/Streamlit app (current approach)
--
-- For dashboard: Option 4 is simplest and most flexible.
-- ============================================================================
