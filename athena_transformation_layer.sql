-- ============================================================================
-- SST to BigQuery Reconciliation Layer
-- ============================================================================
-- Purpose: Transform SST data so dimension values match BigQuery exactly.
-- This enables session-level reconciliation via JOIN on ga_session_id.
--
-- GOAL: After transformation, differences between SST and Direct are REAL
-- (ad-blockers, network) not ARTIFACTS (parsing, naming, filtering).
--
-- Key dimensions for reconciliation:
-- - device_category (desktop/mobile/tablet)
-- - device_browser (Chrome/Safari/Edge/Firefox/Samsung Internet)
-- - device_operating_system (Windows/iOS/Macintosh/Android/Linux)
-- - geo_country (full name, e.g., "Australia" not "AU")
--
-- Version: 3.4 (2026-01-22)
-- ============================================================================

CREATE OR REPLACE VIEW warwick_weave_sst_events.sst_events_transformed AS
WITH parsed_payload AS (
    SELECT
        timestamp,
        event_name,
        ip_address,
        year,
        month,
        day,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.client_id') AS client_id,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id') AS ga_session_id,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.user_agent') AS user_agent,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.page_location') AS page_location,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.page_title') AS page_title,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.page_referrer') AS page_referrer,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.language') AS language,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.screen_resolution') AS screen_resolution,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$["x-ga-measurement_id"]') AS measurement_id,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.event_location.country') AS geo_country_code,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.event_location.region') AS geo_region,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.event_location.city') AS geo_city,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.transaction_id') AS transaction_id,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.value') AS ecommerce_value,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.currency') AS ecommerce_currency,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.source') AS traffic_source,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.medium') AS traffic_medium,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.campaign') AS traffic_campaign
    FROM warwick_weave_sst_events.events
),

with_bot_detection AS (
    SELECT
        *,
        -- Conservative bot detection to match BigQuery's IAB/ABC filtering
        CASE
            WHEN user_agent IS NULL OR user_agent = '' THEN TRUE
            WHEN user_agent LIKE '%Googlebot%' THEN TRUE
            WHEN user_agent LIKE '%bingbot%' THEN TRUE
            WHEN user_agent LIKE '%Baiduspider%' THEN TRUE
            WHEN user_agent LIKE '%YandexBot%' THEN TRUE
            WHEN user_agent LIKE '%DuckDuckBot%' THEN TRUE
            WHEN user_agent LIKE '%facebookexternalhit%' THEN TRUE
            WHEN user_agent LIKE '%Twitterbot%' THEN TRUE
            WHEN user_agent LIKE '%LinkedInBot%' THEN TRUE
            WHEN user_agent LIKE '%Slackbot%' THEN TRUE
            WHEN user_agent LIKE '%AhrefsBot%' THEN TRUE
            WHEN user_agent LIKE '%SemrushBot%' THEN TRUE
            WHEN user_agent LIKE '%MJ12bot%' THEN TRUE
            WHEN user_agent LIKE '%HeadlessChrome%' THEN TRUE
            WHEN user_agent LIKE '%PhantomJS%' THEN TRUE
            WHEN user_agent LIKE '%python-requests%' THEN TRUE
            WHEN user_agent LIKE '%curl/%' THEN TRUE
            WHEN user_agent LIKE '%wget%' THEN TRUE
            WHEN user_agent LIKE '%bot/%' THEN TRUE
            WHEN user_agent LIKE '%Bot/%' THEN TRUE
            WHEN user_agent LIKE '%crawler%' THEN TRUE
            WHEN user_agent LIKE '%spider%' THEN TRUE
            ELSE FALSE
        END AS is_bot
    FROM parsed_payload
)
SELECT
    timestamp,
    event_name,
    measurement_id,
    client_id AS user_pseudo_id,
    ga_session_id,
    is_bot,

    -- =========================================================================
    -- DEVICE CATEGORY (must match BigQuery device.category)
    -- BigQuery uses: desktop, mobile, tablet
    -- Parsed from User-Agent, NOT client_hints (Safari/Firefox don't send those)
    -- =========================================================================
    CASE
        -- Tablets first
        WHEN user_agent LIKE '%iPad%' THEN 'tablet'
        WHEN user_agent LIKE '%Android%' AND user_agent NOT LIKE '%Mobile%' THEN 'tablet'
        -- Mobile phones
        WHEN user_agent LIKE '%iPhone%' THEN 'mobile'
        WHEN user_agent LIKE '%iPod%' THEN 'mobile'
        WHEN user_agent LIKE '%Android%' AND user_agent LIKE '%Mobile%' THEN 'mobile'
        WHEN user_agent LIKE '%Windows Phone%' THEN 'mobile'
        WHEN user_agent LIKE '%BlackBerry%' THEN 'mobile'
        WHEN user_agent LIKE '%Opera Mini%' THEN 'mobile'
        WHEN user_agent LIKE '%IEMobile%' THEN 'mobile'
        -- Desktop (default, matches BigQuery behavior)
        ELSE 'desktop'
    END AS device_category,

    -- =========================================================================
    -- OPERATING SYSTEM (must match BigQuery device.operating_system)
    -- BigQuery uses: Windows, Macintosh, iOS, Android, Linux, Chrome OS, (not set)
    -- =========================================================================
    CASE
        WHEN user_agent LIKE '%iPhone%' OR user_agent LIKE '%iPad%' OR user_agent LIKE '%iPod%' THEN 'iOS'
        WHEN user_agent LIKE '%Android%' THEN 'Android'
        WHEN user_agent LIKE '%Windows Phone%' THEN 'Windows Phone'
        WHEN user_agent LIKE '%CrOS%' THEN 'Chrome OS'
        WHEN user_agent LIKE '%Windows%' THEN 'Windows'
        WHEN user_agent LIKE '%Macintosh%' THEN 'Macintosh'
        WHEN user_agent LIKE '%Linux%' THEN 'Linux'
        ELSE '(not set)'
    END AS device_operating_system,

    -- =========================================================================
    -- BROWSER (must match BigQuery device.web_info.browser)
    -- BigQuery uses: Chrome, Safari, Edge, Firefox, Samsung Internet, Safari (in-app), etc.
    -- Order matters: Edge/Opera contain "Chrome", Chrome contains "Safari"
    -- =========================================================================
    CASE
        WHEN user_agent LIKE '%Edg/%' OR user_agent LIKE '%Edge/%' THEN 'Edge'
        WHEN user_agent LIKE '%OPR/%' OR user_agent LIKE '%Opera%' THEN 'Opera'
        WHEN user_agent LIKE '%SamsungBrowser%' THEN 'Samsung Internet'
        WHEN user_agent LIKE '%Firefox/%' THEN 'Firefox'
        WHEN user_agent LIKE '%CriOS%' THEN 'Chrome'
        WHEN user_agent LIKE '%Chrome/%' THEN 'Chrome'
        WHEN user_agent LIKE '%Safari/%' THEN 'Safari'
        WHEN user_agent LIKE '%MSIE%' OR user_agent LIKE '%Trident%' THEN 'Internet Explorer'
        -- In-app browsers on iOS (Facebook, Instagram, etc.) - no Safari/ but has Mobile/
        WHEN (user_agent LIKE '%iPhone%' OR user_agent LIKE '%iPad%')
             AND user_agent LIKE '%Mobile/%'
             AND user_agent NOT LIKE '%Safari/%' THEN 'Safari (in-app)'
        ELSE '(not set)'
    END AS device_browser,

    -- =========================================================================
    -- COUNTRY (must match BigQuery geo.country)
    -- BigQuery uses full names, SST has ISO codes from CloudFront
    -- =========================================================================
    geo_country_code,
    CASE geo_country_code
        -- Oceania (Warwick's primary markets)
        WHEN 'AU' THEN 'Australia'
        WHEN 'NZ' THEN 'New Zealand'
        WHEN 'FJ' THEN 'Fiji'
        WHEN 'NC' THEN 'New Caledonia'
        WHEN 'PG' THEN 'Papua New Guinea'
        -- Asia
        WHEN 'CN' THEN 'China'
        WHEN 'HK' THEN 'Hong Kong'
        WHEN 'TW' THEN 'Taiwan'
        WHEN 'JP' THEN 'Japan'
        WHEN 'KR' THEN 'South Korea'
        WHEN 'IN' THEN 'India'
        WHEN 'PK' THEN 'Pakistan'
        WHEN 'BD' THEN 'Bangladesh'
        WHEN 'LK' THEN 'Sri Lanka'
        WHEN 'VN' THEN 'Vietnam'
        WHEN 'TH' THEN 'Thailand'
        WHEN 'MY' THEN 'Malaysia'
        WHEN 'SG' THEN 'Singapore'
        WHEN 'ID' THEN 'Indonesia'
        WHEN 'PH' THEN 'Philippines'
        -- Europe
        WHEN 'GB' THEN 'United Kingdom'
        WHEN 'IE' THEN 'Ireland'
        WHEN 'FR' THEN 'France'
        WHEN 'DE' THEN 'Germany'
        WHEN 'NL' THEN 'Netherlands'
        WHEN 'BE' THEN 'Belgium'
        WHEN 'CH' THEN 'Switzerland'
        WHEN 'AT' THEN 'Austria'
        WHEN 'SE' THEN 'Sweden'
        WHEN 'NO' THEN 'Norway'
        WHEN 'DK' THEN 'Denmark'
        WHEN 'FI' THEN 'Finland'
        WHEN 'ES' THEN 'Spain'
        WHEN 'PT' THEN 'Portugal'
        WHEN 'IT' THEN 'Italy'
        WHEN 'GR' THEN 'Greece'
        WHEN 'PL' THEN 'Poland'
        WHEN 'CZ' THEN 'Czechia'
        WHEN 'RU' THEN 'Russia'
        WHEN 'UA' THEN 'Ukraine'
        WHEN 'RS' THEN 'Serbia'
        WHEN 'HR' THEN 'Croatia'
        WHEN 'RO' THEN 'Romania'
        WHEN 'HU' THEN 'Hungary'
        -- Americas
        WHEN 'US' THEN 'United States'
        WHEN 'CA' THEN 'Canada'
        WHEN 'MX' THEN 'Mexico'
        WHEN 'BR' THEN 'Brazil'
        WHEN 'AR' THEN 'Argentina'
        WHEN 'CL' THEN 'Chile'
        WHEN 'CO' THEN 'Colombia'
        WHEN 'PE' THEN 'Peru'
        -- Middle East
        WHEN 'AE' THEN 'United Arab Emirates'
        WHEN 'SA' THEN 'Saudi Arabia'
        WHEN 'IL' THEN 'Israel'
        WHEN 'TR' THEN 'Turkey'
        WHEN 'JO' THEN 'Jordan'
        WHEN 'QA' THEN 'Qatar'
        -- Africa
        WHEN 'ZA' THEN 'South Africa'
        WHEN 'EG' THEN 'Egypt'
        WHEN 'NG' THEN 'Nigeria'
        WHEN 'KE' THEN 'Kenya'
        WHEN 'ZW' THEN 'Zimbabwe'
        WHEN 'MG' THEN 'Madagascar'
        -- Fallback: return code if unmapped (allows tracking gaps)
        ELSE COALESCE(geo_country_code, '(not set)')
    END AS geo_country,

    geo_region,
    geo_city,
    user_agent,
    page_location,
    page_title,
    page_referrer,
    language,
    screen_resolution,

    -- Site identifier for filtering
    CASE
        WHEN page_location LIKE '%warwick.com.au%' THEN 'AU'
        WHEN page_location LIKE '%warwick.co.nz%' THEN 'NZ'
        ELSE 'Other'
    END AS site,

    -- Ecommerce
    transaction_id,
    CAST(ecommerce_value AS DOUBLE) AS ecommerce_value,
    ecommerce_currency,

    -- Traffic source
    traffic_source,
    traffic_medium,
    traffic_campaign,

    -- Synthetic event markers
    CASE WHEN event_name IN ('session_start', 'first_visit') THEN TRUE ELSE FALSE END AS is_synthetic_event,
    CASE WHEN event_name = 'add_to_cart_click_fallback' THEN TRUE ELSE FALSE END AS is_fallback_event,

    -- Partition columns
    year,
    month,
    day,
    ip_address,

    -- Quality flag for filtering
    CASE
        WHEN is_bot THEN FALSE
        WHEN user_agent IS NULL OR user_agent = '' THEN FALSE
        WHEN ga_session_id IS NULL THEN FALSE
        ELSE TRUE
    END AS is_likely_human

FROM with_bot_detection
WHERE measurement_id = 'G-Y0RSKRWP87';


-- ============================================================================
-- VIEW 2: sst_sessions_daily
-- Daily aggregates by key dimensions for trend comparison
-- ============================================================================

CREATE OR REPLACE VIEW warwick_weave_sst_events.sst_sessions_daily AS
SELECT
    CAST(CONCAT(year, '-', month, '-', day) AS DATE) AS date,
    site,
    device_category,
    device_operating_system,
    device_browser,
    geo_country,
    COUNT(DISTINCT ga_session_id) AS sessions,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNT(*) AS events,
    COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS pageviews,
    COUNT(CASE WHEN event_name = 'purchase' THEN 1 END) AS purchases
FROM warwick_weave_sst_events.sst_events_transformed
WHERE NOT is_synthetic_event
  AND NOT is_fallback_event
  AND is_likely_human
GROUP BY 1, 2, 3, 4, 5, 6;


-- ============================================================================
-- VIEW 3: sst_comparison_ready
-- Filtered view for AU SST vs Direct comparisons
-- ============================================================================

CREATE OR REPLACE VIEW warwick_weave_sst_events.sst_comparison_ready AS
SELECT *
FROM warwick_weave_sst_events.sst_events_transformed
WHERE NOT is_synthetic_event
  AND NOT is_fallback_event
  AND is_likely_human
  AND ga_session_id IS NOT NULL
  AND site = 'AU';


-- ============================================================================
-- VIEW 4: sst_sessions
-- Session-level rollup for JOIN-based reconciliation with BigQuery
-- ============================================================================

CREATE OR REPLACE VIEW warwick_weave_sst_events.sst_sessions AS
SELECT
    ga_session_id,
    user_pseudo_id,
    MIN(timestamp) AS session_start,
    MAX(timestamp) AS session_end,
    ARBITRARY(device_category) AS device_category,
    ARBITRARY(device_browser) AS device_browser,
    ARBITRARY(device_operating_system) AS device_operating_system,
    ARBITRARY(geo_country) AS geo_country,
    ARBITRARY(geo_country_code) AS geo_country_code,
    ARBITRARY(site) AS site,
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
GROUP BY ga_session_id, user_pseudo_id;
