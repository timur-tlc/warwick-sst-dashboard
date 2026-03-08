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
-- Version: 3.10 (2026-03-01) - Map geo_region to full names (AU/NZ), infer Weave brand for NULL item_brand in Weave-exclusive categories
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
        COALESCE(json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.transaction_id'), json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.transaction_id')) AS transaction_id,
        COALESCE(json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.value'), json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.value')) AS ecommerce_value,
        COALESCE(json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.currency'), json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ecommerce.currency')) AS ecommerce_currency,
        json_extract(from_utf8(from_base64(raw_payload)), '$.items') AS items_json,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.source') AS traffic_source,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.medium') AS traffic_medium,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.traffic_source.campaign') AS traffic_campaign,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.engagement_time_msec') AS engagement_time_msec,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.link_text') AS link_text,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.link_url') AS link_url,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.search_term') AS search_term,
        json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.client_hints.model') AS client_hints_model
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
    -- DEVICE BRAND (must match BigQuery device.mobile_brand_name)
    -- GA4 uses a full device database (WURFL); this covers the major brands
    -- visible in Warwick's traffic.
    -- v3.9: Check Client Hints model first (Android Chrome sends this),
    -- then fall back to User-Agent parsing (Safari/Firefox/older browsers).
    -- Chrome UA reduction replaces Android models with "K", making UA
    -- useless for brand detection on ~58% of Android traffic.
    -- =========================================================================
    CASE
        -- Apple devices (iOS + macOS) — always identifiable from UA
        WHEN user_agent LIKE '%iPhone%' OR user_agent LIKE '%iPad%' OR user_agent LIKE '%iPod%' OR user_agent LIKE '%Macintosh%' THEN 'Apple'
        -- Desktop: Windows = Microsoft, Chrome OS = Google
        WHEN user_agent LIKE '%Windows%' THEN 'Microsoft'
        WHEN user_agent LIKE '%CrOS%' THEN 'Google'

        -- Client Hints model (Android Chrome populates this)
        WHEN client_hints_model IS NOT NULL AND client_hints_model != '' THEN
            CASE
                WHEN client_hints_model LIKE 'SM-%' OR client_hints_model LIKE 'Galaxy%' THEN 'Samsung'
                WHEN client_hints_model LIKE 'Pixel%' THEN 'Google'
                WHEN client_hints_model LIKE 'CPH%' OR client_hints_model LIKE 'RMX%' THEN 'Oppo'
                WHEN client_hints_model LIKE 'moto%' OR client_hints_model LIKE 'Moto%' OR client_hints_model LIKE 'XT%' THEN 'Motorola'
                WHEN client_hints_model LIKE 'LM-%' OR client_hints_model LIKE 'LG%' THEN 'LG'
                WHEN client_hints_model LIKE 'V%' AND client_hints_model LIKE '%5G%' THEN 'Vivo'
                WHEN client_hints_model LIKE '220%' OR client_hints_model LIKE '230%' OR client_hints_model LIKE '240%' THEN 'Xiaomi'
                WHEN client_hints_model LIKE 'IN%' THEN 'Micromax'
                WHEN client_hints_model LIKE 'Nokia%' THEN 'Nokia'
                WHEN client_hints_model LIKE 'HUAWEI%' OR client_hints_model LIKE 'VOG-%' OR client_hints_model LIKE 'ELS-%' THEN 'Huawei'
                WHEN client_hints_model LIKE 'SAMSUNG%' THEN 'Samsung'
                WHEN client_hints_model LIKE 'Redmi%' OR client_hints_model LIKE 'POCO%' OR client_hints_model LIKE 'M2%' OR client_hints_model LIKE '2201%' OR client_hints_model LIKE '2210%' THEN 'Xiaomi'
                WHEN client_hints_model LIKE 'OnePlus%' OR client_hints_model LIKE 'OPPO%' THEN 'Oppo'
                WHEN client_hints_model LIKE 'Sony%' OR client_hints_model LIKE 'XQ-%' THEN 'Sony'
                ELSE 'Android (other)'
            END

        -- Fallback: UA-based parsing for Safari/Firefox/older browsers (no Client Hints)
        WHEN user_agent LIKE '%SamsungBrowser%' OR user_agent LIKE '%SM-%' OR user_agent LIKE '%Samsung%' OR user_agent LIKE '%SAMSUNG%' THEN 'Samsung'
        WHEN user_agent LIKE '%Pixel%' OR user_agent LIKE '%Nexus%' THEN 'Google'
        WHEN user_agent LIKE '%Huawei%' OR user_agent LIKE '%HUAWEI%' OR user_agent LIKE '%HMSCore%' THEN 'Huawei'
        WHEN user_agent LIKE '%Xiaomi%' OR user_agent LIKE '%Redmi%' OR user_agent LIKE '%POCO%' OR user_agent LIKE '%Mi %' THEN 'Xiaomi'
        WHEN user_agent LIKE '%OPPO%' OR user_agent LIKE '%OnePlus%' OR user_agent LIKE '%Realme%' OR user_agent LIKE '%RMX%' THEN 'Oppo'
        WHEN user_agent LIKE '%Motorola%' OR user_agent LIKE '%moto %' THEN 'Motorola'
        WHEN user_agent LIKE '%LG-%' OR user_agent LIKE '%LG/%' THEN 'LG'
        WHEN user_agent LIKE '%Sony%' OR user_agent LIKE '%Xperia%' THEN 'Sony'
        -- Generic Linux desktop
        WHEN user_agent LIKE '%Linux%' AND user_agent NOT LIKE '%Android%' THEN '(not set)'
        -- Remaining Android devices without identifiable brand
        WHEN user_agent LIKE '%Android%' THEN '(not set)'
        ELSE '(not set)'
    END AS device_brand,

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
        WHEN 'ET' THEN 'Ethiopia'
        WHEN 'UG' THEN 'Uganda'
        WHEN 'CM' THEN 'Cameroon'
        WHEN 'DZ' THEN 'Algeria'
        WHEN 'TN' THEN 'Tunisia'
        WHEN 'MU' THEN 'Mauritius'
        -- Middle East (additional)
        WHEN 'BH' THEN 'Bahrain'
        WHEN 'IR' THEN 'Iran'
        WHEN 'IQ' THEN 'Iraq'
        WHEN 'OM' THEN 'Oman'
        WHEN 'LB' THEN 'Lebanon'
        -- Asia (additional)
        WHEN 'NP' THEN 'Nepal'
        WHEN 'KH' THEN 'Cambodia'
        WHEN 'MO' THEN 'Macao'
        WHEN 'MV' THEN 'Maldives'
        -- Europe (additional)
        WHEN 'LT' THEN 'Lithuania'
        WHEN 'BG' THEN 'Bulgaria'
        WHEN 'IS' THEN 'Iceland'
        WHEN 'MT' THEN 'Malta'
        WHEN 'SK' THEN 'Slovakia'
        WHEN 'LU' THEN 'Luxembourg'
        WHEN 'GE' THEN 'Georgia'
        WHEN 'JE' THEN 'Jersey'
        WHEN 'XK' THEN 'Kosovo'
        -- Americas (additional)
        WHEN 'TT' THEN 'Trinidad and Tobago'
        WHEN 'GY' THEN 'Guyana'
        WHEN 'PR' THEN 'Puerto Rico'
        WHEN 'EC' THEN 'Ecuador'
        WHEN 'NI' THEN 'Nicaragua'
        -- Fallback: return code if unmapped (allows tracking gaps)
        ELSE COALESCE(geo_country_code, '(not set)')
    END AS geo_country,

    -- =========================================================================
    -- REGION (must match BigQuery geo.region)
    -- BigQuery uses full names; SST has abbreviations from CloudFront.
    -- AU = 2-3 letter state codes, NZ = 3-letter ISO codes,
    -- international = numeric ISO 3166-2 codes (left as-is).
    -- =========================================================================
    CASE
        -- Australia (states/territories)
        WHEN geo_country_code = 'AU' THEN CASE geo_region
            WHEN 'NSW' THEN 'New South Wales'
            WHEN 'VIC' THEN 'Victoria'
            WHEN 'QLD' THEN 'Queensland'
            WHEN 'WA' THEN 'Western Australia'
            WHEN 'SA' THEN 'South Australia'
            WHEN 'TAS' THEN 'Tasmania'
            WHEN 'ACT' THEN 'Australian Capital Territory'
            WHEN 'NT' THEN 'Northern Territory'
            ELSE COALESCE(geo_region, '(not set)')
        END
        -- New Zealand (ISO 3166-2:NZ codes)
        WHEN geo_country_code = 'NZ' THEN CASE geo_region
            WHEN 'AUK' THEN 'Auckland'
            WHEN 'BOP' THEN 'Bay of Plenty'
            WHEN 'CAN' THEN 'Canterbury'
            WHEN 'GIS' THEN 'Gisborne'
            WHEN 'HKB' THEN 'Hawke''s Bay'
            WHEN 'MBH' THEN 'Marlborough'
            WHEN 'MWT' THEN 'Manawatu-Wanganui'
            WHEN 'NSN' THEN 'Nelson'
            WHEN 'NTL' THEN 'Northland'
            WHEN 'OTA' THEN 'Otago'
            WHEN 'STL' THEN 'Southland'
            WHEN 'TAS' THEN 'Tasman'
            WHEN 'TKI' THEN 'Taranaki'
            WHEN 'WGN' THEN 'Wellington'
            WHEN 'WKO' THEN 'Waikato'
            WHEN 'WTC' THEN 'West Coast'
            ELSE COALESCE(geo_region, '(not set)')
        END
        -- All other countries: pass through raw code
        ELSE COALESCE(geo_region, '(not set)')
    END AS geo_region,
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
    TRY_CAST(ecommerce_value AS DOUBLE) AS ecommerce_value,
    ecommerce_currency,
    json_format(items_json) AS items_json,

    -- Traffic source (raw fields are always NULL - GTM server doesn't forward these)
    traffic_source,
    traffic_medium,
    traffic_campaign,

    -- =========================================================================
    -- SESSION DEFAULT CHANNEL GROUP (derived from page_referrer)
    -- SST does not receive traffic_source/medium from the GA4 client tag.
    -- This approximation uses the referring domain to classify channels.
    -- Limitation: Cannot distinguish Paid Search from Organic Search,
    -- or Paid Social from Organic Social (no UTM params in SST payload).
    -- =========================================================================
    CASE
        -- Self-referral (internal navigation) — will be attributed at session level
        WHEN page_referrer LIKE '%warwick.com.au%' OR page_referrer LIKE '%warwick.co.nz%' OR page_referrer LIKE '%weavehome.com.au%' THEN NULL
        -- No referrer = Direct
        WHEN page_referrer IS NULL OR page_referrer = '' THEN 'Direct'
        -- Search engines → Organic Search
        WHEN page_referrer LIKE '%google.%' AND page_referrer NOT LIKE '%mail.google.%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%bing.com%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%yahoo.%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%duckduckgo.com%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%ecosia.org%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%baidu.com%' THEN 'Organic Search'
        WHEN page_referrer LIKE '%naver.com%' THEN 'Organic Search'
        -- Social media → Organic Social
        WHEN page_referrer LIKE '%facebook.com%' OR page_referrer LIKE '%fb.com%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%instagram.com%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%pinterest.com%' OR page_referrer LIKE '%pin.it%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%linkedin.com%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%twitter.com%' OR page_referrer LIKE '%t.co%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%youtube.com%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%reddit.com%' THEN 'Organic Social'
        WHEN page_referrer LIKE '%tiktok.com%' THEN 'Organic Social'
        -- Email providers → Email
        WHEN page_referrer LIKE '%mail.google.com%' THEN 'Email'
        WHEN page_referrer LIKE '%outlook.%' OR page_referrer LIKE '%office.%' OR page_referrer LIKE '%teams.%' THEN 'Email'
        -- Everything else → Referral
        ELSE 'Referral'
    END AS session_default_channel_group_event,

    -- Synthetic event markers
    CASE WHEN event_name IN ('session_start', 'first_visit') THEN TRUE ELSE FALSE END AS is_synthetic_event,
    CASE WHEN event_name = 'add_to_cart_click_fallback' THEN TRUE ELSE FALSE END AS is_fallback_event,

    -- Partition columns
    year,
    month,
    day,
    ip_address,

    -- Engagement time (from GA4 payload)
    CAST(engagement_time_msec AS BIGINT) AS engagement_time_msec,

    -- Event parameters (for file_download, click, view_search_results)
    link_text,
    link_url,
    search_term,

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
    ARBITRARY(device_brand) AS device_brand,
    ARBITRARY(geo_country) AS geo_country,
    ARBITRARY(geo_country_code) AS geo_country_code,
    ARBITRARY(site) AS site,
    -- Session channel: first non-null channel event (by timestamp), fallback to Direct
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
GROUP BY ga_session_id, user_pseudo_id;


-- ============================================================================
-- VIEW 5: sst_ecommerce_items
-- Item-level ecommerce data (brand, category, price, quantity)
-- Unnests the items array from purchase and other ecommerce events
-- ============================================================================

CREATE OR REPLACE VIEW warwick_weave_sst_events.sst_ecommerce_items AS
SELECT
    timestamp,
    event_name,
    ga_session_id,
    user_pseudo_id,
    transaction_id,
    ecommerce_value,
    ecommerce_currency,
    json_extract_scalar(item, '$.item_id') AS item_id,
    json_extract_scalar(item, '$.item_name') AS item_name,
    -- Infer Weave brand for items in Weave-exclusive categories where item_brand is missing.
    -- ~75% of Weave revenue has NULL item_brand due to dataLayer bug (see gotcha #38).
    -- These categories (Cushions, Bed Sheets, Floor Rugs, Throws, Cushion Inners) are
    -- exclusively Weave products — Warwick only sells fabric (Upholstery/Drapery).
    CASE
        WHEN json_extract_scalar(item, '$.item_brand') IS NULL
             AND json_extract_scalar(item, '$.item_category') IN ('Cushions', 'Bed Sheets', 'Floor Rugs', 'Throws', 'Cushion Inners')
        THEN 'Weave'
        ELSE json_extract_scalar(item, '$.item_brand')
    END AS item_brand,
    json_extract_scalar(item, '$.item_category') AS item_category,
    json_extract_scalar(item, '$.item_category2') AS item_category2,
    json_extract_scalar(item, '$.item_category3') AS item_category3,
    json_extract_scalar(item, '$.item_variant') AS item_variant,
    CAST(json_extract_scalar(item, '$.price') AS DOUBLE) AS price,
    CAST(json_extract_scalar(item, '$.quantity') AS DOUBLE) AS quantity
FROM warwick_weave_sst_events.sst_events_transformed
CROSS JOIN UNNEST(CAST(json_parse(items_json) AS ARRAY(JSON))) AS t(item)
WHERE items_json IS NOT NULL
  AND is_likely_human;
