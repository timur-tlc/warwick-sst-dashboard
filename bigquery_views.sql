-- BigQuery _ga4 view definitions
-- Auto-exported from BigQuery on 2026-03-03
-- These views are defined in BigQuery, not in Git.
-- This file is for version control and reference only.

-- ============================================================
-- VIEW: sessions_ga4
-- ============================================================
CREATE OR REPLACE VIEW `376132452327.sst_events.sessions_ga4` AS

SELECT
    CAST(s.ga_session_id AS STRING) AS sessionId,
    s.user_pseudo_id AS userPseudoId,

    PARSE_DATE('%Y-%m-%d', SUBSTR(s.session_start, 1, 10)) AS date,
    s.session_start AS sessionStart,
    s.session_end AS sessionEnd,
    s.year AS year,
    s.month AS month,
    s.day AS day,

    s.device_category AS deviceCategory,
    s.device_browser AS browser,
    s.device_operating_system AS operatingSystem,
    COALESCE(s.device_brand, '(not set)') AS deviceBrand,

    s.geo_country AS country,
    s.geo_country_code AS countryId,

    CASE s.geo_region
        WHEN 'NSW' THEN 'New South Wales'
        WHEN 'VIC' THEN 'Victoria'
        WHEN 'QLD' THEN 'Queensland'
        WHEN 'WA' THEN 'Western Australia'
        WHEN 'SA' THEN 'South Australia'
        WHEN 'TAS' THEN 'Tasmania'
        WHEN 'ACT' THEN 'Australian Capital Territory'
        WHEN 'NT' THEN 'Northern Territory'
        WHEN 'AUK' THEN 'Auckland'
        WHEN 'WGN' THEN 'Wellington'
        WHEN 'CAN' THEN 'Canterbury'
        WHEN 'WKO' THEN 'Waikato'
        WHEN 'BOP' THEN 'Bay of Plenty'
        WHEN 'OTA' THEN 'Otago'
        WHEN 'CA' THEN 'California'
        WHEN 'NY' THEN 'New York'
        WHEN 'TX' THEN 'Texas'
        WHEN 'FL' THEN 'Florida'
        WHEN 'IL' THEN 'Illinois'
        WHEN 'OR' THEN 'Oregon'
        WHEN 'MA' THEN 'Massachusetts'
        WHEN 'NJ' THEN 'New Jersey'
        WHEN 'PA' THEN 'Pennsylvania'
        WHEN 'OH' THEN 'Ohio'
        WHEN 'GA' THEN 'Georgia'
        WHEN 'NC' THEN 'North Carolina'
        WHEN 'VA' THEN 'Virginia'
        WHEN 'MI' THEN 'Michigan'
        WHEN 'CO' THEN 'Colorado'
        WHEN 'MN' THEN 'Minnesota'
        WHEN 'MO' THEN 'Missouri'
        WHEN 'MD' THEN 'Maryland'
        WHEN 'AZ' THEN 'Arizona'
        WHEN 'IN' THEN 'Indiana'
        WHEN 'TN' THEN 'Tennessee'
        WHEN 'CT' THEN 'Connecticut'
        WHEN 'WI' THEN 'Wisconsin'
        WHEN 'ENG' THEN 'England'
        WHEN 'SCT' THEN 'Scotland'
        WHEN 'WLS' THEN 'Wales'
        WHEN 'NIR' THEN 'Northern Ireland'
        WHEN 'None' THEN '(not set)'
        WHEN '' THEN '(not set)'
        ELSE COALESCE(s.geo_region, '(not set)')
    END AS region,
    s.site AS site,
    COALESCE(s.session_default_channel_group, 'Direct') AS sessionDefaultChannelGroup,

    1 AS sessions,
    COALESCE(s.event_count, 0) AS eventCount,
    COALESCE(s.pageviews, 0) AS screenPageViews,
    COALESCE(s.pageviews, 0) AS screenPageViewsPerSession,
    COALESCE(s.purchases, 0) AS transactions,
    COALESCE(s.purchase_value, 0.0) AS purchaseRevenue,
    COALESCE(s.purchase_value, 0.0) AS totalRevenue,

    CASE WHEN s.purchases > 0 THEN 1 ELSE 0 END AS totalPurchasers,
    1 AS totalUsers,
    1 AS activeUsers,
    COALESCE(s.new_user, 0) AS newUsers,

    COALESCE(eng.engagement_time_sec, 0.0) AS userEngagementDuration,
    COALESCE(eng.engagement_time_sec, 0.0) AS averageSessionDuration,
    COALESCE(eng.has_scroll, 0) AS scrolledUsers,
    CASE WHEN COALESCE(eng.engagement_time_sec, 0.0) > 0 THEN 1 ELSE 0 END AS engagedSessions,
    COALESCE(s.purchases, 0) AS ecommercePurchases,
    CASE WHEN s.purchase_value > 0 THEN 1 ELSE 0 END AS ecommercePurchasesExCuttings,

    COALESCE(cjk.is_cjk_spam, FALSE) AS isCjkSpam

FROM `376132452327.sst_events.sessions` s
LEFT JOIN (
    SELECT
        CAST(ga_session_id AS STRING) AS ga_session_id,
        user_pseudo_id,
        COALESCE(SUM(engagement_time_msec), 0) / 1000.0 AS engagement_time_sec,
        MAX(CASE WHEN event_name = 'scroll' THEN 1 ELSE 0 END) AS has_scroll
    FROM `376132452327.sst_events.events`
    GROUP BY ga_session_id, user_pseudo_id
) eng
ON s.ga_session_id = eng.ga_session_id
AND s.user_pseudo_id = eng.user_pseudo_id
LEFT JOIN (
    SELECT
        ga_session_id,
        user_pseudo_id,
        TRUE AS is_cjk_spam
    FROM `376132452327.sst_events.events`
    WHERE REGEXP_CONTAINS(
        CONCAT(
            COALESCE(search_term, ''), ' ',
            COALESCE(page_title, ''), ' ',
            COALESCE(link_text, '')
        ),
        r'[一-鿿぀-ゟ゠-ヿ]'
    )
    GROUP BY ga_session_id, user_pseudo_id
) cjk
ON s.ga_session_id = cjk.ga_session_id
AND s.user_pseudo_id = cjk.user_pseudo_id
;

-- ============================================================
-- VIEW: events_ga4
-- ============================================================
CREATE OR REPLACE VIEW `376132452327.sst_events.events_ga4` AS

SELECT
    e.event_name AS eventName,
    REGEXP_EXTRACT(e.page_location, r'^https?://[^/]+(/[^?#]*)') AS pagePath,
    e.page_location AS pageLocation,
    e.page_title AS pageTitle,
    CAST(e.ga_session_id AS STRING) AS sessionId,
    e.user_pseudo_id AS userPseudoId,

    e.device_category AS deviceCategory,
    e.device_browser AS browser,
    e.device_operating_system AS operatingSystem,

    e.geo_country AS country,

    CASE e.geo_region
        WHEN 'NSW' THEN 'New South Wales'
        WHEN 'VIC' THEN 'Victoria'
        WHEN 'QLD' THEN 'Queensland'
        WHEN 'WA' THEN 'Western Australia'
        WHEN 'SA' THEN 'South Australia'
        WHEN 'TAS' THEN 'Tasmania'
        WHEN 'ACT' THEN 'Australian Capital Territory'
        WHEN 'NT' THEN 'Northern Territory'
        WHEN 'AUK' THEN 'Auckland'
        WHEN 'WGN' THEN 'Wellington'
        WHEN 'CAN' THEN 'Canterbury'
        WHEN 'WKO' THEN 'Waikato'
        WHEN 'BOP' THEN 'Bay of Plenty'
        WHEN 'OTA' THEN 'Otago'
        WHEN 'CA' THEN 'California'
        WHEN 'NY' THEN 'New York'
        WHEN 'TX' THEN 'Texas'
        WHEN 'FL' THEN 'Florida'
        WHEN 'IL' THEN 'Illinois'
        WHEN 'OR' THEN 'Oregon'
        WHEN 'MA' THEN 'Massachusetts'
        WHEN 'NJ' THEN 'New Jersey'
        WHEN 'PA' THEN 'Pennsylvania'
        WHEN 'OH' THEN 'Ohio'
        WHEN 'GA' THEN 'Georgia'
        WHEN 'NC' THEN 'North Carolina'
        WHEN 'VA' THEN 'Virginia'
        WHEN 'MI' THEN 'Michigan'
        WHEN 'CO' THEN 'Colorado'
        WHEN 'MN' THEN 'Minnesota'
        WHEN 'MO' THEN 'Missouri'
        WHEN 'MD' THEN 'Maryland'
        WHEN 'AZ' THEN 'Arizona'
        WHEN 'IN' THEN 'Indiana'
        WHEN 'TN' THEN 'Tennessee'
        WHEN 'CT' THEN 'Connecticut'
        WHEN 'WI' THEN 'Wisconsin'
        WHEN 'ENG' THEN 'England'
        WHEN 'SCT' THEN 'Scotland'
        WHEN 'WLS' THEN 'Wales'
        WHEN 'NIR' THEN 'Northern Ireland'
        WHEN 'None' THEN '(not set)'
        WHEN '' THEN '(not set)'
        ELSE COALESCE(e.geo_region, '(not set)')
    END AS region,
    e.site AS site,

    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', e.timestamp) AS eventTimestamp,
    PARSE_DATE('%Y-%m-%d', SUBSTR(e.timestamp, 1, 10)) AS date,
    e.year AS year,
    e.month AS month,
    e.day AS day,

    COALESCE(CAST(e.engagement_time_msec AS INT64), 0) AS engagementTimeMsec,
    COALESCE(e.ecommerce_value, 0.0) AS purchaseRevenue,
    COALESCE(e.ecommerce_value, 0.0) AS itemRevenue,
    CAST(e.transaction_id AS STRING) AS transactionId,
    e.is_likely_human AS isLikelyHuman,

    1 AS eventCount,
    CASE WHEN e.event_name = 'page_view' THEN 1 ELSE 0 END AS isPageView,
    CASE WHEN e.event_name = 'purchase' THEN 1 ELSE 0 END AS isPurchase,
    CASE WHEN e.event_name = 'scroll' THEN 1 ELSE 0 END AS isScroll,
    CASE WHEN e.event_name = 'user_engagement' THEN 1 ELSE 0 END AS isEngagement,
    CASE WHEN COALESCE(eng.engagement_time_sec, 0) > 0 THEN 1 ELSE 0 END AS isActiveUser,
    1 AS totalUsers,
    COALESCE(e.link_text, '(not set)') AS linkText,
    e.link_url AS linkUrl,
    e.search_term AS searchTerm,
    1 AS sessions,
    CASE WHEN COALESCE(sess.new_user, 0) = 1 AND ROW_NUMBER() OVER (PARTITION BY CAST(e.ga_session_id AS STRING), e.user_pseudo_id ORDER BY e.timestamp) = 1 THEN 1 ELSE 0 END AS newUsers

FROM `376132452327.sst_events.events` e
LEFT JOIN `376132452327.sst_events.sessions` sess
ON CAST(e.ga_session_id AS STRING) = sess.ga_session_id
AND e.user_pseudo_id = sess.user_pseudo_id
LEFT JOIN (
    SELECT
        CAST(ga_session_id AS STRING) AS ga_session_id,
        user_pseudo_id,
        COALESCE(SUM(engagement_time_msec), 0) / 1000.0 AS engagement_time_sec
    FROM `376132452327.sst_events.events`
    GROUP BY ga_session_id, user_pseudo_id
) eng
ON CAST(e.ga_session_id AS STRING) = eng.ga_session_id
AND e.user_pseudo_id = eng.user_pseudo_id
;

-- ============================================================
-- VIEW: items_ga4
-- ============================================================
CREATE OR REPLACE VIEW `376132452327.sst_events.items_ga4` AS

SELECT
    i.date AS date,
    i.event_name AS eventName,
    CAST(i.ga_session_id AS STRING) AS sessionId,
    i.user_pseudo_id AS userPseudoId,
    i.transaction_id AS transactionId,
    CAST(i.ecommerce_value AS FLOAT64) AS purchaseRevenue,
    i.ecommerce_currency AS currency,
    i.item_id AS itemId,
    i.item_name AS itemName,

    -- itemBrand: cleaned (Warwick prefix stripped, treatments included)
    CASE
        WHEN i.item_brand IS NULL OR i.item_brand = 'None' THEN '(not set)'
        WHEN i.item_brand = 'Warwick' THEN 'Warwick'
        WHEN STARTS_WITH(i.item_brand, 'Warwick, ') THEN SUBSTR(i.item_brand, 10)
        ELSE i.item_brand
    END AS itemBrand,

    -- primaryBrand: always the top-level brand
    CASE
        WHEN i.item_brand IS NULL OR i.item_brand = 'None' THEN '(not set)'
        WHEN i.item_brand = 'Warwick' OR STARTS_WITH(i.item_brand, 'Warwick, ') THEN 'Warwick'
        WHEN i.item_brand = 'Weave' OR STARTS_WITH(i.item_brand, 'Weave') THEN 'Weave'
        WHEN i.item_brand = 'Thomas Maxwell Leather' THEN 'Thomas Maxwell Leather'
        WHEN STARTS_WITH(i.item_brand, 'Linia') OR STARTS_WITH(i.item_brand, 'Curate') THEN 'Warwick'
        ELSE i.item_brand
    END AS primaryBrand,

    -- secondaryBrand: sub-brand / range (Linia, Curate, Weave, Encore Recycled, Thomas Maxwell Leather)
    CASE
        WHEN i.item_brand IS NULL OR i.item_brand = 'None' THEN '(not set)'
        WHEN CONTAINS_SUBSTR(i.item_brand, 'Linia') THEN 'Linia'
        WHEN CONTAINS_SUBSTR(i.item_brand, 'Curate') THEN 'Curate'
        WHEN CONTAINS_SUBSTR(i.item_brand, 'Encore Recycled') THEN 'Encore Recycled'
        WHEN i.item_brand = 'Weave' THEN 'Weave'
        WHEN i.item_brand = 'Thomas Maxwell Leather' THEN 'Thomas Maxwell Leather'
        WHEN i.item_brand = 'Warwick' OR STARTS_WITH(i.item_brand, 'Warwick, ') THEN 'Warwick'
        ELSE i.item_brand
    END AS secondaryBrand,

    -- fabricTreatments: comma-separated list of treatments only (no brand names)
    CASE
        WHEN i.item_brand IS NULL OR i.item_brand = 'None' THEN NULL
        ELSE NULLIF(TRIM(
            CONCAT(
                IF(CONTAINS_SUBSTR(i.item_brand, 'Halo Easy Care'), 'Halo Easy Care, ', ''),
                IF(CONTAINS_SUBSTR(i.item_brand, 'SunDec Outdoor'), 'SunDec Outdoor, ', ''),
                IF(CONTAINS_SUBSTR(i.item_brand, 'WarGuard Treatment'), 'WarGuard Treatment, ', ''),
                IF(CONTAINS_SUBSTR(i.item_brand, 'HealthGuard'), 'HealthGuard, ', ''),
                IF(CONTAINS_SUBSTR(i.item_brand, 'Lustrell'), 'Lustrell, ', ''),
                IF(CONTAINS_SUBSTR(i.item_brand, 'Tritan Moisture Barrier'), 'Tritan Moisture Barrier, ', '')
            ), ', '  -- trim trailing comma-space
        ), '')
    END AS fabricTreatments,

    -- Individual boolean treatment flags for cross-filtering
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Halo Easy Care') AS isHaloEasyCare,
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'SunDec Outdoor') AS isSunDec,
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'WarGuard Treatment') AS isWarGuard,
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'HealthGuard') AS isHealthGuard,
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Lustrell') AS isLustrell,
    CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Tritan Moisture Barrier') AS isTritan,

    i.item_brand AS itemBrandRaw,
    i.item_category AS itemCategory,
    i.item_category2 AS itemCategory2,
    i.item_category3 AS itemCategory3,
    COALESCE(i.item_variant, '(not set)') AS itemVariant,
    COALESCE(i.price, 0.0) AS price,
    COALESCE(i.quantity, 0.0) AS quantity,
    CASE WHEN COALESCE(i.ecommerce_value, 0.0) > 0
        THEN COALESCE(i.quantity, 0.0)
        ELSE 0.0
    END AS quantityExCuttings,
    COALESCE(i.price, 0.0) * COALESCE(i.quantity, 0.0) AS itemRevenue,
    CASE WHEN COALESCE(i.ecommerce_value, 0.0) > 0
        THEN COALESCE(i.price, 0.0) * COALESCE(i.quantity, 0.0)
        ELSE 0.0
    END AS itemRevenueExCuttings,
    1 AS itemsPurchased,

    -- Weave fields
    CASE
        WHEN NOT (i.item_brand = 'Weave' OR CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Weave')) THEN NULL
        WHEN i.item_category = 'Cushion Inners' THEN NULL
        WHEN STARTS_WITH(i.item_name, 'Super King ') THEN SPLIT(SUBSTR(i.item_name, 12), ' ')[OFFSET(0)]
        WHEN STARTS_WITH(i.item_name, 'King Bed ') THEN SPLIT(SUBSTR(i.item_name, 10), ' ')[OFFSET(0)]
        WHEN STARTS_WITH(i.item_name, 'Queen Bed ') THEN SPLIT(SUBSTR(i.item_name, 11), ' ')[OFFSET(0)]
        WHEN STARTS_WITH(i.item_name, 'Single ') THEN SPLIT(SUBSTR(i.item_name, 8), ' ')[OFFSET(0)]
        ELSE SPLIT(i.item_name, ' ')[OFFSET(0)]
    END AS weaveRange,

    CASE
        WHEN NOT (i.item_brand = 'Weave' OR CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Weave')) THEN NULL
        WHEN i.item_category = 'Cushions' THEN 'Cushion'
        WHEN i.item_category = 'Cushion Inners' THEN 'Cushion Inner'
        WHEN i.item_category = 'Floor Rugs' THEN 'Rug'
        WHEN i.item_category = 'Throws' THEN 'Throw'
        WHEN i.item_category = 'Bed Sheets' THEN
            CASE
                WHEN CONTAINS_SUBSTR(i.item_name, 'Fitted Sheet') THEN 'Fitted Sheet'
                WHEN CONTAINS_SUBSTR(i.item_name, 'Flat Sheet') THEN 'Flat Sheet'
                WHEN CONTAINS_SUBSTR(i.item_name, 'P/case') THEN 'Pillowcase'
                WHEN CONTAINS_SUBSTR(i.item_name, 'Quilt Cover') THEN 'Quilt Cover'
                ELSE 'Bed Linen (other)'
            END
        ELSE i.item_category
    END AS weaveProductType,

    CASE
        WHEN NOT (i.item_brand = 'Weave' OR CONTAINS_SUBSTR(COALESCE(i.item_brand, ''), 'Weave')) THEN NULL
        WHEN CONTAINS_SUBSTR(i.item_name, 'Super King') THEN 'Super King'
        WHEN CONTAINS_SUBSTR(i.item_name, 'King Bed') OR CONTAINS_SUBSTR(i.item_name, 'King P/case') THEN 'King'
        WHEN CONTAINS_SUBSTR(i.item_name, 'Queen Bed') THEN 'Queen'
        WHEN CONTAINS_SUBSTR(i.item_name, 'Single ') THEN 'Single'
        WHEN CONTAINS_SUBSTR(i.item_name, 'Std P/case') THEN 'Standard'
        WHEN CONTAINS_SUBSTR(i.item_name, 'Euro P/case') THEN 'Euro'
        WHEN REGEXP_CONTAINS(i.item_name, r'\d+c?m?\s*[xX]\s*\d+c?m?')
            THEN REGEXP_EXTRACT(i.item_name, r'(\d+c?m?\s*[xX]\s*\d+c?m?)')
        WHEN REGEXP_CONTAINS(i.item_name, r'\d+x\d+')
            THEN REGEXP_EXTRACT(i.item_name, r'(\d+x\d+)')
        ELSE '(not set)'
    END AS weaveSize

FROM `376132452327.sst_events.items` i
;

