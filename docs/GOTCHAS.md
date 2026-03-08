# Gotchas Reference

All numbered gotchas for the Warwick SST project. **Gotcha #1 (fuzzy matching)** is also inlined in `CLAUDE.md` as it directly affects code generation.

## 1. Session Matching - USE FUZZY MATCHING ONLY

**WRONG:** `set(bq_df['ga_session_id']) & set(sst_df['ga_session_id'])`
**CORRECT:** Use `corrected_matching_helpers.py` with timestamp+attribute matching
**Why:** ga_session_id has 1-second granularity but events arrive with sub-second differences.

## 2. SST Outage Jan 15-19, 2026

Lambda wasn't being called from GTM server container. Use Jan 6-13 or Jan 20+ for valid analysis.

## 3. Date Range for Analysis

**Valid:** Jan 6-13 (pre-outage), Jan 20-25 (post-recovery). Combined: 13 "normal days".
**Invalid:** Jan 15-19 (SST outage), Jan 26 (SST spike anomaly, 50.4% match rate).

## 4. Windows == Windows+Desktop (100%)

All Windows sessions are desktop. Windows Phone discontinued 2017, Surface tablets classified as 'desktop'.

## 5. Client Hints vs User-Agent

Safari and Firefox do NOT support User-Agent Client Hints. `client_hints.mobile` is NULL for ~50% of sessions. SAL uses hybrid approach: Client Hints `model` for device_brand (Android Chrome), User-Agent for everything else. See gotcha #25.

## 6. GA4 Missing Fields

Warwick's GA4 does NOT collect: `screen_resolution` (not in BigQuery export), `device.browser` (empty for all sessions).

## 7. Timeseries Data is Australia-Only

Daily, hourly, weekday/weekend charts filtered to `geo_country == 'Australia'` for AEST timezone analysis. Dimension table includes all countries.

## 8. GA4 Engagement Time Requires 2+ Events

Single-event sessions always have 0 engagement time (no subsequent event to carry the value). Zero engagement with 2+ events indicates automated traffic (prefetch/prerender).

## 9. Division by Zero in Analysis Scripts

Use `safe_pct()` helper:
```python
def safe_pct(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0.0
```

## 10. GTM Container Confusion

Correct Warwick AU Web container: **GTM-P8LRDK2**. Do NOT use GTM-NX6WWZM (Weave Shopify) or GTM-KH5P5K8 (Warwick Web - Unified, all tags paused).

## 11. BigQuery GROUP BY with Non-Aggregated Columns

Use `ANY_VALUE()` for non-aggregated columns:
```sql
SELECT ga_session_id, MIN(event_timestamp) as session_start_ts,
    ANY_VALUE(device.category) as device_category
FROM ... GROUP BY 1
```

## 12. GTM API Access

```bash
TOKEN=$(gcloud auth application-default print-access-token)
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://tagmanager.googleapis.com/tagmanager/v2/accounts/6005413178/containers/55289540/workspaces/35/tags"
```
Current gcloud token has read/edit but NOT publish permissions. Publish manually via GTM UI. Always include version name and description when publishing.

## 13. GTM Cache Propagation

Google CDN updates within 5-15 minutes after publish. No manual cache clear available.

## 14. BigQuery Project ID Authentication

Use numeric project ID with gcloud ADC:
```python
# WRONG: client = bigquery.Client(project="warwick-com-au")
# CORRECT:
client = bigquery.Client(project="376132452327")
```

## 15. Daily Breakdown Matching - Match All Days Together

Match all days in a single pass, then group by date. Per-day matching misses cross-midnight sessions (55% vs 82% overlap).

```python
# CORRECT - single matching pass, then attribute to dates
direct_df = query_all_days()
sst_df = query_all_days()
matched = fuzzy_match(direct_df, sst_df)
daily_results = matched.groupby('date').agg(...)
```

## 16. Athena Column Names and Pagination

- SST view uses `timestamp`, not `event_timestamp`
- Athena pagination limit is 1000, use NextToken for larger results
- SST timestamps are tz-aware (UTC), Direct are tz-naive. Call `.dt.tz_localize(None)` before comparing

## 17. GA4 items.quantity is INTEGER — Decimal Quantities Lost

GA4 truncates decimal quantities to integer. Warwick sends metres of fabric (e.g. 20.4m) but GA4 stores `1`. SST preserves decimal quantities. To match old GA4 report's revenue, use `price` alone.

## 18. GA4 Looker Studio Connector Scope Mismatch

Never filter session-scoped metrics by event-scoped dimensions in the GA4 connector. Use BigQuery data sources instead where row-level filtering works correctly.

## 19. BigQuery View Column Names — No Spaces for Looker Studio

Use camelCase GA4 API names (e.g. `deviceCategory` not `Device category`). Looker Studio throws errors on spaces in field names.

## 20. Looker Studio Metrics vs Dimensions for BigQuery Sources

Numeric fields appear as Dimensions. Drag them into the **Metric slot** of a chart to aggregate. Set aggregation (SUM, AVG, CTD) at the chart level.

## 21. Looker Studio Date Range Dimension for BigQuery

Manually set **Date range dimension** to `date` in each chart's properties. Must be DATE type (not Text).

## 22. Pandas datetime64 Units (us vs ns)

BigQuery may return `datetime64[us]` instead of `[ns]`. Check the unit before converting:
```python
def to_unix_seconds(series):
    dtype_str = str(series.dtype)
    if 'datetime64[ns]' in dtype_str:
        return series.astype(np.int64) // 10**9
    elif 'datetime64[us]' in dtype_str:
        return series.astype(np.int64) // 10**6
    else:
        return series.astype('datetime64[ns]').astype(np.int64) // 10**9
```
**Symptom:** Timestamps showing 1970 dates, or fuzzy matching finding 0 matches.

## 23. SST Has No Traffic Source/Medium

GA4 does not forward `traffic_source`, `traffic_medium`, or UTM parameters to the GTM server container. The SAL derives `session_default_channel_group` from `page_referrer` (88% of events):
- `google.*`, `bing.com`, `yahoo.*`, `duckduckgo.com`, `ecosia.org` → Organic Search
- `facebook.com`, `instagram.com`, `pinterest.com`, `linkedin.com` → Organic Social
- `mail.google.com`, `outlook.*`, `office.*`, `teams.*` → Email
- No referrer → Direct
- Everything else → Referral

Cannot distinguish Paid from Organic (no `gclid`/UTM/`fbclid` params).

## 24. SST Has No `first_visit` Event

`new_user` flag computed during Athena→BigQuery export: earliest session per `user_pseudo_id` gets `new_user = 1`. Users who visited before SST data collection (Dec 10, 2025) are incorrectly counted as "new" on first SST-captured session.

## 25. Device Brand — Client Hints + User-Agent Hybrid

Client Hints model first (Android Chrome ~58%), then UA fallback (Safari, Firefox, older browsers). Chrome UA reduction replaces Android models with "K", making UA-only parsing useless.

**Covered (Client Hints):** Samsung (SM-*, Galaxy*), Google (Pixel*), Oppo (CPH*, RMX*), Motorola (moto*, XT*), LG, Vivo, Xiaomi, Nokia, Huawei, Sony (XQ-*), Micromax.
**Covered (UA):** Apple, Microsoft, Samsung, Google, Huawei, Xiaomi, Oppo, Motorola, LG, Sony.

## 26. SST Has No Gender/Demographics Data

GA4 gets gender/age from Google Signals. This never reaches the SST payload. Skip gender/age charts on SST report.

## 27. Looker Studio Percent Format for BigQuery Calculated Fields

To format as percent: click metric pill in chart Setup tab → change Data type to **Percent**.

## 28. Looker Studio Session Counts on Event-Level Data Sources

Use `sessionId` CTD to count sessions on `events_ga4`. Do NOT use `SUM(sessions)`. Use `userPseudoId` CTD for user counts.

## 29. Looker Studio `isActiveUser` and `newUsers`

`SUM(newUsers)` works correctly (deduplicated via ROW_NUMBER). For active users, use `userPseudoId` CTD with filter `engagementTimeMsec > 0`.

## 30. Old Report Session Counts Inflated by Scope Mismatch

Same as gotcha #18. GA4 connector applies event-scoped filter but reports session-scoped metric for all sessions of matching dimension values.

## 31. Athena CAST vs TRY_CAST for Ecommerce Values

Use `TRY_CAST(ecommerce_value AS DOUBLE)` — some payloads have empty strings that cause `INVALID_CAST_ARGUMENT`.

## 32. BigQuery Export Must Match _ga4 View Schemas

Sessions table must include: `year`, `month`, `day`, `new_user`, `geo_region`, `date`. Dropping any causes "Invalid Field Configuration" errors in Looker Studio. Use `export_to_bigquery.py` which handles all of this.

## 33. `itemsPurchased` is Row Count, NOT Quantity

`SUM(itemsPurchased)` = `COUNT(*)` — counts line items, not metres/units. Use `SUM(quantity)` for actual quantities. Affected: Page 3 ("Units Sold"), Page 12 (Brand Performance).

## 34. Looker Studio Calculated Field Syntax ≠ SQL

- No `AS` aliases, no semicolons, no `IN` operator
- Use `CONTAINS_TEXT()` instead of `CONTAINS_SUBSTR()`
- Fields must exist in the data source (`itemCategory` only in `items_ga4`)

## 35. CJK Search Spam

Chinese-language spam in site search (1,945 events). Filter: `REGEXP_MATCH(searchTerm, '.*[\u4e00-\u9fff].*')`. `isCjkSpam` boolean flag in `sessions_ga4` view. See `docs/SEARCH_SPAM_RECOMMENDATION.md`.

## 36. SST Has IP Addresses — GA4 Direct Does Not

SST raw payload contains `ip_override`. Access: `json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ip_override')`. The `ip_address` column contains "unknown" — use the payload field.

## 37. No Internal Traffic Filter Active in GA4

GA4 internal traffic rule not configured. Tony needs to provide office IP(s). Fix: GA4 Admin → Data Streams → Configure tag settings → Define internal traffic.

## 38. Weave Products Missing `item_brand` in DataLayer

~75% of Weave revenue has `item_brand = NULL`. Affected categories: Cushions, Bed Sheets, Floor Rugs, Throws, Cushion Inners (Weave-exclusive). **View fix done** (SAL v3.10 infers brand from category). **DataLayer fix still needed** at source.

## 39. futuret3ch.com.au — Cloned Warwick Content

Clone of warwick.com.au at `www.futuret3ch.com.au/warwicks/` with Warwick's GTM tags still embedded. 170+ SST events polluting analytics. **Fix:** Add hostname condition to GA4 tag trigger in `GTM-P8LRDK2` — only fire on `warwick\.com\.au|warwick\.co\.nz`. See also gotcha #41.

## 40. Page 6 Variant Colour Chart Uses Wrong Dimension

Uses `weaveSize` instead of `itemVariant`. Tony's fix — Looker Studio UI change only.

## 41. GA4 Has No Hostname Filter — Use GTM Instead

Options to block unauthorized domain traffic:
1. **GTM trigger condition (recommended):** `Page Hostname matches RegEx warwick\.com\.au|warwick\.co\.nz` on GA4 tag in `GTM-P8LRDK2`
2. **Server container check:** Hostname validation in `GTM-5L7LCRZ5`
3. **Post-collection filter:** `WHERE page_location NOT LIKE '%futuret3ch%'` in views

## 42. Export Script Must Update All Three BigQuery Tables

`export_to_bigquery.py` exports `sessions`, `events`, and `items`. All three must be kept in sync. The `events_ga4` view requires: `is_likely_human`, `is_bot`, `is_synthetic_event`, `is_fallback_event`, `ip_address`, `user_agent`. No automated refresh exists — all exports are manual.

## 43. GA4 BigQuery Export Has No Historical Backfill

GA4 BigQuery export only captures data from the day it's enabled. For historical data, use GA4 Data API or GA4 Looker Studio connector.
