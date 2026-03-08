# Looker Studio Integration

**Goal:** Connect Looker Studio to SST data for reporting.

**Solution:** Looker Studio has no native Athena connector. SST data is synced from S3 → BigQuery, then accessed via Looker Studio's native BigQuery connector.

**BigQuery Dataset:** `376132452327.sst_events`

| Table/View | Rows | Description |
|------------|------|-------------|
| `sessions` | 214,235 | Session-level metrics (Dec 2025 - Mar 2026) with geo_region (mapped), device_brand (SAL v3.10) |
| `events` | 3,373,931 | Event-level data (Dec 2025 - Mar 2026) |
| `items` | 13,450 | Item-level purchase data (brand, category, price, quantity) with `date` field. Weave brand inferred for NULL item_brand in exclusive categories. |
| `sessions_ga4` | (view) | GA4-compatible field names, JOINs events for engagement/scroll metrics |
| `events_ga4` | (view) | Event-level with GA4 field names (Event name, Page path, Item revenue, etc.) |
| `items_ga4` | (view) | Item-level with GA4 field names + calculated `Item revenue` (price × quantity) |

## Looker Studio Setup

1. In Looker Studio, add data source → **BigQuery** (native Google connector)
2. Enter Project ID manually: `376132452327`
3. Select dataset: `sst_events`
4. Select view: `sessions_ga4`, `events_ga4`, or `items_ga4` (each is a separate data source)

**Data sources for different chart types:**
- **Session metrics** (users, sessions, conversions): `sessions_ga4`
- **Event dimensions** (event name, page path, page title): `events_ga4`
- **Item breakdowns** (brand, item name, item revenue): `items_ga4`

**Bulk data source swap:** File → Make a copy → remap data sources in the copy dialog. Works for all charts at once.

## BigQuery GA4-Compatible Views (2026-02-01)

All three `_ga4` views use **GA4 Data API camelCase field names** (e.g. `deviceCategory`, `purchaseRevenue`, `screenPageViews`) to match the GA4 Looker Studio connector's internal field IDs. This enables auto-remapping when swapping chart data sources from GA4 connector to BigQuery.

**Important:** Do NOT use spaces in BigQuery column names — Looker Studio throws "invalid characters in field names" error. The GA4 connector uses camelCase internal IDs with separate display names.

**Region mapping:** CASE statement maps AU states (NSW→New South Wales), NZ regions (AUK→Auckland), US states (CA→California), UK nations. "None"/empty → "(not set)". Applied consistently across all three views. As of SAL v3.10, the geo_region mapping is also applied at the Athena level (in `sst_events_transformed`), so exported data arrives pre-mapped.

**View definitions version-controlled:** `bigquery_views.sql` in repo root (exported 2026-03-03). Contains `sessions_ga4`, `events_ga4`, `items_ga4` CREATE OR REPLACE VIEW statements.

**GA4 API field name reference:** https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema

### `sessions_ga4` — Session-level metrics
- LEFT JOINs aggregated `events` table for `userEngagementDuration` (seconds), `scrolledUsers`, `engagedSessions`
- Includes: `sessions`, `screenPageViews`, `screenPageViewsPerSession`, `transactions`, `purchaseRevenue`, `totalRevenue`, `totalPurchasers`, `totalUsers`, `activeUsers`
- Added 2026-02-03: `deviceBrand` (parsed from UA), `sessionDefaultChannelGroup` (from referrer), `newUsers` (first appearance of user_pseudo_id)
- Ecommerce: `ecommercePurchases` (all), `ecommercePurchasesExCuttings` (revenue > 0 only), `purchaseRevenueExCuttings`
- Each row = 1 session

### `events_ga4` — Event-level data
- Includes: `eventName`, `pagePath` (extracted from URL, excludes query params), `pageLocation`, `pageTitle`
- Metrics: `purchaseRevenue`, `itemRevenue` (both from `ecommerce_value`), `engagementTimeMsec`
- Convenience flags: `isPageView`, `isPurchase`, `isScroll`, `isEngagement`
- Added 2026-02-03: `linkText`, `linkUrl`, `searchTerm` (from event payload), `isActiveUser` (session has engagement), `newUsers` (deduplicated, from sessions table), `sessions` (=1 per row, use `sessionId` CTD instead for distinct count)

### `items_ga4` — Item-level purchase data
- Includes: `itemName`, `itemBrand`, `primaryBrand`, `itemCategory`, `price`, `quantity`
- Calculated: `itemRevenue` = `price × quantity`, `itemsPurchased` = 1 per row (literal constant — NOT actual quantity)
- Has `date` field (DATE type) for Looker Studio date range filtering
- Note: `quantity` is metres of fabric (decimal), not item count. GA4 truncates this to integer; SST preserves it.
- **`itemsPurchased` gotcha:** `SUM(itemsPurchased)` = `COUNT(*)` — it counts line items/rows, not metres. Do NOT label it "Units Sold". Use `SUM(quantity)` for actual metres sold. Example: 5 itemsPurchased of Burano at $1,120 = 5 line items totaling 14 metres ($15,680 revenue).

## Known Data Gaps

| Issue | Status | Notes |
|-------|--------|-------|
| **$0 sample orders** | Known | 53% of purchases are free fabric samples (value=0.00). Not a bug. |
| **Date range resets** | Gotcha | Copied reports default to original date range (Dec 2025) - must change to Jan 2026 |
| **Chart filters (old report)** | Understood | Old report's `purchaseRevenue > 0` filter has no effect — GA4 connector provides pre-aggregated data, not row-level filtering. See Gotcha #9. |
| **Chart filters (new report)** | Gotcha | New report's BigQuery-based filters DO work at row level. Ensure filters match intent. |
| **SST data starts Dec 10, 2025** | Info | Earliest event in SST dataset is 2025-12-10 |

## Looker Studio Gotchas

1. **Data source caching:** After updating BigQuery view, must refresh Looker Studio data source (Resource → Manage → Refresh Fields). May need 2 refreshes.
2. **Field types matter:** `sessionId` should be Text (not Number with Sum) - it's an identifier, not a metric
3. **Date field:** Must be DATE type, not Text, for time series charts to work
4. **NULL handling:** Use COALESCE in BigQuery view to convert NULLs to 0, otherwise charts may not render
5. **Metric vs Dimension classification (BigQuery sources):** BigQuery is a "flexible schema" source — numeric fields appear as Dimensions and cannot be permanently reclassified in the data source editor. Drag them into chart Metric slots to use as metrics; set aggregation (SUM/AVG) at the chart level.
6. **Brand pie chart uses `items` data source:** The brand chart requires the separate `items` table as its data source with `item_brand` as dimension and `Record Count` as metric. `Record Count` = items purchased (matches GA4's "items purchased" metric). Do NOT use `SUM(quantity)` — quantity is metres of fabric, not item count.
7. **Changing data source on existing chart:** Looker Studio can't cleanly swap data sources on copied charts. Delete the chart and create a fresh one with the correct data source.
8. **Filter field search:** Filters only search within the field classification (metrics vs dimensions). If a field doesn't appear in filter search, check if it's been misclassified.
9. **GA4 connector filters don't work like BigQuery filters:** The GA4 Looker Studio connector provides pre-aggregated metrics and dimensions — not raw rows. A chart-level filter like `purchaseRevenue > 0` has no effect because GA4 has already counted purchases before Looker Studio sees the data. BigQuery data sources provide raw rows, so filters work at the row level. This is why the same `purchaseRevenue > 0` filter works on the new (BigQuery) report but does nothing on the old (GA4 connector) report.
10. **GA4 purchase_revenue is 0, not NULL:** GA4 BigQuery export stores `ecommerce.purchase_revenue = 0.00` for $0 sample orders (not NULL). 83% of purchase events in Jan 22-28 had revenue = 0. Zero NULLs found.
11. **Items table needs `date` field for Looker Studio date range filter:** Without a DATE column, Looker Studio won't show the "Default date range filter" option on charts using that data source. The items table now includes a `date` column derived from the event timestamp.
12. **Pandas dtype mismatches when loading to BigQuery:** Athena CSV exports auto-detect numeric-looking strings (ga_session_id, transaction_id) as int64. Force string dtype on read: `pd.read_csv(obj['Body'], dtype={'ga_session_id': str, 'transaction_id': str})`
13. **GA4 field names — use camelCase API names, not display names:** The GA4 connector display names have spaces ("Event name", "Device category") but internal field IDs are camelCase (`eventName`, `deviceCategory`). The `_ga4` views now use camelCase to match internal IDs, enabling auto-remapping when swapping data sources. Spaces in BigQuery column names cause "invalid characters" errors in Looker Studio.
14. **`itemRevenue` differs by context:** In `events_ga4`, `itemRevenue` = transaction-level `ecommerce_value` (same as `purchaseRevenue`). In `items_ga4`, `itemRevenue` = `price × quantity` per item. Use `items_ga4` for per-item breakdowns.
15. **GA4 connector scope mismatch inflates counts:** Filtering session-scoped metrics (`sessions`) by event-scoped dimensions (`eventName`, `itemRevenue`) produces inflated numbers. The old report's "Orders (Ex cuttings)" showed ~398 but the real count is ~200. BigQuery data sources don't have this problem.
16. **Date range dimension must be set manually for BigQuery sources:** GA4 connector auto-detects date; BigQuery requires setting "Date range dimension" to `date` in each chart's properties panel.
17. **GA4 items.quantity is INTEGER:** GA4 truncates decimal quantities to integer. Warwick's fabric metres (e.g. 20.4m) become 1 in GA4. SST preserves the decimal. This means SST item revenue is higher and more accurate than GA4's.

## Refreshing BigQuery Data

To re-export SST data from Athena to BigQuery (sessions + items):

```bash
aws sso login --profile warwick
source .venv/bin/activate
python export_to_bigquery.py
```

The script queries Athena views (`sst_events_transformed` for sessions, `sst_ecommerce_items` for items), downloads CSV results from S3, and loads to BigQuery with `WRITE_TRUNCATE`. It forces string dtypes on `ga_session_id` and `transaction_id` to prevent int64 coercion, and prints a `device_brand` summary for verification.

All `_ga4` views auto-update (they read from underlying tables). After export, in Looker Studio: Resource → Manage → Refresh Fields on all data sources.

**Last export:** 2026-03-01 — 206,779 sessions, 12,922 items (SAL v3.10: geo_region mapped, Weave brand inferred, Client Hints device_brand)

**Note:** SST doesn't have `session_start` or `first_visit` events (synthetic events filtered).

## Ecommerce JSON Path Fix (2026-01-29)

The GTM Server Container puts ecommerce fields at the **top level** of the payload, not under `$.ecommerce.*`:
```
$.value = "241.90"           ← where data actually is
$.ecommerce.value = NULL     ← where SAL was looking (wrong)
```

**Fix in SAL v3.6:** Uses COALESCE to check both paths:
```sql
COALESCE(json_extract_scalar(..., '$.value'), json_extract_scalar(..., '$.ecommerce.value'))
```

**$0 purchases are real:** 833/1,559 purchases (53%) have `value: 0.00` explicitly in the payload. These are free fabric sample orders — items also have `price: 0.0`. The ~10% difference between `ecommerce_value` and `SUM(item.price * item.quantity)` for non-zero purchases is GST/tax.
