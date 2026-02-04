# Warwick Dashboard

**Purpose:** SST vs Direct GA4 tracking comparison and Looker Studio reporting
**Client:** Warwick Fabrics (warwick.com.au)
**Last Updated:** 2026-02-04
**Status:** ✅ WRAPPING UP - Looker Studio SST report nearly complete, migrated from GA4 connector to BigQuery-backed data sources

## Analysis Summary

**Verdict:** SST captures higher-quality traffic. 82.2% overlap, Direct-only is mostly automated/prefetch traffic (38.4% zero engagement). If decommissioning one source, keep SST.

For full results, dimension tables, daily match rates, and investigation details, see [`docs/ANALYSIS_FINDINGS.md`](docs/ANALYSIS_FINDINGS.md). **When updating analysis findings, update that file, not this one.**

**Analysis parameters:** 13 normal days (Jan 6-13, Jan 21-25), warwick.com.au only, fuzzy matching ±15s window + device_category + geo_country.

## Quick Start

```bash
warwick-dash  # Run from anywhere (uses ~/bin/warwick-dash script)
```

**Prerequisites:**
- AWS SSO login: `aws sso login --profile warwick`
- Script sets `AWS_PROFILE=warwick` automatically

## Key Files

- `app.py` - Main dashboard application (Streamlit, legacy — reporting moved to Looker Studio)
- `corrected_matching_helpers.py` - Fuzzy matching logic (timestamp+attribute based)
- `materialize_matching.py` - Script to regenerate cache (contiguous date ranges only)
- `cache/` - Pre-computed parquet files for instant loading
- `athena_transformation_layer.sql` - Schema Alignment Layer (SAL) v3.9 (Client Hints device_brand, channel group, link_text, search_term)
- `requirements.txt` - Python dependencies
- `.venv/` - Local virtual environment
- `email_draft.txt` - Client email draft with latest figures

## Performance: Materialized Cache

Dashboard loads instantly from pre-computed cache files (844KB total).

**To regenerate cache** (if date range changes or data needs refresh):
```bash
source .venv/bin/activate && python materialize_matching.py
```

**Cache files:**
- `cache/sst_sessions.parquet` - SST session data with categories
- `cache/direct_sessions.parquet` - Direct session data with categories
- `cache/daily.parquet`, `hourly.parquet`, `hourly_weekday.parquet`, `hourly_weekend.parquet`
- `cache/metadata.json` - Totals and profiles

**Note:** Current cache contains 8 days (Jan 6-13). The verified 13-day totals in Daily Match Rates were computed via fresh queries. To update cache for 13 days, modify `materialize_matching.py` to support non-contiguous date ranges.

## Session Matching Methodology

`ga_session_id` matching does NOT work — SST and Direct get different IDs for the same session due to 1-second granularity timing. See [`docs/ANALYSIS_FINDINGS.md`](docs/ANALYSIS_FINDINGS.md) for the full investigation.

**Now using fuzzy matching** via `corrected_matching_helpers.py`:

```python
for each SST session:
    1. Find Direct sessions within ±15 seconds
    2. Filter to matching device + country
    3. Take closest timestamp match
    4. Label both as "Both"
```

**Time window choice:** ±15 seconds captures 99.3% of matches that ±5 minutes gets, with less false positive risk. 95% of real matches are within 0.75 seconds.

**Matching Levels Available:**
- `MATCH_BASIC`: device_category + geo_country (default)
- `MATCH_ENHANCED`: + device_operating_system
- `MATCH_STRICT`: + device_browser
- `MATCH_LANDING`: + landing page URL (highest confidence)

## Colour Scheme (All Charts)

- **Purple (#9b59b6)** = Both (overlap)
- **Green (#2ecc71)** = SST-only
- **Blue (#3498db)** = Direct-only

## Critical Gotchas

### 1. Session Matching - USE FUZZY MATCHING ONLY

**❌ WRONG:** `set(bq_df['ga_session_id']) & set(sst_df['ga_session_id'])`

**✅ CORRECT:** Use `corrected_matching_helpers.py` with timestamp+attribute matching

**Why:** ga_session_id has 1-second granularity but events arrive with sub-second differences.

### 2. SST Outage Jan 15-19, 2026

Lambda wasn't being called from GTM server container. Use Jan 6-13 or Jan 20+ for valid analysis.

### 3. Date Range for Analysis

**Valid periods:**
- Jan 6-13: Pre-outage (8 days)
- Jan 20-25: Post-recovery (excludes Jan 26 anomaly)
- Combined "normal days": 13 days total for quality analysis

**Invalid:**
- Jan 15-19: SST outage (0 SST sessions)
- Jan 26: SST spike anomaly (+742 extra sessions, 50.4% match rate)

### 4. Windows == Windows+Desktop (100%)

All Windows sessions are desktop devices (no Windows mobile/tablet):
- Windows Phone discontinued 2017
- Surface tablets classified as 'desktop' in User-Agent parsing
- Makes "Windows+Desktop" metric redundant

### 5. Client Hints vs User-Agent

Safari and Firefox do NOT support User-Agent Client Hints. The `client_hints.mobile` field is NULL for ~50% of sessions. The SAL uses a hybrid approach: Client Hints `model` for device_brand (Android Chrome), User-Agent for everything else. See gotcha #25.

### 6. GA4 Missing Fields

Warwick's GA4 does NOT collect:
- `screen_resolution` - not in BigQuery export
- `device.browser` - empty for all sessions

### 7. Timeseries Data is Australia-Only

Daily, hourly, weekday/weekend charts are filtered to `geo_country == 'Australia'` to ensure AEST timezone analysis is meaningful. The dimension table includes all countries.

### 8. GA4 Engagement Time Requires 2+ Events

GA4 calculates engagement time from the difference between events. A single-event session (e.g., just `session_start`) will ALWAYS have 0 engagement time because there's no subsequent event to carry the time value.

- **Single-event sessions:** 98.2% have zero engagement (by design)
- **2+ event sessions:** Should have non-zero engagement if human behavior
- **Zero engagement with 2+ events:** Indicates automated traffic (prefetch/prerender)

### 9. Division by Zero in Analysis Scripts

All analysis/validation scripts use `safe_pct()` helper functions and early return guards for empty dataframes. When writing new analysis:
```python
def safe_pct(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0.0
```

### 10. GTM Container Confusion

See [GTM Container Reference](#gtm-container-reference) for the full container list. The correct Warwick AU Web container is **GTM-P8LRDK2**.

Do NOT use:
- GTM-NX6WWZM (Weave Shopify - different brand)
- GTM-KH5P5K8 (Warwick Web - Unified - all tags paused, not in use)

### 11. BigQuery GROUP BY with Non-Aggregated Columns

BigQuery requires all non-aggregated columns in GROUP BY OR use aggregation functions:
```sql
-- WRONG: GROUP BY 1, 3, 4, 5 (column numbers)
-- CORRECT: GROUP BY 1 with ANY_VALUE() for other columns
SELECT
    ga_session_id,
    MIN(event_timestamp) as session_start_ts,
    ANY_VALUE(device.category) as device_category,  -- Use ANY_VALUE
    ...
FROM ...
GROUP BY 1  -- Only group by session_id
```

### 12. GTM API Access

GTM changes can be made via API using gcloud auth:
```bash
TOKEN=$(gcloud auth application-default print-access-token)
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://tagmanager.googleapis.com/tagmanager/v2/accounts/6005413178/containers/55289540/workspaces/35/tags"
```

**Limitation:** Current gcloud token has read/edit but NOT publish permissions. Publish manually via GTM UI.

**Always include version name and description when publishing:**
- Version Name: Brief summary (e.g., "Add browser_signals parameter")
- Description: List changes (variables added, tags updated)

### 13. GTM Cache Propagation

After publishing GTM changes:
- Google CDN updates within 5-15 minutes
- Browser cache is short-lived for gtm.js
- Sessions started before publish keep old config until refresh
- No manual cache clear available - wait for natural propagation

### 14. BigQuery Project ID Authentication

When using google-cloud-bigquery with gcloud ADC, use the **numeric project ID**:
```python
# WRONG - causes "ProjectId must be non-empty" error
client = bigquery.Client(project="warwick-com-au")

# CORRECT
client = bigquery.Client(project="376132452327")
```

### 15. Daily Breakdown Matching - Match All Days Together

When computing daily match rates, **match all days together first, then group by date**:

```python
# WRONG - per-day matching misses cross-midnight sessions (55% overlap)
for date_str in dates:
    direct_df = query_day(date_str)
    sst_df = query_day(date_str)
    matched = fuzzy_match(direct_df, sst_df)  # Separate per day
    results.append((date_str, matched))

# CORRECT - single matching pass, then attribute to dates (82% overlap)
direct_df = query_all_days()  # All 13 days in one query
sst_df = query_all_days()
matched = fuzzy_match(direct_df, sst_df)  # Single matching pass
daily_results = matched.groupby('date').agg(...)  # Then group by date
```

**Why:** Sessions spanning midnight have events in different date buckets. Per-day matching creates artificial "only" sessions at midnight boundaries.

### 16. Athena Column Names and Pagination

- **Column name:** SST view uses `timestamp`, not `event_timestamp`
- **MaxResults:** Athena pagination limit is 1000, use NextToken for larger results
- **Timezone:** SST timestamps are tz-aware (UTC), Direct are tz-naive. Call `.dt.tz_localize(None)` before comparing

### 17. GA4 items.quantity is INTEGER — Decimal Quantities Lost

GA4's BigQuery export defines `items.quantity` as **INT64**. Warwick sends decimal quantities (metres of fabric, e.g. 20.4m) but GA4 truncates to integer (1). SST captures the raw payload and preserves decimal quantities.

- **GA4 Direct:** `quantity: 1` for all items, `item_revenue = price × 1`
- **SST:** `quantity: 20.4`, `item_revenue = price × 20.4`
- SST revenue figures are more accurate for Warwick's fabric-by-the-metre business
- To match the old GA4 report's revenue, use `price` alone (effectively `qty=1`)

### 18. GA4 Looker Studio Connector Scope Mismatch

Combining session-scoped metrics (`sessions`) with event-scoped dimensions/filters (`Event name`, `Item revenue`) inflates counts. The old report's "Orders (Ex cuttings)" uses `sessions` metric filtered by `Event name = purchase AND Item revenue > 0`, producing ~398. The real number of distinct sessions with paid purchases is ~200 (GA4 Direct) / ~196 (SST).

**Rule:** Never filter session-scoped metrics by event-scoped dimensions in the GA4 connector. Use BigQuery data sources instead where row-level filtering works correctly.

### 19. BigQuery View Column Names — No Spaces for Looker Studio

Looker Studio's BigQuery connector throws "invalid characters in field names" error when column names contain spaces (even though BigQuery supports them via backticks). Use camelCase GA4 API names instead (e.g. `deviceCategory` not `Device category`). The GA4 connector uses camelCase internal field IDs with separate display names.

### 20. Looker Studio Metrics vs Dimensions for BigQuery Sources

BigQuery is a "flexible schema" data source. Numeric fields appear as Dimensions with aggregation "None" and cannot be permanently moved to Metrics in the data source editor. Instead, drag them into the **Metric slot** of a chart — they aggregate automatically. Set aggregation (SUM, AVG, CTD) at the chart level.

### 21. Looker Studio Date Range Dimension for BigQuery

The GA4 connector auto-detects date dimensions. BigQuery sources require you to manually set the **Date range dimension** to `date` in each chart's properties. The `date` field must be DATE type (not Text).

### 22. Pandas datetime64 Units (us vs ns)

BigQuery returns timestamps as `Int64` (nullable), and when converted to datetime, pandas may use `datetime64[us]` (microseconds) instead of `datetime64[ns]` (nanoseconds).

```python
# WRONG - assumes nanoseconds, gives values 1000x too small
ts_seconds = df['session_start'].astype(np.int64) // 10**9

# CORRECT - check the unit first
def to_unix_seconds(series):
    dtype_str = str(series.dtype)
    if 'datetime64[ns]' in dtype_str:
        return series.astype(np.int64) // 10**9
    elif 'datetime64[us]' in dtype_str:
        return series.astype(np.int64) // 10**6
    else:
        return series.astype('datetime64[ns]').astype(np.int64) // 10**9
```

**Symptom:** Timestamps showing 1970 dates instead of 2026, or fuzzy matching finding 0 matches despite overlapping data.

### 23. SST Has No Traffic Source/Medium — Channel Group Derived from Referrer

The GA4 client-side tag does **not** forward `traffic_source`, `traffic_medium`, or UTM parameters to the GTM server container. GA4 determines session attribution internally using cookie data and its own attribution model — this never reaches the SST Lambda.

**What we have:** `page_referrer` (88% of events). The SAL derives `session_default_channel_group` from the referring domain:
- `google.*`, `bing.com`, `yahoo.*`, `duckduckgo.com`, `ecosia.org` → **Organic Search**
- `facebook.com`, `instagram.com`, `pinterest.com`, `linkedin.com` → **Organic Social**
- `mail.google.com`, `outlook.*`, `office.*`, `teams.*` → **Email**
- No referrer → **Direct**
- Everything else → **Referral**

**Limitations:**
- **Cannot distinguish Paid Search from Organic Search** (no `gclid`/UTM params)
- **Cannot distinguish Paid Social from Organic Social** (no `fbclid`/UTM params)
- Self-referrals (warwick.com.au → warwick.com.au) are excluded; session inherits channel from first external referrer
- Sessions where all events are self-referral or no-referrer default to "Direct"

### 24. SST Has No `first_visit` Event — New Users Derived from First Appearance

The GTM server container does not forward `first_visit` or `session_start` synthetic events to the SST Lambda. These events are consumed internally by GA4.

**Workaround:** `new_user` flag is computed during the Athena→BigQuery export: for each `user_pseudo_id`, the session with the earliest `session_start` gets `new_user = 1`.

**Limitation:** Users who visited before SST data collection started (Dec 10, 2025) are incorrectly counted as "new" on their first SST-captured session. This inflates new user counts for early dates in the dataset but is accurate for later periods.

### 25. Device Brand — Client Hints + User-Agent Hybrid (SAL v3.9)

SST `device_brand` uses a hybrid approach: **Client Hints model first** (for Android Chrome, ~58% of traffic), then **User-Agent fallback** (for Safari, Firefox, older browsers). GA4 uses a full device database (WURFL or similar).

**Why hybrid:** Chrome UA reduction replaces Android device models with "K", making UA-only parsing useless for brand detection on most Android traffic. However, `client_hints.model` in the SST raw_payload IS populated (e.g., "SM-S926B" → Samsung, "Pixel 8" → Google).

**Covered brands (Client Hints):** Samsung (SM-*, Galaxy*), Google (Pixel*), Oppo (CPH*, RMX*), Motorola (moto*, XT*), LG (LM-*, LG*), Vivo (*5G*), Xiaomi (220*, 230*, 240*, Redmi*, POCO*), Nokia, Huawei, Sony (XQ-*), Micromax (IN*).
**Covered brands (UA fallback):** Apple, Microsoft, Samsung, Google, Huawei, Xiaomi, Oppo, Motorola, LG, Sony.
**Not covered:** Less common Android OEMs without Client Hints whose model strings aren't identifiable. These show as `(not set)` or `Android (other)`.

### 26. SST Has No Gender/Demographics Data

GA4 gets gender/age from Google Signals (logged-in Google users' ad personalization profiles). This data is enriched server-side by Google's infrastructure and never reaches the SST payload. There is no signal in the SST data to derive or approximate demographics. Skip gender/age charts on the SST report.

### 27. Looker Studio Percent Format for BigQuery Calculated Fields

The "FORMAT FORMULA" button in the calculated field editor only reformats code text — it does not change display format. To format a field as percent: click the metric pill in the chart Setup tab, then change **Data type** dropdown from Number to **Percent**. Alternatively, edit the data source (Resource → Manage → Edit) and change the field type there.

### 28. Looker Studio Session Counts on Event-Level Data Sources

For `events_ga4` (event-level, one row per event), use `sessionId` with aggregation **Count Distinct (CTD)** to count sessions. Do NOT use `SUM(sessions)` — the `sessions = 1` field counts events, not distinct sessions. Similarly, use `userPseudoId` CTD for user counts.

### 29. Looker Studio `isActiveUser` and `newUsers` — Event-Level Deduplication

`newUsers` in `events_ga4` is deduplicated via ROW_NUMBER (only first event per new user session = 1). `SUM(newUsers)` gives correct distinct new user counts even with page-level filters.

For active users, use `userPseudoId` CTD with a filter `engagementTimeMsec > 0` — do NOT use `SUM(isActiveUser)` as it counts events not users.

### 30. Athena CAST vs TRY_CAST for Ecommerce Values

Some SST payloads have empty string `""` for `ecommerce_value`. `CAST('' AS DOUBLE)` fails with `INVALID_CAST_ARGUMENT`. Use `TRY_CAST` which returns NULL for unparseable values.

## Schema Alignment Layer (SAL) v3.9

**Purpose:** Transform SST dimension values to match BigQuery exactly.

**Note:** SAL was designed for ga_session_id JOIN, but this approach is now deprecated. SAL views are still useful for dimension normalization.

**v3.9 (2026-02-04):** `device_brand` now uses Client Hints model first, then UA fallback. Samsung (817), Google (130), Oppo (63) now visible. `(not set)` dropped from ~7% to 0.7%.

**Verified match rates:**
- Device/browser/OS: 98%+
- Geo: 96.5% (3.5% mismatch is VPN users)

**Views:**

| View | Purpose |
|------|---------|
| `sst_events_transformed` | Event-level data with BigQuery-aligned dimensions, device_brand (Client Hints + UA hybrid), channel group, link_text, search_term |
| `sst_sessions_daily` | Daily aggregates for trend comparison |
| `sst_comparison_ready` | Filtered events for AU Direct comparison |
| `sst_sessions` | Session-level rollup (deprecate ga_session_id JOIN approach) |
| `sst_ecommerce_items` | Item-level ecommerce data (brand, category, price, quantity) |

**Athena timestamp parsing:** Use `from_iso8601_timestamp(session_start)` NOT `CAST(session_start AS TIMESTAMP)` - the latter fails with ISO8601 'Z' suffix.

**Athena JSON array to VARCHAR:** Use `json_format(json_extract(...))` NOT `CAST(... AS VARCHAR)`. The CAST fails for complex JSON types (arrays/objects). `json_format` serializes JSON to string correctly.

**Athena quantity types:** Item quantity in SST payload can be decimal (metres of fabric). Use `DOUBLE` not `BIGINT` for quantity fields.

**Athena ecommerce cast:** Use `TRY_CAST(ecommerce_value AS DOUBLE)` not `CAST` — some payloads have empty strings.

## GTM Container Reference

| Container ID | Name | Type | Account |
|--------------|------|------|---------|
| GTM-P8LRDK2 | warwick.com.au | Web | 6005413178 |
| GTM-WQM54GN | warwick.co.nz | Web | 6005413178 |
| GTM-5L7LCRZ5 | Warwick SST | Server | 6005413178 |
| GTM-NX6WWZM | Weave Web - Unified | Web (Shopify) | 6005542175 |

**Note:** GTM-NX6WWZM is Weave (Shopify), NOT Warwick. Don't modify it for Warwick changes.

## Lambda & Infrastructure

| Component | Status |
|-----------|--------|
| Lambda | `warwick-weave-sst-event-writer` - 0 errors, 0 throttles |
| API Gateway | Private API `ez9g450hvl` via VPC endpoint |
| VPC Endpoint | `vpce-06fd9e5d58874b30f` - available |

Check Lambda invocations:
```bash
aws cloudwatch get-metric-statistics \
  --profile warwick --region ap-southeast-2 \
  --namespace AWS/Lambda --metric-name Invocations \
  --dimensions Name=FunctionName,Value=warwick-weave-sst-event-writer \
  --start-time 2026-01-13T00:00:00Z --end-time 2026-01-22T00:00:00Z \
  --period 86400 --statistics Sum
```

## Analysis Scripts

| Script | Purpose |
|--------|---------|
| `materialize_matching.py` | **Primary:** Generate cache for instant dashboard loading |
| `corrected_matching_helpers.py` | Core fuzzy matching logic (supports BASIC/ENHANCED/STRICT/LANDING levels) |
| `corrected_analysis.py` | Full corrected categorization with profiles |
| `pairwise_matching.py` | Detect same sessions with different IDs |
| `session_id_pattern_check.py` | Analyze ID differences (proves 1-second granularity) |
| `temporal_alignment_check.py` | Hourly/daily patterns comparison |
| `collision_check.py` | Check for ID collisions (2.3% rate, acceptable) |
| `user_agent_diagnosis.py` | Investigate cross-device sessions |
| `check_windows_devices.py` | Verify Windows=Desktop (100%) |
| `direct_only_attributes.py` | Compare Direct-only vs Both session attributes |
| `sst_filtering_analysis.py` | Check what SST receives vs filters |
| `page_visibility_analysis.py` | Validate prerender hypothesis |
| `sst_vs_direct_quality.py` | Compare total SST vs Direct quality (no segmentation) |
| `event_name_comparison.py` | Compare event names between matched sessions - **identifies synthetic event gap** |

## Validation Scripts (Reconciliation Confidence)

Scripts to validate the fuzzy matching methodology. See `docs/reconciliation_validation.md` for details.

| Script | Purpose | Uses Live Query |
|--------|---------|-----------------|
| `sensitivity_analysis.py` | Test time windows (±30s to ±10min) | Yes |
| `geo_verification.py` | Verify China/geo hypothesis | No (cache) |
| `bot_analysis.py` | Detect bots in Direct-only | Yes (optional) |
| `timestamp_drift.py` | Analyze timing differences | No (cache) |
| `bootstrap_analysis.py` | 95% confidence intervals | No (cache) |
| `sample_inspection.py` | Random session review | No (cache) |
| `event_sequence_analysis.py` | Compare event sequences | Yes |

**Key validation conclusions (2026-01-24):**
- Matching validated: Australia 90.2% Both rate
- No obvious bot UA patterns detected in Direct-only
- Time window sensitivity confirmed: ±15s is optimal

**Note:** All validation scripts include edge case handling (division by zero guards, empty dataframe checks). Use `safe_pct(numerator, denominator)` helper when calculating percentages.

## Workflow

1. Make changes to analysis scripts or matching helpers
2. Auth: `aws sso login --profile warwick`
3. Deploy: `git add -A && git commit -m "message" && git push`

## Next Steps (as of 2026-02-04)

1. **Re-export sessions to BigQuery** — SAL v3.9 deployed to Athena with Client Hints device_brand. Need to re-run Athena→BigQuery export so Looker Studio picks up Samsung/Google/etc.
2. **Re-run `materialize_matching.py`** — Refresh cache with updated device_brand values
3. **Finish Looker Studio chart migration** — Pages 1-7 mostly done. Page 8 (Top Actions, File Downloads, Search Results, Add to Wishlist) in progress. Gender chart skipped (no SST data).
4. **Produce delivery report** (in progress)
5. **Set up scheduled BigQuery refresh** - Automate Athena → BigQuery sync
6. **Finalise documentation and Git repo**
7. **Unify AU and NZ properties**
8. **$0 sample orders** - 53% of purchases are free samples (value=0.00 in payload). Not a bug — Warwick fabric samples are free. `ecommercePurchasesExCuttings` column added to `sessions_ga4` for filtering.

## Looker Studio Integration

For Looker Studio work, see [`docs/LOOKER_STUDIO.md`](docs/LOOKER_STUDIO.md) — setup, BigQuery views, gotchas, data refresh. **When updating context about Looker Studio or BigQuery views, update that file, not this one.**

**BigQuery Dataset:** `376132452327.sst_events` (tables: `sessions`, `events`, `items` + `_ga4` views)

## Documentation (Outline Wiki)

**Primary:** Outline Wiki at http://127.0.0.1:8888 — Collection: "Warwick SST" (ID: `adc776a3-10ae-4bea-9119-088f3c9a33c8`)

```
SST Dashboard Overview             # Key metrics and results
├── Session Matching Methodology   # Why ga_session_id fails, fuzzy matching, validation
├── Page Visibility Analysis       # Browser visibility API validation
├── ML Analysis Technical Appendix # Random Forest + SHAP analysis
└── Validation Scripts             # Script reference and key results
```

```bash
cd ~/outline && make start   # Start Outline (login: admin / outline123)
```

**API access:**
```bash
eval "$(claudebw)"  # Load token from Bitwarden
curl -X POST $OUTLINE_URL/api/documents.list \
  -H "Authorization: Bearer $OUTLINE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"collectionId": "adc776a3-10ae-4bea-9119-088f3c9a33c8"}'
```

**Local backup:** `docs/` directory (markdown source files), `docs/data/` (CSV exports from validation scripts).

## Deploying SAL

The SAL has multiple CREATE VIEW statements. Use Python to deploy them individually:

```bash
source .venv/bin/activate && python3 << 'EOF'
import boto3
import time
import re

with open('athena_transformation_layer.sql', 'r') as f:
    sql_content = f.read()

parts = re.split(r'(?=CREATE OR REPLACE VIEW)', sql_content)
statements = [p.strip() for p in parts if p.strip().startswith('CREATE OR REPLACE VIEW')]

cleaned = []
for stmt in statements:
    match = re.search(r';\s*\n\s*\n\s*--', stmt)
    if match:
        stmt = stmt[:match.start()+1]
    cleaned.append(stmt.rstrip().rstrip(';'))

session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

for stmt in cleaned:
    view_name = stmt.split('VIEW')[1].split('AS')[0].strip()
    print(f"Deploying {view_name}...")
    response = athena.start_query_execution(
        QueryString=stmt,
        QueryExecutionContext={'Database': 'warwick_weave_sst_events'},
        ResultConfiguration={'OutputLocation': 's3://warwick-com-au-athena-results/'},
        WorkGroup='primary'
    )
    query_id = response['QueryExecutionId']
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)
    print(f"  {'OK' if state == 'SUCCEEDED' else 'FAILED'}")
EOF
```
