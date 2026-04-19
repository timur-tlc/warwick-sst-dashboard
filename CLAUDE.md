# Warwick Dashboard

**Purpose:** SST vs Direct GA4 tracking comparison and Looker Studio reporting
**Client:** Warwick Fabrics (warwick.com.au / warwick.co.nz)
**Last Updated:** 2026-04-09
**Status:** Looker Studio SST reports live (AU + NZ). BigQuery: 483K sessions (329K AU + 154K NZ), 6.25M events, 22.8K items (through Apr 8). SAL v3.10. Weekly automated export live (Step Functions + Lambda). Lambda ephemeral storage increased to 10 GB (2026-04-09) after events CSV outgrew 3 GB limit. Hostname filter deployed (GTM + Athena + BQ). Staff IP filter in GA4 Testing mode.

## Analysis Summary

SST captures higher-quality traffic. 82.2% overlap, Direct-only is mostly automated/prefetch (38.4% zero engagement). Keep SST. Details: [`docs/ANALYSIS_FINDINGS.md`](docs/ANALYSIS_FINDINGS.md). Parameters: 13 normal days (Jan 6-13, Jan 21-25), warwick.com.au only, fuzzy matching ±15s + device_category + geo_country.

## Quick Start

```bash
warwick-dash  # Run from anywhere (uses ~/bin/warwick-dash script)
```

Prerequisites: `aws sso login --profile warwick`

## Key Files

- `app.py` - Streamlit dashboard (legacy — reporting moved to Looker Studio)
- `export_to_bigquery.py` - Athena → BigQuery export (sessions + events + items)
- `corrected_matching_helpers.py` - Fuzzy matching logic
- `materialize_matching.py` - Regenerate cache (contiguous date ranges only)
- `athena_transformation_layer.sql` - SAL v3.10
- `bigquery_views.sql` - BigQuery view definitions: `_all` (both sites), `_ga4` (AU), `_nz` (NZ)
- `lambda/` - Automated BQ export Lambda (handler.py, Dockerfile). Deployed via OpenTofu in `warwick-sst-infrastructure`.
- `cache/` - Pre-computed parquet files (8 days Jan 6-13)

## Materialized Cache

Regenerate: `source .venv/bin/activate && python materialize_matching.py`

Current cache: 8 days (Jan 6-13). 13-day totals in Daily Match Rates were computed via fresh queries.

## Session Matching

`ga_session_id` matching does NOT work. Use fuzzy matching via `corrected_matching_helpers.py`: find Direct sessions within ±15s, filter by device + country, take closest match. ±15s captures 99.3% of matches vs ±5min, with 95% within 0.75s.

Levels: `MATCH_BASIC` (device+country, default), `MATCH_ENHANCED` (+OS), `MATCH_STRICT` (+browser), `MATCH_LANDING` (+landing page).

## Colour Scheme

- **Purple (#9b59b6)** = Both (overlap)
- **Green (#2ecc71)** = SST-only
- **Blue (#3498db)** = Direct-only

## Critical Gotchas

Full reference (44 items): [`docs/GOTCHAS.md`](docs/GOTCHAS.md). Most important:

### Fuzzy Matching Only (Gotcha #1)

**WRONG:** `set(bq_df['ga_session_id']) & set(sst_df['ga_session_id'])`
**CORRECT:** Use `corrected_matching_helpers.py` with timestamp+attribute matching

### BigQuery Project ID (Gotcha #14)

```python
client = bigquery.Client(project="376132452327")  # NOT "warwick-com-au"
```

### Athena Tips

- Column name: SST uses `timestamp`, not `event_timestamp`
- Timestamps: SST is tz-aware (UTC), Direct is tz-naive — `.dt.tz_localize(None)` before comparing
- Use `from_iso8601_timestamp()` not `CAST(... AS TIMESTAMP)`
- Use `json_format(json_extract(...))` not `CAST(... AS VARCHAR)` for JSON arrays
- Use `TRY_CAST(ecommerce_value AS DOUBLE)` — some payloads have empty strings
- Use `DOUBLE` not `BIGINT` for quantity fields (decimal metres)

## Schema Alignment Layer (SAL) v3.10

Transforms SST dimensions to match BigQuery. Views:

| View | Purpose |
|------|---------|
| `sst_events_transformed` | Event-level with aligned dimensions, geo_region, device_brand, channel group |
| `sst_sessions_daily` | Daily aggregates |
| `sst_comparison_ready` | Filtered events for AU Direct comparison |
| `sst_sessions` | Session-level rollup |
| `sst_ecommerce_items` | Item-level ecommerce with `site` field (infers Weave brand for NULL brands in Weave-exclusive categories) |

Match rates: Device/browser/OS 98%+, Geo 96.5%.

## Brand Taxonomy in items_ga4

`item_brand` contains comma-separated brand + treatments. The `items_ga4` view parses into:

| Field | Description |
|-------|-------------|
| `primaryBrand` | Warwick, Weave, or Thomas Maxwell Leather |
| `secondaryBrand` | Sub-brand: Warwick, Linia, Curate, Encore Recycled, Weave, Thomas Maxwell Leather |
| `fabricTreatments` | Comma-separated treatments only. NULL if none |
| `itemBrand` | Legacy — treatments with "Warwick, " stripped. For existing charts |
| `itemBrandRaw` | Original untouched value |

Treatment booleans: `isHaloEasyCare`, `isSunDec`, `isWarGuard`, `isHealthGuard`, `isLustrell`, `isTritan`.

Weave hierarchy: `weaveRange`, `weaveProductType`, `weaveSize`. Colour in `itemVariant`.

**Sale Type** (Looker Studio calculated field on `items_ga4`):
```
CASE
  WHEN purchaseRevenue = 0 THEN 'Cuttings'
  WHEN itemCategory = 'Upholstery' OR itemCategory = 'Drapery' THEN 'Metres'
  ELSE 'Units'
END
```

**Revenue:** Use `SUM(itemRevenue)` for item-level revenue. Do NOT `SUM(purchaseRevenue)` across items — it's transaction-level and duplicated per row. ~74% of items are free cuttings (`price = 0`).

## GTM Container Reference

| Container ID | Name | Type | Account |
|--------------|------|------|---------|
| GTM-P8LRDK2 | warwick.com.au | Web | 6005413178 |
| GTM-WQM54GN | warwick.co.nz | Web | 6005413178 |
| GTM-5L7LCRZ5 | Warwick SST | Server | 6005413178 |
| GTM-NX6WWZM | Weave Web - Unified | Web (Shopify) | 6005542175 |

GTM-NX6WWZM is Weave (Shopify), NOT Warwick.

**GA4 Measurement IDs (SST):**
| Measurement ID | Property |
|---------------|----------|
| G-Y0RSKRWP87 | Warwick AU - SST |
| G-H8L20K011G | Warwick NZ - SST |

Both flow through the same SST server container (GTM-5L7LCRZ5) to S3. The Athena `sst_events_transformed` WHERE clause filters on both.

## Lambda & Infrastructure

| Component | Details |
|-----------|---------|
| Lambda | `warwick-weave-sst-event-writer` |
| API Gateway | Private API `ez9g450hvl` via VPC endpoint |
| VPC Endpoint | `vpce-06fd9e5d58874b30f` |

## Workflow

1. Make changes to analysis scripts or matching helpers
2. Auth: `aws sso login --profile warwick`
3. Deploy: `git add -A && git commit -m "message" && git push`

## Next Steps

1. **Fix report audit issues** — See [`docs/REPORT_AUDIT.md`](docs/REPORT_AUDIT.md) (16 items, 4 critical)
2. **DataLayer cleanup** — Tony + dev team: `item_brand` split (brand vs treatments) + missing Weave brand tags
3. **Search spam mitigation** — Block CJK search requests at WAF/application level. See `docs/SEARCH_SPAM_RECOMMENDATION.md`
4. **Staff traffic exclusion (SST)** — Add office IP CIDR filtering to `sst_events_transformed` WHERE clause. GA4 Direct done (Testing mode since 2026-03-16). See gotcha #44
5. **Page 6 variant colour fix** — Tony to change dimension to `itemVariant`. See gotcha #40
6. **Unify AU and NZ properties**
7. **Rebuild Lambda Docker image** — `handler.py` updated with `site` in ITEMS_QUERY, needs Docker rebuild + ECR push. Until then, items BQ table gets `site` via `items_ga4` view JOIN (works but slower).
8. **GCP billing setup** — Trial expires ~2026-04-16. Tony/Nicole to add Warwick credit card. Cost ~$0-2/month (3.2 GB storage within free tier, growing ~1 GB/month).
9. **Finalise documentation and Git repo**

## Automated BigQuery Export

Weekly export runs every Monday 08:00 AEST via Step Functions + Lambda. See [`docs/BQ_EXPORT_AUTOMATION.md`](docs/BQ_EXPORT_AUTOMATION.md) for full details.

- **Schedule:** EventBridge `cron(0 21 ? * SUN *)` (Mon 08:00 AEST)
- **State Machine:** `warwick-weave-sst-bq-export`
- **Lambda:** `warwick-weave-sst-bq-export` (container image in ECR)
- **Notifications:** SNS → `timur@thelightscollective.agency` + `tony@thelightscollective.agency` (re-subscribed 2026-04-09 — previous subscriptions were never confirmed)
- **Infrastructure:** OpenTofu in `warwick-sst-infrastructure`, `bq-export.tf`

**Manual trigger:**
```bash
aws stepfunctions start-execution --profile warwick --region ap-southeast-2 \
  --state-machine-arn arn:aws:states:ap-southeast-2:025066271340:stateMachine:warwick-weave-sst-bq-export \
  --input '{}'
```

**Update Lambda code:** Rebuild image in `lambda/`, push to ECR, then update function:
```bash
cd lambda && docker buildx build --platform linux/amd64 --provenance=false --output type=docker -t warwick-bq-export:latest .
docker tag warwick-bq-export:latest 025066271340.dkr.ecr.ap-southeast-2.amazonaws.com/warwick-weave-sst-bq-export:latest
docker push 025066271340.dkr.ecr.ap-southeast-2.amazonaws.com/warwick-weave-sst-bq-export:latest
aws lambda update-function-code --profile warwick --region ap-southeast-2 \
  --function-name warwick-weave-sst-bq-export \
  --image-uri 025066271340.dkr.ecr.ap-southeast-2.amazonaws.com/warwick-weave-sst-bq-export:latest
```

## Looker Studio Integration

See [`docs/LOOKER_STUDIO.md`](docs/LOOKER_STUDIO.md) for setup, BigQuery views, gotchas, data refresh.

**BigQuery Dataset:** `376132452327.sst_events` (tables: `sessions` 483K, `events` 6.25M, `items` 22.8K)

**View architecture:**
| View | Purpose |
|------|---------|
| `sessions_ga4_all` / `events_ga4_all` / `items_ga4_all` | Both AU + NZ, complex logic (JOINs, region mapping, brand parsing) |
| `sessions_ga4` / `events_ga4` / `items_ga4` | AU-only wrappers (`WHERE site = 'AU'`). **Existing dashboards depend on these — do not rename.** |
| `sessions_ga4_nz` / `events_ga4_nz` / `items_ga4_nz` | NZ-only wrappers (`WHERE site = 'NZ'`). Used by NZ Looker Studio report. |

**Looker Studio reports:**
- **AU:** `sessions_ga4` (Reusable), `events_ga4` + `items_ga4` (Embedded), `warwick.com.au` GA4 Direct (Reusable, 13 charts)
- **NZ:** Created 2026-03-24 by copying AU report and using **Edit Connection** to reconnect each data source to `_nz` tables. NZ has zero purchases — ecommerce pages show "No data" (confirmed in GA4 Direct too).

**Looker Studio calculated fields** (must exist on each data source for charts to work):
- `sessions_ga4`: `averagePurchaseRevenueExCuttings` = `SUM(purchaseRevenue) / SUM(ecommercePurchasesExCuttings)`, `Purchase conv. rate` = `SUM(ecommercePurchases) / SUM(sessions)`
- `events_ga4`: `Average session duration` = `SUM(engagementTimeMsec) / SUM(sessions) / 1000`
- `items_ga4`: none

**Copying Looker Studio reports (NZ from AU):**
- Do NOT swap data sources in the copy dialog — field IDs won't match between independently created sources.
- Instead: copy with same sources, then **Edit Connection** on each data source to point to the `_nz` table. This preserves field IDs and matches by column name.
- NZ calculated fields must use `SUM()` wrappers (e.g. `SUM(x) / SUM(y)` not `x / y`) to force metric type, because NZ has zero purchase data and Looker Studio infers dimensions from all-zero columns.

## Deploying SAL

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
