# Warwick Dashboard

**Purpose:** Streamlit dashboard for comparing SST vs Direct GA4 tracking data
**Client:** Warwick Fabrics (warwick.com.au)
**Last Updated:** 2026-01-23
**Status:** ⚠️ MAJOR METHODOLOGY CORRECTION - Dashboard updated with corrected matching

## Quick Start

```bash
warwick-dash  # Run from anywhere (uses ~/bin/warwick-dash script)
```

**Prerequisites:**
- AWS SSO login: `aws sso login --profile warwick`
- Script sets `AWS_PROFILE=warwick` automatically

## Architecture

| Component | Details |
|-----------|---------|
| Framework | Streamlit 1.40.0 |
| Data Source | AWS Athena (`warwick_weave_sst_events.events`) + BigQuery (`analytics_375839889`) |
| Deployment | Streamlit Community Cloud (auto-deploys on push to main) |
| Live URL | https://warwick-dashboard.streamlit.app |

## Key Files

- `app.py` - Main Streamlit application with corrected matching
- `corrected_matching_helpers.py` - **NEW:** Fuzzy matching logic (timestamp+attribute based)
- `athena_transformation_layer.sql` - Schema Alignment Layer (SAL) v3.5
- `requirements.txt` - Python dependencies
- `.venv/` - Local virtual environment

## ⚠️ CRITICAL DISCOVERY: ga_session_id Matching Flaw (2026-01-23)

### The Problem

**Original analysis used `ga_session_id` for session matching, which FAILED because:**
- Same user session gets **different session IDs** in SST vs Direct
- `ga_session_id` is Unix timestamp in **seconds** (1-second granularity)
- SST and Direct events arrive 0.2-1.5 seconds apart
- 61% of same-session pairs land in different seconds → different IDs

**Example:**
```
User clicks at:     12:34:56.000
SST arrives:        12:34:56.200 → ga_session_id = 1768098275
Direct arrives:     12:34:57.300 → ga_session_id = 1768098276 (DIFFERENT!)
```

### Evidence

From pairwise analysis of "SST-only" vs "Direct-only" sessions:
- **61.2%** had matching timestamp+device+country (within 5 minutes)
- **50.2%** had **consecutive session IDs** (differ by 1)
- **Median time difference:** 0.3 seconds
- **r = 0.635** correlation between timestamp diff and ID diff

**Conclusion:** These were the SAME sessions miscategorized due to timing differences!

### Corrected Methodology

**Now using fuzzy matching** via `corrected_matching_helpers.py`:

```python
for each SST session:
    1. Find Direct sessions within ±5 minutes
    2. Filter to matching device + country
    3. Take closest timestamp match
    4. Label both as "Both"
```

### Impact on Numbers

**OLD (ga_session_id matching - WRONG):**
- Both: 13,754 (69.7%)
- SST-only: 3,233 (16.3%)
- Direct-only: 2,811 (14.2%)

**NEW (timestamp+attribute matching - CORRECT):**
- Both: 15,160 (82.4%) ⬆️ +1,406 sessions
- SST-only: 1,827 (9.9%) ⬇️ -1,406 sessions
- Direct-only: 1,405 (7.6%) ⬇️ -1,406 sessions

**SST Value:** Revised from +14.5% to **+10.5% unique sessions** vs Direct alone.

## Key Insights (Jan 6-13, CORRECTED)

| Metric | Value | Sessions |
|--------|-------|----------|
| Both (overlap) | 82.4% | 15,160 |
| SST-Only (ad-blockers) | 9.9% | 1,827 |
| Direct-Only (corporate) | 7.6% | 1,405 |

**Corporate Hypothesis - STILL VALIDATED ✅**

Direct-only sessions STILL show strong B2B profile (corrected numbers):
- **83.1% Desktop** (vs 68.4% baseline) = +14.7pp ⬆️
- **71.5% Windows** (vs 51.5% baseline) = +20.1pp ⬆️
- **100% of Windows sessions are Desktop** (no Windows mobile/tablet exists)
- χ² = 126.05, p < 0.0001 (highly significant)

**SST-only shows similar profile** (80.2% desktop, 67.8% Windows), suggesting both "only" groups are corporate users blocked by different network policies.

**Conversion Rate Hypothesis - STILL VALIDATED ✅**

Both "only" categories have lower purchase rates:

| Category | Sessions | Purchase Rate |
|----------|----------|---------------|
| Both | 15,160 | **2.51%** |
| Direct-only | 1,405 | **1.42%** (-1.09pp) |
| SST-only | 1,827 | **1.64%** (-0.87pp) |

## Temporal Alignment Analysis

**Hourly correlation:** r = 0.915 (very strong)
- SST-only and Direct-only sessions happen at the **same times of day**
- Peak hours overlap (22:00-23:00 UTC)
- Supports **technical blocking hypothesis** over behavioral differences

**Weekday concentration:**
- Direct-only: 85.2% weekday (vs 83.3% baseline)
- SST-only: 79.1% weekday
- +6.1pp difference validates corporate work-hours pattern

## Dashboard Features

### Colour Scheme
Consistent across all charts:
- **Purple (#9b59b6)** = Both (overlap)
- **Green (#2ecc71)** = SST-only
- **Blue (#3498db)** = Direct-only

### Corrected Comparison Tab
- Shows OLD vs NEW categorization side-by-side
- Live fuzzy matching (cached for 1 hour)
- Statistical validation of hypotheses
- Methodology explanation in expander

## Critical Gotchas

### 1. Session Matching - USE FUZZY MATCHING ONLY

**❌ WRONG:** `set(bq_df['ga_session_id']) & set(sst_df['ga_session_id'])`

**✅ CORRECT:** Use `corrected_matching_helpers.py` with timestamp+attribute matching

**Why:** ga_session_id has 1-second granularity but events arrive with sub-second differences.

### 2. SST Outage Jan 15-19, 2026

Lambda wasn't being called from GTM server container. Use Jan 6-13 or Jan 20+ for valid analysis.

### 3. Date Range for Analysis

**Valid periods:**
- Jan 6-13: Pre-outage (8 days, used for corrected analysis)
- Jan 20+: Post-recovery

**Invalid:** Jan 15-19 shows 0 SST sessions (outage period)

### 4. Windows == Windows+Desktop (100%)

All Windows sessions are desktop devices (no Windows mobile/tablet):
- Windows Phone discontinued 2017
- Surface tablets classified as 'desktop' in User-Agent parsing
- Makes "Windows+Desktop" metric redundant

### 5. Client Hints vs User-Agent

Safari and Firefox do NOT support User-Agent Client Hints. The `client_hints.mobile` field is NULL for ~50% of sessions. Always use SAL views which parse User-Agent.

### 6. GA4 Missing Fields

Warwick's GA4 does NOT collect:
- `screen_resolution` - not in BigQuery export
- `device.browser` - empty for all sessions

### 7. Streamlit Dataframes Don't Render Markdown

`st.dataframe()` shows `**bold**` as literal asterisks. Use plain text in table cells.

### 8. Live Queries Can Be Slow

First load after SSO refresh takes 30-60 seconds. Results are cached for 1 hour via `@st.cache_data(ttl=3600)`.

## Schema Alignment Layer (SAL) v3.5

**Purpose:** Transform SST dimension values to match BigQuery exactly.

**Note:** SAL was designed for ga_session_id JOIN, but this approach is now deprecated. SAL views are still useful for dimension normalization.

**Verified match rates:**
- Device/browser/OS: 98%+
- Geo: 96.5% (3.5% mismatch is VPN users)

**Views:**

| View | Purpose |
|------|---------|
| `sst_events_transformed` | Event-level data with BigQuery-aligned dimensions |
| `sst_sessions_daily` | Daily aggregates for trend comparison |
| `sst_comparison_ready` | Filtered events for AU Direct comparison |
| `sst_sessions` | Session-level rollup (deprecate ga_session_id JOIN approach) |

**Athena timestamp parsing:** Use `from_iso8601_timestamp(session_start)` NOT `CAST(session_start AS TIMESTAMP)` - the latter fails with ISO8601 'Z' suffix.

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
| `corrected_analysis.py` | Full corrected categorization with profiles |
| `pairwise_matching.py` | Detect same sessions with different IDs |
| `session_id_pattern_check.py` | Analyze ID differences (proves 1-second granularity) |
| `temporal_alignment_check.py` | Hourly/daily patterns comparison |
| `collision_check.py` | Check for ID collisions (2.3% rate, acceptable) |
| `user_agent_diagnosis.py` | Investigate cross-device sessions |
| `check_windows_devices.py` | Verify Windows=Desktop (100%) |

## Workflow

1. Make changes to `app.py` or `corrected_matching_helpers.py`
2. Local: `streamlit run app.py` (auto-reloads on save)
3. Test with: `aws sso login --profile warwick` first
4. Deploy: `git add -A && git commit -m "message" && git push`
5. Streamlit Cloud auto-deploys within ~1 minute

## Lessons Learned

### Don't Trust ga_session_id for Cross-Source Matching

When parallel tracking systems fire simultaneously:
- Events arrive at different microsecond timestamps
- GA4 generates session IDs from Unix **seconds** (not milliseconds)
- Same session → different IDs 61% of the time

**Solution:** Fuzzy match on timestamp window + device + country.

### Session ID Granularity Matters

- ga_session_id: **1 second** granularity
- Event timing differences: **0.2-1.5 seconds**
- Result: High false negative rate (same sessions misclassified as "only")

### Always Validate Assumptions

The "similar profiles" finding led to discovering the ID matching flaw. When results seem too convenient, dig deeper.

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
