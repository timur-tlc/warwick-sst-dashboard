# Warwick Dashboard

**Purpose:** Streamlit dashboard for comparing SST vs Direct GA4 tracking data
**Client:** Warwick Fabrics (warwick.com.au)
**Last Updated:** 2026-01-22

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
| Data Source | AWS Athena (`warwick_weave_sst_events.events`) |
| Deployment | Streamlit Community Cloud (auto-deploys on push to main) |
| Live URL | https://warwick-dashboard.streamlit.app |

## Key Files

- `app.py` - Main Streamlit application
- `athena_transformation_layer.sql` - SQL views for SST↔BigQuery reconciliation (v3.4)
- `verify_transformations.py` - Script to verify transformations against BigQuery
- `requirements.txt` - Python dependencies
- `.venv/` - Local virtual environment
- `~/bin/warwick-dash` - Launch script (sets AWS_PROFILE, activates venv)

## Athena Transformation Layer (v3.4)

**Purpose:** Transform SST dimension values to match BigQuery exactly, enabling session-level reconciliation via JOIN on `ga_session_id`.

**Verified match rates:** 98%+ for device_category, browser, OS, country (remaining ~2% are session ID collisions and geo lookup differences between BigQuery and CloudFront).

**Views:**

| View | Purpose |
|------|---------|
| `sst_events_transformed` | Event-level data with BigQuery-aligned dimensions |
| `sst_sessions_daily` | Daily aggregates for trend comparison |
| `sst_comparison_ready` | Filtered events for AU Direct comparison |
| `sst_sessions` | Session-level rollup for JOIN-based reconciliation |

**Key dimensions for reconciliation:**

| SST Field | BigQuery Equivalent | Values |
|-----------|---------------------|--------|
| `device_category` | `device.category` | desktop, mobile, tablet |
| `device_browser` | `device.web_info.browser` | Chrome, Safari, Safari (in-app), Edge, Firefox, Samsung Internet |
| `device_operating_system` | `device.operating_system` | Windows, iOS, Macintosh, Android, Linux |
| `geo_country` | `geo.country` | Full names (Australia, not AU) |

**Critical notes:**
- Device detection uses User-Agent parsing, NOT client_hints (Safari/Firefox don't send those)
- Country codes mapped to full names (~60 countries) to match BigQuery
- In-app browsers (Facebook, Instagram) detected as "Safari (in-app)"
- Bot detection is conservative to match BigQuery's IAB/ABC filtering

**Usage:**
```sql
-- Session-level reconciliation
SELECT ga_session_id, device_category, device_browser, device_operating_system, geo_country
FROM warwick_weave_sst_events.sst_sessions
WHERE year = '2026' AND month = '01' AND site = 'AU';

-- Find unmapped country codes (potential alignment issues)
SELECT geo_country_code, COUNT(*)
FROM warwick_weave_sst_events.sst_sessions
WHERE geo_country = geo_country_code  -- Unmapped = code wasn't in mapping
GROUP BY 1 ORDER BY 2 DESC;
```

## Expected Differences After Transformation

| Difference Type | SST Captures | Direct Captures | Cause |
|-----------------|--------------|-----------------|-------|
| Ad-blocker bypass | ✓ | ✗ | First-party domain bypasses blocklists |
| China/GFW traffic | ✓ | ✗ | google-analytics.com blocked |
| Corporate firewalls | ✗ | ✓ | Unknown domains blocked |
| Safari Private | ✗ | ✗ | GTM script blocked before tags fire |

## Data Sources

| Source | Location | Measurement ID |
|--------|----------|----------------|
| Direct (GA4) | BigQuery `analytics_375839889` | `G-EP4KTC47K3` |
| SST | Athena `warwick_weave_sst_events.events` | `G-Y0RSKRWP87` |

**SST vs Direct tab uses static snapshot data** (Jan 1-13, 2026).

## Key Insights

| Metric | Value | Interpretation |
|--------|-------|----------------|
| Dual-Property Lift | +14.5% | Extra sessions from running both |
| SST-Only Sessions | 12.7% | Ad-blocker bypass wins |
| Direct-Only Sessions | 15.8% | Corporate firewalls blocking SST |
| China SST Advantage | +11% to +255% | Great Firewall blocks google-analytics.com |
| Conversion Parity | 99%+ | Both systems accurate for business metrics |

## Critical Gotchas

### 1. Client Hints vs User-Agent

**Safari and Firefox do NOT support User-Agent Client Hints.** The `client_hints.mobile` field is NULL for ~50% of sessions. Always use the transformation views which parse User-Agent.

### 2. Synthetic Events

GA4 creates `session_start` and `first_visit` server-side. They appear in BigQuery but **never in SST raw data**. The transformation layer flags these with `is_synthetic_event`.

### 3. Browser Detection for In-App Browsers

BigQuery reports the underlying browser for in-app browsers (Safari for iOS Facebook app, Chrome for Android). The transformation layer matches this behavior - do NOT try to detect "Facebook" as a browser.

### 4. Bot Filtering

Use conservative bot detection to match BigQuery. Over-filtering (e.g., aggressive AI crawler detection) causes SST to under-count vs Direct.

### 5. Session Matching

Use `ga_session_id` for matching sessions between sources. JOIN on this field to find SST-only and Direct-only sessions.

## Validation Reports

Cross-reference with `/home/timur/tlc/warwick-sst-infrastructure/clients/warwick.com.au/validation/`:
- `SESSION-LEVEL-ANALYSIS-20260115.md` - Device/country breakdown
- `SST-VS-DIRECT-COMPREHENSIVE-REPORT.md` - Full 7-day comparison (Jan 2-8, 2026)

## Workflow

1. Make changes to `app.py`
2. Local: Streamlit auto-reloads on save
3. Deploy: `git add -A && git commit -m "message" && git push`
4. Streamlit Cloud auto-deploys within ~1 minute

## Deploying Transformation Layer

Run the SQL in `athena_transformation_layer.sql` in Athena to create/update views:
```bash
aws athena start-query-execution \
  --query-string "$(cat athena_transformation_layer.sql)" \
  --work-group primary \
  --profile warwick
```
