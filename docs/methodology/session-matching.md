# Session Matching Methodology

**Date:** 2026-01-23
**Status:** Validated

## The Problem

How do we determine if an SST session and a Direct session are the same user visit?

### Initial Approach (WRONG)

We originally matched sessions using `ga_session_id`:

```python
both = set(sst_df['ga_session_id']) & set(direct_df['ga_session_id'])
```

**This failed because:**
- `ga_session_id` is a Unix timestamp in **seconds** (not milliseconds)
- SST and Direct events arrive 0.2-1.5 seconds apart
- Same user session gets **different session IDs** 61% of the time

### Example

```
User clicks at:     12:34:56.000
SST arrives:        12:34:56.200 → ga_session_id = 1768098275
Direct arrives:     12:34:57.300 → ga_session_id = 1768098276 (DIFFERENT!)
```

## Solution: Fuzzy Matching

Match sessions using timestamp proximity + attributes:

```python
for each SST session:
    1. Find Direct sessions within ±15 seconds
    2. Filter to matching device_category + geo_country
    3. Take closest timestamp match
    4. Label both as "Both"
```

### Time Window Selection

Tested windows from ±1s to ±5min:

| Window | Both Sessions | Change from ±5min |
|--------|---------------|-------------------|
| ±5 min | 15,263 | baseline |
| ±15 sec | 15,160 | -0.7% |
| ±5 sec | 14,984 | -1.8% |
| ±1 sec | 4,364 | -71.4% |

**Chose ±15 seconds:** Captures 99.3% of matches. 95% of real matches occur within 0.75 seconds.

### Matching Levels

| Level | Attributes | Use Case |
|-------|------------|----------|
| BASIC | device_category + geo_country | Default |
| ENHANCED | + device_operating_system | Higher confidence |
| STRICT | + device_browser | Even higher |
| LANDING | + landing page URL | Highest confidence |

## Results

### Before vs After Correction

| Category | OLD (ga_session_id) | NEW (fuzzy) |
|----------|---------------------|-------------|
| Both | 69.7% | **81.5%** |
| SST-only | 16.3% | 10.4% |
| Direct-only | 14.2% | 8.1% |

+1,406 sessions correctly reclassified from "only" to "Both"

## Validation

### Geographic Patterns (Confirmed)

| Country | Both | SST-only | Direct-only |
|---------|------|----------|-------------|
| Australia | 90.2% | 6.0% | 3.8% |
| China | 49.9% | **43.3%** | 6.8% |
| Iran | 28.6% | **71.4%** | 0% |

China and Iran results confirm the Great Firewall hypothesis - google-analytics.com is blocked, but SST gets through.

### Direct-Only Profile (Confirmed)

- 59.9% zero engagement (vs 26.6% for Both)
- 90% desktop, 77% Chrome
- Likely corporate prefetch/prerender traffic

### Sensitivity Analysis

Results stable across time windows (±30s to ±5min). Sharp drop at ±1s confirms window is appropriately tight.

## Implementation

```python
from corrected_matching_helpers import categorize_sessions, MatchLevel

sst_df, direct_df, stats = categorize_sessions(
    sst_df, direct_df,
    time_window_seconds=15,
    match_level=MatchLevel.BASIC
)
```
