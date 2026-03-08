# SST vs Direct Analysis Findings

Complete analysis results from the Warwick SST quality comparison. For development reference, see the main `CLAUDE.md`.

## Filters Applied (All Analysis)

| Filter | Value | Notes |
|--------|-------|-------|
| **Date** | Jan 6-13, Jan 21-25 | 13 normal days |
| **Site** | warwick.com.au (AU) | NZ site excluded |
| **Geo** | All countries | China, Australia, etc. all included |
| **Engagement** | All sessions | Zero-engagement sessions included |

**Matching method:** Fuzzy matching (±15s window + device_category + geo_country)

## Final Verdict: SST Quality Analysis (13 Normal Days)

**SST captures higher-quality traffic than Direct-only.**

| Metric | Both (n=26,120) | SST-only (n=2,862) | Direct-only (n=2,807) |
|--------|-----------------|--------------------|-----------------------|
| Sessions | 82.2% | 9.0% | 8.8% |
| Zero Engagement | 7,091 (27.2%) | 715 (24.8%) | 1,087 (38.4%) |
| Engaged Sessions | 19,006 (72.8%) | 2,170 (75.2%) | 1,743 (61.6%) |
| Mean Engagement (sec) | 172.7 | 124.3 | 115.9 |
| Purchases | 658 (2.52%) | 31 (1.62%) | 18 (1.21%) |
| China % | 843 (3.2%) | 777 (40.6%) | 138 (9.2%) |

**Direct-only is automated traffic:** 38.4% zero engagement, loses 38.4% when filtering to engaged-only.
**SST-only captures real users:** 40.6% from China (Great Firewall blocks google-analytics.com).

**Australia-only match rate:** ~90% (vs 82% globally). Lower global rate is pulled down by China SST-only traffic.

**Recommendation:** If decommissioning one source, keep SST. Full reports in Outline: http://127.0.0.1:8888

### Engaged Sessions Analysis (13 Days)

| Metric | Both | SST-only | Direct-only |
|--------|------|----------|-------------|
| Sessions | 19,694 | 2,238 | 1,734 |
| Purchases | 673 (3.42%) | 46 (2.06%) | 33 (1.90%) |
| China | 6.0% | 43.0% | 9.9% |

- SST captures **2.4% more** engaged sessions than Direct (21,253 vs 20,749)
- SST-only has **29% more** engaged sessions than Direct-only (2,238 vs 1,734)

## ga_session_id Matching Flaw (2026-01-23)

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

## Key Insights (13 Normal Days, ±15s Window)

**Complete Dimension Analysis: 35/36 dimensions differentiated**

All dimensions show significant differentiation except Traffic Source (no data). Key findings:

| Dimension | Both (n=26,120) | SST-only (n=2,862) | Direct-only (n=2,807) | Finding |
|-----------|-----------------|--------------------|-----------------------|---------|
| Desktop % | ~68% | ~80% | ~84% | "Only" = corporate |
| Windows % | ~51% | ~68% | ~72% | "Only" = corporate |
| Australia % | ~78% | ~45% | ~38% | International in "only" |
| China % | 3.2% | **40.6%** | 9.2% | Great Firewall → SST |
| Zero Engagement % | 27.2% | **24.8%** | 38.4% | Direct-only = automated |
| Engaged Sessions % | 72.8% | **75.2%** | 61.6% | SST-only = better quality |
| Purchase Rate | 2.52% | **1.62%** | 1.21% | SST-only has more conversions |

**Key patterns:**
- SST-only and Direct-only are similar to each other, both different from Both
- Direct-only is extreme: 38.2% zero engagement, 70% shallow sessions (2-5 events)
- China accounts for 40.1% of SST-only - Great Firewall blocks google-analytics.com
- Only 1% of Direct-only are single-event sessions - human bounces would be higher

### Direct-Only = Prefetch/Prerender Traffic (Not Human Bounces)

Critical finding from engagement analysis (n=2,821 Direct-only sessions). Additional evidence:
- **~70% shallow sessions (2-5 events)** - typical of automated page loads
- **Only ~1% single-event sessions** - human bounces would show higher rate
- **96% of zero-engagement sessions fire all events within 0.1 seconds**
- Typical sequence: `session_start → user_engagement` with 0.0s span

**Profile of Direct-only:** ~84% desktop, ~72% Windows, Latin America over-represented (prefetch networks common there)

## Prefetch/Prerender Investigation (2026-01-25)

### Discovery: Why Direct-Only Sessions Don't Appear in SST

**Key finding:** SST filters almost nothing (86 sessions), but 1,493 sessions are Direct-only. The traffic never reaches SST in the first place.

**Root cause hypothesis: Browser Prefetch/Prerender**

Modern browsers (especially Chrome) speculatively load pages before users click:
1. Browser predicts which link user might click
2. Page loads in hidden background tab
3. JavaScript executes (GA4 client-side fires immediately)
4. GTM Web Container may not fully execute before prerender is aborted
5. Result: Direct receives event, SST does not

**Evidence:** Direct-only sessions are 84% desktop, 72% Windows, 61% new users, 36% instant (<0.1s), 66% empty referrer. Only 75/16,946 SST sessions flagged as bots (0.4%) - SST is NOT filtering these out; they simply never arrive.

### GTM Variables Deployed (Container GTM-P8LRDK2)

| Variable | Version Date | Output Example |
|----------|-------------|----------------|
| `JS - Page Visibility State` | 2026-01-25 | `visible`, `hidden`, `prerender` |
| `JS - Browser Signals` | 2026-01-26 | `vis:visible\|nav:navigate\|user:active\|cpu:high\|mem:high\|net:4g\|vp:desktop` |

Both added to tags: `GA4 - Config - Warwick AU - SST` (tag 56) and `GA4 - Config - Warwick AU - Direct` (tag 69).

**Note:** `cpu:`, `mem:`, `net:` signals unreliable in Safari/Firefox (fingerprinting protection). `vis:`, `nav:`, `user:`, `vp:` are cross-browser reliable.

### Investigation Results (Jan 25-27, 2026)

**Page visibility:** `visible` sessions reach SST 87% of the time. `hidden` sessions also 86% - background tabs are NOT a cause of Direct-only. Prerender is negligible (5 sessions). Direct-only sessions are primarily pre-GTM-propagation traffic, China/Great Firewall, and network edge cases.

**Browser signals:** `user:active` is the strongest signal for real users - only 7.1% Direct-only vs 20.4% for `user:passive`. Sessions with user interaction reach SST 93% of the time.

## Daily Match Rates (13 Normal Days - Verified 2026-01-27)

| Date | Direct | SST | Both | Both % | S-only | D-only | S vs D |
|------|--------|-----|------|--------|--------|--------|--------|
| Jan 06 | 2,061 | 2,099 | 1,485 | 68.4% | 111 | 576 | -81% |
| Jan 07 | 2,184 | 2,146 | 2,058 | 87.5% | 167 | 126 | +33% |
| Jan 08 | 2,033 | 2,062 | 1,874 | 85.1% | 170 | 159 | +7% |
| Jan 09 | 1,922 | 1,684 | 1,751 | 85.5% | 127 | 171 | -26% |
| Jan 10 | 1,232 | 1,257 | 1,151 | 87.3% | 87 | 81 | +7% |
| Jan 11 | 1,147 | 1,479 | 1,085 | 88.9% | 73 | 62 | +18% |
| Jan 12 | 2,868 | 3,041 | 2,734 | 88.9% | 206 | 134 | +54% |
| Jan 13 | 3,118 | 3,219 | 2,944 | 72.1% | 964 | 174 | +454% |
| Jan 21 | 3,425 | 3,457 | 2,470 | 68.4% | 187 | 955 | -80% |
| Jan 22 | 3,288 | 3,442 | 3,162 | 91.2% | 180 | 126 | +43% |
| Jan 23 | 3,175 | 2,604 | 3,060 | 93.2% | 110 | 115 | -4% |
| Jan 24 | 1,405 | 1,314 | 1,341 | 90.7% | 73 | 64 | +14% |
| Jan 25 | 1,069 | 1,178 | 1,005 | 68.1% | 407 | 64 | +536% |
| **Total** | **28,927** | **28,982** | **26,120** | **82.2%** | **2,862** | **2,807** | **+2%** |

**Notes:**
- **Excludes:** Jan 14-20 (SST outage Jan 15-19, partial days around it)
- **Jan 06, 21 anomalies:** Low match rate (~68%) - early morning timezone edge effects
- **Normal days:** 85-93% match rate
- **Totals verified:** Direct = Both + D-only, SST = Both + S-only (exact match)

## Lessons Learned

### Always Validate Assumptions

The "similar profiles" finding led to discovering the ID matching flaw. When results seem too convenient, dig deeper.

### Event Count Differences Between Sources

`event_count` measures how many events **each source received**, not session quality. Do NOT use it to compare SST vs Direct quality — use `engagement_time` and `has_purchase` instead.

**Root cause:** SST does not capture synthetic events (`session_start`, `first_visit`), creating a +1/+2 event gap per session:

| Event | Direct | SST | Difference |
|-------|--------|-----|------------|
| `session_start` | 26,695 | **0** | +26,695 (100% missing) |
| `first_visit` | 10,984 | **0** | +10,984 (100% missing) |
| `view_item_list` | 107,415 | 120,495 | -13,080 (SST has MORE) |

SST actually captures MORE user-initiated events (e.g. `view_item_list`). The gap is entirely synthetic events. In "Both" sessions, SST sees 17.1% as single-event vs 0.02% in Direct — SST often only receives the initial event(s).

**Impact:**
- Session counts / purchases / conversions: No impact
- Event counts: SST undercounts by ~1-2 per session (synthetic events only)
- User behavior events: SST captures these well (sometimes better)

### How GA4 Engagement Time is Calculated

GA4 measures engagement via the `user_engagement` event:

1. User arrives → `session_start` fires
2. GA4 tracks time while page is in **foreground** and user is **active**
3. User leaves/navigates → `user_engagement` fires with `engagement_time_msec`
4. GA4 sums `engagement_time_msec` across the session

**Key:** If SST's GA4 property misses some `user_engagement` events, engagement time will be undercounted. This explains why SST has more single-event sessions - it got `session_start` but missed the subsequent `user_engagement`.

### SST vs Direct Totals Are Nearly Identical (81.6% Overlap)

When comparing total SST (16,987) vs total Direct (16,565), quality scores are nearly identical because 81.6% of unique sessions appear in both sources. The marginal differences from SST-only (+1,915) and Direct-only (+1,493) are diluted.

**Use `sst_vs_direct_quality.py` for total comparison, but the segmented analysis (Both/SST-only/Direct-only) reveals more meaningful differences.**
