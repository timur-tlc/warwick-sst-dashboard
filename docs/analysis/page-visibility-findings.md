# Page Visibility Analysis

**Date:** 2026-01-27 (updated)
**Data period:** 2026-01-25 to 2026-01-27 (3 days)
**Purpose:** Validate the hypothesis that Direct-only sessions are caused by prefetch/prerender traffic

## Background

We deployed a `page_visibility` parameter to both GA4 tags (SST and Direct) on 2026-01-25 to test whether Direct-only sessions correlate with browser prefetch/prerender behavior.

### GTM Implementation

**Container:** warwick.com.au (GTM-P8LRDK2)

**Variable:** `JS - Page Visibility State` (Custom JavaScript)

```javascript
function() {
  if (document.prerendering) return "prerender";
  if (document.visibilityState === "hidden") return "hidden";
  return "visible";
}
```

**Tags updated:**
- GA4 - Config - Warwick AU - SST (tag 56)
- GA4 - Config - Warwick AU - Direct (tag 69)

## Results (Full 3-Day Period)

### Direct Sessions by page_visibility (Jan 25-27)

**Total Direct sessions:** 2,751

| page_visibility | Sessions | % of Total | Instant (<0.1s) | Zero Engagement | Desktop | Australia |
|-----------------|----------|------------|-----------------|-----------------|---------|-----------|
| (not set) | 1,276 | 46.4% | 29.9% (381) | 40.5% (517) | 63.7% | 70.6% |
| visible | 1,232 | 44.8% | 17.9% (221) | **20.9% (258)** | 59.0% | 72.8% |
| hidden | 238 | 8.7% | 31.1% (74) | 55.0% (131) | 71.4% | 66.0% |
| prerender | 5 | 0.2% | 20.0% (1) | 20.0% (1) | 100.0% | 60.0% |

### SST Sessions by page_visibility (Jan 25-27)

**Total SST sessions:** 5,249

| page_visibility | Sessions | Events |
|-----------------|----------|--------|
| (not set) | 3,219 | 36,856 |
| visible | 1,699 | 41,463 |
| hidden | 325 | 3,870 |
| prerender | 6 | 77 |

### Cross-Reference: SST Hit Rate by page_visibility

Do Direct sessions with each page_visibility also appear in SST?

| page_visibility | Direct Sessions | Also in SST | SST Hit Rate |
|-----------------|-----------------|-------------|--------------|
| visible | 1,205 | 1,048 | **87.0%** |
| hidden | 157 | 135 | **86.0%** |
| prerender | 5 | 2 | 40.0% |
| (not set) | 1,246 | 713 | 57.2% |

## Key Findings

### 1. `visible` Sessions: High SST Hit Rate (87%)

- **87.0% reach SST** - When users actively view pages, both tracking systems work
- Low zero-engagement (21%) - Real user interaction
- 73% Australia - Matches expected customer base
- **The 13% gap is explainable by:**
  - Network timing edge cases
  - China traffic (Great Firewall)
  - Users navigating away before SST completes

### 2. `hidden` Sessions: Also High SST Hit Rate (86%)

- **86.0% reach SST** - Background tabs CAN complete SST requests
- 55% zero-engagement - Tabs opened but never viewed
- **Hidden tabs are NOT the main cause of Direct-only sessions**
- Similar SST hit rate to `visible` sessions

### 3. `prerender` = Rare but Does Cause Direct-Only

- Only 5 prerender sessions detected in Direct
- Only 2 (40%) reached SST
- **Confirms prerender CAN cause Direct-only, but volume is negligible**
- Chrome speculation rules aren't commonly triggered on warwick.com.au

### 4. `(not set)` = Tracking Gap

- 57.2% SST rate (lowest of all categories)
- Represents sessions where the GTM variable wasn't populated:
  - Sessions from before GTM cache propagated
  - Returning users with cached old GTM version
  - Non-browser traffic (bots, crawlers)

## Conclusions

1. **Tracking works correctly for visible pages** - 87% of `visible` sessions reach SST

2. **Hidden tabs don't explain Direct-only** - 86% hit rate is nearly identical to visible

3. **Prerender is rare** - Only 5 sessions over 3 days, too few to explain the Direct-only volume

4. **The remaining Direct-only sessions are likely:**
   - China/Great Firewall traffic (blocks google-analytics.com)
   - Sessions from before GTM propagated
   - Bot/crawler traffic without JavaScript execution
   - Edge cases in network timing

## browser_signals Parameter (Deployed Jan 26)

To gather more diagnostic data, we deployed a comprehensive `browser_signals` parameter on Jan 26.

**Variable:** `JS - Browser Signals` (Custom JavaScript)

```javascript
function() {
  var signals = [];
  // Page visibility
  if (document.prerendering) signals.push("vis:prerender");
  else if (document.visibilityState === "hidden") signals.push("vis:hidden");
  else signals.push("vis:visible");
  // Navigation type
  try {
    var nav = performance.getEntriesByType("navigation")[0];
    if (nav && nav.type) signals.push("nav:" + nav.type);
  } catch(e) {}
  // User activation
  try {
    if (navigator.userActivation) {
      signals.push(navigator.userActivation.hasBeenActive ? "user:active" : "user:passive");
    }
  } catch(e) {}
  // Webdriver (bot detection)
  if (navigator.webdriver === true) signals.push("bot:webdriver");
  // Hardware (Chrome-only)
  try {
    var cores = navigator.hardwareConcurrency;
    if (cores <= 2) signals.push("cpu:low");
    else if (cores <= 4) signals.push("cpu:med");
    else signals.push("cpu:high");
  } catch(e) {}
  // Device memory (Chrome-only)
  try {
    var mem = navigator.deviceMemory;
    if (mem <= 2) signals.push("mem:low");
    else if (mem <= 4) signals.push("mem:med");
    else if (mem) signals.push("mem:high");
  } catch(e) {}
  // Connection (Chrome-only)
  try {
    if (navigator.connection) signals.push("net:" + navigator.connection.effectiveType);
  } catch(e) {}
  // Viewport bucket
  var w = window.innerWidth;
  if (w < 768) signals.push("vp:mobile");
  else if (w < 1024) signals.push("vp:tablet");
  else if (w < 1440) signals.push("vp:desktop");
  else signals.push("vp:large");
  return signals.join("|");
}
```

**Example output:** `vis:visible|nav:navigate|user:active|cpu:high|mem:high|net:4g|vp:desktop`

### Early browser_signals Data (Jan 26-27)

- **543 sessions** with browser_signals in Direct
- **71 distinct signal combinations**
- Data collection is working in both Direct and SST

Note: Some signals (`cpu:`, `mem:`, `net:`) are Chrome-only. Safari/Firefox block these APIs for fingerprinting protection. The `vis:`, `nav:`, `user:`, `vp:` signals are cross-browser compatible.
