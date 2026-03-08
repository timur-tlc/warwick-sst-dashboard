# Site Search Spam — CJK Character Injection

**Date:** 2026-02-24
**Prepared for:** Tony (to forward to dev team)

## Problem

Warwick's site search is being used by bots to inject Chinese-language spam content (escort service ads). This is automated activity, not real users.

**Scale:**
- 2,021 spam sessions identified (1.1% of all sessions)
- 1,945 spam search events, 316 polluted page titles
- **0 transactions, $0 revenue** — purely junk traffic

**Evidence it's bots:**
- 100% Chrome/Windows/Desktop — zero mobile (real traffic is ~30% mobile)
- 100% Direct channel — no organic search, no referrals
- Identical event pattern every session: page_view → search → filter → scroll
- Flat 24-hour activity — no human sleep cycle
- 90% concentrated in a 4-day burst (Jan 27-30)
- Brazil accounts for 23.8% of CJK sessions — bots using Brazilian proxies to search in Chinese

**Top offending countries:**

| Country | Spam Sessions | % of country's sessions |
|---------|---------------|------------------------|
| China | 1,494 | 7.3% |
| Brazil | 381 | 23.8% (bots on Brazilian proxies) |
| Hong Kong | 11 | — |
| Taiwan | 8 | — |
| **Australia** | **6** | **negligible** |

Australia has only 6 sessions with CJK characters — false positive risk is effectively zero.

## Impact on Reporting

- **Top search terms charts are polluted** — spam appears alongside legitimate fabric searches (lustrell, eastwood, outdoor)
- **Search event counts are inflated** by ~1,945 events
- **No impact on revenue or transaction data** — 0 purchases from spam sessions
- We've added an `isCjkSpam` flag to the dashboard to filter these out in reporting

## Recommendations

### 1. Best Fix — Block at WAF/CDN Level (prevents sessions entirely)

If Warwick uses Cloudflare, AWS CloudFront, or similar CDN, add a rule to **block requests** to the search endpoint (`/products/?query=`) when the query string contains CJK characters (Unicode ranges `U+4E00–U+9FFF`, `U+3040–U+309F`, `U+30A0–U+30FF`).

This prevents the page from loading at all — no session is created, no events fire, no data pollution. The bot gets a 403 and moves on.

### 2. Application-Level Fix — Reject CJK Search Requests (dev team)

If WAF rules aren't available, add a server-side check on the search controller/API endpoint: if the search query contains CJK characters, **return an empty results page or 400 error** instead of processing the search.

This is a simple regex check — a few lines of code. The key is to **reject the request entirely** (not just strip the characters), so no `view_search_results` event fires and the session generates no meaningful analytics data.

```python
# Example (Python/Django)
import re
CJK_PATTERN = re.compile(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]')
if CJK_PATTERN.search(query):
    return HttpResponse("No results", status=400)
```

```javascript
// Example (Node.js/Express)
const CJK_PATTERN = /[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]/;
if (CJK_PATTERN.test(req.query.q)) {
    return res.status(400).send('No results');
}
```

Warwick sells fabric in AU/NZ — there is no legitimate business need for CJK search terms.

### 3. Additional Hardening (if spam returns after fix)

- **Rate limiting** — max 5 search requests per IP per minute (real users don't fire dozens of searches in succession)
- **Honeypot field** — hidden form field that bots fill but humans never see
- **reCAPTCHA** on search if volume escalates
- **Bot user-agent blocking** at WAF level
