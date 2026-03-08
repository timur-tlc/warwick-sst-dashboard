# Looker Studio Report Audit & Tony's Feedback

Report URL: `c1163e9e-adc7-4bd7-bd7a-fae2d17d1ffa`

## Page Inventory (15 pages as of 2026-02-28)

1. Business Overview — KPIs, sessions-to-revenue timeseries, revenue by state
2. Monthly Performance — Category/brand/product tables, revenue scorecard
3. Business Overview YoY — Year-over-year charts (2023-2026), Tony's page
4. Range Performance Detail — Product tables with quantity/revenue, brand/variant dropdowns
5. Weave Performance Overview — Weave scorecards, product type table, range/variant charts
6. Weave Product Detail — Range breakdown, variant tables, pivot table (BRAND x Item category)
7. Resource Page — Resource page engagement metrics
8. Blog — Blog page engagement metrics
9. Website Events — Top actions, search terms, AR pages
10. Browser + Device Performance — Device/browser charts, traffic sources
11. Scratchpad — Internal notes (hide from client)
12. Untitled Page — WIP sessions/transactions chart (hide from client)
13. Brand Performance — Brand-by-category charts, Weave top products
14. Search Terms — Search term analysis
15. Untitled Page — WIP

## Critical Issues

- [ ] **Page 3 — Wrong year label:** Scorecard says "2025 Total Sales" but shows 2026 data ($136K)
- [ ] **Page 1 — Purchase conv rate = 0.02:** Should display as 2% (percent format, not decimal)
- [ ] **Pages 3, 12 — `itemsPurchased` mislabeled as "Units Sold":** Row count, not actual quantities (gotcha #33)
- [ ] **Page 3 — YoY misleading:** 2025 shows -95.7% because SST data starts Dec 2025 only

## Label/Cosmetic Issues

- [ ] **Page 1 — Raw field names as KPI labels:** `purchaseRevenue`, `ecommercePurchasesExCuttings`, etc.
- [ ] **Page 1 — Revenue KPIs missing $ prefix**
- [ ] **Page 2 — Raw field names in column headers:** `itemBrand`, `quantityExCuttings`, `itemRevenue`
- [ ] **Page 6 — "Resource Pages // List of popular blog posts"** — should say "resource pages"

## Structural Issues

- [ ] **All pages — "Invalid" in Blended Data (7):** Broken blended data source
- [ ] **Page 2 — `itemBrand` shows combo strings:** Use `primaryBrand` for cleaner grouping
- [x] **Page 2 — `(not set)` brand at #3:** Fixed via SAL v3.10 Weave brand inference
- [ ] **Page 1 — SST outage gap (Jan 15-19):** No annotation explaining the gap

## Housekeeping

- [ ] **Page 10 (Scratchpad):** Internal notes visible — hide from client
- [ ] **Page 11 (Untitled Page):** WIP chart — hide or complete
- [ ] **Page 12 — Trello card heading visible:** Remove "NEW. Trello: WARWICK | Rethink Page 3 layout..."

## Tony's Feedback (2026-02-05) — Open Action Items

### Ours

- [ ] **Date range → February full month** — Tony uses "custom last month" not "last 28 days"
- [ ] **Page 1** — Separate transaction types (orders, cuttings/samples, rolls)
- [ ] **Page 3** — Rethink layout: Top Categories → Top Brands → Top Products
- [ ] **Page 3** — Handle unit/revenue distortion (rugs at $1000 vs fabric at $50/m)
- [ ] **Page 3** — Consider master chart with Category/Brand/Product filters
- [ ] **Page 4** — Resolve "(not set)" / "unassigned" values
- [ ] **Page 4** — Add separate browser chart alongside device chart
- [ ] **Page 5** — Isolate to Weave brand only
- [ ] **Page 5** — If Weave hierarchy works, backtrack to master chart for all brands

### Tony's Tasks

- **Page 2** — Building out 2025 yearly trends (one graph per year with quantity + $)
- **Pages 6-8** — Rethinking actions, surfacing key actions to front of report

### Open Question

- **Page screenshots?** — Especially pages 1, 3, 4, 5 so we can see current layout before modifying
