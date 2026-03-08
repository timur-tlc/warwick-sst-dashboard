# Warwick SST Dashboard

Comparing SST (Server-Side Tracking) vs Direct GA4 tracking for warwick.com.au.

## Key Finding

**SST captures 10% additional sessions** that Direct misses (ad-blockers, China/Great Firewall).

## Results (Jan 6-13, 2026)

| Category | Sessions | % |
|----------|----------|---|
| Both | 15,072 | 81.5% |
| SST-only | 1,915 | 10.4% |
| Direct-only | 1,493 | 8.1% |

**What each category means:**
- **Both:** Normal users - tracked by both systems
- **SST-only:** Ad-blocker users + China (Great Firewall blocks google-analytics.com)
- **Direct-only:** Corporate networks + prefetch traffic (block unfamiliar domains)

## Dashboard

[Live Dashboard](https://warwick-dashboard.streamlit.app) (Streamlit Cloud)

## Documentation

| Document | Purpose |
|----------|---------|
| [Session Matching Methodology](methodology/session-matching.md) | Why ga_session_id fails, fuzzy matching solution, validation results |
| [Page Visibility Analysis](analysis/page-visibility-findings.md) | Testing the prefetch hypothesis with browser visibility API |
| [ML Analysis Technical Appendix](ML_ANALYSIS_TECHNICAL_APPENDIX.md) | Random Forest + SHAP analysis of user characteristics |
| [Validation Scripts](reconciliation_validation.md) | Scripts to verify matching methodology |

## Data Files

- `data/geo_verification_results.csv` - Country-level matching validation
- `data/bootstrap_results.csv` - Confidence interval calculations
- `data/sample_inspection.csv` - Random session audit
- `data/timestamp_drift_pairs.csv` - Timing difference analysis
