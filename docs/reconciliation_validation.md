# Validation Scripts

**Purpose:** Scripts to validate the session matching methodology
**Date:** 2026-01-24

## Quick Reference

```bash
source .venv/bin/activate

# Run validation suite
python sensitivity_analysis.py    # Time window stability
python geo_verification.py        # China/geo hypothesis
python bot_analysis.py            # Direct-only bot check
python timestamp_drift.py         # Timing differences
python bootstrap_analysis.py      # Confidence intervals
python sample_inspection.py       # Random sample review
```

## Scripts

| Script | Purpose | Data Source |
|--------|---------|-------------|
| `sensitivity_analysis.py` | Test time windows ±30s to ±10min | Live query |
| `geo_verification.py` | Country-level matching rates | Cache |
| `bot_analysis.py` | Bot patterns in Direct-only | Live (optional) |
| `timestamp_drift.py` | SST vs Direct timing | Cache |
| `bootstrap_analysis.py` | 95% confidence intervals | Cache |
| `sample_inspection.py` | Random session audit | Cache |
| `event_sequence_analysis.py` | Event sequence comparison | Live |

## Key Results (2026-01-24)

### Geographic Validation ✓

- **China:** 43.3% SST-only (Great Firewall confirmed)
- **Australia:** 90.2% Both (matching works)
- **Iran:** 71.4% SST-only (Google blocked)
- **Brazil/Argentina:** ~95% Direct-only (bot/prefetch traffic)

### Engagement Analysis ✓

- Direct-only: 59.9% zero engagement
- Both: 26.6% zero engagement
- Direct-only is 90% desktop, 77% Chrome

### Timing Analysis ✓

- 95% of matches within 0.75 seconds
- ±15s window captures 99.3% of ±5min matches

## Output Files

Generated in `docs/data/`:

| File | Contents |
|------|----------|
| `geo_verification_results.csv` | Country breakdown |
| `timestamp_drift_pairs.csv` | Pair timing analysis |
| `bootstrap_results.csv` | CI calculations |
| `sample_inspection.csv` | Random samples |

## Notes

- Scripts use `cache/` parquet files when available
- Run `materialize_matching.py` first to refresh cache
- All scripts handle division by zero and empty dataframes
