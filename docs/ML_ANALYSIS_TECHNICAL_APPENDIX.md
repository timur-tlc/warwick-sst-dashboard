# Technical Appendix: ML Analysis of SST vs Direct Tracking Differences

## Executive Summary

We used machine learning (Random Forest, XGBoost) to identify which user characteristics predict whether a session appears in SST-only vs Direct-only. The model achieved **ROC-AUC of 0.59-0.65**, indicating **weak predictive power**. This is actually the key finding: the two populations are largely similar, with geographic location being the only meaningful differentiator.

---

## What is Feature Importance?

Feature importance measures how much a variable contributes to the model's predictions. We used two methods:

### 1. SHAP (SHapley Additive exPlanations)
- Based on game theory - measures each feature's contribution to each prediction
- Values are additive: sum of SHAP values = model output
- **Interpretation**: Mean |SHAP| of 0.34 for Country means country shifts predictions by ±0.34 on average

### 2. Random Forest Importance
- Measures how much each feature reduces prediction error (Gini impurity)
- **Interpretation**: Importance of 0.28 means 28% of the model's predictive power comes from this feature

---

## How to Interpret Importance Values

### Reference Scale for Binary Classification

| Importance | Interpretation | Real-World Example |
|------------|----------------|-------------------|
| **> 0.50** | Dominant predictor - this feature alone largely determines outcome | Gender predicting pregnancy |
| **0.30 - 0.50** | Strong predictor - major factor in classification | Credit score predicting loan default |
| **0.15 - 0.30** | Moderate predictor - meaningful but not decisive | Age predicting music preferences |
| **0.05 - 0.15** | Weak predictor - contributes but many other factors matter | Weather predicting ice cream sales |
| **< 0.05** | Negligible - essentially noise | Shoe size predicting job performance |

### What We Found (Warwick Data)

| Feature | SHAP Importance | Interpretation |
|---------|-----------------|----------------|
| Country | 0.343 | **Strong** - geography meaningfully predicts SST vs Direct |
| Hour | 0.114 | **Weak** - time of day has minor influence |
| Weekend | 0.054 | **Negligible** - weekday/weekend barely matters |
| Device | 0.051 | **Negligible** |
| Browser | 0.044 | **Negligible** |
| OS | 0.037 | **Negligible** |

---

## Model Performance Context

### ROC-AUC Interpretation

| ROC-AUC | Meaning |
|---------|---------|
| 1.00 | Perfect separation - model always correct |
| 0.90+ | Excellent - features strongly predict outcome |
| 0.80-0.90 | Good - meaningful prediction possible |
| 0.70-0.80 | Fair - some predictive power |
| 0.60-0.70 | Poor - barely better than random |
| 0.50 | Random - no predictive power (coin flip) |

**Our result: ROC-AUC = 0.59-0.65 (Poor)**

This means: **SST-only and Direct-only users are largely indistinguishable by their observable characteristics.** The model can barely do better than guessing.

### Why This Matters

If device, browser, OS, and time strongly predicted SST vs Direct, it would suggest systematic differences in user populations. Instead, we find:

1. **The populations are similar** - same mix of devices, browsers, operating systems
2. **Individual behavior dominates** - whether someone has an ad-blocker or sits behind a corporate firewall is personal, not demographic
3. **Geography is the exception** - China and a few other countries show distinct patterns

---

## Detailed SHAP Analysis

### Country Effects (Strongest Signal)

SHAP values show which countries push toward SST-only (+) vs Direct-only (-):

```
SST-only direction (ad-blocker/VPN users):
  China:        +0.48  (strong positive effect)
  New Zealand:  +0.17
  Ireland:      +0.14
  Hong Kong:    +0.11
  Australia:    +0.02  (slight positive)

Direct-only direction (corporate/firewall):
  Brazil:       -2.99  (extreme - 0% SST rate)
  Argentina:    -2.91
  Bangladesh:   -2.71
  Morocco:      -2.35
```

The extreme negative values for Brazil/Argentina indicate the model learned these countries are strong predictors of Direct-only status.

### Browser Effects (Weak Signal)

```
SST-only direction:
  Safari (in-app): +1.07  (privacy-focused)
  Firefox:         +0.36  (privacy-focused)

Direct-only direction:
  Chrome:          -0.005  (essentially neutral)
```

Note: Browser importance is low (0.044) because most users across both groups use Chrome.

### Device Effects (Weak Signal)

```
SST-only direction:
  mobile:  +0.10
  tablet:  +0.06

Direct-only direction:
  desktop: -0.03
```

Mobile slightly favors SST-only (personal devices with ad-blockers), but the effect is small.

---

## Why Country Dominates

### Hypothesis 1: China's Great Firewall Inverse Effect
- Chinese users often use VPNs to access international sites
- VPNs may block `google-analytics.com` but allow `sst.warwick.com.au`
- Result: 64% of Chinese sessions are SST-only

### Hypothesis 2: Regional Corporate Infrastructure
- Brazil/Argentina show 0% SST rate
- Possible: Regional ISPs or corporate networks block non-Google tracking domains
- Alternatively: Bot traffic from these regions that only hits Direct

### Hypothesis 3: Ad-Blocker Adoption Varies by Country
- Ireland (73.9% SST rate) has high privacy tool adoption
- Developing markets may have lower ad-blocker usage

---

## Methodology

### Data
- Period: January 6-13, 2026
- Sessions analyzed: 5,981 (SST-only + Direct-only, excluding Both)
- Features: device, OS, browser, country, hour, day of week, business hours flag

### Models
1. **Random Forest**: 100 trees, max depth 10
2. **XGBoost**: 100 estimators, max depth 5, learning rate 0.1

### Train/Test Split
- 80% train, 20% test
- Stratified by target variable

### Feature Engineering
- Categorical encoding via LabelEncoder
- Derived features: is_weekend, is_business_hours, is_morning, is_evening

---

## Limitations

1. **Correlation ≠ Causation**: Country predicting SST vs Direct doesn't mean country *causes* the difference - it's a proxy for underlying factors (VPN usage, firewall policies, ad-blocker adoption).

2. **Small Sample for Some Countries**: Brazil (69), Argentina (33) have limited sessions - conclusions should be tentative.

3. **Missing Features**: We don't have:
   - Network/ISP information
   - Whether user has ad-blocker installed
   - Corporate vs personal device flag

   These would likely be the true causal factors.

4. **Time Period**: One week of data may not capture seasonal or promotional variations.

---

## Conclusion

The ML analysis confirms:

1. **SST-only and Direct-only populations are demographically similar** (low model performance)
2. **Country is the only strong predictor** (SHAP 0.34), driven by China and Latin America anomalies
3. **Device, browser, OS, and time are weak predictors** (all < 0.12)

The difference between SST and Direct tracking is primarily explained by **individual user behavior** (ad-blocker installation, corporate network policies) rather than observable demographics. The geographic patterns suggest **regional infrastructure and privacy tool adoption** as secondary factors.

---

## Appendix: What Would "Strong" Look Like?

If we saw feature importance like this:

| Feature | Importance |
|---------|------------|
| Browser | 0.45 |
| OS | 0.25 |
| Device | 0.15 |

It would mean: "Safari users on macOS are almost always SST-only, while Chrome users on Windows are almost always Direct-only." This would indicate fundamental differences in who uses ad-blockers vs who sits behind corporate firewalls.

Instead, we found both populations use similar technology mixes, with only geography showing meaningful differentiation. This is actually good news: **SST isn't systematically missing any important user segment** - it's capturing a representative sample plus recovering users that Direct misses.
