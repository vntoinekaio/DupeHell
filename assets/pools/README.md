# DupeHell Pool Data

## Purpose

These JSON files contain synthetic data pools used by DupeHell to generate
realistic-looking but fully synthetic multi-domain datasets for **record linkage
benchmarking**.

## Design principles

- **Domain-driven, not identity-driven** — pools are organized by functional
  domain (KYC, healthcare, fintech, etc.), never by ethnicity, nationality,
  religion, or any other protected category.
- **Domain-agnostic naming** — file and key names describe what the data *is*
  (e.g., `first_name.json`, `city.json`), not who it *represents*.
- **No sensitive category pools** — no pools exist for race, ethnicity,
  religion, sexual orientation, disability, caste, clan, political affiliation,
  criminal history, or biometric/genetic data.
- **Synthetic only** — these pools are generated or derived from public sources
  and do not correspond to any real individuals.
- **Uniform sampling** — all pools are flat arrays with equal-weight random
  selection. This is a deliberate choice for benchmark fairness (no frequency
  bias); see `DISTRIBUTION_NOTES.md` for details.

## Domains covered

academia, agriculture, automotive, aviation, banking, biotech, blockchain,
construction, crm, cybersecurity, ecommerce, education, energy, fashion,
fintech, food_beverage, gaming, healthcare, hospitality, hr,
insurance, kyc, legal, logistics, manufacturing, maritime, media, mining,
nonprofit, pharma, publishing, realestate, renewable_energy, retail, social_media,
sports, supplychain, technology, telecom, travel

## Ethical use

- These pools generate **synthetic data only**. Do not use outputs as real PII.
- Do not use for ML training on real-world data — synthetic distributions do
  not generalize to real-world scenarios.
- Do not use for fraud, identity theft, impersonation, surveillance, or any
  use that violates applicable laws.
- See [ETHICS.md](../../ETHICS.md) at the project root for full terms.

## Maintenance

- Pools are plain JSON arrays per locale (en, fr, de, es, it, pt).
- **Exception:** `french_cities.json` uses French region keys (e.g., `"ile_de_france"`) instead of locale codes, and stores `[city, postal_code]` pairs. This is intentional — it provides French geographic data with postal codes organized by administrative region.
- **Exception:** `currency.json` contains ISO currency codes (identical across locales since codes are universal).
- New pools should follow the same domain-agnostic pattern.
- Any pool containing values that map to protected categories (see
  ETHICS.md) must be reviewed before addition.
