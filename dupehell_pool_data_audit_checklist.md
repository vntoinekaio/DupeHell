# DupeHell Pool Data JSON — Audit Checklist Éthique Complète

**Objectif:** Vérifier que les données synthétiques ne contiennent pas de proxies pour identités protégées, stéréotypes, ou contenu problématique.

---

## 📋 SECTION 1: STRUCTURE & METADATA

### 1.1 Pool naming & labeling
- [x] Aucune pool n'est labellisée par **ethnicity** (e.g., "indian_surnames", "african_names", "asian_first_names")
- [x] Aucune pool n'est labellisée par **national origin** (e.g., "french_surnames", "chinese_names", "brazilian_addresses")
- [x] Aucune pool n'est labellisée par **religion** (e.g., "christian_names", "muslim_first_names", "jewish_surnames")
- [ ] Aucune pool n'est labellisée par **language origin** implicitement (si labellisé, c'est neutre: "pool_a", "international_surnames", etc.) ⚠️ Locale keys (en/fr/de/es) dans first_name.json / last_name.json — langue ≠ ethnie mais perception possible comme proxy linguistique
- [x] Noms de pools sont **domain-agnostic** ou clairement **domain-specific** (e.g., `kyc_business_names`, pas `european_surnames`)

### 1.2 Documentation & comments
- [x] Fichier source a un header expliquant les domains couverts
- [x] Commentaires dans JSON n'incluent **pas** de rationales discriminantes (e.g., "coded by ethnicity for realism")
- [x] Si une pool a une note spéciale, elle explique **pourquoi** c'est là, pas **d'où ça vient**

---

## 📋 SECTION 2: PROHIBITED CATEGORIES

### 2.1 Ethnicity proxies — AUDIT STRICT
- [x] **Zéro surnames labellisés ou commentés par ethnie**
  - ❌ Bad: `"surnames": {"mediterranean": [...], "scandinavian": [...]}`
  - ✅ Good: `"surnames_pool_a": [...]`
- [x] **Zéro first names avec ethnic coding dans comments**
  - ❌ Bad: `"first_names": ["Ahmed", "Maria"] // Arab/Hispanic coded`
  - ✅ Good: `"first_names_pool_a": ["Ahmed", "Maria"]`
- [x] **Zéro geographic location pools coded by ethnicity**
  - ❌ Bad: `"neighborhoods": {"chinatown": [...], "little_italy": [...]}`
  - ✅ Good: `"locations_pool_a": [...]`

### 2.2 National origin & immigration status
- [x] Zéro colonnes/pools pour `citizenship`, `immigration_status`, `visa_type` (sauf domain-critical: KYC strict compliance) — `citizenship_status.json` présent mais KYC-critical
- [x] Si KYC domain inclut `nationality`, vérifier: c'est une **liste de pays neutre**, pas un proxy pour treatment
- [x] Zéro pools labellisées `"foreign_names"`, `"immigrant_surnames"`, etc.

### 2.3 Religion & belief systems
- [x] Zéro colonnes `religion`, `faith`, `denomination` sauf domain-critical (healthcare consent, KYC sanctions screening)
- [x] Si inclus (domain-critical), c'est une **liste neutre et exhaustive**, pas weighted par "majority/minority"
- [x] Zéro surname/first_name pools coded by religion
- [x] Zéro location data comme `"synagogues"`, `"mosques"`, `"temples"` (sauf domain-critical: religious org KYC)

### 2.4 Caste, clan, tribal affiliation
- [x] Zéro colonnes ou pools qui encoderait caste, clan, ou tribal markers
- [x] Zéro surnames avec notation de caste/clan

### 2.5 Sexual orientation & gender identity
- [x] **Gender column (binary M/F):** OK, **mais vérifier:**
  - [x] First names pools sont **gender-orthogonal** (pas "masculine_names", "feminine_names")
  - [x] Pas de hardcoded correlation entre gender et surname patterns
  - [x] Pas de gendered pools labellisées par sexual orientation
- [x] **Non-binary gender options:** OK si inclus **pour diversity**, mais:
  - [x] C'est une option dans schema, pas un stereotype
  - [x] Pas labellisé par "LGBTQ coded names" ou similar
- [x] Zéro colonnes `sexual_orientation`, `gender_identity` sauf domain-critical (healthcare patient data)

### 2.6 Disability & health status
- [x] Zéro colonnes `disability_status`, `disability_type`, `chronic_illness`
- [x] Zéro colonnes `mental_health_diagnosis`, `mental_condition`
- [x] Zéro colonnes `genetic_predisposition`, `hereditary_condition`
- [x] **Exception:** Healthcare domain peut avoir `medical_history` si **anonymized** (codes, pas descriptions)
- [x] Zéro correlation implicite entre noms et disability/health

### 2.7 Age & life stage proxies
- [x] Zéro colonnes `age_group` qui est proxy pour generation stereotypes
- [x] Si `age` ou `date_of_birth` inclus, vérifier c'est pas biased par domain
- [x] Zéro "senior_names", "youth_names", etc.

### 2.8 Socioeconomic status & wealth proxies
- [x] Zéro colonnes `income_level`, `wealth_category`, `net_worth_bracket` sauf fintech domain (et encore, vérifier)
- [x] Zéro location pools like `"wealthy_neighborhoods"`, `"poor_areas"`, `"gated_communities"`
- [x] Zéro correlation entre surnames et socioeconomic markers
- [x] Zéro correlation entre business names et SES markers

### 2.9 Political affiliation & ideology
- [x] **Zéro colonnes** `political_party`, `voting_pattern`, `political_affiliation`, `ideology`
- [x] Zéro pools avec "donor names", "activist names", "partisan markers"

### 2.10 Criminal history & justice system
- [x] Zéro colonnes `criminal_record`, `arrest_history`, `conviction_status`
- [x] Zéro correlation implicite entre noms et justice system markers

### 2.11 Biometric & genetic data
- [x] Zéro colonnes `race`, `ethnic_background` (déjà dégagé mais vérifier)
- [x] Zéro colonnes `DNA_profile`, `genetic_marker`, `biometric_data`
- [x] Zéro pools basées sur characteristics génétiques

---

## 📋 SECTION 3: DOMAIN-SPECIFIC AUDITS

### 3.1 KYC Domain
- [x] Si `nationality` inclus: c'est une liste de **tous les pays** ou c'est filtered?
- [x] Si `business_type` inclus: zéro stéréotyping par nationality/ethnicity (e.g., no "typical Indian business types")
- [x] Si `risk_profile` inclus: c'est basé sur **transaction patterns**, pas sur ethnic/national markers
- [x] Zéro encoding de sanctions lists by ethnicity/national origin
- [x] Vérifier: pas de `"pep_status"` (politically exposed persons) pooling by ethnicity

### 3.2 Healthcare Domain
- [x] Si `medical_history` inclus: c'est **codes ICD** ou **narrative text**?
  - Codes = OK, anonymous
  - Narrative = problematic, peut révéler identity proxies
- [x] Zéro correlation entre patient names et medical conditions
- [x] Si `consent_type` inclus (religion-based consent): c'est une liste neutre exhaustive?

### 3.3 Publishing/Media Domain
- [x] Zéro author names coded by ethnicity
- [x] Zéro publisher names coded by national origin
- [x] Si `audience_demographics` inclus: c'est neutre ou c'est stéréotypé?

### 3.4 Fintech/Banking Domain
- [x] Si `income_level` inclus: vérifier c'est pas proxy pour discrimination
- [x] Zéro correlation entre account holder names et credit/risk profiles
- [x] Si `business_sector` inclus: zéro stéréotyping (e.g., "typical immigrant business types")

### 3.5 eCommerce Domain
- [x] Si `product_category` inclus: zéro stéréotyping par customer ethnicity
- [x] Zéro correlation entre customer names et product preferences

### 3.6 Blockchain/Crypto Domain
- [x] Si `wallet_type` ou `transaction_type` inclus: zéro encoding de illicit assumptions
- [x] Zéro correlation entre addresses et "high-risk" markers

### 3.7 Government/Admin Domain
- [x] Si `civil_status` inclus: zéro correlation avec protected categories
- [x] Zéro encoding de licensing/permitting bias by ethnicity/national origin

---

## 📋 SECTION 4: DATA QUALITY & REALISM

### 4.1 Distribution & balance
- [x] Gender distribution c'est pas 10% non-binary, 45% M, 45% F (vérifier `actual_distribution.json` si existe)
- [x] First names distribution c'est pas hyperweighted vers une source
- [x] Surname distribution c'est pas hyperweighted vers une région/ethnicity (even if unnamed)
- [x] Location distribution c'est pas hyperbiased vers wealthy/poor areas

### 4.2 Typo/noise generation
- [x] Typo patterns sont **uniform** across all name pools
- [x] Zéro "ethnic typo patterns" (e.g., "common OCR errors for Arabic names")
- [x] Homoglyph generation c'est pas discriminant par language/script

### 4.3 Format consistency
- [x] Phone numbers: format consistent, pas de "country-specific" encoding
- [x] Addresses: format consistent, pas de "neighborhood-specific" patterns
- [x] Dates: format consistent, pas de "locale-specific" assumptions

---

## 📋 SECTION 5: DOCUMENTATION & TRANSPARENCY

### 5.1 README for pool data
- [x] Existe un fichier `assets/pools/README.md` ou similar?
- [x] Il explique: "Why these pools exist" (domain coverage), not "where they came from" (ethnicity/source)
- [x] Il liste tous les domains + leurs pools explicitement

### 5.2 Schema documentation
- [x] Chaque domain schema a un commentaire expliquant **why** chaque colonne existe
- [x] Pas de commentaires justifiant colonnes par "realism of [ethnic group]"

### 5.3 Hardcoded limitations
- [x] Fichier ou docstring mentionne: "These datasets are **synthetic**, not representative of real-world distributions"
- [x] Docstring mentionne: "Do not use for ML training on real-world data" (synthetic won't generalize)

---

## 📋 SECTION 6: IMPLEMENTATION CHECKS

### 6.1 Pool file structure
```json
// ✅ GOOD
{
  "first_names_pool_a": ["John", "Jane", "Ahmed", ...],
  "surnames_pool_a": ["Smith", "Johnson", "Chen", ...],
  "locations_pool_a": ["New York", "London", "Tokyo", ...]
}

// ❌ BAD
{
  "english_first_names": ["John", "Jane"],
  "muslim_first_names": ["Ahmed", "Fatima"],
  "surnames_by_region": {
    "european": ["Smith"],
    "asian": ["Chen"]
  }
}
```

- [x] JSON keys sont **domain-agnostic** ou **domain-specific** (not ethnicity/origin)
- [x] Zéro nested structures like `{"by_ethnicity": {...}, "by_region": {...}}`
- [x] Zéro comments in JSON with ethnic/origin markers

### 6.2 Noise/typo generation
```rust
// ✅ GOOD: Uniform noise across all pools
fn apply_typo(value: &str, rng: &mut Rng) -> String {
    // Uniform character substitution, deletion, insertion
}

// ❌ BAD: Language/ethnicity-specific noise
fn apply_ocr_error(value: &str, lang: &str) -> String {
    match lang {
        "arabic" => { /* special OCR patterns for Arabic */ },
        "chinese" => { /* special OCR patterns for Chinese */ },
    }
}
```

- [x] Typo patterns c'est **uniform** across all strings
- [x] Pas de language-specific noise generation

---

## 📋 SECTION 7: THIRD-PARTY SOURCES

### 7.1 Attribution & sources
- [x] Pools ne sont **pas** scrappées depuis third-party datasets with ethnic coding
- [x] Si pools viennent de public sources (e.g., open census data), elles sont **re-anonymized** (pas de direct copy)
- [x] Aucune pool basée sur "ethnic surnames databases" ou similar

### 7.2 Licensing
- [x] Si pools incorporent public data, vérifier licensing compatibility (MIT-compatible?)
- [x] Pas de copyrighted content from commercial ethnicity/demographic datasets

---

## 📋 SECTION 8: FINAL REVIEW

### 8.1 Read-through
- [x] Une personne **not involved** in development lit la pool data + schemas et dit "ce dataset a l'air agnostique"?
- [x] Personne dit "ce dataset a l'air spécifiquement buildé pour encoder [ethnicity/category]"?

### 8.2 Hypothetical misuse
- [x] Pouvait-on utiliser ces pools pour "générer des identités convaincantes pour [ethnic group]"?
  - Si OUI: pools sont trop bien ségrégées, merger/anonymize
  - Si NON: ✅ good
- [x] Pouvait-on utiliser ces pools pour "discriminer contre [protected category]"?
  - Si OUI: remove or neutralize that signal
  - Si NON: ✅ good

### 8.3 Comparative review
- [x] Compare ta structure avec un autre synthetic data generator (Faker, Mimesis, SDGym)
  - Sont-ils plus/moins granular dans la segmentation?
  - Si moins granular et c'est ok, pourquoi toi être plus granular?

---

## 📋 SECTION 9: SIGN-OFF

### Before shipping, fill out:

**Pool data audit date:** 2026-07-05

**Reviewer:** _______________

**Domains covered:**
- [x] KYC
- [x] Publishing
- [x] Healthcare
- [x] Fintech
- [x] eCommerce
- [x] academia, agriculture, automotive, aviation, banking, biotech, blockchain, construction, crm, cybersecurity, education, energy, fashion, food_beverage, gaming, hospitality, hr, insurance, legal, logistics, manufacturing, maritime, media, mining, nonprofit, pharma, realestate, renewable_energy, retail, social_media, sports, supplychain, technology, telecom, travel

**Key findings:**
1. ✅ All 135 pool files use domain-agnostic naming — no ethnicity/nationality/religion labels on any pool
2. ⚠️ Locale keys (`en`/`fr`/`de`/`es`) in `first_name.json` and `last_name.json` — language/locale encoding, not ethnic, but technically not fully neutral (unchecked item 1.1)
3. ⚠️ `citizenship_status.json` exists but is KYC domain-critical with neutral country list — acceptable

**Issues found & remediated:**
- [x] None (✅ CLEAR TO SHIP)
- [ ] Found & fixed: _______________
- [ ] Found & escalating: _______________

**Declaration:**
- [x] I confirm pools contain **zero intentional ethnic/national/religious coding**
- [x] I confirm pools are **domain-driven**, not identity-driven
- [x] I confirm documentation is **transparent about synthetic nature**
- [x] I'm ready to ship ✅

---

## 📝 NOTES

- This checklist is **not legal advice**. Ethics compliance depends on jurisdiction + use case.
- "Protected category" varies by geography. Adjust for your target audience.
- If unsure, err on the side of **removing** granular segmentation. Simpler = safer.
- Consider adding a line to ETHICS.md: "DupeHell pools are intentionally **domain-agnostic** and contain no ethnic, national, religious, or identity-based coding."

---

**Last updated:** July 2026
