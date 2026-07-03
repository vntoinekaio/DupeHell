// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow::array::{ArrayRef, StringBuilder};

use crate::context::Context;
use crate::rng::Rng;

/// Sample `n` random values from a named pool.
pub fn pool_values(pool_name: &str, n: usize, rng: &mut Rng, ctx: &Context) -> ArrayRef {
    let pool = match ctx.pool_store.get(pool_name) {
        Some(p) => p,
        None => {
            let mut builder = StringBuilder::with_capacity(n, 4);
            for _ in 0..n {
                builder.append_value("");
            }
            return Arc::new(builder.finish());
        }
    };
    if pool.is_empty() {
        let mut builder = StringBuilder::with_capacity(n, 4);
        for _ in 0..n {
            builder.append_value("");
        }
        return Arc::new(builder.finish());
    }
    let len = pool.len();
    let mut builder = StringBuilder::with_capacity(n, 16);
    for _ in 0..n {
        builder.append_value(&pool[rng.next_usize(len)]);
    }
    Arc::new(builder.finish())
}

/// Strip known prefixes from a column name (e.g. "pat_first_name" -> "first_name").
pub fn strip_prefix(name: &str) -> String {
    let prefixes = [
        "pat_",
        "prov_",
        "cur_",
        "alias_",
        "insured_",
        "residential_",
        "registered_",
        "billing_",
        "shipping_",
        "default_",
        "agent_",
        "mailing_",
        "personal_",
        "contact_",
    ];
    let lower = name.to_lowercase().replace(' ', "_");
    for pfx in &prefixes {
        if let Some(stripped) = lower.strip_prefix(pfx) {
            return stripped.to_string();
        }
    }
    lower
}

/// Guess a pool name from a column name using heuristic pattern matching.
pub fn guess_pool_name(col_name: &str) -> Option<&'static str> {
    let key = strip_prefix(col_name);
    POOL_MAP.get(key.as_str()).copied()
}

static POOL_MAP: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("first_name", "first_name");
    m.insert("given_name", "first_name");
    m.insert("middle_name", "first_name");
    m.insert("last_name", "last_name");
    m.insert("family_name", "last_name");
    m.insert("city", "city");
    m.insert("birth_place", "city");
    m.insert("residential_city", "city");
    m.insert("registered_city", "city");
    m.insert("state", "state");
    m.insert("state_code", "state");
    m.insert("postal_code", "postal_code");
    m.insert("zip_code", "postal_code");
    m.insert("zip", "postal_code");
    m.insert("residential_postal_code", "postal_code");
    m.insert("registered_postal_code", "postal_code");
    m.insert("country", "country_code");
    m.insert("country_code", "country_code");
    m.insert("residential_country", "country_code");
    m.insert("registered_country", "country_code");
    m.insert("company_name", "company");
    m.insert("company", "company");
    m.insert("legal_name", "company");
    m.insert("trading_name", "company");
    m.insert("vendor", "vendor");
    m.insert("occupation", "occupation");
    m.insert("product_name", "product_names");
    m.insert("product_type", "product_category");
    m.insert("gender", "gender");
    m.insert("gender_code", "gender");
    m.insert("insurance_plan_name", "insurance_plans");
    m.insert("payer_name", "insurance_plans");
    m.insert("department_id", "department");
    m.insert("department", "department");
    m.insert("encounter_type", "product_category");
    m.insert("specialty", "specialty");
    m.insert("fin_class", "fin_class");
    m.insert("document_type", "document_type");
    m.insert("document_number", "document_number_prefixes");
    m.insert("language", "language");
    m.insert("nationality", "country_code");
    m.insert("industry_code", "industry_code");
    m.insert("legal_form", "legal_form");
    m.insert("payment_method", "payment_method");
    m.insert("financial_status", "financial_status");
    m.insert("fulfillment_status", "fulfillment_status");
    m.insert("customer_group", "customer_group");
    m.insert("address_line2", "address_type");
    m.insert("status", "status_active");
    m.insert("account_status", "status_account");
    m.insert("policy_status", "status_policy");
    m.insert("claim_status", "status_claim");
    m.insert("benefit_status", "status_benefit");
    m.insert("listing_status", "status_listing");
    m.insert("booking_status", "status_booking");
    m.insert("enrollment_status", "status_enrollment");
    m.insert("marital_status", "marital_status");
    m.insert("filing_status", "filing_status");
    m.insert("citizenship_status", "citizenship_status");
    m.insert("policy_type", "policy_type");
    m.insert("account_type", "account_type");
    m.insert("property_type", "property_type");
    m.insert("room_type", "room_type");
    m.insert("line_type", "line_type");
    m.insert("call_type", "call_type");
    m.insert("claim_type", "claim_type");
    m.insert("benefit_type", "benefit_type");
    m.insert("permit_type", "permit_type");
    m.insert("owner_type", "owner_type");
    m.insert("employee_type", "employee_type");
    m.insert("transaction_type", "transaction_type");
    m.insert("lead_status", "lead_status");
    m.insert("tier", "tier");
    m.insert("loyalty_tier", "loyalty_tier");
    m.insert("risk_score", "risk_score");
    m.insert("risk_rating", "risk_score");
    m.insert("industry", "industry_sector");
    m.insert("job_title", "job_title");
    m.insert("make", "device_make");
    m.insert("model", "device_model");
    m.insert("hotel_name", "hotel_chain");
    m.insert("chain_name", "hotel_chain");
    m.insert("agency_name", "company");
    m.insert("carrier_name", "company");
    m.insert("warehouse_name", "company");
    m.insert("carrier_type", "carrier_type");
    m.insert("preferred_language", "language");
    m.insert("citizenship", "country_code");
    m.insert("course_name", "product_names");
    m.insert("previous_institution", "company");
    m.insert("department_name", "department");
    m.insert("cost_center", "department");
    m.insert("work_location", "city");
    m.insert("category", "product_category");
    m.insert("merchant_name", "company");
    m.insert("merchant_category", "product_category");
    m.insert("plan_name", "product_names");
    m.insert("issuing_authority", "company");
    m.insert("grade", "gender");
    m.insert("title", "occupation");
    m
});

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};

    #[test]
    fn test_guess_known() {
        assert_eq!(guess_pool_name("first_name"), Some("first_name"));
        assert_eq!(guess_pool_name("city"), Some("city"));
        assert_eq!(guess_pool_name("company"), Some("company"));
        assert_eq!(guess_pool_name("gender"), Some("gender"));
    }

    #[test]
    fn test_guess_stripped() {
        assert_eq!(guess_pool_name("pat_first_name"), Some("first_name"));
        assert_eq!(guess_pool_name("residential_city"), Some("city"));
        assert_eq!(guess_pool_name("billing_city"), Some("city"));
        assert_eq!(guess_pool_name("personal_first_name"), Some("first_name"));
    }

    #[test]
    fn test_guess_unknown() {
        assert_eq!(guess_pool_name("nonexistent_column"), None);
    }

    #[test]
    fn test_strip_prefix() {
        assert_eq!(strip_prefix("pat_first_name"), "first_name");
        assert_eq!(strip_prefix("residential_city"), "city");
        assert_eq!(strip_prefix("plain_name"), "plain_name");
    }

    #[test]
    fn test_pool_values_lookup() {
        use crate::context::Context;
        let pools_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("dupehell/assets/pools");
        let ctx = Context::new("kyc", pools_dir.to_str().unwrap()).unwrap();
        let mut rng = Rng::new(42);
        let arr = pool_values("first_name", 10, &mut rng, &ctx);
        assert_eq!(arr.len(), 10);
        let s = arr.as_string::<i32>();
        for i in 0..10 {
            assert!(!s.value(i).is_empty(), "first_name[{i}] empty");
        }
    }
}
