// DupeHell -- MIT License . Educational Use Only
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// EDUCATIONAL AND RESEARCH PURPOSES ONLY -- see ETHICS.md for prohibited uses.
// No liability for misuse.

pub mod addresses;
pub mod companies;
pub mod dates;
pub mod extra;
pub mod identifiers;
pub mod names;
pub mod typos;
pub mod visual;

use arrow::array::{Array, ArrayRef};

use crate::rng::Rng;

/// Length threshold constants matching Python noise functions.
pub const MIN_LEN_TYPO: usize = 2;
pub const MIN_LEN_AGGR: usize = 3;
pub const MIN_LEN_EXTREME: usize = 4;
pub const MIN_LEN_DROPOUT: usize = 4;
pub const MIN_LEN_UNICODE: usize = 3;

/// Read a mutable Vec<char> from a StringArray at index `i`.
/// Returns None for null entries or strings shorter than `min_len`.
pub fn get_chars(arr: &arrow::array::StringArray, i: usize, min_len: usize) -> Option<Vec<char>> {
    if arr.is_null(i) {
        return None;
    }
    let s = arr.value(i);
    if s.len() < min_len {
        return None;
    }
    Some(s.chars().collect())
}

/// Dispatch hub: maps noise type string to the actual noise function.
pub fn apply_noise_to_column(
    col: &dyn Array,
    noise_type: &str,
    rng: &mut Rng,
) -> Result<ArrayRef, String> {
    Ok(match noise_type {
        // Typos
        "typo" | "typos" => typos::apply_typos_str(col, rng, 2),
        "typo_aggressive" | "typos_aggressive" => typos::apply_typos_aggressive(col, rng),
        "typo_extreme" | "typos_extreme" => typos::apply_typos_extreme(col, rng),
        "qwerty_azerty" => typos::apply_qwerty_azerty(col, rng),
        // Visual (category → random sub-type)
        "visual" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 5] = [
                visual::apply_homoglyph,
                visual::apply_unicode_pollution,
                visual::apply_ocr_errors,
                visual::apply_case_swap,
                visual::apply_char_dropout,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "homoglyph" => visual::apply_homoglyph(col, rng),
        "unicode_pollution" => visual::apply_unicode_pollution(col, rng),
        "ocr_errors" => visual::apply_ocr_errors(col, rng),
        "case_swap" => visual::apply_case_swap(col, rng),
        "char_dropout" => visual::apply_char_dropout(col, rng),
        // Dates (category → noise_dates)
        "dates" | "date_error" | "date_chaotic" => dates::noise_dates(col, rng),
        "date_format_mix" | "date_mix" => dates::noise_dates_mix(col, rng),
        "age_impossible" => dates::apply_age_impossible(col, rng),
        // Identifiers (category → random sub-type)
        "identifiers" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 4] = [
                identifiers::corrupt_email,
                identifiers::corrupt_phone,
                identifiers::corrupt_national_id,
                identifiers::corrupt_siren,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "email_corrupt" | "corrupt_email" => identifiers::corrupt_email(col, rng),
        "phone_corrupt" | "corrupt_phone" => identifiers::corrupt_phone(col, rng),
        "national_id_corrupt" | "corrupt_national_id" => identifiers::corrupt_national_id(col, rng),
        "siren_corrupt" | "corrupt_siren" => identifiers::corrupt_siren(col, rng),
        // Names (category → random sub-type)
        "names" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 4] = [
                names::apply_nickname,
                |c, _| names::apply_initials(c),
                names::apply_partial,
                names::apply_name_compound,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "nickname" => names::apply_nickname(col, rng),
        "initials" => names::apply_initials(col),
        "partial" => names::apply_partial(col, rng),
        "name_compound" => names::apply_name_compound(col, rng),
        // Companies (category → random sub-type)
        "companies" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 4] = [
                |c, _| companies::drop_legal_form(c),
                companies::apply_word_dropout,
                companies::apply_company_scramble,
                companies::apply_acronym,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "legal_form_drop" => companies::drop_legal_form(col),
        "word_dropout" => companies::apply_word_dropout(col, rng),
        "company_scramble" => companies::apply_company_scramble(col, rng),
        "acronym" => companies::apply_acronym(col, rng),
        // Addresses (category → random sub-type)
        "addresses" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 3] = [
                addresses::apply_address_scramble,
                addresses::apply_language_mix,
                addresses::apply_postal_corrupt,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "address_scramble" => addresses::apply_address_scramble(col, rng),
        "language_mix" => addresses::apply_language_mix(col, rng),
        "postal_corrupt" => addresses::apply_postal_corrupt(col, rng),
        // Extra (category → random sub-type)
        "extra" => {
            let fns: [fn(&dyn Array, &mut Rng) -> ArrayRef; 7] = [
                extra::apply_missing,
                extra::apply_nullify,
                extra::apply_exact,
                extra::apply_blocking_initial,
                extra::apply_blocking_partial,
                extra::apply_fuzzy_match,
                extra::apply_phonetic,
            ];
            fns[rng.next_usize(fns.len())](col, rng)
        }
        "missing" => extra::apply_missing(col, rng),
        "name_null" | "dob_null" => extra::apply_nullify(col, rng),
        "exact" => extra::apply_exact(col, rng),
        "blocking_fail_initial" => extra::apply_blocking_initial(col, rng),
        "blocking_fail_partial" => extra::apply_blocking_partial(col, rng),
        "fuzzy_match" => extra::apply_fuzzy_match(col, rng),
        "phonetic" => extra::apply_phonetic(col, rng),
        _ => return Err(format!("unknown noise type: {noise_type}")),
    })
}
