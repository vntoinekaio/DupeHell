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

use arrow::array::{Array, ArrayRef, AsArray, StringArray, UInt32Array};
use arrow::compute::take;
use std::sync::Arc;

use crate::rng::Rng;

/// Length threshold constants matching Python noise functions.
pub const MIN_LEN_TYPO: usize = 2;
pub const MIN_LEN_AGGR: usize = 3;
pub const MIN_LEN_EXTREME: usize = 4;
pub const MIN_LEN_DROPOUT: usize = 4;
pub const MIN_LEN_UNICODE: usize = 3;

/// Load the chars of a `StringArray` value at index `i` into a caller-owned,
/// reusable buffer (cleared first) instead of allocating a fresh `Vec<char>`
/// per row. Returns `false` (buffer left empty) for null entries or strings
/// shorter than `min_len`.
pub fn get_chars_into(
    arr: &arrow::array::StringArray,
    i: usize,
    min_len: usize,
    buf: &mut Vec<char>,
) -> bool {
    buf.clear();
    if arr.is_null(i) {
        return false;
    }
    let s = arr.value(i);
    if s.len() < min_len {
        return false;
    }
    buf.extend(s.chars());
    true
}

/// Applies a randomly-chosen sub-function from `fns` to each ROW
/// independently, instead of drawing one sub-function for the *entire*
/// column at once.
///
/// Every "category → random sub-type" dispatch below (`visual`,
/// `identifiers`, `names`, `companies`, `addresses`, `extra`) used to make a
/// single `rng.next_usize(fns.len())` draw and apply the winning function to
/// every row of the noise batch. That draw's outcome depends only on how
/// much RNG state was consumed *before* reaching this call — which shifts
/// with unrelated things (total dataset size, other entities generated
/// first, batch count) even for the same seed and difficulty tier. So the
/// aggregate mix of sub-types (e.g. what fraction of `extra`-noised rows end
/// up with a column fully nulled by `apply_nullify` vs lightly corrupted by
/// `apply_missing`) was an all-or-nothing coin flip per run instead of
/// converging near `1/fns.len()` regardless of scale — traced from a
/// concrete case (aviation `last_name` null rate: 12.5% at one size, 1.0% at
/// another, same seed/difficulty, only the total `--size` differed) back to
/// this dispatch pattern.
///
/// Per-row assignment fixes it the same way `distribute_by_weight`
/// (`pipeline.rs`) fixed duplicate sampling concentrating on one batch: give
/// every unit (there, a batch of masters; here, a row) its fair, independent
/// share instead of one draw deciding the whole population's fate. Rows are
/// grouped by their chosen sub-type index, each sub-function runs once on
/// just its assigned subset (via `take`, not the full column — no wasted
/// work), and results are scattered back into their original positions.
fn apply_random_subtype(
    col: &dyn Array,
    rng: &mut Rng,
    fns: &[fn(&dyn Array, &mut Rng) -> ArrayRef],
) -> ArrayRef {
    let n = col.len();
    if fns.len() <= 1 || n == 0 {
        return match fns.first() {
            Some(f) => f(col, rng),
            None => arrow::array::new_null_array(col.data_type(), n),
        };
    }

    let mut groups: Vec<Vec<u32>> = vec![Vec::new(); fns.len()];
    for i in 0..n {
        groups[rng.next_usize(fns.len())].push(i as u32);
    }

    let mut out: Vec<Option<String>> = vec![None; n];
    for (k, idxs) in groups.into_iter().enumerate() {
        if idxs.is_empty() {
            continue;
        }
        let idx_arr = UInt32Array::from(idxs.clone());
        let sub = take(col, &idx_arr, None).expect("take for noise sub-type group");
        let noised = fns[k](&*sub, rng);
        let noised_s = noised.as_string::<i32>();
        for (pos, &orig_i) in idxs.iter().enumerate() {
            out[orig_i as usize] =
                (!noised_s.is_null(pos)).then(|| noised_s.value(pos).to_string());
        }
    }
    Arc::new(StringArray::from(out))
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
            apply_random_subtype(col, rng, &fns)
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
            apply_random_subtype(col, rng, &fns)
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
            apply_random_subtype(col, rng, &fns)
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
            apply_random_subtype(col, rng, &fns)
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
            apply_random_subtype(col, rng, &fns)
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
            apply_random_subtype(col, rng, &fns)
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
