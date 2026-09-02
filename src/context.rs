// DupeHell -- MIT License
//
// Synthetic multi-domain dataset generator for record linkage benchmarking.
// No liability for misuse.

use std::collections::HashMap;
use std::path::Path;

use rayon::prelude::*;

/// A loaded pool entry — flat list of strings.
pub type Pool = Vec<String>;

/// All pools keyed by name.
#[derive(Debug)]
pub struct PoolStore {
    pub pools: HashMap<String, Pool>,
}

/// Known locale codes supported by pool files.
const LOCALES: &[&str] = &["en", "fr", "de", "es", "it", "pt"];

fn is_locale_key(key: &str) -> bool {
    LOCALES.contains(&key)
}

/// Load and parse a single pool file, returning `(name, pool)` unless the
/// file yields no usable pool (an object with neither a matching locale key
/// nor region-keyed arrays) — split out of `PoolStore::load` so it can run
/// on `rayon`'s pool, one call per file, independently of every other file.
fn load_one(entry: &std::fs::DirEntry, locale: &str) -> Result<Option<(String, Pool)>, String> {
    let name = entry
        .path()
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .to_string();
    // `from_reader` on a raw `File` reads via small unbuffered calls (no
    // internal buffering in serde_json) — reading the whole small pool
    // file into memory first and parsing with `from_str` is markedly
    // faster, and this runs once per pool file (151 of them) at every
    // startup.
    let raw = std::fs::read_to_string(entry.path())
        .map_err(|e| format!("cannot open {name}.json: {e}"))?;
    let data: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("cannot parse {name}.json: {e}"))?;

    let pool = match &data {
        serde_json::Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str())
            .map(String::from)
            .collect::<Pool>(),
        serde_json::Value::Object(_map) => {
            if let Some(arr) = data
                .get(locale)
                .or_else(|| data.get("en"))
                .and_then(|v| v.as_array())
            {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect::<Pool>()
            } else if let Some(obj) = data.as_object() {
                // No locale key found — check if this is a region-keyed pool
                // (e.g. french_cities.json with "ile_de_france" keys).
                let has_locale_keys = obj.keys().any(|k| is_locale_key(k));
                if !has_locale_keys {
                    obj.values()
                        .filter_map(|v| v.as_array())
                        .flatten()
                        .filter_map(|v| v.as_str().or_else(|| v.get(0).and_then(|s| s.as_str())))
                        .map(String::from)
                        .collect::<Pool>()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        }
        _ => return Ok(None),
    };
    if pool.is_empty() {
        return Ok(None);
    }
    Ok(Some((name, pool)))
}

impl PoolStore {
    /// Load all JSON files from `pools_dir` (non-recursive), selecting `locale` data.
    /// Falls back to `"en"` if the requested locale is not found.
    pub fn load(pools_dir: &str, locale: &str) -> Result<Self, String> {
        let dir = Path::new(pools_dir);
        if !dir.is_dir() {
            return Err(format!("pools dir not found: {pools_dir}"));
        }
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| format!("cannot read {pools_dir}: {e}"))?
            .filter_map(|r| r.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort_by_key(|e| e.file_name());

        // Each file is read + parsed independently of every other
        // (perf-hunt hunt0109/H12, measured isolated ~2.6-3.4x on the
        // read+parse portion) — the previous sequential loop paid this
        // startup cost once per file, one process at a time; a `--force`
        // rebuild or a `validate_all_domains.py`-style campaign that spawns
        // many short-lived processes back to back pays it many times over.
        // `collect::<Result<Vec<_>, _>>()` (not `filter_map`+`ok()`)
        // preserves the previous behavior of propagating the FIRST read/
        // parse error instead of silently dropping it.
        let loaded: Vec<Option<(String, Pool)>> = entries
            .par_iter()
            .map(|entry| load_one(entry, locale))
            .collect::<Result<Vec<_>, String>>()?;
        // A `HashMap`'s contents don't depend on insertion order, and pool
        // file names are unique (drawn from distinct `file_stem`s of one
        // directory listing), so collecting from `par_iter`'s
        // possibly-reordered output is exactly as deterministic as the
        // previous sequential-order insertion.
        let pools: HashMap<String, Pool> = loaded.into_iter().flatten().collect();
        Ok(Self { pools })
    }

    pub fn get(&self, name: &str) -> Option<&Pool> {
        self.pools.get(name)
    }
}

/// The engine context holding config and pool data.
///
/// Deliberately not `Clone` (perf-hunt hunt0109/H13): `pool_store` holds
/// ~2.8 MB across 151 pools, so a `Context::clone()` would be a silent deep
/// copy of tens of thousands of `String`s. Nothing in the crate ever clones
/// a `Context` — pass `&Context` (the pattern already used throughout
/// `pipeline.rs`/`entity_gen.rs`/`column_gen.rs`) or wrap it in `Arc` if a
/// genuine shared-ownership need comes up.
#[derive(Debug)]
pub struct Context {
    pub pool_store: PoolStore,
    pub locale: String,
    pub watermark_map: std::collections::HashMap<u64, u64>, // col_tag → masked value
}

impl Context {
    const WATERMARK_SECRET: &'static str = "DupeHell-WATERMARK-v0.4-educational-only-2026";

    /// Build a Context from a domain name + locale + path to pools directory.
    pub fn new(domain: &str, locale: &str, pools_dir: &str) -> Result<Self, String> {
        let pool_store = PoolStore::load(pools_dir, locale)?;
        log::info!(
            "Context loaded: domain={domain}, locale={locale}, pools={}",
            pool_store.pools.len()
        );
        Ok(Self {
            pool_store,
            locale: locale.to_string(),
            watermark_map: std::collections::HashMap::new(),
        })
    }

    /// Enable watermarking by computing per-column tags from the pipeline config.
    pub fn enable_watermark(&mut self, domain: &str, size: usize, seed: u64) {
        use sha2::{Digest, Sha256};
        for &tag in &[
            0x53534e,   // "SSN"
            0x50484f4e, // "PHONE"
            0x50414e,   // "PAN"
            0x4d4544,   // "MEDICARE"
            0x4f4643,   // "OFFICE_PHONE"
            0x504153,   // "PASSPORT"
            0x414354,   // "ACCOUNT"
            0x424152,   // "BARCODE"
            0x494343,   // "ICCID"
            0x555043,   // "UPC"
        ] {
            let input = format!(
                "{}{}{}{}{}",
                Self::WATERMARK_SECRET,
                domain,
                size,
                seed,
                tag
            );
            let hash = Sha256::digest(input.as_bytes());
            let wm = u64::from_le_bytes(hash[..8].try_into().unwrap());
            self.watermark_map.insert(tag, wm);
        }
    }

    /// Return the watermark mask for a given column tag (last 3 digits, 0..999).
    pub fn watermark_3digits(&self, tag: u64) -> u64 {
        self.watermark_map.get(&tag).copied().unwrap_or(0) % 1000
    }

    /// Return the watermark mask for a given column tag (last 2 digits, 0..99).
    pub fn watermark_2digits(&self, tag: u64) -> u64 {
        self.watermark_map.get(&tag).copied().unwrap_or(0) % 100
    }

    /// Create a minimal context for testing (no pools loaded, watermark disabled).
    /// Watermark helpers return 0, ensuring deterministic test output.
    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            pool_store: PoolStore {
                pools: HashMap::new(),
            },
            locale: "en".to_string(),
            watermark_map: HashMap::new(),
        }
    }
}
