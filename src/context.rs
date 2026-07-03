use std::collections::HashMap;
use std::path::Path;

/// A loaded pool entry — flat list of strings.
pub type Pool = Vec<String>;

/// All pools keyed by name.
#[derive(Clone, Debug)]
pub struct PoolStore {
    pub pools: HashMap<String, Pool>,
}

impl PoolStore {
    /// Load all JSON files from `pools_dir` (non-recursive).
    pub fn load(pools_dir: &str) -> Result<Self, String> {
        let dir = Path::new(pools_dir);
        if !dir.is_dir() {
            return Err(format!("pools dir not found: {pools_dir}"));
        }
        let mut pools = HashMap::new();
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| format!("cannot read {pools_dir}: {e}"))?
            .filter_map(|r| r.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let name = entry
                .path()
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();
            let data: serde_json::Value = serde_json::from_reader(
                std::fs::File::open(entry.path())
                    .map_err(|e| format!("cannot open {name}.json: {e}"))?,
            )
            .map_err(|e| format!("cannot parse {name}.json: {e}"))?;

            match &data {
                serde_json::Value::Array(arr) => {
                    let pool = arr
                        .iter()
                        .filter_map(|v| v.as_str())
                        .map(String::from)
                        .collect::<Pool>();
                    pools.insert(name, pool);
                }
                serde_json::Value::Object(_map) => {
                    if let Some(en_arr) = data.get("en").and_then(|v| v.as_array()) {
                        let pool = en_arr
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(String::from)
                            .collect::<Pool>();
                        pools.insert(name, pool);
                    }
                }
                _ => {}
            };
        }
        Ok(Self { pools })
    }

    pub fn get(&self, name: &str) -> Option<&Pool> {
        self.pools.get(name)
    }
}

/// The engine context holding config and pool data.
#[derive(Clone, Debug)]
pub struct Context {
    pub pool_store: PoolStore,
    pub watermark_map: std::collections::HashMap<u64, u64>, // col_tag → masked value
}

impl Context {
    const WATERMARK_SECRET: &'static str = "DupeHell-WATERMARK-v0.4-educational-only-2026";

    /// Build a Context from a domain name + path to pools directory.
    pub fn new(domain: &str, pools_dir: &str) -> Result<Self, String> {
        let pool_store = PoolStore::load(pools_dir)?;
        log::info!(
            "Context loaded: domain={domain}, pools={}",
            pool_store.pools.len()
        );
        Ok(Self {
            pool_store,
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
            watermark_map: HashMap::new(),
        }
    }
}
