use std::collections::HashMap;
use std::path::Path;

/// A loaded pool entry — flat list of strings.
pub type Pool = Vec<String>;

/// All pools keyed by name.
#[derive(Clone, Debug)]
pub struct PoolStore {
    pub pools: HashMap<String, Pool>,
    /// Nested pools (dict-of-structures, e.g. french_cities, foreign_names) stored as raw JSON values.
    pub nested_pools: HashMap<String, serde_json::Value>,
}

impl PoolStore {
    /// Load all JSON files from `pools_dir` (non-recursive).
    pub fn load(pools_dir: &str) -> Result<Self, String> {
        let dir = Path::new(pools_dir);
        if !dir.is_dir() {
            return Err(format!("pools dir not found: {pools_dir}"));
        }
        let mut pools = HashMap::new();
        let mut nested_pools = HashMap::new();
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| format!("cannot read {pools_dir}: {e}"))?
            .filter_map(|r| r.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let name = entry.path().file_stem().unwrap().to_string_lossy().to_string();
            let data: serde_json::Value = serde_json::from_reader(
                std::fs::File::open(entry.path()).map_err(|e| format!("cannot open {name}.json: {e}"))?,
            )
            .map_err(|e| format!("cannot parse {name}.json: {e}"))?;

            match &data {
                serde_json::Value::Array(arr) => {
                    let pool = arr.iter().filter_map(|v| v.as_str()).map(String::from).collect::<Pool>();
                    pools.insert(name, pool);
                }
                serde_json::Value::Object(_map) => {
                    nested_pools.insert(name.clone(), data.clone());
                    if let Some(en_arr) = data.get("en").and_then(|v| v.as_array()) {
                        let pool = en_arr.iter().filter_map(|v| v.as_str()).map(String::from).collect::<Pool>();
                        pools.insert(name, pool);
                    }
                }
                _ => {}
            };
        }
        Ok(Self { pools, nested_pools })
    }

    pub fn get(&self, name: &str) -> Option<&Pool> {
        self.pools.get(name)
    }

    pub fn get_nested(&self, name: &str) -> Option<&serde_json::Value> {
        self.nested_pools.get(name)
    }
}

/// The engine context holding config and pool data.
#[derive(Clone, Debug)]
pub struct Context {
    pub domain: String,
    pub pool_store: PoolStore,
}

impl Context {
    /// Build a Context from a domain name + path to pools directory.
    pub fn new(domain: &str, pools_dir: &str) -> Result<Self, String> {
        let pool_store = PoolStore::load(pools_dir)?;
        log::info!(
            "Context loaded: domain={domain}, pools={}",
            pool_store.pools.len()
        );
        Ok(Self {
            domain: domain.to_string(),
            pool_store,
        })
    }

    /// Return the number of loaded pools.
    pub fn pool_count(&self) -> usize {
        self.pool_store.pools.len()
    }

    /// Return all pool names.
    pub fn pool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.pool_store.pools.keys().cloned().collect();
        names.sort();
        names
    }

    /// Return JSON string of a nested pool (e.g. french_cities), or empty string.
    pub fn get_nested_pool_json(&self, name: &str) -> String {
        self.pool_store
            .get_nested(name)
            .map(|v| v.to_string())
            .unwrap_or_default()
    }
}
