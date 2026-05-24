use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

#[derive(Clone, Default)]
pub struct Store {
    data: Arc<RwLock<BTreeMap<String, Value>>>,
}

pub fn create_store() -> Store {
    Store::default()
}

impl Store {
    pub fn set(&self, key: impl Into<String>, value: impl Into<Value>) {
        self.data
            .write()
            .expect("store lock poisoned")
            .insert(key.into(), value.into());
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        self.data
            .read()
            .expect("store lock poisoned")
            .get(key)
            .cloned()
    }

    pub fn has(&self, key: &str) -> bool {
        self.data
            .read()
            .expect("store lock poisoned")
            .contains_key(key)
    }

    pub fn delete(&self, key: &str) -> bool {
        self.data
            .write()
            .expect("store lock poisoned")
            .remove(key)
            .is_some()
    }

    pub fn clear(&self) {
        self.data.write().expect("store lock poisoned").clear();
    }

    pub fn keys(&self) -> Vec<String> {
        self.data
            .read()
            .expect("store lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str().map(ToOwned::to_owned))
    }

    pub fn number(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(|v| v.as_f64())
    }

    pub fn bool(&self, key: &str) -> Option<bool> {
        self.get(key).and_then(|v| v.as_bool())
    }

    pub fn object(&self, key: &str) -> Option<serde_json::Map<String, Value>> {
        self.get(key).and_then(|v| v.as_object().cloned())
    }

    pub fn array(&self, key: &str) -> Option<Vec<Value>> {
        self.get(key).and_then(|v| v.as_array().cloned())
    }
}
