//! JSON building from owned values: `json!` re-serializes (deep-copies) every
//! interpolated value, so wire bodies assembled from moved parts go through here.

use serde_json::Value;

/// A JSON object from moved `(key, value)` pairs.
pub fn object<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    Value::Object(pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}
