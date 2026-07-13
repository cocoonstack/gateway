//! Declarative response-transform DSL.
//!
//! Converts one vendor's response shape into another (e.g. OpenAI → Anthropic)
//! by interpreting a JSON `MappingSpec` of rules, rather than hardcoding each
//! conversion. Supported ops: `copy`, `default`, `map_enum`, `copy_if` (field
//! remapping + enum translation: finish_reason ↔ stop_reason, usage token
//! renames) and `collect` (wrapping message content/tool_calls into typed
//! content blocks, with `$value` templates, `#` array-wildcard sources, and
//! `parse_json` — this is the OpenAI→Anthropic content conversion).

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Deserialize)]
pub struct EnumSpec {
    #[serde(default)]
    pub mapping: HashMap<String, String>,
    #[serde(default)]
    pub default: String,
}

/// A sub-operation of `collect`: pull `source` from each collected item and wrap
/// it into a content block.
#[derive(Debug, Clone, Deserialize)]
pub struct CollectOp {
    pub source: String,
    #[serde(default)]
    pub is_array: bool,
    /// wrap template for a scalar field (`$value` / `$value.path` substituted).
    #[serde(default)]
    pub wrap: Option<Value>,
    /// wrap template applied per array element.
    #[serde(default)]
    pub item_wrap: Option<Value>,
    /// target fields (post-wrap) whose string value should be JSON-parsed.
    #[serde(default)]
    pub parse_json: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Rule {
    pub op: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub default: Option<Value>,
    #[serde(rename = "enum", default)]
    pub enum_spec: Option<EnumSpec>,
    /// for copy_if: the source path whose truthiness gates the copy.
    #[serde(default)]
    pub condition: Option<String>,
    /// for collect: per-item wrap sub-operations.
    #[serde(default)]
    pub collect_ops: Vec<CollectOp>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MappingSpec {
    pub rules: Vec<Rule>,
}

/// The canonical OpenAI `chat.completion` → Anthropic `message` mapping.
/// Used when /v1/messages routes to an OpenAI-family model. Reusable,
/// data-driven — no hardcode.
#[allow(clippy::expect_used)] // parses a compile-time literal, covered by the mapping tests
pub fn openai_to_anthropic() -> MappingSpec {
    serde_json::from_str(
        r#"{"rules":[
            {"op":"copy","source":"id","target":"id"},
            {"op":"default","target":"type","default":"message"},
            {"op":"default","target":"role","default":"assistant"},
            {"op":"copy","source":"model","target":"model"},
            {"op":"map_enum","source":"choices.0.finish_reason","target":"stop_reason",
             "enum":{"mapping":{"stop":"end_turn","length":"max_tokens","tool_calls":"tool_use","content_filter":"stop_sequence"},"default":"end_turn"}},
            {"op":"collect","source":"choices.#.message","target":"content","collect_ops":[
                {"source":"content","is_array":false,"wrap":{"type":"text","text":"$value"}},
                {"source":"tool_calls","is_array":true,"item_wrap":{"type":"tool_use","id":"$value.id","name":"$value.function.name","input":"$value.function.arguments"},"parse_json":["input"]}
            ]},
            {"op":"copy","source":"usage.prompt_tokens","target":"usage.input_tokens"},
            {"op":"copy","source":"usage.completion_tokens","target":"usage.output_tokens"}
        ]}"#,
    )
    .expect("built-in openai→anthropic mapping is valid")
}

/// Read a dotted path with numeric array indexing (`choices.0.finish_reason`).
fn get_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for seg in path.split('.') {
        cur = match seg.parse::<usize>() {
            Ok(idx) => cur.get(idx)?,
            Err(_) => cur.get(seg)?,
        };
    }
    Some(cur)
}

/// Write a dotted object path, creating intermediate objects.
fn set_path(root: &mut Value, path: &str, val: Value) {
    let segs: Vec<&str> = path.split('.').collect();
    let mut cur = root;
    for (i, seg) in segs.iter().enumerate() {
        if !cur.is_object() {
            *cur = json!({});
        }
        let Value::Object(obj) = cur else { return };
        if i == segs.len() - 1 {
            obj.insert((*seg).to_owned(), val);
            return;
        }
        cur = obj.entry((*seg).to_owned()).or_insert_with(|| json!({}));
    }
}

/// Resolve a source that may contain a `#` array-wildcard (`choices.#.message`):
/// returns the collected values across the wildcarded array.
fn resolve_source_multi(input: &Value, source: &str) -> Vec<Value> {
    if let Some((prefix, suffix)) = source.split_once(".#.") {
        get_path(input, prefix)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| get_path(e, suffix).cloned())
                    .collect()
            })
            .unwrap_or_default()
    } else {
        get_path(input, source).cloned().into_iter().collect()
    }
}

/// Substitute `$value` / `$value.path` placeholders in a wrap template, then
/// JSON-parse any `parse_json` fields.
fn apply_wrap(template: &Value, value: &Value, parse_json: &[String]) -> Value {
    fn subst(t: &Value, value: &Value) -> Value {
        match t {
            Value::String(s) if s == "$value" => value.clone(),
            Value::String(s) if s.starts_with("$value.") => get_path(value, &s["$value.".len()..])
                .cloned()
                .unwrap_or(Value::Null),
            Value::Object(o) => Value::Object(
                o.iter()
                    .map(|(k, v)| (k.clone(), subst(v, value)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(|v| subst(v, value)).collect()),
            other => other.clone(),
        }
    }
    let mut result = subst(template, value);
    if let Some(obj) = result.as_object_mut() {
        for field in parse_json {
            if let Some(Value::String(s)) = obj.get(field)
                && let Ok(parsed) = serde_json::from_str::<Value>(s)
            {
                obj.insert(field.clone(), parsed);
            }
        }
    }
    result
}

fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Apply a mapping spec to an input value, producing the transformed output.
pub fn transform(input: &Value, spec: &MappingSpec) -> Value {
    let mut out = json!({});
    for rule in &spec.rules {
        match rule.op.as_str() {
            "copy" => {
                if let (Some(src), Some(tgt)) = (&rule.source, &rule.target)
                    && let Some(v) = get_path(input, src)
                {
                    set_path(&mut out, tgt, v.clone());
                }
            }
            "default" => {
                if let Some(tgt) = &rule.target
                    && get_path(&out, tgt).is_none()
                {
                    set_path(&mut out, tgt, rule.default.clone().unwrap_or(Value::Null));
                }
            }
            "map_enum" => {
                if let (Some(src), Some(tgt), Some(en)) =
                    (&rule.source, &rule.target, &rule.enum_spec)
                {
                    let key = get_path(input, src).and_then(|v| v.as_str()).unwrap_or("");
                    let mapped = en
                        .mapping
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| en.default.clone());
                    set_path(&mut out, tgt, Value::String(mapped));
                }
            }
            "copy_if" => {
                if let (Some(src), Some(tgt), Some(cond)) =
                    (&rule.source, &rule.target, &rule.condition)
                {
                    let gate = get_path(input, cond).map(is_truthy).unwrap_or(false);
                    if gate && let Some(v) = get_path(input, src) {
                        set_path(&mut out, tgt, v.clone());
                    }
                }
            }
            "collect" => {
                if let (Some(src), Some(tgt)) = (&rule.source, &rule.target) {
                    let items = resolve_source_multi(input, src);
                    let mut blocks: Vec<Value> = Vec::new();
                    for item in &items {
                        for cop in &rule.collect_ops {
                            let Some(field) = get_path(item, &cop.source) else {
                                continue;
                            };
                            if cop.is_array {
                                if let (Some(elems), Some(wrap)) =
                                    (field.as_array(), &cop.item_wrap)
                                {
                                    for e in elems {
                                        blocks.push(apply_wrap(wrap, e, &cop.parse_json));
                                    }
                                }
                            } else if let Some(wrap) = &cop.wrap {
                                blocks.push(apply_wrap(wrap, field, &cop.parse_json));
                            }
                        }
                    }
                    set_path(&mut out, tgt, Value::Array(blocks));
                }
            }
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_applies_scalar_and_collect_ops() {
        let input: Value = serde_json::from_str(
            r#"{"id":"chatcmpl-123","model":"gpt-4o",
                "choices":[{"message":{"content":"Hello!","role":"assistant"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":10,"completion_tokens":5}}"#,
        )
        .unwrap();
        let spec: MappingSpec = serde_json::from_str(
            r#"{"rules":[
                {"op":"copy","source":"id","target":"id"},
                {"op":"default","target":"type","default":"message"},
                {"op":"default","target":"role","default":"assistant"},
                {"op":"copy","source":"model","target":"model"},
                {"op":"map_enum","source":"choices.0.finish_reason","target":"stop_reason",
                 "enum":{"mapping":{"stop":"end_turn","length":"max_tokens","tool_calls":"tool_use"},"default":"end_turn"}},
                {"op":"collect","source":"choices.#.message","target":"content","collect_ops":[
                    {"source":"content","is_array":false,"wrap":{"type":"text","text":"$value"}},
                    {"source":"tool_calls","is_array":true,"item_wrap":{"type":"tool_use","id":"$value.id","name":"$value.function.name","input":"$value.function.arguments"},"parse_json":["input"]}
                ]},
                {"op":"copy","source":"usage.prompt_tokens","target":"usage.input_tokens"},
                {"op":"copy","source":"usage.completion_tokens","target":"usage.output_tokens"}
            ]}"#,
        )
        .unwrap();

        let out = transform(&input, &spec);
        assert_eq!(out["id"], "chatcmpl-123");
        assert_eq!(out["type"], "message");
        assert_eq!(out["role"], "assistant");
        assert_eq!(out["model"], "gpt-4o");
        assert_eq!(out["stop_reason"], "end_turn");
        assert_eq!(out["content"][0]["type"], "text");
        assert_eq!(out["content"][0]["text"], "Hello!");
        assert_eq!(out["usage"]["input_tokens"], 10);
        assert_eq!(out["usage"]["output_tokens"], 5);
    }

    #[test]
    fn collect_wraps_tool_calls_into_tool_use_blocks() {
        let input = json!({"choices":[{"message":{"tool_calls":[
            {"id":"call_1","function":{"name":"get_weather","arguments":"{\"city\":\"sf\"}"}}
        ]}}]});
        let spec: MappingSpec = serde_json::from_str(
            r#"{"rules":[{"op":"collect","source":"choices.#.message","target":"content","collect_ops":[
                {"source":"tool_calls","is_array":true,"item_wrap":{"type":"tool_use","id":"$value.id","name":"$value.function.name","input":"$value.function.arguments"},"parse_json":["input"]}
            ]}]}"#,
        )
        .unwrap();
        let out = transform(&input, &spec);
        assert_eq!(out["content"][0]["type"], "tool_use");
        assert_eq!(out["content"][0]["id"], "call_1");
        assert_eq!(out["content"][0]["name"], "get_weather");
        assert_eq!(out["content"][0]["input"]["city"], "sf");
    }

    #[test]
    fn builtin_openai_to_anthropic_full_conversion() {
        let openai: Value = serde_json::from_str(
            r#"{"id":"chatcmpl-test","object":"chat.completion","model":"test-model",
                "choices":[{"index":0,"message":{"role":"assistant","content":"Hello!"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}"#,
        )
        .unwrap();
        let anthropic = transform(&openai, &openai_to_anthropic());
        assert_eq!(anthropic["id"], "chatcmpl-test");
        assert_eq!(anthropic["type"], "message");
        assert_eq!(anthropic["role"], "assistant");
        assert_eq!(anthropic["model"], "test-model");
        assert_eq!(anthropic["stop_reason"], "end_turn");
        assert_eq!(anthropic["content"][0]["type"], "text");
        assert_eq!(anthropic["content"][0]["text"], "Hello!");
        assert_eq!(anthropic["usage"]["input_tokens"], 5);
        assert_eq!(anthropic["usage"]["output_tokens"], 3);
        let typed: crate::anthropic::MessagesResponse =
            serde_json::from_value(anthropic).expect("valid anthropic message");
        assert_eq!(typed.stop_reason, "end_turn");
        assert_eq!(typed.usage.input_tokens, 5);
    }

    #[test]
    fn map_enum_falls_back_to_default() {
        let input = json!({"choices":[{"finish_reason":"weird_reason"}]});
        let spec: MappingSpec = serde_json::from_str(
            r#"{"rules":[{"op":"map_enum","source":"choices.0.finish_reason","target":"stop_reason",
                "enum":{"mapping":{"stop":"end_turn"},"default":"end_turn"}}]}"#,
        )
        .unwrap();
        assert_eq!(transform(&input, &spec)["stop_reason"], "end_turn");
    }

    #[test]
    fn copy_if_gates_on_condition() {
        let spec: MappingSpec = serde_json::from_str(
            r#"{"rules":[{"op":"copy_if","source":"a","target":"a","condition":"present"}]}"#,
        )
        .unwrap();
        assert_eq!(transform(&json!({"a":1,"present":true}), &spec)["a"], 1);
        assert!(
            transform(&json!({"a":1,"present":false}), &spec)
                .get("a")
                .is_none()
        );
    }
}
