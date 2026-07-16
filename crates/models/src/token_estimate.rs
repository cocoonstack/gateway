//! Prompt-token estimation — the up-front estimate only; the authoritative
//! count always comes back in the vendor's usage payload.
//!
//! Intended to feed PTU account selection (NOT the TPM limiter); wiring it into
//! a token-aware selector is deferred. The structural accounting follows the
//! standard OpenAI chat-token formula exactly; the text→token encoding sits
//! behind the [`TokenEncoder`] seam — real tiktoken `cl100k_base` by default,
//! [`HeuristicEncoder`] as the zero-dependency fallback.

use std::sync::LazyLock;

use serde_json::Value;

use crate::request::domain::ChatMsg;

/// Encodes text to an (estimated) token count. The BPE seam — see module docs.
pub trait TokenEncoder: Send + Sync {
    fn encode_len(&self, text: &str) -> usize;
}

/// Real tiktoken `cl100k_base` BPE — the tokenizer OpenAI's models use.
pub struct TiktokenEncoder {
    bpe: tiktoken_rs::CoreBPE,
}

impl TiktokenEncoder {
    /// Fails only if the embedded vocabulary fails to load.
    pub fn new() -> Result<Self, String> {
        let bpe = tiktoken_rs::cl100k_base().map_err(|e| format!("load cl100k_base: {e}"))?;
        Ok(Self { bpe })
    }
}

impl TokenEncoder for TiktokenEncoder {
    fn encode_len(&self, text: &str) -> usize {
        self.bpe.encode_ordinary(text).len()
    }
}

/// Process-wide default encoder: cl100k BPE, falling back to the heuristic if
/// the embedded vocabulary cannot be loaded.
pub fn default_encoder() -> &'static dyn TokenEncoder {
    static ENC: LazyLock<Box<dyn TokenEncoder>> = LazyLock::new(|| match TiktokenEncoder::new() {
        Ok(t) => Box::new(t),
        Err(_) => Box::new(HeuristicEncoder),
    });
    &**ENC
}

/// Documented approximation of cl100k_base token counting — NOT real tiktoken.
/// Captures the dominant error sources of a naive bytes/4 estimate:
/// - runs of ASCII letters → ~1 token per 4 chars (subword merges)
/// - runs of digits → cl100k emits ≤3-digit groups → 1 tok / 3
/// - ASCII punctuation/symbols → ~1 token each
/// - non-ASCII (CJK etc.) → ~1 token per char (rarely merged)
///
/// Whitespace folds into the following word, contributing no standalone tokens.
#[derive(Debug, Default, Clone, Copy)]
pub struct HeuristicEncoder;

impl HeuristicEncoder {
    const LETTERS_PER_TOKEN: usize = 4;
    const DIGITS_PER_TOKEN: usize = 3;
}

impl TokenEncoder for HeuristicEncoder {
    fn encode_len(&self, text: &str) -> usize {
        let mut tokens = 0usize;
        let mut run = Run::None;
        for c in text.chars() {
            if c.is_ascii_alphabetic() {
                match &mut run {
                    Run::Letters(n) => *n += 1,
                    other => {
                        other.flush(&mut tokens);
                        run = Run::Letters(1);
                    }
                }
            } else if c.is_ascii_digit() {
                match &mut run {
                    Run::Digits(n) => *n += 1,
                    other => {
                        other.flush(&mut tokens);
                        run = Run::Digits(1);
                    }
                }
            } else {
                run.flush(&mut tokens);
                if !c.is_whitespace() {
                    tokens += 1;
                }
            }
        }
        run.flush(&mut tokens);
        tokens
    }
}

/// The kind of character run currently being accumulated.
enum Run {
    None,
    Letters(usize),
    Digits(usize),
}

impl Run {
    fn flush(&mut self, tokens: &mut usize) {
        match self {
            Run::Letters(n) => {
                *tokens += n.div_ceil(HeuristicEncoder::LETTERS_PER_TOKEN);
            }
            Run::Digits(n) => {
                *tokens += n.div_ceil(HeuristicEncoder::DIGITS_PER_TOKEN);
            }
            Run::None => {}
        }
        *self = Run::None;
    }
}

/// Estimate the prompt tokens a chat request will cost upstream. `tools` is the
/// request's tool definitions (OpenAI wire shape); their serialized schema is
/// encoded as text.
pub fn estimate_prompt_tokens(
    messages: &[ChatMsg],
    tools: Option<&Value>,
    model_name: &str,
    enc: &dyn TokenEncoder,
) -> i64 {
    let per_msg = tokens_per_message(model_name);
    let mut num = 0usize;

    for msg in messages {
        // storage-role messages are internal bookkeeping, not sent upstream.
        if msg.role == gw_consts::role::STORAGE {
            continue;
        }
        num += per_msg;
        num += enc.encode_len(&message_text(msg));
        num += enc.encode_len(&msg.role);
        if let Some(id) = &msg.tool_call_id {
            num += enc.encode_len(id);
        }
        // assistant tool_calls: each call adds overhead (+3) plus encoded name and args.
        if let Some(Value::Array(calls)) = &msg.tool_calls {
            for call in calls {
                num += 3;
                if let Some(name) = call["function"]["name"].as_str() {
                    num += enc.encode_len(name);
                }
                if let Some(args) = call["function"]["arguments"].as_str() {
                    num += enc.encode_len(args);
                }
            }
        }
    }

    if let Some(t) = tools
        && !t.is_null()
    {
        num += enc.encode_len(&t.to_string());
    }

    // every reply is primed with <|start|>assistant<|message|> → +3.
    num += 3;

    num as i64
}

/// Per-message structural overhead. gpt-3.5 uses 4, everything modern uses 3.
fn tokens_per_message(model_name: &str) -> usize {
    let m = model_name.to_ascii_lowercase();
    if m.contains("gpt-3.5") || m.contains("gpt-35") || m.contains("gpt3.5") {
        4
    } else {
        3
    }
}

/// Extract the text a message contributes: multimodal `parts` → concatenated
/// text parts only (vision tokens are the vendor's to count); else `content`,
/// borrowed — the common case allocates nothing.
fn message_text(msg: &ChatMsg) -> std::borrow::Cow<'_, str> {
    if let Some(Value::Array(parts)) = &msg.parts {
        let mut out = String::new();
        for p in parts {
            if p["type"] == "text"
                && let Some(t) = p["text"].as_str()
            {
                out.push_str(t);
            }
        }
        if !out.is_empty() {
            return std::borrow::Cow::Owned(out);
        }
    }
    std::borrow::Cow::Borrowed(&msg.content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn heuristic_classifies_runs() {
        let e = HeuristicEncoder;
        assert_eq!(e.encode_len("hello"), 2);
        assert_eq!(e.encode_len("   "), 0);
        assert_eq!(e.encode_len("12345"), 2);
        assert_eq!(e.encode_len("!?."), 3);
        assert_eq!(e.encode_len("你好"), 2);
        assert_eq!(e.encode_len(""), 0);
    }

    #[test]
    fn heuristic_mixed_text() {
        let e = HeuristicEncoder;
        assert_eq!(e.encode_len("hi there"), 3);
    }

    #[test]
    fn tiktoken_matches_known_cl100k_counts() {
        let e = TiktokenEncoder::new().unwrap();
        assert_eq!(e.encode_len(""), 0);
        assert_eq!(e.encode_len("tiktoken is great!"), 6);
        assert_eq!(e.encode_len("hello world"), 2);
    }

    #[test]
    fn default_encoder_is_tiktoken() {
        assert_eq!(default_encoder().encode_len("tiktoken is great!"), 6);
    }

    #[test]
    fn estimate_includes_structural_overhead() {
        let e = HeuristicEncoder;
        let msgs = vec![
            ChatMsg::text("system", "be brief"),
            ChatMsg::text("user", "hello"),
        ];
        let n = estimate_prompt_tokens(&msgs, None, "gpt-4o", &e);
        let content_only = e.encode_len("be brief") + e.encode_len("hello");
        assert!(
            n > (content_only as i64) + 6,
            "estimate {n} should exceed content({content_only}) + 2×3 msg overhead"
        );
    }

    #[test]
    fn gpt35_uses_higher_per_message_overhead() {
        let e = HeuristicEncoder;
        let msgs = vec![ChatMsg::text("user", "x")];
        let modern = estimate_prompt_tokens(&msgs, None, "gpt-4o", &e);
        let gpt35 = estimate_prompt_tokens(&msgs, None, "gpt-3.5-turbo", &e);
        assert_eq!(gpt35 - modern, 1, "gpt-3.5 adds 1 token/message");
    }

    #[test]
    fn tools_and_tool_calls_are_counted() {
        let e = HeuristicEncoder;
        let plain = vec![ChatMsg::text("user", "hi")];
        let base = estimate_prompt_tokens(&plain, None, "gpt-4o", &e);
        let tools = json!([{"type":"function","function":{"name":"get_weather","parameters":{}}}]);
        let with_tools = estimate_prompt_tokens(&plain, Some(&tools), "gpt-4o", &e);
        assert!(with_tools > base, "tool defs must add tokens");

        let mut asst = ChatMsg::text("assistant", "");
        asst.tool_calls = Some(json!([
            {"function":{"name":"get_weather","arguments":"{\"city\":\"NYC\"}"}}
        ]));
        let with_call = estimate_prompt_tokens(&[asst], None, "gpt-4o", &e);
        let empty_asst =
            estimate_prompt_tokens(&[ChatMsg::text("assistant", "")], None, "gpt-4o", &e);
        assert!(with_call >= empty_asst + 3, "tool_call adds ≥3 overhead");
    }

    #[test]
    fn multimodal_counts_text_parts_only() {
        let e = HeuristicEncoder;
        let mut msg = ChatMsg::text("user", "ignored-when-parts-present");
        msg.parts = Some(json!([
            {"type":"text","text":"describe"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}
        ]));
        let n = estimate_prompt_tokens(&[msg], None, "gpt-4o", &e);
        assert_eq!(n, 9, "only text parts counted, image excluded");
    }
}
