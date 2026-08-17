//! The non-chat protocol engines, one per Protocol variant. Each engine only
//! does "build request → Transport → parse response" — nothing else crosses
//! that boundary. The mock protocol flags byte-level vendor differences as
//! deferred to a later fidelity pass.

use base64::Engine as _;
use gw_models::{GResult, GatewayError, GatewayRequest, GatewayResponse, TypedParams};
use serde_json::{Value, json};

use crate::base::{Base, base_engine};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::multipart::{Form, audio_kind, image_kind};
use crate::sse::SseDecoder;
use crate::transport::{SharedTransport, UpstreamBody};

/// Build Gemini `parts` from a unified message: text → `{"text":…}`, data-URI
/// images → `{"inlineData":…}`. Non-data image URLs can't be inlined offline
/// (no fetch), so they're skipped rather than forwarded as an unusable OpenAI
/// block; without this, multimodal requests silently drop every image.
fn gemini_parts(m: &gw_models::ChatMsg) -> Vec<Value> {
    if let Some(Value::Array(parts)) = &m.parts {
        let mut out = Vec::new();
        for p in parts {
            match p["type"].as_str() {
                Some("text") => {
                    if let Some(t) = p["text"].as_str() {
                        out.push(json!({"text": t}));
                    }
                }
                Some("image_url") => {
                    let url = p["image_url"]["url"].as_str().unwrap_or_default();
                    if let Some((mime, data)) = parse_data_uri(url) {
                        out.push(json!({"inlineData": {"mimeType": mime, "data": data}}));
                    }
                }
                _ => {}
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    vec![json!({"text": m.content})]
}

/// Parse a `data:<mime>;base64,<payload>` URI into `(mime, payload)`.
fn parse_data_uri(url: &str) -> Option<(&str, &str)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let mime = meta.strip_suffix(";base64").unwrap_or(meta);
    if mime.is_empty() || data.is_empty() {
        return None;
    }
    Some((mime, data))
}

base_engine!(VertexEngine);

impl VertexEngine {
    /// Gemini API auth: the x-goog-api-key header — an API key is not an OAuth
    /// Bearer token and Google rejects it as one.
    fn gemini_headers(&self) -> Vec<(String, String)> {
        vec![
            ("content-type".into(), "application/json".into()),
            ("x-goog-api-key".into(), self.base.api_key()),
        ]
    }

    fn build_body(&self) -> Value {
        // system turns go to systemInstruction, never the contents: Gemini has
        // no system content role, and a `user`-role downgrade both loses the
        // directive's authority and breaks turn alternation
        let contents: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .filter(|m| m.role != gw_consts::role::SYSTEM)
            .map(|m| {
                let role = if m.role == gw_consts::role::AI {
                    gw_consts::role::MODEL
                } else {
                    gw_consts::role::USER
                };
                json!({"role": role, "parts": gemini_parts(m)})
            })
            .collect();
        let mut body = json!({});
        body["contents"] = Value::Array(contents);
        let system = self.base.system_text();
        if !system.is_empty() {
            body["systemInstruction"] = json!({"parts": [{"text": system}]});
        }
        // sampling params → generationConfig — else Gemini silently uses defaults
        if let Some(p) = self.base.chat_params() {
            let mut gen_cfg = json!({});
            if let Some(t) = p.temperature {
                gen_cfg["temperature"] = json!(t);
            }
            if let Some(t) = p.top_p {
                gen_cfg["topP"] = json!(t);
            }
            if let Some(mt) = p.max_tokens {
                gen_cfg["maxOutputTokens"] = json!(mt);
            }
            if gen_cfg.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                body["generationConfig"] = gen_cfg;
            }
        }
        body
    }

    /// Native Gemini streaming: `:streamGenerateContent?alt=sse` frames decoded
    /// as they arrive and forwarded through `stream_tx` (the live-pump contract).
    async fn run_stream(&self) -> GResult<EngineOutcome> {
        let body = self.build_body();
        let url = format!(
            "{}/v1beta/models/{}:streamGenerateContent?alt=sse",
            self.base.base_url("mock://vertex.googleapis.com"),
            self.base.param()?.model_name
        );
        let reply = self
            .base
            .send_upstream_raw(&url, self.gemini_headers(), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.base.param()?.model_name.clone(),
            ..Default::default()
        };
        crate::pump::reject_json_error("gemini", status, &reply.body)?;
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "gemini",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| vertex_apply_frame(&v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = vertex_common_usage(&resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for VertexEngine {
    /// Gemini generateContent: contents/parts request, candidates/usageMetadata
    /// response; `:streamGenerateContent?alt=sse` when the request streams.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body();
        let url = format!(
            "{}/v1beta/models/{}:generateContent",
            self.base.base_url("mock://vertex.googleapis.com"),
            self.base.param()?.model_name
        );
        let (status, v) = self
            .base
            .round_trip_with(&url, self.gemini_headers(), body)
            .await?;
        let text: String = v["candidates"][0]["content"]["parts"]
            .as_array()
            .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
            .unwrap_or_default();
        let mut resp = GatewayResponse {
            message: text,
            model: self.base.param()?.model_name.clone(),
            finish_reason: vertex_finish_reason(
                v["candidates"][0]["finishReason"]
                    .as_str()
                    .unwrap_or_default(),
            ),
            ..Default::default()
        };
        vertex_apply_usage(&v["usageMetadata"], &mut resp);
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = vertex_common_usage(&resp);
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// Gemini finishReason → the shared vocabulary: safety-family values become
/// `content_filter` so clients detect moderation blocks; the rest lowercase
/// (`finish_openai` already maps `max_tokens` → `length`).
fn vertex_finish_reason(fr: &str) -> String {
    match fr {
        "SAFETY" | "RECITATION" | "PROHIBITED_CONTENT" | "SPII" | "BLOCKLIST" => {
            "content_filter".to_owned()
        }
        other => other.to_lowercase(),
    }
}

/// Apply one `streamGenerateContent` frame to the accumulating response;
/// returns the chunks it yields. usageMetadata is cumulative — last frame wins.
fn vertex_apply_frame(
    v: &Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    let text: String = v["candidates"][0]["content"]["parts"]
        .as_array()
        .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
        .unwrap_or_default();
    if !text.is_empty() {
        full.push_str(&text);
        chunks.push(StreamChunk {
            delta: text,
            ..Default::default()
        });
    }
    if let Some(fr) = v["candidates"][0]["finishReason"].as_str() {
        resp.finish_reason = vertex_finish_reason(fr);
        chunks.push(StreamChunk {
            finish_reason: Some(resp.finish_reason.clone()),
            ..Default::default()
        });
    }
    vertex_apply_usage(&v["usageMetadata"], resp);
    Ok(chunks)
}

/// Fold a cumulative `usageMetadata` object into the response (last frame wins).
/// Thinking models report `thoughtsTokenCount` outside `candidatesTokenCount`;
/// OpenAI semantics fold reasoning into completion, so map thoughts → reasoning
/// ⊆ completion or billing loses them.
fn vertex_apply_usage(um: &Value, resp: &mut GatewayResponse) {
    if um.is_null() {
        return;
    }
    if let Some(pt) = um["promptTokenCount"].as_i64() {
        resp.prompt_tokens = pt.max(0);
    }
    let thoughts = crate::engine::tok(&um["thoughtsTokenCount"]);
    if let Some(cand) = um["candidatesTokenCount"].as_i64() {
        resp.completion_tokens = cand.max(0).saturating_add(thoughts);
        resp.reasoning_tokens = thoughts;
    }
    if let Some(tt) = um["totalTokenCount"].as_i64() {
        resp.total_tokens = tt.max(0);
    }
}

fn vertex_common_usage(resp: &GatewayResponse) -> Option<gw_models::CommonUsage> {
    Some(gw_models::CommonUsage::from_openai_parts(
        resp.prompt_tokens,
        resp.completion_tokens,
        0,
        resp.reasoning_tokens,
    ))
}

base_engine!(EmbeddingsEngine);

#[async_trait::async_trait]
impl ModelEngine for EmbeddingsEngine {
    /// Merges the openai/ali/vertex embedding engines to the openai shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // the batch moves: json! would re-copy every input string
        let (input, dimensions) = match self.base.take_typed() {
            Some(TypedParams::Embeddings(p)) => (
                Value::Array(p.input.into_iter().map(Value::String).collect()),
                p.dimensions,
            ),
            _ => (
                Value::Array(
                    std::mem::take(&mut self.base.request.message)
                        .into_iter()
                        .map(|m| Value::String(m.content))
                        .collect(),
                ),
                None,
            ),
        };
        if input.as_array().is_none_or(|a| a.is_empty()) {
            return Err(GatewayError::bad_request(
                "embeddings input must not be empty",
            ));
        }
        let mut body = json!({"model": model});
        body["input"] = input;
        if let Some(d) = dimensions {
            body["dimensions"] = json!(d);
        }
        let (status, v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "embeddings"),
                body,
            )
            .await?;
        let pt = crate::engine::tok(&v["usage"]["prompt_tokens"]);
        let resp = GatewayResponse {
            model,
            prompt_tokens: pt,
            total_tokens: pt,
            raw_usage: (!v["usage"].is_null()).then(|| v["usage"].clone()),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// The uniform family-engine tail: a summary message plus the native payload,
/// finished at "stop".
fn family_outcome(
    message: String,
    model: &str,
    v: serde_json::Value,
    status: u16,
) -> EngineOutcome {
    EngineOutcome::with_status(
        GatewayResponse {
            message,
            model: model.to_owned(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        },
        status,
    )
}

fn require_non_empty(v: &str, what: &str) -> GResult<()> {
    if v.is_empty() {
        return Err(GatewayError::bad_request(format!(
            "{what} must not be empty"
        )));
    }
    Ok(())
}

base_engine!(ImageEngine);

#[async_trait::async_trait]
impl ModelEngine for ImageEngine {
    /// Merges the dalle/wanx/flux/stability/... engines to the images/generations shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let (prompt, n, size, image, mask) = match self.base.take_typed() {
            Some(TypedParams::Image(p)) => (p.prompt, p.n, p.size, p.image, p.mask),
            _ => (
                self.base.last_message_text().to_owned(),
                1,
                None,
                None,
                None,
            ),
        };
        require_non_empty(&prompt, "image prompt")?;
        let (status, v, is_edit) = if let Some(image) = image {
            // edits upload the source (and mask) as files
            let image = decode_b64(&image, "image")?;
            let mut form = Form::new(&image);
            form.text("model", &model);
            form.text("prompt", &prompt);
            form.text("n", &n.to_string());
            if let Some(size) = &size {
                form.text("size", size);
            }
            let (ext, content_type) = image_kind(&image);
            form.file("image", &format!("image.{ext}"), content_type, &image);
            if let Some(mask) = mask {
                let mask = decode_b64(&mask, "mask")?;
                let (ext, content_type) = image_kind(&mask);
                form.file("mask", &format!("mask.{ext}"), content_type, &mask);
            }
            let (content_type, body) = form.finish();
            let headers = vec![
                ("content-type".into(), content_type),
                (
                    "authorization".into(),
                    format!("Bearer {}", self.base.api_key()),
                ),
            ];
            let reply = self
                .base
                .send_bytes(
                    &self
                        .base
                        .openai_url("mock://api.openai.com", "images/edits"),
                    headers,
                    body,
                    false,
                )
                .await?;
            let (status, v) = crate::base::parse_json_reply(reply)?;
            (status, v, true)
        } else {
            let mut body = json!({"model": model, "n": n});
            body["prompt"] = prompt.into();
            if let Some(s) = size {
                body["size"] = s.into();
            }
            let (status, v) = self
                .base
                .round_trip(
                    &self
                        .base
                        .openai_url("mock://api.openai.com", "images/generations"),
                    body,
                )
                .await?;
            (status, v, false)
        };
        let count = v["data"].as_array().map(|a| a.len()).unwrap_or(0);
        let verb = if is_edit { "edited" } else { "generated" };
        // gpt-image usage: input/output tokens (image + text details)
        let (input, output) = (
            crate::engine::tok(&v["usage"]["input_tokens"]),
            crate::engine::tok(&v["usage"]["output_tokens"]),
        );
        let mut outcome = family_outcome(format!("{count} image(s) {verb}"), &model, v, status);
        outcome.response.prompt_tokens = input;
        outcome.response.completion_tokens = output;
        crate::engine::fill_total_if_zero(&mut outcome.response);
        Ok(outcome)
    }
}

/// Decode a client-supplied base64 payload; a bad payload is the client's 400,
/// not an upstream failure.
fn decode_b64(payload: &str, what: &str) -> GResult<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|e| GatewayError::bad_request(format!("{what} is not valid base64: {e}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioKind {
    Tts,
    Stt,
    Other,
}

pub struct AudioEngine {
    base: Base,
    kind: AudioKind,
}

impl AudioEngine {
    pub fn new(request: GatewayRequest, transport: SharedTransport, kind: AudioKind) -> Self {
        Self {
            base: Base::new(request, transport),
            kind,
        }
    }
}

#[async_trait::async_trait]
impl ModelEngine for AudioEngine {
    /// Merges the openai_tts/whisper/azure_asr/elevenlabs/cosyvoice/minimax_t2a etc. engines.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let (status, v) = match self.kind {
            AudioKind::Tts => {
                let (input, voice, format) = match &self.base.param()?.typed {
                    Some(TypedParams::AudioTts(p)) => (
                        p.input.as_str(),
                        p.voice.as_deref(),
                        p.response_format.as_deref(),
                    ),
                    _ => (self.base.last_message_text(), None, None),
                };
                require_non_empty(input, "tts input")?;
                let mut b = json!({"model": model, "input": input});
                if let Some(v) = voice {
                    b["voice"] = json!(v);
                }
                if let Some(f) = format {
                    b["response_format"] = json!(f);
                }
                let reply = self
                    .base
                    .send_upstream(
                        &self
                            .base
                            .openai_url("mock://api.openai.com", "audio/speech"),
                        self.base.bearer_headers(),
                        b,
                        false,
                    )
                    .await?;
                // the vendor answers with the audio bytes themselves; a JSON
                // body is an error envelope (or a compatible upstream's b64)
                let status = reply.status;
                match reply.body {
                    UpstreamBody::Json(bytes) if status < 400 && !looks_like_json(&bytes) => {
                        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        (status, json!({"audio_b64": b64}))
                    }
                    body => crate::base::parse_json_reply(crate::transport::UpstreamResponse {
                        status,
                        body,
                    })?,
                }
            }
            AudioKind::Stt => {
                let (audio, language, translate) = match self.base.take_typed() {
                    Some(TypedParams::AudioStt(p)) => (p.audio_b64, p.language, p.translate),
                    _ => (String::new(), None, false),
                };
                require_non_empty(&audio, "stt audio_b64")?;
                let audio = decode_b64(&audio, "audio_b64")?;
                let path = if translate {
                    "audio/translations"
                } else {
                    "audio/transcriptions"
                };
                let mut form = Form::new(&audio);
                form.text("model", &model);
                if let Some(language) = &language {
                    form.text("language", language);
                }
                let (ext, content_type) = audio_kind(&audio);
                form.file("file", &format!("audio.{ext}"), content_type, &audio);
                let (content_type, body) = form.finish();
                let headers = vec![
                    ("content-type".into(), content_type),
                    (
                        "authorization".into(),
                        format!("Bearer {}", self.base.api_key()),
                    ),
                ];
                let reply = self
                    .base
                    .send_bytes(
                        &self.base.openai_url("mock://api.openai.com", path),
                        headers,
                        body,
                        false,
                    )
                    .await?;
                crate::base::parse_json_reply(reply)?
            }
            AudioKind::Other => {
                let mut b = json!({"model": model});
                b["raw"] = self.base.take_raw();
                self.base
                    .round_trip(
                        &self.base.openai_url("mock://api.openai.com", "audio/other"),
                        b,
                    )
                    .await?
            }
        };
        let message = match self.kind {
            AudioKind::Stt => v["text"].as_str().unwrap_or_default().to_owned(),
            _ => format!(
                "audio payload ({} b64 bytes)",
                v["audio_b64"].as_str().map(str::len).unwrap_or(0)
            ),
        };
        // token-priced transcription models report input/output tokens
        let (input, output) = (
            crate::engine::tok(&v["usage"]["input_tokens"]),
            crate::engine::tok(&v["usage"]["output_tokens"]),
        );
        let mut outcome = family_outcome(message, &model, v, status);
        outcome.response.prompt_tokens = input;
        outcome.response.completion_tokens = output;
        crate::engine::fill_total_if_zero(&mut outcome.response);
        Ok(outcome)
    }
}

fn looks_like_json(bytes: &[u8]) -> bool {
    matches!(
        bytes.iter().find(|b| !b.is_ascii_whitespace()),
        Some(b'{' | b'[')
    )
}

base_engine!(VideoEngine);

#[async_trait::async_trait]
impl ModelEngine for VideoEngine {
    /// Merges the sora/veo/kling/runway/vidu/minimax_video engines (async-task
    /// type; mock completes immediately).
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let prompt = match &param.typed {
            Some(TypedParams::Video(p)) => p.prompt.as_str(),
            _ => self.base.last_message_text(),
        };
        require_non_empty(prompt, "video prompt")?;
        let mut body = json!({"model": param.model_name, "prompt": prompt});
        if let Some(TypedParams::Video(p)) = &param.typed {
            if let Some(d) = p.duration_seconds {
                body["duration_seconds"] = json!(d);
            }
            if let Some(r) = &p.resolution {
                body["resolution"] = json!(r);
            }
        }
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/videos/generations",
                    self.base.base_url("mock://api.vendor.com")
                ),
                body,
            )
            .await?;
        let message = v["video_url"].as_str().unwrap_or_default().to_owned();
        let step = v["status"].as_str().unwrap_or_default().to_owned();
        let mut out = family_outcome(message, &param.model_name, v, status);
        out.response.step = step;
        Ok(out)
    }
}

base_engine!(SearchEngine);

#[async_trait::async_trait]
impl ModelEngine for SearchEngine {
    /// Merges the bingsearch/brave/serp/google_custom_search engines.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let (query, count) = match &param.typed {
            Some(TypedParams::Search(p)) => (p.query.as_str(), p.count),
            _ => (self.base.last_message_text(), 3),
        };
        require_non_empty(query, "search query")?;
        let body = json!({"query": query, "count": count});
        let (status, v) = self
            .base
            .round_trip(
                &format!("{}/v1/search", self.base.base_url("mock://api.vendor.com")),
                body,
            )
            .await?;
        let titles: Vec<String> = v["results"]
            .as_array()
            .map(|rs| {
                rs.iter()
                    .filter_map(|r| r["title"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok(family_outcome(
            titles.join("; "),
            &param.model_name,
            v,
            status,
        ))
    }
}

base_engine!(ModerationsEngine);

#[async_trait::async_trait]
impl ModelEngine for ModerationsEngine {
    /// OpenAI moderations shape: `{model, input: [..]}` → per-input verdicts.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let Some(TypedParams::Moderation(p)) = &param.typed else {
            return Err(GatewayError::bad_request("moderations params are required"));
        };
        if p.input.is_empty() {
            return Err(GatewayError::bad_request(
                "moderations input must not be empty",
            ));
        }
        let body = json!({"model": param.model_name, "input": p.input});
        let (status, v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "moderations"),
                body,
            )
            .await?;
        let flagged = v["results"]
            .as_array()
            .map(|rs| {
                rs.iter()
                    .filter(|r| r["flagged"].as_bool().unwrap_or(false))
                    .count()
            })
            .unwrap_or(0);
        Ok(family_outcome(
            format!("{flagged} flagged"),
            &param.model_name,
            v,
            status,
        ))
    }
}

base_engine!(RerankEngine);

#[async_trait::async_trait]
impl ModelEngine for RerankEngine {
    /// Cohere/Jina-compatible rerank: `{model, query, documents, top_n?}` →
    /// `{results: [{index, relevance_score}]}`.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let Some(TypedParams::Rerank(p)) = self.base.take_typed() else {
            return Err(GatewayError::bad_request("rerank params are required"));
        };
        require_non_empty(&p.query, "rerank query")?;
        if p.documents.is_empty() {
            return Err(GatewayError::bad_request(
                "rerank documents must not be empty",
            ));
        }
        // the document set moves — json! would re-copy every string
        let mut body = json!({"model": model});
        body["query"] = p.query.into();
        body["documents"] = Value::Array(p.documents.into_iter().map(Value::String).collect());
        if let Some(n) = p.top_n {
            body["top_n"] = json!(n);
        }
        let (status, v) = self
            .base
            .round_trip(
                &format!("{}/v1/rerank", self.base.base_url("mock://api.vendor.com")),
                body,
            )
            .await?;
        let n = v["results"].as_array().map(Vec::len).unwrap_or(0);
        let tokens = rerank_tokens(&v);
        let mut out = family_outcome(format!("{n} results"), &model, v, status);
        out.response.prompt_tokens = tokens;
        out.response.total_tokens = tokens;
        Ok(out)
    }
}

/// Rerank usage across the two wire dialects: Jina-style `usage.total_tokens`,
/// else Cohere/SiliconFlow-style `meta.tokens.{input,output}_tokens`.
fn rerank_tokens(v: &Value) -> i64 {
    let total = crate::engine::tok(&v["usage"]["total_tokens"]);
    if total > 0 {
        return total;
    }
    crate::engine::tok(&v["meta"]["tokens"]["input_tokens"])
        .saturating_add(crate::engine::tok(&v["meta"]["tokens"]["output_tokens"]))
}

base_engine!(PassthroughEngine);

#[async_trait::async_trait]
impl ModelEngine for PassthroughEngine {
    /// Dedicated integration surfaces: request body passed through as-is,
    /// placeholder protocol (byte-level alignment deferred).
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // the arbitrary vendor blob moves — json! would re-copy it whole
        let mut body = json!({"model": model});
        body["payload"] = self.base.take_raw();
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/passthrough",
                    self.base.base_url("mock://api.vendor.com")
                ),
                body,
            )
            .await?;
        let message = if v["ok"].as_bool().unwrap_or(false) {
            "ok"
        } else {
            "error"
        };
        Ok(family_outcome(message.to_owned(), &model, v, status))
    }
}

base_engine!(CompletionsEngine);

#[async_trait::async_trait]
impl ModelEngine for CompletionsEngine {
    /// The legacy openai text-completions endpoint: `{model, prompt}` request
    /// (not chat messages), `{choices:[{text}]}` response.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let prompt: String = self
            .base
            .request
            .message
            .iter()
            .map(|m| m.content.as_str())
            .collect();
        let mut body = json!({"model": param.model_name});
        body["prompt"] = prompt.into();
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                body["max_tokens"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        let (status, mut v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "completions"),
                body,
            )
            .await?;
        let usage = &v["usage"];
        let (pt, ct) = (
            crate::engine::tok(&usage["prompt_tokens"]),
            crate::engine::tok(&usage["completion_tokens"]),
        );
        // floor a present-but-negative upstream total too, not just the sum
        let total = usage["total_tokens"]
            .as_i64()
            .unwrap_or(pt.saturating_add(ct))
            .max(0);
        let raw_usage = v.get_mut("usage").map(Value::take).filter(|u| !u.is_null());
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/choices/0/text").unwrap_or_default(),
            model: crate::engine::take_string(&mut v, "/model")
                .unwrap_or_else(|| param.model_name.clone()),
            finish_reason: crate::engine::take_string(&mut v, "/choices/0/finish_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: total,
            raw_usage,
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(ResponsesEngine);

impl ResponsesEngine {
    fn model_name(&self) -> String {
        self.base.model_name().unwrap_or_default().to_owned()
    }

    /// Native passthrough: the client's Responses-shaped body moves through
    /// verbatim, with `model` ensured.
    fn build_body(&mut self) -> GResult<Value> {
        let mut body = match self.base.take_raw() {
            raw @ Value::Object(_) => raw,
            _ => json!({}),
        };
        if let Some(map) = body.as_object_mut()
            && !map.contains_key("model")
        {
            map.insert("model".to_owned(), self.base.model_name()?.into());
        }
        Ok(body)
    }

    fn url(&self) -> String {
        self.base.openai_url("mock://api.openai.com", "responses")
    }

    /// Streaming Responses pumped live: delta frames forwarded through
    /// `stream_tx` as they arrive; `response.completed` carries final usage.
    async fn run_stream(&mut self) -> GResult<EngineOutcome> {
        let body = self.build_body()?;
        let reply = self
            .base
            .send_upstream_raw(&self.url(), self.base.bearer_headers(), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.model_name(),
            finish_reason: "completed".to_owned(),
            ..Default::default()
        };
        crate::pump::reject_json_error("responses", status, &reply.body)?;
        let mut full = String::new();
        let model_override = self.base.model_override();
        let r = crate::pump::pump_sse(
            "responses",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| responses_apply_frame(v, status, model_override, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        Ok(EngineOutcome::from_pump(resp, status, r))
    }

    /// Non-streaming Responses reply: full `output` array + `usage`.
    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse responses reply").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let (text, tool_calls) = responses_output(&v);
        let (input, output, common_usage) = responses_usage(&v["usage"]);
        let resp = GatewayResponse {
            message: text,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(Value::Array(tool_calls))
            },
            model: v["model"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| self.model_name()),
            finish_reason: v["status"].as_str().unwrap_or("completed").to_owned(),
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            common_usage,
            response_v2: Some(v),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    /// Buffered Responses SSE: the same [`responses_apply_frame`] semantics the
    /// live pump drives, over pre-decoded events.
    fn parse_sse(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let (events, _done) = SseDecoder::decode_all(bytes)
            .map_err(|e| GatewayError::internal(format!("decode responses sse body: {e}")))?;
        let mut resp = GatewayResponse {
            model: self.model_name(),
            finish_reason: "completed".to_owned(),
            ..Default::default()
        };
        let mut full = String::new();
        let mut chunks = Vec::new();
        let model_override = self.base.model_override();
        for ev in events {
            let v: Value = serde_json::from_slice(ev.as_bytes())
                .map_err(|e| GatewayError::internal("parse responses sse frame").with_source(e))?;
            chunks.extend(responses_apply_frame(
                v,
                status,
                model_override,
                &mut resp,
                &mut full,
            )?);
        }
        resp.message = full;
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks,
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl ModelEngine for ResponsesEngine {
    /// OpenAI Responses API (POST /v1/responses): native body passthrough with
    /// the `model` field ensured; usage normalized to the openai shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body()?;
        let reply = self
            .base
            .send_upstream(&self.url(), self.base.bearer_headers(), body, false)
            .await?;
        match &reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, b),
            UpstreamBody::Sse(b) => self.parse_sse(reply.status, b),
            UpstreamBody::SseStream(_) => Err(GatewayError::internal(
                "unbuffered stream reached responses engine",
            )),
        }
    }
}

/// Extract assistant text from a Responses `output` array (message items'
/// `output_text` content), plus any function_call items.
fn responses_output(v: &Value) -> (String, Vec<Value>) {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    if let Some(items) = v["output"].as_array() {
        for item in items {
            match item["type"].as_str() {
                Some("message") => {
                    if let Some(content) = item["content"].as_array() {
                        for c in content {
                            if c["type"] == "output_text"
                                && let Some(t) = c["text"].as_str()
                            {
                                text.push_str(t);
                            }
                        }
                    }
                }
                Some("function_call") => tool_calls.push(item.clone()),
                _ => {} // reasoning / other item types carry no assistant text
            }
        }
    }
    (text, tool_calls)
}

/// Normalize a Responses `usage` object; returns (input, output, common usage).
fn responses_usage(usage: &Value) -> (i64, i64, Option<gw_models::CommonUsage>) {
    if usage.is_null() {
        return (0, 0, None);
    }
    let input = crate::engine::tok(&usage["input_tokens"]);
    let output = crate::engine::tok(&usage["output_tokens"]);
    let common = gw_models::CommonUsage::from_openai_parts(
        input,
        output,
        crate::engine::tok(&usage["input_tokens_details"]["cached_tokens"]),
        crate::engine::tok(&usage["output_tokens_details"]["reasoning_tokens"]),
    );
    (input, output, Some(common))
}

/// Apply one Responses SSE frame: every frame moves out as a native event for
/// the view to forward verbatim; text accumulates from `output_text.delta`,
/// usage/status come from `response.completed`.
fn responses_apply_frame(
    mut v: Value,
    status: u16,
    model_override: Option<&str>,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, &v) {
        return Err(err);
    }
    if let Some(model) = model_override
        && let Some(response) = v.get_mut("response").and_then(Value::as_object_mut)
        && response.contains_key("model")
    {
        response.insert("model".to_owned(), model.into());
    }
    let mut chunk = StreamChunk::default();
    match v["type"].as_str().unwrap_or_default() {
        "response.output_text.delta" => {
            if let Some(d) = v["delta"].as_str() {
                full.push_str(d);
            }
        }
        "response.completed" => {
            let r = &v["response"];
            if let Some(m) = r["model"].as_str() {
                resp.model = m.to_owned();
            }
            if let Some(st) = r["status"].as_str() {
                resp.finish_reason = st.to_owned();
            }
            let (input, output, common) = responses_usage(&r["usage"]);
            resp.prompt_tokens = input;
            resp.completion_tokens = output;
            crate::engine::fill_total_if_zero(resp);
            resp.common_usage = common;
            chunk.finish_reason = Some(resp.finish_reason.clone());
        }
        _ => {}
    }
    chunk.native_event = Some(v);
    Ok(vec![chunk])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{
        ChatMsg, EmbeddingParams, ImageParams, ModelParamV2, SearchParams, SttParams, TtsParams,
        VideoParams,
    };

    use super::*;
    use crate::transport::{MockTransport, UpstreamRequest, UpstreamResponse};

    fn req(mt: Protocol, name: &str, typed: Option<TypedParams>) -> GatewayRequest {
        let mut p = ModelParamV2::with_name(mt, name);
        p.typed = typed;
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hello families")],
            model_param_v2: Some(p),
            ..Default::default()
        }
    }

    fn t() -> SharedTransport {
        Arc::new(MockTransport)
    }

    #[tokio::test]
    async fn vertex_round_trip() {
        let mut e = VertexEngine::new(req(Protocol::Gemini, "gemini-pro", None), t());
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: hello families"));
        assert!(out.response.total_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }

    #[tokio::test]
    async fn vertex_stream_decodes_frames() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.stream = true;
        let mut e = VertexEngine::new(r, t());
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3, "chunks: {:?}", out.chunks);
        assert!(out.response.message.contains("you said: hello families"));
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
    }

    #[tokio::test]
    async fn embeddings_round_trip() {
        let mut e = EmbeddingsEngine::new(
            req(
                Protocol::Embeddings,
                "text-embedding-mock",
                Some(TypedParams::Embeddings(EmbeddingParams {
                    input: vec!["abc".into(), "def".into()],
                    dimensions: None,
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        let dims = out.response.response_v2.as_ref().unwrap()["data"][0]["embedding"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(dims, 8);
        assert!(out.response.prompt_tokens > 0);
    }

    #[tokio::test]
    async fn image_round_trip() {
        let mut e = ImageEngine::new(
            req(
                Protocol::Image,
                "img-mock",
                Some(TypedParams::Image(ImageParams {
                    prompt: "a cat".into(),
                    n: 2,
                    size: None,
                    ..Default::default()
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "2 image(s) generated");
        assert!(out.response.response_v2.is_some());
    }

    #[tokio::test]
    async fn moderations_flags_and_validates() {
        let mut e = ModerationsEngine::new(
            req(
                Protocol::Moderations,
                "text-moderation",
                Some(TypedParams::Moderation(gw_models::ModerationParams {
                    input: vec!["fine".into(), "really unsafe".into()],
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "1 flagged");
        assert_eq!(
            out.response.response_v2.unwrap()["results"][1]["flagged"],
            true
        );

        for typed in [
            None,
            Some(TypedParams::Moderation(gw_models::ModerationParams {
                input: vec![],
            })),
        ] {
            let mut e = ModerationsEngine::new(req(Protocol::Moderations, "m", typed), t());
            assert_eq!(e.run().await.unwrap_err().http_status, 400);
        }
    }

    #[tokio::test]
    async fn rerank_orders_and_validates() {
        let params = |query: &str, documents: Vec<&str>| {
            Some(TypedParams::Rerank(gw_models::RerankParams {
                query: query.into(),
                documents: documents.into_iter().map(str::to_owned).collect(),
                top_n: Some(1),
            }))
        };
        let mut e = RerankEngine::new(
            req(
                Protocol::Rerank,
                "rerank-mini",
                params("rust gateway", vec!["cooking", "a rust gateway"]),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "1 results");
        let v = out.response.response_v2.unwrap();
        assert_eq!(v["results"][0]["index"], 1, "the matching doc ranks first");
        assert!(out.response.total_tokens > 0, "usage flows from the vendor");

        for typed in [None, params("", vec!["doc"]), params("query", vec![])] {
            let mut e = RerankEngine::new(req(Protocol::Rerank, "m", typed), t());
            assert_eq!(e.run().await.unwrap_err().http_status, 400);
        }
    }

    #[test]
    fn rerank_tokens_reads_both_dialects() {
        assert_eq!(rerank_tokens(&json!({"usage": {"total_tokens": 7}})), 7);
        assert_eq!(
            rerank_tokens(&json!({"meta": {"tokens": {"input_tokens": 5, "output_tokens": 2}}})),
            7,
            "SiliconFlow/Cohere meta.tokens shape bills too"
        );
        assert_eq!(rerank_tokens(&json!({})), 0);
    }

    #[tokio::test]
    async fn audio_tts_and_stt() {
        let mut tts = AudioEngine::new(
            req(
                Protocol::Tts,
                "tts-mock",
                Some(TypedParams::AudioTts(TtsParams {
                    input: "read this".into(),
                    voice: Some("alloy".into()),
                    response_format: None,
                })),
            ),
            t(),
            AudioKind::Tts,
        );
        assert!(
            tts.run()
                .await
                .unwrap()
                .response
                .message
                .contains("audio payload")
        );

        let mut stt = AudioEngine::new(
            req(
                Protocol::Stt,
                "whisper-mock",
                Some(TypedParams::AudioStt(SttParams {
                    audio_b64: "TU9DSw==".into(),
                    language: Some("en".into()),
                    translate: false,
                })),
            ),
            t(),
            AudioKind::Stt,
        );
        assert!(
            stt.run()
                .await
                .unwrap()
                .response
                .message
                .contains("transcribed")
        );
    }

    #[tokio::test]
    async fn video_and_search_and_passthrough() {
        let mut v = VideoEngine::new(
            req(
                Protocol::Video,
                "kling-mock",
                Some(TypedParams::Video(VideoParams {
                    prompt: "a dog surfing".into(),
                    duration_seconds: None,
                    resolution: None,
                })),
            ),
            t(),
        );
        let out = v.run().await.unwrap();
        assert_eq!(out.response.message, "mock://videos/out.mp4");
        assert_eq!(out.response.step, "succeeded");

        let mut s = SearchEngine::new(
            req(
                Protocol::Search,
                "brave-mock",
                Some(TypedParams::Search(SearchParams {
                    query: "rust dag".into(),
                    count: 2,
                })),
            ),
            t(),
        );
        let out = s.run().await.unwrap();
        assert!(out.response.message.contains("result 1 for rust dag"));

        let mut p = PassthroughEngine::new(req(Protocol::Passthrough, "e2b", None), t());
        assert_eq!(p.run().await.unwrap().response.message, "ok");
    }

    #[tokio::test]
    async fn down_account_fails_upstream() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.account = Some(std::sync::Arc::new(gw_models::Account {
            name: "mock-vertex-down".into(),
            ..Default::default()
        }));
        let mut e = VertexEngine::new(r, t());
        let err = e.run().await.err().unwrap();
        assert_eq!(err.http_status, 503);
    }

    #[tokio::test]
    async fn responses_api_round_trip() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({
            "input": "summarize this",
            "instructions": "be terse",
        });
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        assert!(
            out.response.message.contains("you said: summarize this"),
            "message: {}",
            out.response.message
        );
        assert_eq!(out.response.finish_reason, "completed");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert_eq!(
            out.response.total_tokens,
            out.response.prompt_tokens + out.response.completion_tokens
        );
        let usage = out.response.common_usage.unwrap();
        assert!(usage.prompt_total() > 0 && usage.completion_total() > 0);
    }

    #[tokio::test]
    async fn responses_api_streaming() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.stream = true;
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({"input": "stream this"});
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        assert!(out.chunks.len() >= 2, "chunks: {:?}", out.chunks);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
        assert!(
            out.response.message.contains("you said: stream this"),
            "message: {}",
            out.response.message
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().all(|c| c.native_event.is_some()));
    }

    #[derive(Debug)]
    struct SseReply(&'static str);

    #[async_trait::async_trait]
    impl crate::transport::Transport for SseReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(self.0.as_bytes().to_vec()),
            })
        }
    }

    #[derive(Debug)]
    struct BytesReply(&'static [u8]);

    #[async_trait::async_trait]
    impl crate::transport::Transport for BytesReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Json(self.0.to_vec().into()),
            })
        }
    }

    #[tokio::test]
    async fn tts_audio_bytes_become_the_b64_payload() {
        let mut r = req(Protocol::Tts, "gpt-4o-mini-tts", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::AudioTts(TtsParams {
            input: "hi".into(),
            voice: None,
            response_format: Some("mp3".into()),
        }));
        let out = AudioEngine::new(r, Arc::new(BytesReply(b"\xff\xfbaudio")), AudioKind::Tts)
            .run()
            .await
            .unwrap();
        assert_eq!(
            out.response.response_v2.unwrap()["audio_b64"],
            base64::engine::general_purpose::STANDARD.encode(b"\xff\xfbaudio")
        );
    }

    #[tokio::test]
    async fn image_and_transcription_usage_reach_the_response() {
        let mut r = req(Protocol::Image, "gpt-image-1", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::Image(ImageParams {
            prompt: "a circle".into(),
            n: 1,
            size: None,
            image: None,
            mask: None,
        }));
        let out = ImageEngine::new(
            r,
            Arc::new(BytesReply(
                br#"{"data":[{"b64_json":"AA=="}],"usage":{"input_tokens":15,"output_tokens":1056}}"#,
            )),
        )
        .run()
        .await
        .unwrap();
        assert_eq!(
            (
                out.response.prompt_tokens,
                out.response.completion_tokens,
                out.response.total_tokens
            ),
            (15, 1056, 1071)
        );

        let mut r = req(Protocol::Stt, "gpt-4o-mini-transcribe", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::AudioStt(SttParams {
            audio_b64: "TU9DSw==".into(),
            language: None,
            translate: false,
        }));
        let out = AudioEngine::new(
            r,
            Arc::new(BytesReply(
                br#"{"text":"mock","usage":{"type":"tokens","input_tokens":32,"output_tokens":12}}"#,
            )),
            AudioKind::Stt,
        )
        .run()
        .await
        .unwrap();
        assert_eq!(out.response.message, "mock");
        assert_eq!(
            (out.response.prompt_tokens, out.response.completion_tokens),
            (32, 12)
        );
    }

    #[tokio::test]
    async fn responses_stream_forwards_reasoning_and_function_call_events_verbatim() {
        let sse = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-served\",\"status\":\"in_progress\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"opaque\"}}\n\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,\"summary_index\":0,\"delta\":\"thinking about it\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"thinking about it\"}],\"encrypted_content\":\"opaque\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"now\",\"arguments\":\"\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{}\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"now\",\"arguments\":\"{}\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":2,\"delta\":\"done\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-served\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":9,\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n",
        );
        let mut r = req(Protocol::Responses, "gpt-5-served", None);
        r.stream = true;
        let param = r.model_param_v2.as_mut().unwrap();
        param.raw = serde_json::json!({"input": "go"});
        param.fallback_from = Some("gpt-5-public".to_owned());
        let out = ResponsesEngine::new(r, Arc::new(SseReply(sse)))
            .run()
            .await
            .unwrap();
        let events: Vec<&Value> = out
            .chunks
            .iter()
            .filter_map(|c| c.native_event.as_ref())
            .collect();
        assert_eq!(events.len(), 9);
        assert_eq!(events[1]["item"]["encrypted_content"], "opaque");
        assert_eq!(events[2]["delta"], "thinking about it");
        assert_eq!(events[6]["item"]["arguments"], "{}");
        assert_eq!(events[0]["response"]["model"], "gpt-5-public");
        assert_eq!(events[8]["response"]["model"], "gpt-5-public");
        assert!(out.chunks.iter().all(|c| c.delta.is_empty()));
        assert_eq!(out.response.message, "done");
        assert_eq!(out.response.finish_reason, "completed");
        assert_eq!(out.response.common_usage.unwrap().reason, 4);
    }
}
