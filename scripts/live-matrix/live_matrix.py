"""Live vendor matrix: every provider through a running gateway, with billing, cache and thinking oracles.

Usage:
    export ANTHROPIC_API_KEY=... OPENAI_API_KEY=... (every api_key_env named in live.yaml)
    GW_ADMIN_TOKEN=admin-live GW_CONFIG=scripts/live-matrix/live.yaml ./target/release/gw &
    python3 scripts/live-matrix/live_matrix.py [--gateway http://127.0.0.1:18080] [group ...]

Each case calls a surface, reads the newest ledger row and recomputes the
weighted total and cost from the WIRE usage with the model's configured prices
and weights (an independent oracle); thinking cases additionally assert that
reasoning reached the client, cache cases that a write is followed by a read.
Groups: see GROUPS (default: all).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any

GROUPS = [
    "anthropic",
    "openai",
    "gemini",
    "deepseek",
    "minimax",
    "qwen",
    "qianfan",
    "moonshot",
    "siliconflow",
    "openrouter",
    "rerank",
    "bedrock",
    "xai",
    "video",
    "search",
    "openai-aux",
    "dashscope-native",
    "gemini-rt",
    "openai-rt",
    "ollama",
]
MODELS: dict[str, dict[str, Any]] = {}
RESULTS: list[tuple[str, bool, str]] = []
PREFIX_SENTENCE = "The gateway is a Rust service that fronts many model vendors. "

WEATHER_TOOL_ANTHROPIC = [
    {
        "name": "get_weather",
        "description": "Weather for a city",
        "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
    }
]
WEATHER_TOOL_OPENAI = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Weather for a city",
            "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]},
        },
    }
]


class Gateway:
    """HTTP client for one gateway: the client key for surfaces, the admin token for the ledger."""

    def __init__(self, base: str, ak: str, admin: str) -> None:
        self.base = base
        self.ak = ak
        self.admin = admin

    def call(self, path: str, body: Any = None, admin: bool = False, timeout: int = 300) -> tuple[int, str]:
        headers = {"content-type": "application/json", "Authorization": f"Bearer {self.admin if admin else self.ak}"}
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, headers=headers, method="POST" if data else "GET")
        status, raw = self._open(req, timeout)
        return status, raw.decode(errors="replace")

    def call_raw(self, path: str) -> tuple[int, bytes]:
        return self._open(urllib.request.Request(self.base + path, headers={"Authorization": f"Bearer {self.ak}"}), 300)

    def _open(self, req: urllib.request.Request, timeout: int) -> tuple[int, bytes]:
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def ledger(self) -> tuple[int, dict[str, Any]]:
        """(row count, newest row) — read after a short settle so the async ledger write has landed."""
        time.sleep(0.3)
        _, body = self.call("/internal/ledger?limit=1", admin=True)
        j = json.loads(body)
        return j["count"], (j["records"][-1] if j["records"] else {})


def load_models(yaml_path: str) -> None:
    """Prices and weights per model from the flow-mapping lines of live.yaml (the oracle's inputs)."""
    with open(yaml_path) as f:
        lines = f.read().splitlines()
    for line in lines:
        m = re.match(r'- \{name: "?([^,"]+)"?, (.*)\}$', line.strip())
        if not m:
            continue
        name, rest = m.group(1), m.group(2)
        conf: dict[str, Any] = {"in": 0, "out": 0, "unit": 0, "rate": {}}
        for key, field in (
            ("in", "input_price_per_1k_micros"),
            ("out", "output_price_per_1k_micros"),
            ("unit", "unit_price_micros"),
        ):
            mm = re.search(field + r": (\d+)", rest)
            conf[key] = int(mm.group(1)) if mm else 0
        mm = re.search(r"token_rate: \{([^}]*)\}", rest)
        if mm:
            for kv in mm.group(1).split(","):
                k, v = kv.split(":")
                conf["rate"][k.strip()] = float(v)
        MODELS[name] = conf


def rnd(x: float) -> int:
    return 0 if x < 0 else int(math.floor(x + 0.5))


def oracle(model: str, wire: dict[str, Any], messages_protocol: bool) -> tuple[int, int, int, int]:
    """(prompt_total, completion_total, weighted_total, cost_micros) recomputed from the wire usage."""
    conf = MODELS.get(model, {"in": 0, "out": 0, "unit": 0, "rate": {}})
    rate = conf["rate"]

    def w(k: str, d: float = 1.0) -> float:
        return rate.get(k, d)

    if messages_protocol:
        inp = max(wire.get("input_tokens", 0), 0)
        out = max(wire.get("output_tokens", 0), 0)
        rc = max(wire.get("cache_read_input_tokens") or 0, 0)
        wc = max(wire.get("cache_creation_input_tokens") or 0, 0)
        wc1h = min(max((wire.get("cache_creation") or {}).get("ephemeral_1h_input_tokens", 0), 0), wc)
        prompt_total, completion_total = inp + rc + wc, out
        bp = rnd(
            inp * w("prompt")
            + rc * w("read_cache")
            + (wc - wc1h) * w("write_cache")
            + wc1h * w("write_cache_1h", w("write_cache"))
        )
        bc = rnd(out * w("completion"))
    else:
        p = max(wire.get("prompt_tokens", 0), 0)
        c = max(wire.get("completion_tokens", 0), 0)
        details = wire.get("prompt_tokens_details") or {}
        cached = min(max(details.get("cached_tokens") or 0, 0), p)
        written = min(max(details.get("cache_creation_input_tokens") or 0, 0), p - cached)
        reason = min(max((wire.get("completion_tokens_details") or {}).get("reasoning_tokens") or 0, 0), c)
        prompt_total, completion_total = p, c
        bp = rnd((p - cached - written) * w("prompt") + cached * w("read_cache") + written * w("write_cache"))
        bc = rnd((c - reason) * w("completion") + reason * w("reasoning"))
    return prompt_total, completion_total, bp + bc, (bp * conf["in"]) // 1000 + (bc * conf["out"]) // 1000


def record(name: str, ok: bool, detail: str) -> None:
    RESULTS.append((name, ok, detail))
    print(("PASS " if ok else "FAIL ") + name + " — " + detail, flush=True)


def reasoning_of(message: dict[str, Any]) -> str:
    """The reasoning prose a chat message/delta carries, with a marker when it carries units."""
    prose = message.get("reasoning_content") or message.get("reasoning") or ""
    return prose + ("[details]" if message.get("reasoning_details") else "")


def parse_sse(text: str) -> list[Any]:
    events: list[Any] = []
    for block in text.split("\n\n"):
        data = [line[5:].strip() for line in block.split("\n") if line.startswith("data:")]
        if not data:
            continue
        joined = "\n".join(data)
        if joined == "[DONE]":
            continue
        try:
            events.append(json.loads(joined))
        except ValueError:
            events.append(joined)
    return events


def check_ledger(
    gw: Gateway, name: str, model: str, wire: dict[str, Any], messages_protocol: bool, before: int, note: str
) -> None:
    count, row = gw.ledger()
    if count != before + 1:
        record(name, False, f"ledger count {before}->{count} (expected +1) {note}")
        return
    pt, ct, tot, cost = oracle(model, wire, messages_protocol)
    got = (row["prompt_tokens"], row["completion_tokens"], row["total_tokens"], row["cost_micros"])
    ok = got == (pt, ct, tot, cost)
    record(
        name,
        ok,
        f"wire={json.dumps(wire, separators=(',', ':'))} ledger p/c/t/cost={got} oracle={(pt, ct, tot, cost)} {note}",
    )


def case_chat(
    gw: Gateway,
    model: str,
    label: str = "",
    stream: bool = False,
    prompt: str = "Reply with exactly one word: hello",
    expect_reasoning: bool = False,
    **extra: Any,
) -> None:
    name = f"{model} chat{' stream' if stream else ''}{(' ' + label) if label else ''}"
    before, _ = gw.ledger()
    body: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
        **extra,
    }
    st, txt = gw.call("/v1/chat/completions", body)
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:300]}")
        return
    text, reasoning = "", ""
    if stream:
        usage = None
        events = parse_sse(txt)
        for e in events:
            if not isinstance(e, dict):
                continue
            if e.get("usage"):
                usage = e["usage"]
            for ch in e.get("choices", []):
                d = ch.get("delta", {})
                text += d.get("content") or ""
                reasoning += reasoning_of(d)
        if usage is None:
            faults = [e for e in events if isinstance(e, dict) and e.get("error")]
            count, row = gw.ledger()
            if faults:
                record(
                    name + " [upstream fault]",
                    count == before + 1,
                    f"vendor failed mid-stream; terminal frame {json.dumps(faults[-1])[:200]}; "
                    f"billed p/c/t={row.get('prompt_tokens')}/{row.get('completion_tokens')}/{row.get('total_tokens')} "
                    f"estimated={row.get('estimated')}",
                )
            else:
                record(name, False, f"stream carried no usage frame; frames={len(events)} tail={txt[-300:]!r}")
            return
        wire = usage
    else:
        j = json.loads(txt)
        wire = j.get("usage") or {}
        m = j["choices"][0]["message"]
        text = m.get("content") or ""
        reasoning = reasoning_of(m)
    note = f"text={text[:30]!r}" + (f" reasoning_len={len(reasoning)}" if expect_reasoning else "")
    check_ledger(gw, name, model, wire, False, before, note)
    if expect_reasoning:
        record(name + " [reasoning present]", bool(reasoning), f"reasoning_len={len(reasoning)}")


def case_messages(
    gw: Gateway,
    model: str,
    label: str = "",
    stream: bool = False,
    prompt: str = "Reply with exactly one word: hello",
    thinking: dict[str, Any] | None = None,
    expect_thinking: bool = False,
    max_tokens: int = 4000,
) -> None:
    name = f"{model} messages{' stream' if stream else ''}{(' ' + label) if label else ''}"
    before, _ = gw.ledger()
    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
    }
    if thinking:
        body["thinking"] = thinking
    st, txt = gw.call("/v1/messages", body)
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:300]}")
        return
    thinking_blocks = signatures = 0
    text = ""
    if stream:
        wire: dict[str, Any] = {}
        for e in parse_sse(txt):
            if not isinstance(e, dict):
                continue
            t = e.get("type")
            if t == "message_start":
                wire.update(e["message"].get("usage") or {})
            elif t == "message_delta":
                wire.update(e.get("usage") or {})
            elif t == "content_block_start" and e["content_block"].get("type") == "thinking":
                thinking_blocks += 1
            elif t == "content_block_delta":
                d = e["delta"]
                if d.get("type") == "text_delta":
                    text += d["text"]
                if d.get("type") == "signature_delta":
                    signatures += 1
            elif t == "error":
                record(name + " [upstream fault]", False, f"stream ended with {json.dumps(e)[:200]}")
                return
    else:
        j = json.loads(txt)
        wire = j.get("usage") or {}
        for b in j.get("content") or []:
            if b.get("type") == "thinking":
                thinking_blocks += 1
                signatures += 1 if b.get("signature") else 0
            if b.get("type") == "text":
                text += b.get("text", "")
    note = f"text={text[:30]!r} thinking_blocks={thinking_blocks} signatures={signatures}"
    check_ledger(gw, name, model, wire, True, before, note)
    if expect_thinking:
        record(name + " [thinking present]", thinking_blocks > 0, note)


def case_embeddings(gw: Gateway, model: str) -> None:
    name = f"{model} embeddings"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/embeddings", {"model": model, "input": ["gateway live test", "second input"]})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    j = json.loads(txt)
    wire = {"prompt_tokens": (j.get("usage") or {}).get("prompt_tokens", 0), "completion_tokens": 0}
    check_ledger(gw, name, model, wire, False, before, f"dims={len(j['data'][0]['embedding'])} n={len(j['data'])}")


def case_image(gw: Gateway, model: str) -> None:
    """One generated image bills one unit at the model's unit price (plus any token usage the vendor reports)."""
    name = f"{model} image"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/images/generations", {"model": model, "prompt": "a lighthouse at dusk, flat vector", "n": 1})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    j = json.loads(txt)
    count, row = gw.ledger()
    conf = MODELS[model]
    usage = j.get("usage") or {}
    tokens = oracle(
        model,
        {"prompt_tokens": usage.get("input_tokens", 0), "completion_tokens": usage.get("output_tokens", 0)},
        False,
    )
    expected_cost = tokens[3] + conf["unit"] * len(j.get("data", []))
    ok = count == before + 1 and row["billed_units"] == len(j.get("data", [])) and row["cost_micros"] == expected_cost
    record(
        name,
        ok,
        f"images={len(j.get('data', []))} ledger units/cost={row['billed_units']}/{row['cost_micros']} "
        f"expected {len(j.get('data', []))}/{expected_cost} usage={json.dumps(usage)}",
    )


def video_handle(j: dict[str, Any]) -> str | None:
    """The async handle, whichever dialect answered (mirrors the gateway's extraction)."""
    return (
        (j.get("output") or {}).get("task_id")
        or (j.get("data") or {}).get("task_id")
        or j.get("requestId")
        or j.get("task_id")
        or j.get("request_id")
        or (j.get("id") if j.get("object") == "video" else None)
    )


def video_state(j: dict[str, Any]) -> str:
    return str(
        j.get("status")
        or (j.get("output") or {}).get("task_status")
        or (j.get("data") or {}).get("task_status")
        or ""
    )


def _video_duration(j: dict[str, Any]) -> float:
    videos = ((j.get("data") or {}).get("task_result") or {}).get("videos") or []
    duration = (j.get("video") or {}).get("duration")
    if duration is None and videos:
        duration = videos[0].get("duration") or len(videos)
    return float(duration or 0)


def case_video(
    gw: Gateway,
    model: str,
    body: dict[str, Any] | None = None,
    done: str = "done",
    units: int | None = None,
    content: bool = False,
    timeout: int = 900,
) -> None:
    """Async video: submit lands a 0-unit row; the first terminal poll bills once at the quoted unit price; a re-poll adds nothing."""
    name = f"{model} video"
    before, _ = gw.ledger()
    req = {"model": model, "prompt": "a paper boat drifting on a pond, gentle ripples"}
    req.update(body if body is not None else {"duration": 2, "resolution": "480p"})
    st, txt = gw.call("/v1/videos/generations", req)
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:300]}")
        return
    rid = video_handle(json.loads(txt))
    if not rid:
        record(name, False, f"no async handle: {txt[:300]}")
        return
    deadline = time.time() + timeout
    j: dict[str, Any] = {}
    terminal = {done, "failed", "expired", "Failed", "Fail", "FAILED", "CANCELED"}
    while time.time() < deadline:
        st, txt = gw.call(f"/v1/videos/{rid}")
        j = json.loads(txt) if st == 200 else {}
        if video_state(j) in terminal:
            break
        time.sleep(5)
    if video_state(j) != done:
        record(name, False, f"state={video_state(j)} HTTP {st}: {txt[:300]}")
        return
    gw.call(f"/v1/videos/{rid}")
    count, row = gw.ledger()
    ticks = (j.get("usage") or {}).get("cost_in_usd_ticks")
    expected_units = units if units is not None else math.ceil(_video_duration(j))
    expected_vendor = ticks // 10_000 if ticks else row["vendor_cost_micros"]
    ok = (
        count == before + 2
        and row["billed_units"] == expected_units
        and row["cost_micros"] == expected_units * MODELS[model]["unit"]
        and row["vendor_cost_micros"] == expected_vendor
        and row["request_id"] == rid
    )
    detail = (
        f"rows+{count - before} ledger units/cost/vendor={row['billed_units']}/{row['cost_micros']}/"
        f"{row['vendor_cost_micros']} expected {expected_units}/{expected_units * MODELS[model]['unit']}/{expected_vendor}"
    )
    if content and ok:
        st, blob = gw.call_raw(f"/v1/videos/{rid}/content")
        ok = st == 200 and len(blob) > 10_000
        detail += f" content={len(blob)}B"
    record(name, ok, detail)


def case_moderations(gw: Gateway, model: str) -> None:
    """Moderation verdicts pass through; the call lands one ledger row."""
    name = f"{model} moderations"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/moderations", {"model": model, "input": ["I want to hurt them badly", "good morning"]})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    j = json.loads(txt)
    results = j.get("results", [])
    count, _ = gw.ledger()
    ok = count == before + 1 and len(results) == 2 and results[0].get("flagged") is True
    record(name, ok, f"results={len(results)} flagged0={results and results[0].get('flagged')}")


def case_tts(gw: Gateway, model: str, text: str = "Hello from the gateway, this is a voice check.") -> None:
    """TTS bills one unit per input character."""
    name = f"{model} tts"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/audio/speech", {"model": model, "input": text, "voice": "alloy"})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    count, row = gw.ledger()
    chars = len(text)
    ok = (
        count == before + 1
        and row["billed_units"] == chars
        and row["cost_micros"] == chars * MODELS[model]["unit"]
        and len(txt) > 1000
    )
    record(name, ok, f"audio={len(txt)}B ledger units/cost={row['billed_units']}/{row['cost_micros']} expected {chars}/{chars * MODELS[model]['unit']}")


def case_stt(gw: Gateway, model: str) -> None:
    """STT bills whole seconds: the vendor's duration, else the upload's own play length."""
    import base64
    import io
    import math as m
    import struct
    import wave

    name = f"{model} stt"
    seconds, rate = 2, 8000
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(b"".join(struct.pack("<h", int(12000 * m.sin(2 * m.pi * 440 * i / rate))) for i in range(rate * seconds)))
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/audio/transcriptions", {"model": model, "audio_b64": base64.b64encode(buf.getvalue()).decode()})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    count, row = gw.ledger()
    ok = (
        count == before + 1
        and row["billed_units"] == seconds
        and row["cost_micros"] == seconds * MODELS[model]["unit"]
    )
    record(name, ok, f"ledger units/cost={row['billed_units']}/{row['cost_micros']} expected {seconds}/{seconds * MODELS[model]['unit']} text={txt[:60]}")


def case_realtime_gemini(gw: Gateway, model: str) -> None:
    """One Live-API text turn through the bridge: usageMetadata settles the ledger row."""
    name = f"{model} realtime"
    try:
        import websocket
    except ImportError:
        record(name, False, "websocket-client not installed")
        return
    before, _ = gw.ledger()
    ws = websocket.create_connection(
        gw.base.replace("http", "ws", 1) + f"/v1/realtime?model={model}",
        header={"Authorization": f"Bearer {gw.ak}"},
        timeout=90,
    )
    ws.send(json.dumps({"setup": {"model": f"models/{model}",
                                   "generationConfig": {"responseModalities": ["AUDIO"]},
                                   "outputAudioTranscription": {}}}))
    setup = json.loads(ws.recv())
    if "setupComplete" not in setup:
        record(name, False, f"no setupComplete: {json.dumps(setup)[:200]}")
        ws.close()
        return
    ws.send(json.dumps({"clientContent": {"turns": [{"role": "user", "parts": [{"text": "Reply with the single word: pong"}]}], "turnComplete": True}}))
    usage: dict[str, Any] = {}
    text = ""
    deadline = time.time() + 90
    while time.time() < deadline:
        frame = json.loads(ws.recv())
        sc = frame.get("serverContent") or {}
        text += (sc.get("outputTranscription") or {}).get("text", "")
        if sc.get("turnComplete"):
            usage = frame.get("usageMetadata") or {}
            break
    ws.close()
    it = usage.get("promptTokenCount", 0)
    ot = usage.get("responseTokenCount") or usage.get("candidatesTokenCount") or 0
    cost = (MODELS[model]["in"] * it) // 1000 + (MODELS[model]["out"] * ot) // 1000
    count, row = gw.ledger()
    ok = (
        count == before + 1
        and (row["prompt_tokens"], row["completion_tokens"]) == (it, ot)
        and row["cost_micros"] == cost
        and not row["estimated"]
        and "pong" in text.lower()
    )
    record(
        name,
        ok,
        f"wire it/ot={it}/{ot} text='{text[:24]}' ledger p/c/cost/est=({row['prompt_tokens']}, {row['completion_tokens']}, "
        f"{row['cost_micros']}, {row['estimated']}) oracle=({it}, {ot}, {cost})",
    )


def case_search(gw: Gateway, model: str) -> None:
    """One web search bills one unit at the model's unit price; results pass through."""
    name = f"{model} search"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/search", {"model": model, "query": "what is an api gateway", "count": 3})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    j = json.loads(txt)
    results = (j.get("web") or {}).get("results") or j.get("results") or []
    count, row = gw.ledger()
    ok = (
        count == before + 1
        and len(results) > 0
        and row["billed_units"] == 1
        and row["cost_micros"] == MODELS[model]["unit"]
    )
    record(name, ok, f"results={len(results)} ledger units/cost={row['billed_units']}/{row['cost_micros']}")


def case_realtime(gw: Gateway, model: str) -> None:
    """One gated realtime turn with audio output: the vendor's usage frame settles the ledger row (not the estimate)."""
    name = f"{model} realtime"
    try:
        import websocket
    except ImportError:
        record(name, False, "websocket-client not installed")
        return
    before, _ = gw.ledger()
    ws = websocket.create_connection(
        gw.base.replace("http", "ws", 1) + f"/v1/realtime?model={model}",
        header={"Authorization": f"Bearer {gw.ak}"},
        timeout=90,
    )
    ws.send(json.dumps({"type": "session.update", "session": {"type": "realtime", "output_modalities": ["audio"]}}))
    ws.send(json.dumps({"type": "conversation.item.create", "item": {
        "type": "message", "role": "user",
        "content": [{"type": "input_text", "text": "Say the single word: pong"}]}}))
    ws.send(json.dumps({"type": "response.create"}))
    done: dict[str, Any] = {}
    deadline = time.time() + 120
    while time.time() < deadline:
        frame = json.loads(ws.recv())
        if frame.get("type") == "response.done":
            done = frame
            break
        if frame.get("type") == "error":
            record(name, False, json.dumps(frame)[:300])
            ws.close()
            return
    ws.close()
    u = (done.get("response") or {}).get("usage") or {}
    it, ot = u.get("input_tokens", 0), u.get("output_tokens", 0)
    ap = (u.get("input_token_details") or {}).get("audio_tokens", 0)
    ac = (u.get("output_token_details") or {}).get("audio_tokens", 0)
    rate = MODELS[model]["rate"]
    bp = rnd((it - ap) + ap * rate.get("audio_prompt", 1.0))
    bc = rnd((ot - ac) + ac * rate.get("audio_completion", 1.0))
    cost = (MODELS[model]["in"] * bp) // 1000 + (MODELS[model]["out"] * bc) // 1000
    count, row = gw.ledger()
    ok = (
        count == before + 1
        and (row["prompt_tokens"], row["completion_tokens"]) == (it, ot)
        and row["total_tokens"] == bp + bc
        and row["cost_micros"] == cost
        and not row["estimated"]
        and ac > 0
    )
    record(
        name,
        ok,
        f"wire it/ot/ap/ac={it}/{ot}/{ap}/{ac} ledger p/c/t/cost/est="
        f"({row['prompt_tokens']}, {row['completion_tokens']}, {row['total_tokens']}, {row['cost_micros']}, {row['estimated']}) "
        f"oracle=({it}, {ot}, {bp + bc}, {cost})",
    )


def case_rerank(gw: Gateway, model: str, unit_priced: bool = False) -> None:
    name = f"{model} rerank"
    before, _ = gw.ledger()
    docs = [
        "A gateway routes requests to model providers.",
        "Bananas are yellow.",
        "An API gateway fronts upstream services.",
    ]
    st, txt = gw.call("/v1/rerank", {"model": model, "query": "what is a gateway", "documents": docs, "top_n": 2})
    if st != 200:
        record(name, False, f"HTTP {st}: {txt[:200]}")
        return
    j = json.loads(txt)
    count, row = gw.ledger()
    conf = MODELS[model]
    results = len(j.get("results", []))
    if unit_priced:
        units = ((j.get("meta") or {}).get("billed_units") or {}).get("search_units", 0)
        ok = (
            count == before + 1
            and row["billed_units"] == units
            and row["cost_micros"] == units * conf["unit"]
            and row["total_tokens"] == 0
        )
        record(
            name,
            ok,
            f"units={units} ledger units/cost={row['billed_units']}/{row['cost_micros']} "
            f"expected {units}/{units * conf['unit']} results={results}",
        )
        return
    meta_tokens = (j.get("meta") or {}).get("tokens") or {}
    tokens = (j.get("usage") or {}).get("total_tokens", 0) or (
        meta_tokens.get("input_tokens", 0) + meta_tokens.get("output_tokens", 0)
    )
    ok = count == before + 1 and row["prompt_tokens"] == tokens and row["cost_micros"] == (tokens * conf["in"]) // 1000
    record(
        name,
        ok,
        f"tokens={tokens} ledger p/cost={row['prompt_tokens']}/{row['cost_micros']} "
        f"expected {tokens}/{(tokens * conf['in']) // 1000} results={results}",
    )


def case_response_cache(gw: Gateway, model: str) -> None:
    """A model with cache_ttl_seconds: the second identical request is served unbilled from the response cache."""
    name = f"{model} response cache"
    prompt = f"Cache probe: reply with the single word cache-{int(time.time())}"
    body = {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0}
    before, _ = gw.ledger()
    st1, t1 = gw.call("/v1/chat/completions", body)
    mid, _ = gw.ledger()
    st2, t2 = gw.call("/v1/chat/completions", body)
    after, _ = gw.ledger()
    if st1 != 200 or st2 != 200:
        record(name, False, f"HTTP {st1}/{st2}")
        return
    j1, j2 = json.loads(t1), json.loads(t2)
    same = (
        j1["choices"][0]["message"]["content"] == j2["choices"][0]["message"]["content"] and j1["usage"] == j2["usage"]
    )
    record(
        name,
        mid == before + 1 and after == mid and same,
        f"first billed (+{mid - before}), second unbilled (+{after - mid}), identical body={same}",
    )


def case_prompt_cache(
    gw: Gateway, model: str, native: bool = False, words: int = 1300, expect_write: bool = False
) -> None:
    """Two calls sharing a long prefix (haiku 4.5 needs >= 4096 tokens): the second must read the cache;
    on Anthropic wires the first must also write it."""
    surface = "messages" if native else "chat"
    name = f"{model} prompt cache ({surface})"
    prefix = f"Run nonce {int(time.time())}. " + PREFIX_SENTENCE * words

    def go(q: str) -> tuple[int, str]:
        if native:
            body = {"model": model, "max_tokens": 50, "system": prefix, "messages": [{"role": "user", "content": q}]}
            return gw.call("/v1/messages", body)
        messages = [{"role": "system", "content": prefix}, {"role": "user", "content": q}]
        return gw.call("/v1/chat/completions", {"model": model, "max_tokens": 50, "messages": messages})

    before, _ = gw.ledger()
    st1, t1 = go("Say ONE word: alpha")
    c1, row1 = gw.ledger()
    st2, t2 = go("Say ONE word: beta")
    c2, row2 = gw.ledger()
    if st1 != 200 or st2 != 200:
        record(name, False, f"HTTP {st1}/{st2}: {t1[:200]} {t2[:200]}")
        return
    u1, u2 = json.loads(t1)["usage"], json.loads(t2)["usage"]
    if native:
        written, read = u1.get("cache_creation_input_tokens") or 0, u2.get("cache_read_input_tokens") or 0
    else:
        written = (u1.get("prompt_tokens_details") or {}).get("cache_creation_input_tokens") or 0
        read = (u2.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
    o1, o2 = oracle(model, u1, native), oracle(model, u2, native)
    ok = (
        c1 == before + 1
        and c2 == c1 + 1
        and (written > 0 or not expect_write)
        and read > 0
        and (row1["total_tokens"], row1["cost_micros"]) == o1[2:]
        and (row2["total_tokens"], row2["cost_micros"]) == o2[2:]
    )
    record(
        name,
        ok,
        f"write={written} read={read} | row1 t/cost={row1['total_tokens']}/{row1['cost_micros']} oracle={o1[2:]} | "
        f"row2 t/cost={row2['total_tokens']}/{row2['cost_micros']} oracle={o2[2:]} | "
        f"wire1={json.dumps(u1)} wire2={json.dumps(u2)}",
    )


def case_thinking_replay(gw: Gateway, model: str, native: bool = True) -> None:
    """Tool loop with thinking: turn 1 yields signed thinking + a tool call, turn 2 replays both with the result."""
    name = f"{model} signed thinking replay ({'messages' if native else 'chat'})"
    question = [{"role": "user", "content": "What is the weather in Paris? Use the tool."}]
    before, _ = gw.ledger()
    if native:
        b1: dict[str, Any] = {
            "model": model,
            "max_tokens": 4000,
            "thinking": {"type": "enabled", "budget_tokens": 1024},
            "tools": WEATHER_TOOL_ANTHROPIC,
            "messages": question,
        }
        st, t = gw.call("/v1/messages", b1)
        if st != 200:
            record(name, False, f"turn1 HTTP {st}: {t[:300]}")
            return
        content = json.loads(t)["content"]
        thinking = [b for b in content if b["type"] == "thinking"]
        tool_use = [b for b in content if b["type"] == "tool_use"]
        if not tool_use:
            record(name, False, f"turn1 produced no tool_use: {json.dumps(content)[:300]}")
            return
        b2 = {
            **b1,
            "messages": question
            + [
                {"role": "assistant", "content": content},
                {
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": tool_use[0]["id"], "content": "Sunny, 25C"}],
                },
            ],
        }
        st2, t2 = gw.call("/v1/messages", b2)
        answer = (
            "".join(b.get("text", "") for b in json.loads(t2).get("content", []) if b.get("type") == "text")
            if st2 == 200
            else t2[:200]
        )
        signed = sum(1 for b in thinking if b.get("signature"))
        ok = st2 == 200 and thinking and signed == len(thinking)
        detail = (
            f"turn1 thinking={len(thinking)} signed={signed} tool_use={len(tool_use)}; "
            f"turn2 HTTP {st2} text={answer[:40]!r}"
        )
    else:
        b1 = {
            "model": model,
            "max_tokens": 4000,
            "reasoning_effort": "low",
            "tools": WEATHER_TOOL_OPENAI,
            "messages": question,
        }
        st, t = gw.call("/v1/chat/completions", b1)
        if st != 200:
            record(name, False, f"turn1 HTTP {st}: {t[:300]}")
            return
        m = json.loads(t)["choices"][0]["message"]
        calls, details = m.get("tool_calls") or [], m.get("reasoning_details") or []
        if not calls:
            record(name, False, f"turn1 produced no tool_calls: {json.dumps(m)[:300]}")
            return
        assistant: dict[str, Any] = {"role": "assistant", "content": m.get("content"), "tool_calls": calls}
        if details:
            assistant["reasoning_details"] = details
        b2 = {
            **b1,
            "messages": question
            + [assistant, {"role": "tool", "tool_call_id": calls[0]["id"], "content": "Sunny, 25C"}],
        }
        st2, t2 = gw.call("/v1/chat/completions", b2)
        answer = (json.loads(t2)["choices"][0]["message"].get("content") or "") if st2 == 200 else t2[:200]
        signed = sum(1 for d in details if d.get("signature"))
        ok = st2 == 200 and details and signed > 0
        detail = (
            f"turn1 details={len(details)} signed={signed} tool_calls={len(calls)}; "
            f"turn2 HTTP {st2} text={answer[:40]!r}"
        )
    after, _ = gw.ledger()
    record(name, bool(ok) and after == before + 2, detail + f"; ledger +{after - before}")


def case_thinking_tiers(gw: Gateway, model: str, native: bool, tiers: list[Any], expect_reasoning: bool) -> None:
    """Every effort/budget tier through one model: budgets on /v1/messages (max_tokens above the budget,
    as Anthropic requires), efforts on chat; the reasoning share per tier lands in the note."""
    prompt = "Solve 23*47 step by step briefly."
    for tier in tiers:
        if native:
            case_messages(
                gw,
                model,
                f"budget {tier}",
                thinking={"type": "enabled", "budget_tokens": tier},
                prompt=prompt,
                expect_thinking=expect_reasoning,
                max_tokens=tier + 4000,
            )
        else:
            case_chat(
                gw,
                model,
                f"effort {tier}",
                prompt=prompt,
                expect_reasoning=expect_reasoning,
                reasoning_effort=tier,
                max_tokens=40000,
            )


def case_responses_surfaces(gw: Gateway, model: str) -> None:
    """A `protocol: responses` model reached natively and from the chat/messages surfaces, streaming."""
    prompt = "Reply with exactly one word: hello"
    name = f"{model} responses native stream"
    before, _ = gw.ledger()
    st, txt = gw.call("/v1/responses", {"model": model, "input": prompt, "stream": True})
    events = parse_sse(txt) if st == 200 else []
    text = "".join(
        e.get("delta", "") for e in events if isinstance(e, dict) and e.get("type") == "response.output_text.delta"
    )
    done = [e for e in events if isinstance(e, dict) and e.get("type") == "response.completed"]
    if st != 200 or not done:
        record(name, False, f"HTTP {st}: {txt[:300]}")
    else:
        usage = done[-1]["response"]["usage"]
        wire = {
            "prompt_tokens": usage.get("input_tokens", 0),
            "completion_tokens": usage.get("output_tokens", 0),
            "prompt_tokens_details": {
                "cached_tokens": (usage.get("input_tokens_details") or {}).get("cached_tokens", 0)
            },
            "completion_tokens_details": {
                "reasoning_tokens": (usage.get("output_tokens_details") or {}).get("reasoning_tokens", 0)
            },
        }
        check_ledger(gw, name, model, wire, False, before, f"text={text[:30]!r}")
    case_chat(gw, model, "responses model", stream=True, prompt=prompt)
    case_messages(gw, model, "responses model", stream=True, prompt=prompt)
    name = f"{model} responses model tool loop (chat)"
    question = [{"role": "user", "content": "What is the weather in Paris? Use the tool."}]
    before, _ = gw.ledger()
    st, t1 = gw.call("/v1/chat/completions", {"model": model, "tools": WEATHER_TOOL_OPENAI, "messages": question})
    calls = json.loads(t1)["choices"][0]["message"].get("tool_calls") or [] if st == 200 else []
    if not calls:
        record(name, False, f"turn1 HTTP {st}: {t1[:200]}")
        return
    replay = question + [
        {"role": "assistant", "content": None, "tool_calls": calls},
        {"role": "tool", "tool_call_id": calls[0]["id"], "content": "Sunny, 25C"},
    ]
    st2, t2 = gw.call("/v1/chat/completions", {"model": model, "tools": WEATHER_TOOL_OPENAI, "messages": replay})
    answer = (json.loads(t2)["choices"][0]["message"].get("content") or "") if st2 == 200 else t2[:200]
    after, _ = gw.ledger()
    record(
        name,
        st2 == 200 and after == before + 2,
        f"turn1 tool_calls={len(calls)}; turn2 HTTP {st2} text={answer[:40]!r}",
    )
    name = f"{model} responses model anthropic tools (messages)"
    st, txt = gw.call(
        "/v1/messages", {"model": model, "max_tokens": 200, "tools": WEATHER_TOOL_ANTHROPIC, "messages": question}
    )
    blocks = json.loads(txt).get("content", []) if st == 200 else []
    tool_use = [b for b in blocks if b.get("type") == "tool_use"]
    record(name, st == 200 and bool(tool_use), f"HTTP {st} tool_use={len(tool_use)} {txt[:120]!r}")


def run_group(gw: Gateway, group: str) -> None:
    prime = "Is 17 prime? One word."
    if group == "anthropic":
        haiku, sonnet45, sonnet46 = "claude-haiku-4-5-20251001", "claude-sonnet-4-5-20250929", "claude-sonnet-4-6"
        case_chat(gw, haiku)
        case_chat(gw, haiku, stream=True)
        case_messages(gw, haiku)
        case_messages(gw, haiku, stream=True)
        case_messages(
            gw,
            haiku,
            "thinking budget",
            thinking={"type": "enabled", "budget_tokens": 1024},
            prompt=prime,
            expect_thinking=True,
        )
        case_messages(
            gw,
            sonnet46,
            "thinking adaptive",
            stream=True,
            thinking={"type": "adaptive"},
            prompt="Solve 23*47 step by step briefly.",
            expect_thinking=True,
        )
        case_chat(gw, haiku, "reasoning_effort low", prompt=prime, expect_reasoning=True, reasoning_effort="low")
        case_chat(
            gw, haiku, "reasoning_effort", stream=True, prompt=prime, expect_reasoning=True, reasoning_effort="low"
        )
        case_prompt_cache(gw, haiku, native=True, words=340, expect_write=True)
        case_prompt_cache(gw, sonnet45, words=220, expect_write=True)
        case_thinking_replay(gw, haiku, native=True)
        case_thinking_replay(gw, haiku, native=False)
        case_thinking_tiers(gw, haiku, native=True, tiers=[1024, 4096, 16384], expect_reasoning=True)
        # adaptive thinking may skip reasoning at low effort on an easy prompt: presence is reported, not asserted
        case_thinking_tiers(gw, sonnet46, native=False, tiers=["low", "medium", "high", "max"], expect_reasoning=False)
    elif group == "openai":
        case_chat(gw, "gpt-4o-mini")
        case_chat(gw, "gpt-4o-mini", stream=True)
        case_response_cache(gw, "gpt-4o-mini")
        case_chat(gw, "gpt-5-mini", "reasoning_effort low", prompt=prime, reasoning_effort="low")
        case_chat(gw, "gpt-5-mini", "reasoning", stream=True, prompt=prime, reasoning_effort="low")
        case_prompt_cache(gw, "gpt-4.1-mini")
        case_messages(gw, "gpt-4o-mini", "cross-protocol")
        case_messages(
            gw,
            "gpt-5-mini",
            "cross-protocol thinking→effort",
            thinking={"type": "enabled", "budget_tokens": 2048},
            prompt=prime,
        )
        # gpt-5-mini accepts minimal..high, gpt-5.4-mini none..xhigh; past the last tier the vendor answers 400
        case_thinking_tiers(gw, "gpt-5-mini", native=True, tiers=[1024, 4096, 16384], expect_reasoning=False)
        case_thinking_tiers(gw, "gpt-5.4-mini", native=True, tiers=[1024, 4096, 16384, 24576], expect_reasoning=False)
        case_embeddings(gw, "text-embedding-3-small")
        case_responses_surfaces(gw, "gpt-4.1-nano")
    elif group == "gemini":
        # free tier: 5 RPM per model, so pace the calls
        for model in ("gemini-3.6-flash",):
            case_chat(gw, model)
            time.sleep(13)
            case_chat(gw, model, stream=True)
            time.sleep(13)
            case_chat(gw, model, "reasoning_effort low", prompt=prime, reasoning_effort="low")
            time.sleep(13)
            case_messages(gw, model, "cross-protocol")
    elif group == "deepseek":
        case_chat(gw, "deepseek-chat")
        case_chat(gw, "deepseek-chat", stream=True)
        case_chat(gw, "deepseek-reasoner", "reasoning", prompt=prime, expect_reasoning=True)
        case_chat(gw, "deepseek-reasoner", "reasoning", stream=True, prompt=prime, expect_reasoning=True)
        case_prompt_cache(gw, "deepseek-chat", words=400)
        case_messages(gw, "deepseek-chat", "cross-protocol")
    elif group == "minimax":
        case_messages(gw, "MiniMax-M2.5", "anthropic-compat")
        case_messages(
            gw,
            "MiniMax-M2.5",
            "anthropic-compat thinking",
            stream=True,
            thinking={"type": "enabled", "budget_tokens": 1024},
            prompt=prime,
            expect_thinking=True,
        )
        case_chat(gw, "MiniMax-M2.5", "cross-protocol chat")
        case_chat(
            gw,
            "MiniMax-M2.7",
            "openai-compat reasoning_split",
            prompt=prime,
            expect_reasoning=True,
            reasoning_split=True,
        )
        case_chat(gw, "MiniMax-M2.7", "openai-compat", stream=True)
    elif group == "qwen":
        case_chat(gw, "qwen-plus")
        case_chat(
            gw, "qwen-plus", "enable_thinking", stream=True, prompt=prime, expect_reasoning=True, enable_thinking=True
        )
        case_chat(gw, "qwen3-max", stream=True)
        case_messages(
            gw,
            "qwen3-235b-a22b",
            "anthropic-compat thinking",
            stream=True,
            thinking={"type": "enabled", "budget_tokens": 1024},
            prompt=prime,
            expect_thinking=True,
        )
        case_prompt_cache(gw, "qwen-plus", words=400)
    elif group == "qianfan":
        case_chat(gw, "ernie-4.5-turbo-128k")
        case_chat(gw, "ernie-x1.1", "reasoning", prompt=prime, expect_reasoning=True)
        case_chat(gw, "ernie-x1.1", "reasoning", stream=True, prompt=prime, expect_reasoning=True)
    elif group == "moonshot":
        # the org's RPM is 3: pace the calls
        case_chat(gw, "kimi-k2.6", prompt=prime, expect_reasoning=True)
        time.sleep(21)
        case_chat(gw, "kimi-k2.6", stream=True, prompt=prime, expect_reasoning=True)
        time.sleep(21)
        case_messages(gw, "kimi-k2.6", "cross-protocol")
    elif group == "siliconflow":
        case_chat(
            gw,
            "Qwen/Qwen3-8B",
            "enable_thinking",
            stream=True,
            prompt=prime,
            expect_reasoning=True,
            enable_thinking=True,
        )
        case_chat(gw, "Qwen/Qwen3-8B", enable_thinking=False)
        case_embeddings(gw, "BAAI/bge-m3")
        case_rerank(gw, "BAAI/bge-reranker-v2-m3")
    elif group == "openrouter":
        case_chat(
            gw,
            "openai/gpt-oss-20b:free",
            "reasoning_effort low",
            prompt=prime,
            expect_reasoning=True,
            reasoning_effort="low",
        )
        case_chat(
            gw, "openai/gpt-oss-20b:free", stream=True, prompt=prime, expect_reasoning=True, reasoning_effort="low"
        )
    elif group == "rerank":
        case_rerank(gw, "rerank-v3.5", unit_priced=True)
        case_rerank(gw, "jina-reranker-v3")
    elif group == "xai":
        case_chat(gw, "grok-4.3", expect_reasoning=True)
        case_chat(gw, "grok-4.3", stream=True, prompt=prime, expect_reasoning=True)
        case_chat(gw, "grok-4.20-0309-non-reasoning")
        case_thinking_tiers(
            gw, "grok-4.6", native=False, tiers=["low", "medium", "high", "xhigh"], expect_reasoning=False
        )
        case_chat(gw, "grok-4.3", "reasoning_effort none", prompt=prime, reasoning_effort="none")
        case_messages(gw, "grok-4.3", "cross-protocol")
        case_messages(
            gw,
            "grok-4.6",
            "cross-protocol thinking→effort",
            stream=True,
            thinking={"type": "enabled", "budget_tokens": 4096},
            prompt=prime,
        )
        case_prompt_cache(gw, "grok-4.3", words=400)
        case_responses_surfaces(gw, "grok-4.5")
        case_image(gw, "grok-imagine-image-2.0")
        case_video(gw, "grok-imagine-video-1.5")
    elif group == "video":
        case_video(gw, "sora-2", {"duration": 4, "resolution": "720x1280"}, done="completed", units=4, content=True)
        case_video(gw, "Wan-AI/Wan2.2-T2V-A14B", {"resolution": "1280x720"}, done="Succeed", units=1)
        # parameters (size/duration) silently kill the task into UNKNOWN on intl — submit bare
        case_video(gw, "wan2.2-t2v-plus", {}, done="SUCCEEDED", units=5)
        case_video(gw, "MiniMax-Hailuo-02", {"duration": 6}, done="Success", units=1, content=True)
        case_video(gw, "kling-v1-6", {"duration": 5, "aspect_ratio": "16:9"}, done="succeed")
    elif group == "search":
        case_search(gw, "brave-search")
    elif group == "openai-rt":
        case_realtime(gw, "gpt-realtime-mini")
    elif group == "openai-aux":
        case_moderations(gw, "omni-moderation-latest")
        case_tts(gw, "gpt-4o-mini-tts")
        case_stt(gw, "whisper-1")
    elif group == "dashscope-native":
        case_chat(gw, "qwen-turbo")
    elif group == "gemini-rt":
        case_realtime_gemini(gw, "gemini-3.1-flash-live-preview")
    elif group == "ollama":
        case_chat(gw, "qwen2.5:0.5b")
        case_chat(gw, "qwen2.5:0.5b", stream=True)
        case_messages(gw, "qwen2.5:0.5b", "cross-protocol")
    elif group == "bedrock":
        haiku, sonnet = "us.anthropic.claude-haiku-4-5-20251001-v1:0", "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
        case_messages(gw, haiku, "aws-anthropic")
        case_messages(
            gw,
            haiku,
            "aws-anthropic thinking",
            stream=True,
            thinking={"type": "enabled", "budget_tokens": 1024},
            prompt=prime,
            expect_thinking=True,
        )
        case_chat(
            gw, haiku, "aws-anthropic chat reasoning", prompt=prime, expect_reasoning=True, reasoning_effort="low"
        )
        case_messages(gw, sonnet, "converse")
        case_messages(
            gw,
            sonnet,
            "converse thinking",
            stream=True,
            thinking={"type": "enabled", "budget_tokens": 1024},
            prompt=prime,
            expect_thinking=True,
        )
        case_prompt_cache(gw, sonnet, native=True, words=220, expect_write=True)
        case_thinking_replay(gw, sonnet, native=True)
        case_chat(gw, "us.amazon.nova-lite-v1:0", "converse chat")
        case_chat(gw, "us.amazon.nova-lite-v1:0", "converse chat", stream=True)
        case_chat(gw, "us.deepseek.r1-v1:0", "converse reasoning", prompt=prime, expect_reasoning=True)
        case_chat(gw, "us.meta.llama3-1-8b-instruct-v1:0", "aws-llama")
        case_chat(gw, "us.meta.llama3-1-8b-instruct-v1:0", "aws-llama", stream=True)
    else:
        raise SystemExit(f"unknown group {group}")


def write_report(path: str) -> int:
    ok = sum(1 for _, passed, _ in RESULTS if passed)
    lines = [f"# Live matrix — {ok}/{len(RESULTS)} passed", "", "| result | case | detail |", "|---|---|---|"]
    for name, passed, detail in RESULTS:
        lines.append(f"| {'PASS' if passed else 'FAIL'} | {name} | {detail.replace('|', '/')} |")
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\n{ok}/{len(RESULTS)} passed; report {path}")
    return 0 if ok == len(RESULTS) else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("groups", nargs="*", help="provider groups to run (default: all)")
    ap.add_argument("--gateway", default="http://127.0.0.1:18080")
    ap.add_argument("--ak", default="ak-live")
    ap.add_argument("--admin-token", default="admin-live")
    ap.add_argument(
        "--config",
        default=__file__.rsplit("/", 1)[0] + "/live.yaml",
        help="the gateway config the oracle reads prices from",
    )
    ap.add_argument("--report", default="live-matrix-report.md")
    args = ap.parse_args()
    load_models(args.config)
    gw = Gateway(args.gateway, args.ak, args.admin_token)
    for group in args.groups or GROUPS:
        run_group(gw, group)
    return write_report(args.report)


if __name__ == "__main__":
    sys.exit(main())
