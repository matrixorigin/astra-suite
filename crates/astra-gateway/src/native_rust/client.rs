//! LLM stream abstraction for the NativeRust runtime.
//!
//! [`LlmStream`] is what [`super::loop_runner`] calls. Two impls:
//! - [`MockLlmStream`] — deterministic replay of a canned event list; used
//!   by unit tests so the tool-use loop can be exercised without network
//! - [`BedrockClient`] — real `converse_stream` over aws-sdk-bedrockruntime
//!
//! Both implementations emit [`StreamEvent`] sequences that
//! [`super::loop_runner`] consumes.
//!
//! Reasoning kinds mirror [`crate::cli_bridge::ReasoningKind`] so events
//! can be forwarded straight into `CliProgress` without a second enum.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use aws_sdk_bedrockruntime::types as bedrock;
use aws_smithy_types::Document;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::cli_bridge::ReasoningKind;

use super::history::{ContentBlock, Message, Role};
use super::tools::ToolSpec;

/// Why the model stopped producing this turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    Other,
}

/// Token usage reported at message stop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Transport-agnostic stream event. Emitted by the LLM client as the model
/// response streams in; consumed by `loop_runner` which forwards the
/// relevant pieces to `progress_tx` as [`crate::cli_bridge::CliProgress`].
#[derive(Debug, Clone)]
pub enum StreamEvent {
    TextDelta(String),
    ReasoningDelta {
        kind: ReasoningKind,
        text: String,
    },
    /// A complete thinking block with signature (for `--extended-thinking`
    /// mode). Loop_runner appends this to the assistant message so future
    /// turns include the thinking chain.
    ThinkingBlock {
        text: String,
        signature: Option<String>,
    },
    /// Tool invocation begins. Emitted when Anthropic's stream announces
    /// `content_block_start` for a tool_use block.
    ToolUseStart {
        id: String,
        name: String,
        params_preview: Option<String>,
    },
    /// Tool invocation fully parsed (JSON input complete).
    ToolUseComplete {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    /// Turn finished.
    MessageStop {
        stop_reason: StopReason,
        usage: Usage,
    },
    /// Late-arriving token usage accounting. Emitted by Bedrock's Metadata
    /// event, which arrives AFTER MessageStop and carries the real input/
    /// output token counts. Consumers should add these to their running
    /// totals without touching stop_reason.
    UsageUpdate(Usage),
}

/// Request shape sent to the LLM. Kept transport-agnostic so tests and the
/// real Bedrock client share it.
#[derive(Debug, Clone)]
pub struct LlmRequest<'a> {
    pub system: &'a str,
    pub tools: &'a [ToolSpec],
    pub messages: &'a [Message],
    pub model: &'a str,
    pub max_tokens: u32,
    pub reasoning: bool,
}

/// The LLM streaming contract. Implementors read request params and push
/// [`StreamEvent`]s onto `out` until the turn ends or cancel triggers.
#[async_trait]
pub trait LlmStream: Send + Sync {
    async fn stream_turn(
        &self,
        req: LlmRequest<'_>,
        out: mpsc::Sender<StreamEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), String>;
}

/// Scripted LLM for tests: each call to `stream_turn` replays the next
/// `Vec<StreamEvent>` in the queue. Panics if the queue is empty — tests
/// should pre-seed exactly as many scripts as the loop is expected to
/// iterate so an off-by-one fails loudly instead of hanging.
pub struct MockLlmStream {
    scripts: tokio::sync::Mutex<VecDeque<Vec<StreamEvent>>>,
    pub calls: std::sync::atomic::AtomicUsize,
    /// Captured messages arrays from each `stream_turn` call, in order.
    /// Lets tests verify that tool_result was correctly appended between
    /// turns.
    pub observed_inputs: tokio::sync::Mutex<Vec<Vec<Message>>>,
}

impl MockLlmStream {
    pub fn new(scripts: Vec<Vec<StreamEvent>>) -> Self {
        Self {
            scripts: tokio::sync::Mutex::new(VecDeque::from(scripts)),
            calls: std::sync::atomic::AtomicUsize::new(0),
            observed_inputs: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl LlmStream for MockLlmStream {
    async fn stream_turn(
        &self,
        req: LlmRequest<'_>,
        out: mpsc::Sender<StreamEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.observed_inputs
            .lock()
            .await
            .push(req.messages.to_vec());

        let events = {
            let mut scripts = self.scripts.lock().await;
            scripts
                .pop_front()
                .ok_or_else(|| "MockLlmStream: no more scripted turns".to_string())?
        };

        for ev in events {
            if cancel.is_cancelled() {
                return Err("cancelled".into());
            }
            if out.send(ev).await.is_err() {
                // Receiver dropped — treat as cancellation.
                return Ok(());
            }
        }
        Ok(())
    }
}

// ── Bedrock client ─────────────────────────────────────────────────────

/// Real [`LlmStream`] impl backed by `aws-sdk-bedrockruntime::converse_stream`.
///
/// Wires:
/// - `AWS_BEARER_TOKEN_BEDROCK` env var → picked up by `aws_config::load_from_env`
/// - `HTTPS_PROXY` env var → respected by default hyper http client
/// - Region from `cfg.region` or env `AWS_REGION`, defaults to us-east-1
///
/// Each `stream_turn` is a fresh HTTP request; the `CancellationToken` is
/// watched in the event-receive loop and drops the stream when triggered,
/// which immediately kills the HTTP connection (no jsonl residue).
pub struct BedrockClient {
    client: aws_sdk_bedrockruntime::Client,
}

impl BedrockClient {
    /// Build a client from an [`aws_config::SdkConfig`]. Callers typically
    /// get that from `init_sdk_config(region).await`.
    pub fn from_sdk_config(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: aws_sdk_bedrockruntime::Client::new(config),
        }
    }

    /// Convenience: build an SDK config with the given region, falling
    /// back to us-east-1 if None. Picks up `AWS_BEARER_TOKEN_BEDROCK`,
    /// `AWS_REGION`, `HTTPS_PROXY` from env.
    ///
    /// Installs a single rustls CryptoProvider on first call (aws-lc-rs).
    /// Required because the binary links multiple rustls-using crates
    /// (sqlx, aws-sdk), each of which would otherwise panic at first use
    /// with "Could not automatically determine the process-level
    /// CryptoProvider from Rustls crate features".
    pub async fn init_sdk_config(region: Option<&str>) -> aws_config::SdkConfig {
        install_default_crypto_provider();
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = region {
            loader = loader.region(aws_config::Region::new(r.to_string()));
        } else if std::env::var_os("AWS_REGION").is_none() {
            loader = loader.region(aws_config::Region::new("us-east-1".to_string()));
        }
        loader.load().await
    }
}

/// Install the aws-lc-rs rustls CryptoProvider exactly once across the
/// process. Call before any aws-sdk HTTPS request. Idempotent — the second
/// call is a no-op because `install_default()` returns Err on a double
/// install, which we intentionally ignore.
fn install_default_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[async_trait]
impl LlmStream for BedrockClient {
    async fn stream_turn(
        &self,
        req: LlmRequest<'_>,
        out: mpsc::Sender<StreamEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let bedrock_messages = messages_to_bedrock(req.messages)?;
        let tools_cfg = build_tool_config(req.tools)?;

        let mut builder = self
            .client
            .converse_stream()
            .model_id(req.model)
            .set_messages(Some(bedrock_messages))
            .inference_config(
                bedrock::InferenceConfiguration::builder()
                    .max_tokens(req.max_tokens as i32)
                    .build(),
            );

        if !req.system.is_empty() {
            builder = builder.system(bedrock::SystemContentBlock::Text(req.system.to_string()));
        }
        if let Some(cfg) = tools_cfg {
            builder = builder.tool_config(cfg);
        }

        let response = builder
            .send()
            .await
            .map_err(|e| format!("bedrock converse_stream send: {e:?}"))?;

        let mut stream = response.stream;

        // Tool_use blocks are streamed in pieces: ContentBlockStart carries
        // (name, id); subsequent ContentBlockDelta::ToolUse events carry
        // partial_json; ContentBlockStop finalizes it. Accumulate by block
        // index.
        let mut tool_accum: HashMap<i32, ToolAccum> = HashMap::new();
        // Reasoning blocks can fragment across deltas too; keep the latest
        // signature we see per block index so we can emit it on stop.
        let mut reasoning_accum: HashMap<i32, ReasoningAccum> = HashMap::new();

        loop {
            if cancel.is_cancelled() {
                return Err("cancelled".into());
            }
            let next = tokio::select! {
                _ = cancel.cancelled() => return Err("cancelled".into()),
                res = stream.recv() => res,
            };
            match next {
                Ok(Some(event)) => {
                    handle_bedrock_event(
                        event,
                        &out,
                        &mut tool_accum,
                        &mut reasoning_accum,
                    )
                    .await?;
                }
                Ok(None) => break, // stream ended naturally
                Err(e) => return Err(format!("bedrock stream error: {e:?}")),
            }
        }
        Ok(())
    }
}

struct ToolAccum {
    id: String,
    name: String,
    input_partial: String,
}

#[derive(Default)]
struct ReasoningAccum {
    text: String,
    signature: Option<String>,
}

async fn handle_bedrock_event(
    event: bedrock::ConverseStreamOutput,
    out: &mpsc::Sender<StreamEvent>,
    tool_accum: &mut HashMap<i32, ToolAccum>,
    reasoning_accum: &mut HashMap<i32, ReasoningAccum>,
) -> Result<(), String> {
    use bedrock::ContentBlockDelta as CBD;
    use bedrock::ContentBlockStart as CBS;
    use bedrock::ConverseStreamOutput as Ev;
    use bedrock::ReasoningContentBlockDelta as RCBD;

    match event {
        Ev::ContentBlockStart(s) => {
            let idx = s.content_block_index;
            if let Some(CBS::ToolUse(tu)) = s.start {
                tool_accum.insert(
                    idx,
                    ToolAccum {
                        id: tu.tool_use_id.clone(),
                        name: tu.name.clone(),
                        input_partial: String::new(),
                    },
                );
                let _ = out
                    .send(StreamEvent::ToolUseStart {
                        id: tu.tool_use_id,
                        name: tu.name,
                        params_preview: None,
                    })
                    .await;
            }
        }
        Ev::ContentBlockDelta(d) => {
            let idx = d.content_block_index;
            if let Some(delta) = d.delta {
                match delta {
                    CBD::Text(t) => {
                        let _ = out.send(StreamEvent::TextDelta(t)).await;
                    }
                    CBD::ToolUse(tu_delta) => {
                        if let Some(acc) = tool_accum.get_mut(&idx) {
                            acc.input_partial.push_str(&tu_delta.input);
                        }
                    }
                    CBD::ReasoningContent(rc_delta) => match rc_delta {
                        RCBD::Text(t) => {
                            reasoning_accum.entry(idx).or_default().text.push_str(&t);
                            let _ = out
                                .send(StreamEvent::ReasoningDelta {
                                    kind: ReasoningKind::Raw,
                                    text: t,
                                })
                                .await;
                        }
                        RCBD::Signature(sig) => {
                            reasoning_accum.entry(idx).or_default().signature = Some(sig);
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
        Ev::ContentBlockStop(stop) => {
            let idx = stop.content_block_index;
            if let Some(acc) = tool_accum.remove(&idx) {
                let input: serde_json::Value = if acc.input_partial.is_empty() {
                    serde_json::json!({})
                } else {
                    serde_json::from_str(&acc.input_partial).map_err(|e| {
                        format!(
                            "bedrock tool_use input parse failed: {e} (raw: {})",
                            acc.input_partial
                        )
                    })?
                };
                let _ = out
                    .send(StreamEvent::ToolUseComplete {
                        id: acc.id,
                        name: acc.name,
                        input,
                    })
                    .await;
            }
            if let Some(acc) = reasoning_accum.remove(&idx) {
                let _ = out
                    .send(StreamEvent::ThinkingBlock {
                        text: acc.text,
                        signature: acc.signature,
                    })
                    .await;
            }
        }
        Ev::MessageStop(stop) => {
            let stop_reason = map_stop_reason(&stop.stop_reason);
            let _ = out
                .send(StreamEvent::MessageStop {
                    stop_reason,
                    usage: Usage::default(),
                })
                .await;
        }
        Ev::Metadata(meta) => {
            // Metadata carries the real usage numbers but NOT a stop_reason.
            // We send a dedicated UsageUpdate event so loop_runner can add
            // the token counts without overwriting the stop_reason that
            // arrived via MessageStop. (Earlier we emitted a second
            // MessageStop with stop_reason=EndTurn here — that was a bug
            // because it overwrote a real `ToolUse` stop_reason and made
            // the loop skip tool execution, leaving orphaned tool_use
            // blocks in persisted history that Bedrock then rejected
            // on the next turn.)
            if let Some(u) = meta.usage {
                let _ = out
                    .send(StreamEvent::UsageUpdate(Usage {
                        input_tokens: u.input_tokens.max(0) as u32,
                        output_tokens: u.output_tokens.max(0) as u32,
                    }))
                    .await;
            }
        }
        _ => {}
    }
    Ok(())
}

fn map_stop_reason(r: &bedrock::StopReason) -> StopReason {
    match r {
        bedrock::StopReason::EndTurn => StopReason::EndTurn,
        bedrock::StopReason::ToolUse => StopReason::ToolUse,
        bedrock::StopReason::MaxTokens => StopReason::MaxTokens,
        _ => StopReason::Other,
    }
}

// ── Message ↔ Bedrock conversion ───────────────────────────────────────

fn messages_to_bedrock(msgs: &[Message]) -> Result<Vec<bedrock::Message>, String> {
    msgs.iter().map(message_to_bedrock).collect()
}

fn message_to_bedrock(msg: &Message) -> Result<bedrock::Message, String> {
    let role = match msg.role {
        Role::User => bedrock::ConversationRole::User,
        Role::Assistant => bedrock::ConversationRole::Assistant,
    };
    let content: Vec<bedrock::ContentBlock> = msg
        .content
        .iter()
        .map(content_block_to_bedrock)
        .collect::<Result<_, _>>()?;
    bedrock::Message::builder()
        .role(role)
        .set_content(Some(content))
        .build()
        .map_err(|e| format!("build bedrock message: {e}"))
}

fn content_block_to_bedrock(block: &ContentBlock) -> Result<bedrock::ContentBlock, String> {
    Ok(match block {
        ContentBlock::Text { text } => bedrock::ContentBlock::Text(text.clone()),
        ContentBlock::ToolUse { id, name, input } => {
            let doc = json_to_document(input);
            bedrock::ContentBlock::ToolUse(
                bedrock::ToolUseBlock::builder()
                    .tool_use_id(id)
                    .name(name)
                    .input(doc)
                    .build()
                    .map_err(|e| format!("build tool_use: {e}"))?,
            )
        }
        ContentBlock::ToolResult {
            tool_use_id,
            content,
            is_error,
        } => bedrock::ContentBlock::ToolResult(
            bedrock::ToolResultBlock::builder()
                .tool_use_id(tool_use_id)
                .content(bedrock::ToolResultContentBlock::Text(content.clone()))
                .status(if *is_error {
                    bedrock::ToolResultStatus::Error
                } else {
                    bedrock::ToolResultStatus::Success
                })
                .build()
                .map_err(|e| format!("build tool_result: {e}"))?,
        ),
        ContentBlock::Thinking { text, signature } => bedrock::ContentBlock::ReasoningContent(
            bedrock::ReasoningContentBlock::ReasoningText({
                let mut builder = bedrock::ReasoningTextBlock::builder().text(text);
                if let Some(s) = signature {
                    builder = builder.signature(s);
                }
                builder
                    .build()
                    .map_err(|e| format!("build reasoning_text: {e}"))?
            }),
        ),
    })
}

fn build_tool_config(
    specs: &[ToolSpec],
) -> Result<Option<bedrock::ToolConfiguration>, String> {
    if specs.is_empty() {
        return Ok(None);
    }
    let mut tools: Vec<bedrock::Tool> = Vec::with_capacity(specs.len());
    for s in specs {
        let tool_spec = bedrock::ToolSpecification::builder()
            .name(&s.name)
            .description(&s.description)
            .input_schema(bedrock::ToolInputSchema::Json(json_to_document(
                &s.input_schema,
            )))
            .build()
            .map_err(|e| format!("build tool spec: {e}"))?;
        tools.push(bedrock::Tool::ToolSpec(tool_spec));
    }
    Ok(Some(
        bedrock::ToolConfiguration::builder()
            .set_tools(Some(tools))
            .build()
            .map_err(|e| format!("build tool_config: {e}"))?,
    ))
}

/// Convert `serde_json::Value` → `aws_smithy_types::Document`.
fn json_to_document(v: &serde_json::Value) -> Document {
    match v {
        serde_json::Value::Null => Document::Null,
        serde_json::Value::Bool(b) => Document::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Document::Number(aws_smithy_types::Number::NegInt(i))
            } else if let Some(u) = n.as_u64() {
                Document::Number(aws_smithy_types::Number::PosInt(u))
            } else if let Some(f) = n.as_f64() {
                Document::Number(aws_smithy_types::Number::Float(f))
            } else {
                Document::Null
            }
        }
        serde_json::Value::String(s) => Document::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Document::Array(arr.iter().map(json_to_document).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut map: HashMap<String, Document> = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_document(v));
            }
            Document::Object(map)
        }
    }
}

#[cfg(test)]
mod bedrock_conv_tests {
    use super::*;

    #[test]
    fn json_to_doc_preserves_types() {
        let v = serde_json::json!({
            "s": "x",
            "n": 42,
            "f": 1.5,
            "b": true,
            "arr": [1, 2, 3],
            "null": null,
        });
        let d = json_to_document(&v);
        match d {
            Document::Object(m) => {
                assert!(matches!(m.get("s"), Some(Document::String(s)) if s == "x"));
                // serde_json tries as_i64 first, which succeeds for 42,
                // so we get NegInt (signed) rather than PosInt (unsigned).
                assert!(matches!(
                    m.get("n"),
                    Some(Document::Number(aws_smithy_types::Number::NegInt(42)))
                ));
                assert!(matches!(m.get("b"), Some(Document::Bool(true))));
                assert!(matches!(m.get("null"), Some(Document::Null)));
                assert!(matches!(m.get("arr"), Some(Document::Array(a)) if a.len() == 3));
            }
            other => panic!("expected Object, got {other:?}"),
        }
    }

    #[test]
    fn message_round_trip_text_only() {
        let m = Message::user_text("hello");
        let b = message_to_bedrock(&m).unwrap();
        assert_eq!(b.role, bedrock::ConversationRole::User);
        assert_eq!(b.content.len(), 1);
        match &b.content[0] {
            bedrock::ContentBlock::Text(t) => assert_eq!(t, "hello"),
            other => panic!("expected text, got {other:?}"),
        }
    }

    #[test]
    fn tool_use_block_converts() {
        let m = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: "t1".into(),
                name: "bash".into(),
                input: serde_json::json!({"command": "ls"}),
            }],
        };
        let b = message_to_bedrock(&m).unwrap();
        match &b.content[0] {
            bedrock::ContentBlock::ToolUse(tu) => {
                assert_eq!(tu.tool_use_id, "t1");
                assert_eq!(tu.name, "bash");
            }
            other => panic!("expected tool_use, got {other:?}"),
        }
    }

    #[test]
    fn tool_result_error_sets_status() {
        let m = Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: "t1".into(),
                content: "failed".into(),
                is_error: true,
            }],
        };
        let b = message_to_bedrock(&m).unwrap();
        match &b.content[0] {
            bedrock::ContentBlock::ToolResult(tr) => {
                assert_eq!(tr.tool_use_id, "t1");
                assert_eq!(tr.status, Some(bedrock::ToolResultStatus::Error));
            }
            other => panic!("expected tool_result, got {other:?}"),
        }
    }

    #[test]
    fn thinking_without_signature_converts() {
        let m = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Thinking {
                text: "ponder".into(),
                signature: None,
            }],
        };
        let b = message_to_bedrock(&m).unwrap();
        match &b.content[0] {
            bedrock::ContentBlock::ReasoningContent(rc) => match rc {
                bedrock::ReasoningContentBlock::ReasoningText(rt) => {
                    assert_eq!(rt.text, "ponder");
                    assert!(rt.signature.is_none());
                }
                other => panic!("expected reasoning_text, got {other:?}"),
            },
            other => panic!("expected reasoning_content, got {other:?}"),
        }
    }

    #[test]
    fn tool_config_builds_when_specs_present() {
        let spec = ToolSpec {
            name: "bash".into(),
            description: "run".into(),
            input_schema: serde_json::json!({"type":"object"}),
        };
        let cfg = build_tool_config(&[spec]).unwrap().unwrap();
        assert_eq!(cfg.tools.len(), 1);
    }

    #[test]
    fn tool_config_empty_returns_none() {
        assert!(build_tool_config(&[]).unwrap().is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_replays_scripts_in_order() {
        let mock = MockLlmStream::new(vec![
            vec![
                StreamEvent::TextDelta("hello ".into()),
                StreamEvent::TextDelta("world".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                    usage: Usage {
                        input_tokens: 10,
                        output_tokens: 2,
                    },
                },
            ],
            vec![StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: Usage::default(),
            }],
        ]);

        let (tx, mut rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let tools: Vec<ToolSpec> = Vec::new();
        let messages = vec![Message::user_text("hi")];
        let req = LlmRequest {
            system: "",
            tools: &tools,
            messages: &messages,
            model: "m",
            max_tokens: 100,
            reasoning: false,
        };
        mock.stream_turn(req, tx, &cancel).await.unwrap();
        drop(mock);

        let mut collected = Vec::new();
        while let Some(ev) = rx.recv().await {
            collected.push(ev);
        }
        assert_eq!(collected.len(), 3);
    }

    #[tokio::test]
    async fn mock_errors_when_out_of_scripts() {
        let mock = MockLlmStream::new(vec![]);
        let (tx, _rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let msgs: Vec<Message> = Vec::new();
        let tools: Vec<ToolSpec> = Vec::new();
        let req = LlmRequest {
            system: "",
            tools: &tools,
            messages: &msgs,
            model: "m",
            max_tokens: 10,
            reasoning: false,
        };
        let err = mock.stream_turn(req, tx, &cancel).await.unwrap_err();
        assert!(err.contains("no more scripted"));
    }

    #[tokio::test]
    async fn mock_observes_input_messages() {
        let mock = MockLlmStream::new(vec![vec![StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }]]);
        let (tx, _rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let tools: Vec<ToolSpec> = Vec::new();
        let msgs = vec![Message::user_text("probe")];
        let req = LlmRequest {
            system: "",
            tools: &tools,
            messages: &msgs,
            model: "m",
            max_tokens: 10,
            reasoning: false,
        };
        mock.stream_turn(req, tx, &cancel).await.unwrap();
        let obs = mock.observed_inputs.lock().await;
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0], msgs);
    }
}
