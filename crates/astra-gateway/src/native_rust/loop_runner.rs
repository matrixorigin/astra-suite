//! Tool-use loop.
//!
//! Drives the core cycle that the NativeRust runtime exists for:
//!
//! 1. Stream one turn from the [`LlmStream`], accumulating text,
//!    reasoning, thinking, and tool_use blocks as [`StreamEvent`]s.
//! 2. Forward the streamable pieces (text, reasoning, tool start/done) to
//!    `progress_tx` as [`CliProgress`] so `runner.rs` can flush them to
//!    WeCom exactly the way it does for the subprocess CLIs.
//! 3. On `MessageStop { stop_reason: ToolUse, .. }`, execute each `tool_use`
//!    block with [`tools::bash::run_bash`], build a matching user message
//!    carrying `tool_result` blocks, and loop back to step 1.
//! 4. On `EndTurn` / `MaxTokens` / `Other`, return the accumulated output.
//! 5. Capped by `max_iters` to protect against a model that refuses to
//!    stop requesting tools.
//!
//! Cancellation: any `CliProgress` send failure or a triggered
//! `CancellationToken` terminates the loop. The partial assistant content
//! already streamed out stays with the user (gateway's progressive-send
//! path flushes `Token` events as they arrive). The assistant turn is
//! never persisted on cancel so the next request doesn't see a truncated
//! reply in the history.

use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::cli_bridge::CliProgress;

use super::client::{LlmRequest, LlmStream, StopReason, StreamEvent};
use super::history::{ContentBlock, Message, Role};
use super::tools::bash::{self, BashInput};
use super::tools::{BASH_TOOL_NAME, ToolSpec};

/// Result of a completed tool-use loop.
#[derive(Debug)]
pub struct LoopOutcome {
    /// Final visible assistant text (concatenation of all text deltas from
    /// the last turn that didn't request a tool).
    pub final_text: String,
    /// Updated message history, ready to persist. Includes the original
    /// user message, all assistant turns, and all tool_result user turns.
    pub history: Vec<Message>,
    /// Total input tokens reported by the model across all turns.
    pub tokens_prompt: u64,
    /// Total output tokens reported.
    pub tokens_completion: u64,
    /// Names of tools invoked (in invocation order, duplicates preserved).
    pub tools_used: Vec<String>,
    /// How many turns the loop ran through.
    pub iterations: u32,
    /// True iff the loop bailed out because it hit `max_iters`.
    pub stopped_by_max_iters: bool,
}

/// Parameters for [`run_agent_loop`]. Grouped to avoid
/// `too_many_arguments` clippy lint.
pub struct LoopConfig<'a> {
    pub system: &'a str,
    pub tools: &'a [ToolSpec],
    pub model: &'a str,
    pub max_tokens: u32,
    pub reasoning: bool,
    pub max_iters: u32,
    pub bash_timeout: Duration,
    pub bash_max_bytes: usize,
    /// Passed to bash tool `current_dir`. `None` = inherit gateway cwd.
    pub bash_working_dir: Option<&'a std::path::Path>,
}

/// Run the tool-use loop until the model ends the turn or a limit hits.
///
/// `initial_history` is the messages array BEFORE appending the user
/// prompt for this request; `user_message` is appended internally so the
/// caller doesn't have to.
pub async fn run_agent_loop(
    llm: &dyn LlmStream,
    cfg: &LoopConfig<'_>,
    mut history: Vec<Message>,
    user_message: String,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    cancel: &CancellationToken,
) -> Result<LoopOutcome, String> {
    history.push(Message::user_text(user_message));

    let mut tokens_prompt: u64 = 0;
    let mut tokens_completion: u64 = 0;
    let mut tools_used: Vec<String> = Vec::new();
    let mut final_text = String::new();
    let mut iterations: u32 = 0;
    let mut stopped_by_max_iters = false;

    loop {
        if iterations >= cfg.max_iters {
            stopped_by_max_iters = true;
            // Give the user something instead of silent truncation.
            final_text.push_str(&format!(
                "\n\n⚠️ tool-use loop hit max_iters={} — stopping",
                cfg.max_iters
            ));
            break;
        }
        iterations += 1;

        if cancel.is_cancelled() {
            return Err("cancelled".into());
        }

        // Stream one turn. Scoped in a block so `llm_fut`'s `&history`
        // borrow is dropped before we mutate history afterwards.
        let mut turn_text = String::new();
        let mut turn_tool_uses: Vec<(String, String, serde_json::Value)> = Vec::new();
        let mut turn_thinking: Vec<(String, Option<String>)> = Vec::new();
        let mut turn_stop_reason: Option<StopReason> = None;
        let mut turn_usage = super::client::Usage::default();

        {
            let (ev_tx, mut ev_rx) = mpsc::channel::<StreamEvent>(64);
            let llm_fut = llm.stream_turn(
                LlmRequest {
                    system: cfg.system,
                    tools: cfg.tools,
                    messages: &history,
                    model: cfg.model,
                    max_tokens: cfg.max_tokens,
                    reasoning: cfg.reasoning,
                },
                ev_tx,
                cancel,
            );
            tokio::pin!(llm_fut);
            let mut llm_done = false;

            loop {
                tokio::select! {
                    res = &mut llm_fut, if !llm_done => {
                        res?; // propagate llm error (including cancel)
                        llm_done = true;
                    }
                    Some(ev) = ev_rx.recv() => {
                        handle_event(
                            ev,
                            &mut turn_text,
                            &mut turn_tool_uses,
                            &mut turn_thinking,
                            &mut turn_stop_reason,
                            &mut turn_usage,
                            &progress_tx,
                        )
                        .await;
                    }
                    else => break,
                }
            }
        }

        tokens_prompt += turn_usage.input_tokens as u64;
        tokens_completion += turn_usage.output_tokens as u64;

        // Build the assistant message and append it to history.
        let mut assistant_content: Vec<ContentBlock> = Vec::new();
        for (text, sig) in &turn_thinking {
            assistant_content.push(ContentBlock::Thinking {
                text: text.clone(),
                signature: sig.clone(),
            });
        }
        if !turn_text.is_empty() {
            assistant_content.push(ContentBlock::Text {
                text: turn_text.clone(),
            });
        }
        for (id, name, input) in &turn_tool_uses {
            assistant_content.push(ContentBlock::ToolUse {
                id: id.clone(),
                name: name.clone(),
                input: input.clone(),
            });
        }

        // Edge case: empty assistant turn (e.g. model sent only a stop).
        // Still push it so history stays well-formed for the next turn.
        if assistant_content.is_empty() {
            assistant_content.push(ContentBlock::Text {
                text: String::new(),
            });
        }

        history.push(Message {
            role: Role::Assistant,
            content: assistant_content,
        });

        match turn_stop_reason {
            Some(StopReason::ToolUse) => {
                // Execute each tool_use serially and append tool_results.
                let mut tool_results: Vec<ContentBlock> = Vec::new();
                for (id, name, input) in &turn_tool_uses {
                    tools_used.push(name.clone());
                    let started = Instant::now();

                    let (content, is_error) = if name == BASH_TOOL_NAME {
                        match serde_json::from_value::<BashInput>(input.clone()) {
                            Ok(bi) => {
                                let out = bash::run_bash(
                                    &bi,
                                    cfg.bash_working_dir,
                                    cfg.bash_timeout,
                                    cfg.bash_max_bytes,
                                    cancel,
                                )
                                .await;
                                match out {
                                    Ok(o) => (o.to_tool_result_content(), o.is_error()),
                                    Err(e) => (format!("bash runtime error: {e}"), true),
                                }
                            }
                            Err(e) => (
                                format!("invalid bash input: {e}"),
                                true,
                            ),
                        }
                    } else {
                        (
                            format!("unknown tool: {name}"),
                            true,
                        )
                    };

                    let duration_ms = started.elapsed().as_millis() as u64;
                    if let Some(tx) = &progress_tx {
                        let _ = tx
                            .send(CliProgress::ToolDone {
                                name: name.clone(),
                                duration_ms: Some(duration_ms),
                            })
                            .await;
                    }

                    tool_results.push(ContentBlock::ToolResult {
                        tool_use_id: id.clone(),
                        content,
                        is_error,
                    });
                }
                // Anthropic requires tool_result to come as the next user
                // message. Pack all results into one message regardless of
                // how many tool_uses the assistant emitted.
                history.push(Message {
                    role: Role::User,
                    content: tool_results,
                });
                // Loop back — model needs to see the results.
                continue;
            }
            Some(StopReason::EndTurn) | Some(StopReason::MaxTokens) | Some(StopReason::Other)
            | None => {
                final_text = turn_text;
                break;
            }
        }
    }

    Ok(LoopOutcome {
        final_text,
        history,
        tokens_prompt,
        tokens_completion,
        tools_used,
        iterations,
        stopped_by_max_iters,
    })
}

async fn handle_event(
    ev: StreamEvent,
    turn_text: &mut String,
    turn_tool_uses: &mut Vec<(String, String, serde_json::Value)>,
    turn_thinking: &mut Vec<(String, Option<String>)>,
    turn_stop_reason: &mut Option<StopReason>,
    turn_usage: &mut super::client::Usage,
    progress_tx: &Option<mpsc::Sender<CliProgress>>,
) {
    match ev {
        StreamEvent::TextDelta(t) => {
            turn_text.push_str(&t);
            if let Some(tx) = progress_tx {
                let _ = tx.send(CliProgress::Token(t)).await;
            }
        }
        StreamEvent::ReasoningDelta { kind, text } => {
            if let Some(tx) = progress_tx {
                let _ = tx.send(CliProgress::ReasoningBlock { kind, text }).await;
            }
        }
        StreamEvent::ThinkingBlock { text, signature } => {
            turn_thinking.push((text, signature));
        }
        StreamEvent::ToolUseStart {
            name,
            params_preview,
            ..
        } => {
            if let Some(tx) = progress_tx {
                let _ = tx
                    .send(CliProgress::ToolStarted {
                        name,
                        params: params_preview,
                    })
                    .await;
            }
        }
        StreamEvent::ToolUseComplete { id, name, input } => {
            turn_tool_uses.push((id, name, input));
        }
        StreamEvent::MessageStop { stop_reason, usage } => {
            // Only set stop_reason the first time — never overwrite a real
            // ToolUse with a later EndTurn/Other (prior bug: Metadata event
            // was overwriting and orphaning tool_use blocks).
            if turn_stop_reason.is_none() {
                *turn_stop_reason = Some(stop_reason);
            }
            // usage is 0 on MessageStop (Bedrock sends real numbers on
            // Metadata). Keep whatever's already there if non-zero.
            if turn_usage.input_tokens == 0 && turn_usage.output_tokens == 0 {
                *turn_usage = usage;
            }
        }
        StreamEvent::UsageUpdate(usage) => {
            // Metadata came through; replace the zero placeholder.
            *turn_usage = usage;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_rust::client::{MockLlmStream, Usage};

    fn usage(prompt: u32, completion: u32) -> super::super::client::Usage {
        Usage {
            input_tokens: prompt,
            output_tokens: completion,
        }
    }

    fn default_cfg<'a>() -> LoopConfig<'a> {
        LoopConfig {
            system: "sys",
            tools: &[],
            model: "test-model",
            max_tokens: 1000,
            reasoning: false,
            max_iters: 5,
            bash_timeout: Duration::from_secs(5),
            bash_max_bytes: 32 * 1024,
            bash_working_dir: None,
        }
    }

    #[tokio::test]
    async fn single_turn_text_only() {
        let mock = MockLlmStream::new(vec![vec![
            StreamEvent::TextDelta("hello ".into()),
            StreamEvent::TextDelta("world".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: usage(10, 5),
            },
        ]]);
        let cancel = CancellationToken::new();
        let out = run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "hi".into(),
            None,
            &cancel,
        )
        .await
        .unwrap();

        assert_eq!(out.final_text, "hello world");
        assert_eq!(out.iterations, 1);
        assert_eq!(out.tokens_prompt, 10);
        assert_eq!(out.tokens_completion, 5);
        assert!(out.tools_used.is_empty());
        assert!(!out.stopped_by_max_iters);
        // history: user(hi), assistant(text)
        assert_eq!(out.history.len(), 2);
        assert_eq!(out.history[0].role, Role::User);
        assert_eq!(out.history[1].role, Role::Assistant);
    }

    #[tokio::test]
    async fn tool_use_roundtrip_injects_tool_result() {
        // Turn 1: tool_use(bash "echo x")
        // Turn 2: text reply using the result
        let mock = MockLlmStream::new(vec![
            vec![
                StreamEvent::ToolUseStart {
                    id: "toolu_1".into(),
                    name: BASH_TOOL_NAME.into(),
                    params_preview: Some("echo x".into()),
                },
                StreamEvent::ToolUseComplete {
                    id: "toolu_1".into(),
                    name: BASH_TOOL_NAME.into(),
                    input: serde_json::json!({"command": "echo hi_from_bash"}),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                    usage: usage(20, 8),
                },
            ],
            vec![
                StreamEvent::TextDelta("saw the output".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                    usage: usage(30, 3),
                },
            ],
        ]);

        let cancel = CancellationToken::new();
        let out = run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "run echo".into(),
            None,
            &cancel,
        )
        .await
        .unwrap();

        assert_eq!(out.final_text, "saw the output");
        assert_eq!(out.iterations, 2);
        assert_eq!(out.tokens_prompt, 50);
        assert_eq!(out.tokens_completion, 11);
        assert_eq!(out.tools_used, vec![BASH_TOOL_NAME.to_string()]);
        // history: user, assistant(tool_use), user(tool_result), assistant(text)
        assert_eq!(out.history.len(), 4);
        assert_eq!(out.history[1].role, Role::Assistant);
        match &out.history[1].content[0] {
            ContentBlock::ToolUse { name, .. } => assert_eq!(name, BASH_TOOL_NAME),
            other => panic!("expected ToolUse, got {other:?}"),
        }
        assert_eq!(out.history[2].role, Role::User);
        match &out.history[2].content[0] {
            ContentBlock::ToolResult {
                content, is_error, ..
            } => {
                assert!(!is_error);
                assert!(
                    content.contains("hi_from_bash"),
                    "tool_result content should embed bash stdout; got: {content}"
                );
            }
            other => panic!("expected ToolResult, got {other:?}"),
        }

        // Verify mock's second call saw the injected tool_result.
        let observed = mock.observed_inputs.lock().await;
        assert_eq!(observed.len(), 2);
        let second_call_msgs = &observed[1];
        // [user, assistant(tool_use), user(tool_result)]
        assert_eq!(second_call_msgs.len(), 3);
        assert!(matches!(
            second_call_msgs[2].content[0],
            ContentBlock::ToolResult { .. }
        ));
    }

    #[tokio::test]
    async fn max_iters_bail_out() {
        // Mock always requests a tool, so the loop would run forever.
        // Script enough turns to exceed max_iters, then run the loop with
        // a small cap.
        let turn = || {
            vec![
                StreamEvent::ToolUseStart {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    params_preview: None,
                },
                StreamEvent::ToolUseComplete {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    input: serde_json::json!({"command": "echo loop"}),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                    usage: usage(1, 1),
                },
            ]
        };
        let mock = MockLlmStream::new(vec![turn(), turn(), turn(), turn(), turn()]);

        let cancel = CancellationToken::new();
        let cfg = LoopConfig {
            max_iters: 3,
            ..default_cfg()
        };
        let out = run_agent_loop(&mock, &cfg, Vec::new(), "loop".into(), None, &cancel)
            .await
            .unwrap();

        assert!(out.stopped_by_max_iters);
        assert_eq!(out.iterations, 3);
        assert!(out.final_text.contains("max_iters=3"));
    }

    #[tokio::test]
    async fn progress_tx_receives_token_and_tool_events() {
        let mock = MockLlmStream::new(vec![
            vec![
                StreamEvent::TextDelta("before tool. ".into()),
                StreamEvent::ToolUseStart {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    params_preview: Some("echo x".into()),
                },
                StreamEvent::ToolUseComplete {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    input: serde_json::json!({"command": "echo x"}),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                    usage: usage(1, 1),
                },
            ],
            vec![
                StreamEvent::TextDelta("after tool.".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                    usage: usage(1, 1),
                },
            ],
        ]);

        let (tx, mut rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();
        let _ = run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "hi".into(),
            Some(tx),
            &cancel,
        )
        .await
        .unwrap();

        let mut tokens = Vec::new();
        let mut tool_started = 0;
        let mut tool_done = 0;
        while let Some(ev) = rx.recv().await {
            match ev {
                CliProgress::Token(t) => tokens.push(t),
                CliProgress::ToolStarted { .. } => tool_started += 1,
                CliProgress::ToolDone { .. } => tool_done += 1,
                _ => {}
            }
        }
        assert_eq!(tokens.join(""), "before tool. after tool.");
        assert_eq!(tool_started, 1);
        assert_eq!(tool_done, 1);
    }

    #[tokio::test]
    async fn metadata_usage_update_does_not_overwrite_stop_reason() {
        // Regression: Bedrock emits MessageStop first (with real stop_reason,
        // usage=0) then Metadata (with real usage). Earlier we translated
        // Metadata into a second MessageStop with stop_reason=EndTurn,
        // which overwrote a legitimate ToolUse stop_reason and made the
        // loop skip tool execution — leaving orphaned tool_use blocks in
        // persisted history that Bedrock then rejected on the next turn
        // with ValidationException "tool_use ... without tool_result".
        let mock = MockLlmStream::new(vec![
            vec![
                StreamEvent::ToolUseStart {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    params_preview: None,
                },
                StreamEvent::ToolUseComplete {
                    id: "t".into(),
                    name: BASH_TOOL_NAME.into(),
                    input: serde_json::json!({"command": "echo hi"}),
                },
                StreamEvent::MessageStop {
                    stop_reason: StopReason::ToolUse,
                    usage: Usage::default(),
                },
                // Metadata arrives after MessageStop.
                StreamEvent::UsageUpdate(Usage {
                    input_tokens: 100,
                    output_tokens: 30,
                }),
            ],
            vec![
                StreamEvent::TextDelta("done".into()),
                StreamEvent::MessageStop {
                    stop_reason: StopReason::EndTurn,
                    usage: Usage::default(),
                },
                StreamEvent::UsageUpdate(Usage {
                    input_tokens: 50,
                    output_tokens: 5,
                }),
            ],
        ]);

        let cancel = CancellationToken::new();
        let out = run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "run it".into(),
            None,
            &cancel,
        )
        .await
        .unwrap();

        // Critical: the tool must have been executed — i.e. the loop saw
        // ToolUse and ran a follow-up turn. If the bug recurs, tools_used
        // is empty and history has only 2 messages.
        assert_eq!(out.tools_used, vec![BASH_TOOL_NAME.to_string()]);
        assert_eq!(out.iterations, 2);
        // history: user, assistant(tool_use), user(tool_result), assistant(text)
        assert_eq!(out.history.len(), 4);
        assert!(matches!(
            out.history[2].content[0],
            crate::native_rust::history::ContentBlock::ToolResult { .. }
        ));
        // Usage reflects the Metadata values (not the zero placeholder).
        assert_eq!(out.tokens_prompt, 150);
        assert_eq!(out.tokens_completion, 35);
    }

    #[tokio::test]
    async fn cancel_before_first_turn_errors_out() {
        let mock = MockLlmStream::new(vec![vec![StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }]]);
        let cancel = CancellationToken::new();
        cancel.cancel();

        let err = run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "hi".into(),
            None,
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(err.contains("cancel"), "{err}");
    }

    #[tokio::test]
    async fn reasoning_delta_forwards_to_progress() {
        use crate::cli_bridge::ReasoningKind;
        let mock = MockLlmStream::new(vec![vec![
            StreamEvent::ReasoningDelta {
                kind: ReasoningKind::Raw,
                text: "pondering".into(),
            },
            StreamEvent::TextDelta("done".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: Usage::default(),
            },
        ]]);
        let (tx, mut rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        run_agent_loop(
            &mock,
            &default_cfg(),
            Vec::new(),
            "hi".into(),
            Some(tx),
            &cancel,
        )
        .await
        .unwrap();

        let mut saw_reasoning = false;
        while let Some(ev) = rx.recv().await {
            if matches!(ev, CliProgress::ReasoningBlock { .. }) {
                saw_reasoning = true;
            }
        }
        assert!(saw_reasoning);
    }
}
