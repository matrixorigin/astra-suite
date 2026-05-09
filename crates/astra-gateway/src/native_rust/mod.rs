//! Rust-native Claude agent runtime.
//!
//! Replaces the `exec claude -p --resume <sid>` subprocess path with an
//! in-process tool-use loop that talks directly to Bedrock Converse streaming
//! API. Abort via `CancellationToken` drops the HTTP stream cleanly — no
//! SIGKILL, no residual `~/.claude/projects/.../<sid>.jsonl` pollution.
//!
//! Wiring overview:
//!
//! ```text
//! runner.rs:handle_message_inner
//!   │
//!   ├── profile == NativeRust ──► native_rust::run_native_rust
//!   │                              │
//!   │                              ├── history::load_history (sqlite)
//!   │                              ├── loop_runner::run_agent_loop
//!   │                              │    ├── client::LlmStream::stream_turn
//!   │                              │    └── tools::bash::run_bash (per tool_use)
//!   │                              └── history::save_history (sqlite)
//!   │
//!   └── otherwise ──► cli_bridge::run_cli_with_cancel (subprocess)
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::cli_bridge::{CliProfile, CliProgress, CliResult};
use crate::store::GatewayStore;

pub mod client;
pub mod history;
pub mod loop_runner;
pub mod tools;

use client::LlmStream;
use history::{load_history, save_history};
use loop_runner::{LoopConfig, run_agent_loop};
use tools::bash_spec;

/// Identifies a conversation row in `gw_session_messages`.
#[derive(Debug, Clone)]
pub struct SessionKey {
    pub platform: String,
    pub chat_id: String,
    pub cli_profile: String,
}

/// NativeRust-only config slice extracted from [`CliProfile::NativeRust`].
#[derive(Debug, Clone)]
pub struct NativeRustConfig {
    pub model: String,
    pub max_iters: u32,
    pub max_tokens: u32,
    pub reasoning: bool,
    pub region: Option<String>,
    pub bash_timeout_ms: u64,
    pub bash_max_bytes: usize,
}

impl NativeRustConfig {
    /// Extract config from a [`CliProfile::NativeRust`] variant.
    ///
    /// # Panics
    /// Panics if `profile` is any other variant — callers must dispatch on
    /// profile type before calling this.
    pub fn from_profile(profile: &CliProfile) -> Self {
        match profile {
            CliProfile::NativeRust {
                model,
                max_iters,
                max_tokens,
                reasoning,
                region,
                bash_timeout_ms,
                bash_max_bytes,
            } => Self {
                model: model.clone(),
                max_iters: *max_iters,
                max_tokens: *max_tokens,
                reasoning: *reasoning,
                region: region.clone(),
                bash_timeout_ms: *bash_timeout_ms,
                bash_max_bytes: *bash_max_bytes,
            },
            _ => panic!("NativeRustConfig::from_profile requires CliProfile::NativeRust"),
        }
    }
}

/// Entry point dispatched by runner for `CliProfile::NativeRust`.
///
/// The parameter list mirrors [`crate::cli_bridge::run_cli_with_cancel`] plus
/// a store reference and a session key so the loop can persist message
/// history in `gw_session_messages`.
///
/// Generic in [`LlmStream`] so unit tests can inject [`client::MockLlmStream`]
/// without opening a real Bedrock connection; the runner instantiates a
/// real Bedrock client in checkpoint 5.
#[allow(clippy::too_many_arguments)]
pub async fn run_native_rust(
    cfg: &NativeRustConfig,
    llm: &dyn LlmStream,
    store: Arc<dyn GatewayStore>,
    session_key: SessionKey,
    session_id: &str,
    message: &str,
    working_dir: Option<&Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    system_prompt: Option<&str>,
    cancel: Option<CancellationToken>,
) -> Result<CliResult, String> {
    let cancel = cancel.unwrap_or_default();

    let history = load_history(&*store, &session_key, session_id)
        .await
        .map_err(|e| format!("load_history: {e}"))?
        .unwrap_or_default();

    let tools = vec![bash_spec()];
    let system = system_prompt.unwrap_or("");

    let loop_cfg = LoopConfig {
        system,
        tools: &tools,
        model: &cfg.model,
        max_tokens: cfg.max_tokens,
        reasoning: cfg.reasoning,
        max_iters: cfg.max_iters,
        bash_timeout: Duration::from_millis(cfg.bash_timeout_ms),
        bash_max_bytes: cfg.bash_max_bytes,
        bash_working_dir: working_dir,
    };

    let outcome =
        run_agent_loop(llm, &loop_cfg, history, message.to_string(), progress_tx, &cancel).await?;

    // Persist history only on success. Cancelled runs return an `Err`
    // above and skip this point so aborted partial turns never reach
    // the store.
    save_history(&*store, &session_key, session_id, &outcome.history)
        .await
        .map_err(|e| format!("save_history: {e}"))?;

    let text = if outcome.final_text.is_empty() {
        "(no reply)".to_string()
    } else {
        outcome.final_text.clone()
    };

    Ok(CliResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code: 0,
        success: true,
        error_kind: None,
        trace_id: None,
        request_id: None,
        run_id: None,
        session_id: Some(session_id.to_string()),
        text: Some(text),
        tool_calls_count: Some(outcome.tools_used.len() as u32),
        tools_used: outcome.tools_used,
        tokens_prompt: Some(outcome.tokens_prompt),
        tokens_completion: Some(outcome.tokens_completion),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use client::{MockLlmStream, StopReason, StreamEvent, Usage};

    async fn make_store() -> Arc<dyn GatewayStore> {
        use crate::store::sqlite::SqliteGatewayStore;
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store = SqliteGatewayStore::new(pool);
        store.ensure_schema().await.unwrap();
        Arc::new(store)
    }

    fn test_cfg() -> NativeRustConfig {
        NativeRustConfig {
            model: "us.anthropic.claude-opus-4-7".into(),
            max_iters: 5,
            max_tokens: 1000,
            reasoning: false,
            region: None,
            bash_timeout_ms: 5000,
            bash_max_bytes: 32 * 1024,
        }
    }

    #[tokio::test]
    async fn end_to_end_persists_and_returns_text() {
        let store = make_store().await;
        let key = SessionKey {
            platform: "test".into(),
            chat_id: "chat_e2e".into(),
            cli_profile: "claude-direct".into(),
        };
        let sid = "sid-e2e";

        let mock = MockLlmStream::new(vec![vec![
            StreamEvent::TextDelta("hello ".into()),
            StreamEvent::TextDelta("from native".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: Usage {
                    input_tokens: 12,
                    output_tokens: 3,
                },
            },
        ]]);

        let cancel = CancellationToken::new();
        let result = run_native_rust(
            &test_cfg(),
            &mock,
            store.clone(),
            key.clone(),
            sid,
            "hi",
            None,
            None,
            Some("you are helpful"),
            Some(cancel),
        )
        .await
        .unwrap();

        assert!(result.success);
        assert_eq!(result.text.as_deref(), Some("hello from native"));
        assert_eq!(result.tokens_prompt, Some(12));
        assert_eq!(result.tokens_completion, Some(3));
        assert_eq!(result.session_id.as_deref(), Some(sid));

        // History landed in the store.
        let loaded = load_history(&*store, &key, sid).await.unwrap().unwrap();
        assert_eq!(loaded.len(), 2); // user + assistant
    }

    #[tokio::test]
    async fn resumes_from_persisted_history() {
        let store = make_store().await;
        let key = SessionKey {
            platform: "test".into(),
            chat_id: "chat_resume".into(),
            cli_profile: "claude-direct".into(),
        };
        let sid = "sid-resume";

        // Turn 1.
        let mock1 = MockLlmStream::new(vec![vec![
            StreamEvent::TextDelta("first reply".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: Usage::default(),
            },
        ]]);
        run_native_rust(
            &test_cfg(),
            &mock1,
            store.clone(),
            key.clone(),
            sid,
            "first",
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // Turn 2: mock should see the full prior history + new user msg.
        let mock2 = MockLlmStream::new(vec![vec![
            StreamEvent::TextDelta("second reply".into()),
            StreamEvent::MessageStop {
                stop_reason: StopReason::EndTurn,
                usage: Usage::default(),
            },
        ]]);
        run_native_rust(
            &test_cfg(),
            &mock2,
            store.clone(),
            key.clone(),
            sid,
            "second",
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let observed = mock2.observed_inputs.lock().await;
        assert_eq!(observed.len(), 1, "turn 2 should have made exactly one call");
        let msgs = &observed[0];
        // Expected: [user("first"), assistant("first reply"), user("second")]
        assert_eq!(msgs.len(), 3, "turn 2 should see 3 prior messages");

        // Final state: 4 messages total.
        let final_hist = load_history(&*store, &key, sid).await.unwrap().unwrap();
        assert_eq!(final_hist.len(), 4);
    }

    #[tokio::test]
    async fn cancel_skips_save() {
        let store = make_store().await;
        let key = SessionKey {
            platform: "test".into(),
            chat_id: "chat_cancel".into(),
            cli_profile: "claude-direct".into(),
        };
        let sid = "sid-cancel";

        let cancel = CancellationToken::new();
        cancel.cancel();

        let mock = MockLlmStream::new(vec![vec![StreamEvent::MessageStop {
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        }]]);
        let err = run_native_rust(
            &test_cfg(),
            &mock,
            store.clone(),
            key.clone(),
            sid,
            "hi",
            None,
            None,
            None,
            Some(cancel),
        )
        .await
        .unwrap_err();
        assert!(err.contains("cancel"));

        // No row was written.
        assert!(load_history(&*store, &key, sid).await.unwrap().is_none());
    }
}
