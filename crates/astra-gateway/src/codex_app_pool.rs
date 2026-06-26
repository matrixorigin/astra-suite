//! Long-lived Codex app-server pool.
//!
//! Codex persistent mode uses the app-server JSON-RPC protocol over stdio:
//! create/resume a thread once, then start turns on that thread.

use crate::cli_bridge::{
    CliProfile, CliProgress, CliResult, ReasoningKind, apply_provider_environment,
};
use crate::mcp::config::CodexMcpConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type ConversationKey = String;
type PendingResponses = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;
const TOOL_PARAM_MAX_CHARS: usize = 160;
const TOOL_NAME_MAX_CHARS: usize = 80;
const APP_SERVER_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const APPROVAL_RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) struct CodexAppPool {
    processes: HashMap<ConversationKey, ProcessHandle>,
}

struct ProcessHandle {
    request_tx: mpsc::Sender<Value>,
    pending: PendingResponses,
    next_id: AtomicU64,
    progress_slot: Arc<Mutex<Option<mpsc::Sender<CliProgress>>>>,
    cancel: CancellationToken,
    thread_id: Arc<Mutex<Option<String>>>,
    active_turn_id: Arc<Mutex<Option<String>>>,
    last_text: Arc<Mutex<String>>,
    tokens_prompt: Arc<Mutex<Option<u64>>>,
    tokens_completion: Arc<Mutex<Option<u64>>>,
    cached_input_tokens: Arc<Mutex<Option<u64>>>,
    reasoning_output_tokens: Arc<Mutex<Option<u64>>>,
    total_tokens: Arc<Mutex<Option<u64>>>,
    context_window: Arc<Mutex<Option<u64>>>,
    raw_usage_json: Arc<Mutex<Option<String>>>,
    tool_calls_count: Arc<Mutex<u32>>,
    tools_used: Arc<Mutex<Vec<String>>>,
    last_error: Arc<Mutex<Option<String>>>,
    turn_failed: Arc<Mutex<bool>>,
    approval_id: Arc<Mutex<Option<String>>>,
}

impl CodexAppPool {
    pub fn new() -> Self {
        Self {
            processes: HashMap::new(),
        }
    }

    pub fn supports_persistent(profile: &CliProfile) -> bool {
        matches!(
            profile,
            CliProfile::Codex {
                stream_json: true,
                ..
            } | CliProfile::Astra { .. }
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn begin_turn(
        &mut self,
        key: &str,
        message: &str,
        profile: &CliProfile,
        session_id: Option<&str>,
        working_dir: Option<&Path>,
        system_prompt: Option<&str>,
        provider_config: Option<&crate::config::ProviderConfig>,
        github_token: Option<&str>,
        mcp_config: Option<&CodexMcpConfig>,
    ) -> Result<mpsc::Receiver<CliProgress>, String> {
        if !self.processes.contains_key(key) || !self.is_alive(key) {
            self.processes.remove(key);
            self.spawn(
                key,
                profile,
                session_id,
                working_dir,
                system_prompt,
                provider_config,
                github_token,
                mcp_config,
            )
            .await?;
        }

        let handle = self.processes.get(key).ok_or("codex app-server missing")?;
        let (progress_tx, progress_rx) = mpsc::channel(256);
        *handle.progress_slot.lock().await = Some(progress_tx);
        *handle.last_text.lock().await = String::new();
        *handle.tokens_prompt.lock().await = None;
        *handle.tokens_completion.lock().await = None;
        *handle.cached_input_tokens.lock().await = None;
        *handle.reasoning_output_tokens.lock().await = None;
        *handle.total_tokens.lock().await = None;
        *handle.context_window.lock().await = None;
        *handle.raw_usage_json.lock().await = None;
        *handle.tool_calls_count.lock().await = 0;
        *handle.tools_used.lock().await = Vec::new();
        *handle.last_error.lock().await = None;
        *handle.turn_failed.lock().await = false;
        *handle.approval_id.lock().await = None;
        *handle.active_turn_id.lock().await = None;

        let thread_id = handle
            .thread_id
            .lock()
            .await
            .clone()
            .ok_or("codex app-server has no thread id")?;

        let mut params = serde_json::json!({
            "threadId": thread_id,
            "input": [{
                "type": "text",
                "text": message,
                "text_elements": []
            }],
            "approvalPolicy": "never",
        });

        if let Some(dir) = working_dir {
            params["cwd"] = Value::String(dir.to_string_lossy().to_string());
        }

        match profile {
            CliProfile::Codex {
                model: Some(model), ..
            } => {
                params["model"] = Value::String(model.clone());
            }
            CliProfile::Astra {
                model,
                permission_mode,
                ..
            } => {
                if let Some(model) = model {
                    params["model"] = Value::String(model.clone());
                }
                params["permissionMode"] = Value::String(permission_mode.clone());
            }
            _ => {}
        }

        let response = handle.request("turn/start", params).await?;
        if let Some(turn_id) = response["turn"]["id"].as_str() {
            *handle.active_turn_id.lock().await = Some(turn_id.to_string());
        }

        Ok(progress_rx)
    }

    pub async fn interrupt(&self, key: &str) -> Result<(), String> {
        let handle = self
            .processes
            .get(key)
            .ok_or("no codex app-server process for conversation")?;
        let thread_id = handle
            .thread_id
            .lock()
            .await
            .clone()
            .ok_or("no codex thread id")?;
        let turn_id = handle
            .active_turn_id
            .lock()
            .await
            .clone()
            .ok_or("no active codex turn id")?;
        let _ = handle
            .request(
                "turn/interrupt",
                serde_json::json!({
                    "threadId": thread_id,
                    "turnId": turn_id,
                }),
            )
            .await?;
        Ok(())
    }

    pub fn kill(&mut self, key: &str) {
        if let Some(handle) = self.processes.remove(key) {
            handle.cancel.cancel();
        }
        crate::mcp::config::cleanup_mcp_config(key);
    }

    pub async fn respond_current_approval(&self, key: &str, decision: &str) -> Result<(), String> {
        let handle = self
            .processes
            .get(key)
            .ok_or("no app-server process for conversation")?;
        let approval_id = handle
            .approval_id
            .lock()
            .await
            .clone()
            .ok_or("当前没有待确认操作")?;
        handle
            .request_with_timeout(
                "approval/respond",
                serde_json::json!({
                    "approvalId": approval_id,
                    "decision": decision,
                }),
                APPROVAL_RESPONSE_TIMEOUT,
            )
            .await?;
        *handle.approval_id.lock().await = None;
        Ok(())
    }

    pub async fn result(&self, key: &str) -> Option<CliResult> {
        let handle = self.processes.get(key)?;
        let text = handle.last_text.lock().await.clone();
        let tools_used = handle.tools_used.lock().await.clone();
        let tool_count = *handle.tool_calls_count.lock().await;
        let turn_failed = *handle.turn_failed.lock().await;
        let error = handle.last_error.lock().await.clone();
        Some(CliResult {
            stdout: String::new(),
            stderr: error.unwrap_or_default(),
            exit_code: if turn_failed { 1 } else { 0 },
            success: !turn_failed,
            error_kind: None,
            trace_id: None,
            request_id: None,
            run_id: None,
            session_id: handle.thread_id.lock().await.clone(),
            text: if turn_failed || text.is_empty() {
                None
            } else {
                Some(text)
            },
            tool_calls_count: if tool_count == 0 {
                None
            } else {
                Some(tool_count)
            },
            tools_used,
            tokens_prompt: *handle.tokens_prompt.lock().await,
            tokens_completion: *handle.tokens_completion.lock().await,
            cached_input_tokens: *handle.cached_input_tokens.lock().await,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_output_tokens: *handle.reasoning_output_tokens.lock().await,
            total_tokens: *handle.total_tokens.lock().await,
            context_window: *handle.context_window.lock().await,
            max_output_tokens: None,
            cost_usd: None,
            raw_usage_json: handle.raw_usage_json.lock().await.clone(),
        })
    }

    fn is_alive(&self, key: &str) -> bool {
        self.processes
            .get(key)
            .map(|h| !h.cancel.is_cancelled())
            .unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn(
        &mut self,
        key: &str,
        profile: &CliProfile,
        session_id: Option<&str>,
        working_dir: Option<&Path>,
        system_prompt: Option<&str>,
        provider_config: Option<&crate::config::ProviderConfig>,
        github_token: Option<&str>,
        mcp_config: Option<&CodexMcpConfig>,
    ) -> Result<(), String> {
        let mut cmd = build_app_server_command(profile, working_dir, provider_config, github_token)
            .ok_or("profile does not support codex app-server mode")?;
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn codex app-server: {e}"))?;

        let pid = child.id().unwrap_or(0);
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let stderr = child.stderr.take().ok_or("no stderr")?;

        let cancel = CancellationToken::new();
        let (request_tx, request_rx) = mpsc::channel::<Value>(64);
        let pending: PendingResponses = Arc::new(Mutex::new(HashMap::new()));
        let progress_slot = Arc::new(Mutex::new(None));
        let thread_id = Arc::new(Mutex::new(None));
        let active_turn_id = Arc::new(Mutex::new(None));
        let last_text = Arc::new(Mutex::new(String::new()));
        let tokens_prompt = Arc::new(Mutex::new(None));
        let tokens_completion = Arc::new(Mutex::new(None));
        let cached_input_tokens = Arc::new(Mutex::new(None));
        let reasoning_output_tokens = Arc::new(Mutex::new(None));
        let total_tokens = Arc::new(Mutex::new(None));
        let context_window = Arc::new(Mutex::new(None));
        let raw_usage_json = Arc::new(Mutex::new(None));
        let tool_calls_count = Arc::new(Mutex::new(0));
        let tools_used = Arc::new(Mutex::new(Vec::new()));
        let last_error = Arc::new(Mutex::new(None));
        let turn_failed = Arc::new(Mutex::new(false));
        let approval_id = Arc::new(Mutex::new(None));

        tokio::spawn(stdin_writer_task(stdin, request_rx, cancel.clone()));
        tokio::spawn(stderr_drainer_task(stderr, cancel.clone()));
        tokio::spawn(stdout_reader_task(
            stdout,
            pending.clone(),
            progress_slot.clone(),
            thread_id.clone(),
            active_turn_id.clone(),
            last_text.clone(),
            tokens_prompt.clone(),
            tokens_completion.clone(),
            cached_input_tokens.clone(),
            reasoning_output_tokens.clone(),
            total_tokens.clone(),
            context_window.clone(),
            raw_usage_json.clone(),
            tool_calls_count.clone(),
            tools_used.clone(),
            last_error.clone(),
            turn_failed.clone(),
            approval_id.clone(),
            cancel.clone(),
        ));

        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    tracing::debug!(?status, "codex app-server process exited");
                    cancel_clone.cancel();
                }
                _ = cancel_clone.cancelled() => {
                    let _ = child.kill().await;
                }
            }
        });

        let handle = ProcessHandle {
            request_tx,
            pending,
            next_id: AtomicU64::new(1),
            progress_slot,
            cancel,
            thread_id,
            active_turn_id,
            last_text,
            tokens_prompt,
            tokens_completion,
            cached_input_tokens,
            reasoning_output_tokens,
            total_tokens,
            context_window,
            raw_usage_json,
            tool_calls_count,
            tools_used,
            last_error,
            turn_failed,
            approval_id,
        };

        handle
            .request(
                "initialize",
                serde_json::json!({
                    "clientInfo": {
                        "name": "astra-gateway",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                    "capabilities": {
                        "experimentalApi": true,
                    },
                }),
            )
            .await?;

        let thread = if let Some(session_id) = session_id.filter(|s| !s.trim().is_empty()) {
            match handle
                .request(
                    "thread/resume",
                    thread_resume_params(
                        session_id,
                        profile,
                        working_dir,
                        system_prompt,
                        mcp_config,
                    ),
                )
                .await
            {
                Ok(thread) => thread,
                Err(e) => {
                    tracing::warn!(error = %e, "failed to resume codex thread; starting a new one");
                    handle
                        .request(
                            "thread/start",
                            thread_start_params(profile, working_dir, system_prompt, mcp_config),
                        )
                        .await?
                }
            }
        } else {
            handle
                .request(
                    "thread/start",
                    thread_start_params(profile, working_dir, system_prompt, mcp_config),
                )
                .await?
        };
        let tid = thread["thread"]["id"]
            .as_str()
            .ok_or("codex thread/start response missing thread.id")?
            .to_string();
        *handle.thread_id.lock().await = Some(tid);

        self.processes.insert(key.to_string(), handle);
        tracing::info!(pid, key, "spawned codex app-server process");
        Ok(())
    }
}

impl ProcessHandle {
    async fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        self.request_with_timeout(method, params, APP_SERVER_RPC_TIMEOUT)
            .await
    }

    async fn request_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout_duration: Duration,
    ) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        let msg = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        if self.request_tx.send(msg).await.is_err() {
            let _ = self.pending.lock().await.remove(&id);
            return Err("codex app-server stdin closed".to_string());
        }
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(response) => {
                response.map_err(|_| "codex app-server response channel closed".to_string())?
            }
            Err(_) => {
                let _ = self.pending.lock().await.remove(&id);
                Err(format!(
                    "codex app-server `{method}` timed out after {}s",
                    timeout_duration.as_secs()
                ))
            }
        }
    }
}

fn build_app_server_command(
    profile: &CliProfile,
    working_dir: Option<&Path>,
    provider_config: Option<&crate::config::ProviderConfig>,
    github_token: Option<&str>,
) -> Option<Command> {
    let mut cmd = match profile {
        CliProfile::Codex {
            bin, extra_args, ..
        } => {
            let mut cmd = Command::new(bin);
            cmd.arg("app-server").arg("--listen").arg("stdio://");
            for arg in extra_args {
                cmd.arg(arg);
            }
            cmd
        }
        CliProfile::Astra {
            bin,
            app_server_url,
            ..
        } => {
            let mut cmd = Command::new(bin);
            cmd.env("no_proxy", "127.0.0.1,localhost");
            if let Some(url) = app_server_url
                .as_deref()
                .filter(|url| !url.trim().is_empty())
            {
                cmd.env("ASTRA_API_URL", url);
            }
            cmd.arg("serve").arg("stdio");
            cmd
        }
        _ => return None,
    };
    if let Some(provider_config) = provider_config
        && let Err(e) = apply_provider_environment(&mut cmd, provider_config)
    {
        tracing::warn!(error = %e, "failed to apply provider environment to app-server command");
    }
    if let Some(token) = github_token {
        cmd.env("GH_TOKEN", token);
        cmd.env("GITHUB_TOKEN", token);
    }
    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    }
    Some(cmd)
}

fn thread_start_params(
    profile: &CliProfile,
    working_dir: Option<&Path>,
    system_prompt: Option<&str>,
    mcp_config: Option<&CodexMcpConfig>,
) -> Value {
    let mut params = serde_json::json!({
        "approvalPolicy": "never",
        "ephemeral": false,
        "experimentalRawEvents": false,
        "persistExtendedHistory": false,
    });

    apply_thread_context_params(&mut params, working_dir, system_prompt, mcp_config);

    match profile {
        CliProfile::Codex {
            model, ephemeral, ..
        } => {
            if let Some(model) = model {
                params["model"] = Value::String(model.clone());
            }
            params["ephemeral"] = Value::Bool(*ephemeral);
        }
        CliProfile::Astra {
            model,
            permission_mode,
            ..
        } => {
            if let Some(model) = model {
                params["model"] = Value::String(model.clone());
            }
            params["permissionMode"] = Value::String(permission_mode.clone());
        }
        _ => {}
    }

    params
}

fn thread_resume_params(
    thread_id: &str,
    profile: &CliProfile,
    working_dir: Option<&Path>,
    system_prompt: Option<&str>,
    mcp_config: Option<&CodexMcpConfig>,
) -> Value {
    let mut params = serde_json::json!({
        "threadId": thread_id,
        "excludeTurns": true,
        "persistExtendedHistory": false,
        "approvalPolicy": "never",
    });

    apply_thread_context_params(&mut params, working_dir, system_prompt, mcp_config);

    match profile {
        CliProfile::Codex {
            model: Some(model), ..
        } => {
            params["model"] = Value::String(model.clone());
        }
        CliProfile::Astra {
            model,
            permission_mode,
            ..
        } => {
            if let Some(model) = model {
                params["model"] = Value::String(model.clone());
            }
            params["permissionMode"] = Value::String(permission_mode.clone());
        }
        _ => {}
    }

    params
}

fn apply_thread_context_params(
    params: &mut Value,
    working_dir: Option<&Path>,
    system_prompt: Option<&str>,
    mcp_config: Option<&CodexMcpConfig>,
) {
    if let Some(dir) = working_dir {
        params["cwd"] = Value::String(dir.to_string_lossy().to_string());
    }

    if let Some(sp) = system_prompt.filter(|s| !s.trim().is_empty()) {
        params["developerInstructions"] = Value::String(sp.to_string());
    }

    if let Some(mcp_config) = mcp_config {
        params["config"] = mcp_config.thread_config();
    }
}

async fn stdin_writer_task(
    mut stdin: tokio::process::ChildStdin,
    mut rx: mpsc::Receiver<Value>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                let Ok(line) = serde_json::to_string(&msg) else { continue };
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.write_all(b"\n").await.is_err() {
                    break;
                }
                let _ = stdin.flush().await;
            }
            _ = cancel.cancelled() => break,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stdout_reader_task(
    stdout: tokio::process::ChildStdout,
    pending: PendingResponses,
    progress_slot: Arc<Mutex<Option<mpsc::Sender<CliProgress>>>>,
    thread_id: Arc<Mutex<Option<String>>>,
    active_turn_id: Arc<Mutex<Option<String>>>,
    last_text: Arc<Mutex<String>>,
    tokens_prompt: Arc<Mutex<Option<u64>>>,
    tokens_completion: Arc<Mutex<Option<u64>>>,
    cached_input_tokens: Arc<Mutex<Option<u64>>>,
    reasoning_output_tokens: Arc<Mutex<Option<u64>>>,
    total_tokens: Arc<Mutex<Option<u64>>>,
    context_window: Arc<Mutex<Option<u64>>>,
    raw_usage_json: Arc<Mutex<Option<String>>>,
    tool_calls_count: Arc<Mutex<u32>>,
    tools_used: Arc<Mutex<Vec<String>>>,
    last_error: Arc<Mutex<Option<String>>>,
    turn_failed: Arc<Mutex<bool>>,
    approval_id: Arc<Mutex<Option<String>>>,
    cancel: CancellationToken,
) {
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let Ok(v) = serde_json::from_str::<Value>(trimmed) else {
                            tracing::debug!(line = %trimmed, "invalid codex app-server stdout");
                            continue;
                        };
                        if let Some(id) = v["id"].as_u64() {
                            let tx = pending.lock().await.remove(&id);
                            if let Some(tx) = tx {
                                let res = if let Some(err) = v.get("error") {
                                    Err(err.to_string())
                                } else {
                                    Ok(v.get("result").cloned().unwrap_or(Value::Null))
                                };
                                let _ = tx.send(res);
                            }
                            continue;
                        }
                        handle_notification(
                            v,
                            &progress_slot,
                            &thread_id,
                            &active_turn_id,
                            &last_text,
                            &tokens_prompt,
                            &tokens_completion,
                            &cached_input_tokens,
                            &reasoning_output_tokens,
                            &total_tokens,
                            &context_window,
                            &raw_usage_json,
                            &tool_calls_count,
                            &tools_used,
                            &last_error,
                            &turn_failed,
                            &approval_id,
                        )
                        .await;
                    }
                    Ok(None) => {
                        let error = "codex app-server stdout closed";
                        mark_process_failed(
                            &progress_slot,
                            &active_turn_id,
                            &last_error,
                            &turn_failed,
                            &approval_id,
                            error,
                        )
                        .await;
                        fail_pending(&pending, error).await;
                        cancel.cancel();
                        break;
                    }
                    Err(e) => {
                        let error = format!("codex app-server stdout error: {e}");
                        mark_process_failed(
                            &progress_slot,
                            &active_turn_id,
                            &last_error,
                            &turn_failed,
                            &approval_id,
                            &error,
                        )
                        .await;
                        fail_pending(&pending, &error).await;
                        cancel.cancel();
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => {
                let error = "codex app-server cancelled";
                mark_process_failed(
                    &progress_slot,
                    &active_turn_id,
                    &last_error,
                    &turn_failed,
                    &approval_id,
                    error,
                )
                .await;
                fail_pending(&pending, error).await;
                break;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_notification(
    v: Value,
    progress_slot: &Arc<Mutex<Option<mpsc::Sender<CliProgress>>>>,
    thread_id: &Arc<Mutex<Option<String>>>,
    active_turn_id: &Arc<Mutex<Option<String>>>,
    last_text: &Arc<Mutex<String>>,
    tokens_prompt: &Arc<Mutex<Option<u64>>>,
    tokens_completion: &Arc<Mutex<Option<u64>>>,
    cached_input_tokens: &Arc<Mutex<Option<u64>>>,
    reasoning_output_tokens: &Arc<Mutex<Option<u64>>>,
    total_tokens: &Arc<Mutex<Option<u64>>>,
    context_window: &Arc<Mutex<Option<u64>>>,
    raw_usage_json: &Arc<Mutex<Option<String>>>,
    tool_calls_count: &Arc<Mutex<u32>>,
    tools_used: &Arc<Mutex<Vec<String>>>,
    last_error: &Arc<Mutex<Option<String>>>,
    turn_failed: &Arc<Mutex<bool>>,
    approval_id: &Arc<Mutex<Option<String>>>,
) {
    let Some(method) = v["method"].as_str() else {
        return;
    };
    let params = &v["params"];
    match method {
        "thread/started" => {
            if let Some(tid) = params["thread"]["id"].as_str() {
                *thread_id.lock().await = Some(tid.to_string());
            }
        }
        "turn/started" => {
            if let Some(turn_id) = params["turn"]["id"].as_str() {
                *active_turn_id.lock().await = Some(turn_id.to_string());
            }
        }
        "item/agentMessage/delta" => {
            if let Some(delta) = params["delta"].as_str()
                && !delta.is_empty()
            {
                last_text.lock().await.push_str(delta);
                send_progress(progress_slot, CliProgress::Token(delta.to_string())).await;
            }
        }
        "item/reasoning/textDelta" => {
            if let Some(delta) = params["delta"].as_str()
                && !delta.is_empty()
            {
                send_progress(
                    progress_slot,
                    CliProgress::ReasoningBlock {
                        kind: ReasoningKind::Raw,
                        text: delta.to_string(),
                    },
                )
                .await;
            }
        }
        "item/reasoning/summaryTextDelta" => {
            if let Some(delta) = params["delta"].as_str()
                && !delta.is_empty()
            {
                send_progress(
                    progress_slot,
                    CliProgress::ReasoningBlock {
                        kind: ReasoningKind::Summary,
                        text: delta.to_string(),
                    },
                )
                .await;
            }
        }
        "item/started" => {
            if let Some(ev) = item_started_progress(&params["item"]) {
                send_progress(progress_slot, ev).await;
            }
        }
        "item/completed" => {
            let item = &params["item"];
            if item["type"].as_str() == Some("agentMessage") {
                if last_text.lock().await.is_empty()
                    && let Some(text) = item["text"].as_str()
                {
                    *last_text.lock().await = text.to_string();
                }
                return;
            }
            if let Some(ev) = item_completed_progress(item, tool_calls_count, tools_used).await {
                send_progress(progress_slot, ev).await;
            }
        }
        "thread/tokenUsage/updated" => {
            let usage = &params["tokenUsage"]["last"];
            *tokens_prompt.lock().await = usage["inputTokens"].as_u64();
            *tokens_completion.lock().await = usage["outputTokens"].as_u64();
            *cached_input_tokens.lock().await = usage["cachedInputTokens"].as_u64();
            *reasoning_output_tokens.lock().await = usage["reasoningOutputTokens"].as_u64();
            *total_tokens.lock().await = usage["totalTokens"].as_u64();
            *context_window.lock().await = params["tokenUsage"]["modelContextWindow"].as_u64();
            *raw_usage_json.lock().await = serde_json::to_string(&params["tokenUsage"]).ok();
        }
        "turn/completed" => {
            if params["status"].as_str() == Some("failed") {
                *turn_failed.lock().await = true;
            }
            *active_turn_id.lock().await = None;
            *progress_slot.lock().await = None;
            *approval_id.lock().await = None;
        }
        "approval/requested" => {
            let Some(progress) = approval_requested_progress(params) else {
                return;
            };
            let CliProgress::ApprovalRequested { id, .. } = &progress else {
                return;
            };
            *approval_id.lock().await = Some(id.clone());
            send_progress(progress_slot, progress).await;
        }
        "error" => {
            let msg = params["message"]
                .as_str()
                .or_else(|| params["error"].as_str())
                .unwrap_or("codex app-server error")
                .to_string();
            *last_error.lock().await = Some(msg.clone());
            send_progress(progress_slot, CliProgress::Status(format!("[error] {msg}"))).await;
        }
        _ => {}
    }
}

fn approval_requested_progress(params: &Value) -> Option<CliProgress> {
    let approval = params.get("approval")?;
    let id = approval.get("id")?.as_str()?.trim();
    if id.is_empty() {
        return None;
    }
    let tool = approval
        .get("tool")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("tool")
        .trim()
        .to_string();
    let header = approval
        .get("header")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("工具需要确认")
        .trim()
        .to_string();
    let detail = approval
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    let reason = approval
        .get("reason")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("需要用户确认")
        .trim()
        .to_string();

    Some(CliProgress::ApprovalRequested {
        id: id.to_string(),
        tool,
        header,
        detail,
        reason,
    })
}

fn item_started_progress(item: &Value) -> Option<CliProgress> {
    match item["type"].as_str()? {
        "reasoning" => Some(CliProgress::Thinking(true)),
        "commandExecution" | "fileChange" | "mcpToolCall" | "dynamicToolCall" => {
            let summary = summarize_codex_tool_item(item);
            Some(CliProgress::ToolStarted {
                name: summary.name,
                params: summary.params,
            })
        }
        _ => None,
    }
}

async fn item_completed_progress(
    item: &Value,
    tool_calls_count: &Arc<Mutex<u32>>,
    tools_used: &Arc<Mutex<Vec<String>>>,
) -> Option<CliProgress> {
    match item["type"].as_str()? {
        "reasoning" => Some(CliProgress::Thinking(false)),
        "commandExecution" => {
            let name = summarize_codex_tool_item(item).name;
            record_tool(&name, tool_calls_count, tools_used).await;
            Some(CliProgress::ToolDone {
                name,
                duration_ms: item["durationMs"].as_u64(),
            })
        }
        "fileChange" => {
            let name = summarize_codex_tool_item(item).name;
            record_tool(&name, tool_calls_count, tools_used).await;
            Some(CliProgress::ToolDone {
                name,
                duration_ms: None,
            })
        }
        "mcpToolCall" => {
            let name = summarize_codex_tool_item(item).name;
            record_tool(&name, tool_calls_count, tools_used).await;
            Some(CliProgress::ToolDone {
                name,
                duration_ms: item["durationMs"].as_u64(),
            })
        }
        "dynamicToolCall" => {
            let name = summarize_codex_tool_item(item).name;
            record_tool(&name, tool_calls_count, tools_used).await;
            Some(CliProgress::ToolDone {
                name,
                duration_ms: item["durationMs"].as_u64(),
            })
        }
        _ => None,
    }
}

struct ToolSummary {
    name: String,
    params: Option<String>,
}

fn summarize_codex_tool_item(item: &Value) -> ToolSummary {
    match item["type"].as_str().unwrap_or("tool") {
        "commandExecution" => ToolSummary {
            name: "shell".to_string(),
            params: item["command"].as_str().map(summarize_text),
        },
        "fileChange" => ToolSummary {
            name: "file_change".to_string(),
            params: summarize_file_changes(&item["changes"]),
        },
        "mcpToolCall" => {
            let tool = item["tool"].as_str().unwrap_or("mcp_tool");
            let server = item["server"].as_str();
            ToolSummary {
                name: summarize_tool_name(match server {
                    Some(server) if !server.is_empty() => format!("{server}/{tool}"),
                    _ => tool.to_string(),
                }),
                params: summarize_json_value(&item["arguments"]),
            }
        }
        "dynamicToolCall" => {
            let tool = item["tool"].as_str().unwrap_or("dynamic_tool");
            let namespace = item["namespace"].as_str();
            ToolSummary {
                name: summarize_tool_name(match namespace {
                    Some(namespace) if !namespace.is_empty() => format!("{namespace}/{tool}"),
                    _ => tool.to_string(),
                }),
                params: summarize_json_value(&item["arguments"]),
            }
        }
        other => ToolSummary {
            name: summarize_tool_name(other.to_string()),
            params: None,
        },
    }
}

fn summarize_tool_name(name: String) -> String {
    truncate_unicode(&collapse_whitespace(&name), TOOL_NAME_MAX_CHARS)
}

fn summarize_text(text: &str) -> String {
    truncate_unicode(&collapse_whitespace(text), TOOL_PARAM_MAX_CHARS)
}

fn summarize_json_value(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(summarize_text(s)),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(items) => {
            if items.is_empty() {
                None
            } else {
                let parts: Vec<String> = items
                    .iter()
                    .take(3)
                    .filter_map(summarize_json_value)
                    .collect();
                let mut summary = format!("[{}]", parts.join(", "));
                if items.len() > 3 {
                    summary.push_str(&format!(" +{} more", items.len() - 3));
                }
                Some(truncate_unicode(&summary, TOOL_PARAM_MAX_CHARS))
            }
        }
        Value::Object(map) => {
            for key in [
                "command",
                "cmd",
                "query",
                "q",
                "pattern",
                "path",
                "file_path",
                "filePath",
                "url",
                "uri",
            ] {
                if let Some(summary) = map.get(key).and_then(summarize_json_value)
                    && !summary.is_empty()
                {
                    return Some(format!("{key}: {summary}"));
                }
            }

            let parts: Vec<String> = map
                .iter()
                .filter_map(|(key, value)| {
                    summarize_json_value(value).map(|summary| format!("{key}: {summary}"))
                })
                .take(3)
                .collect();
            if parts.is_empty() {
                None
            } else {
                let mut summary = parts.join(", ");
                if map.len() > 3 {
                    summary.push_str(&format!(" +{} more", map.len() - 3));
                }
                Some(truncate_unicode(&summary, TOOL_PARAM_MAX_CHARS))
            }
        }
    }
}

fn summarize_file_changes(changes: &Value) -> Option<String> {
    let changes = changes.as_array()?;
    if changes.is_empty() {
        return None;
    }
    let paths: Vec<String> = changes
        .iter()
        .take(3)
        .filter_map(|change| {
            ["path", "file", "filePath", "target"]
                .iter()
                .find_map(|key| change[*key].as_str())
                .map(summarize_text)
        })
        .collect();
    if paths.is_empty() {
        return Some(format!("{} file change(s)", changes.len()));
    }
    let mut summary = paths.join(", ");
    if changes.len() > 3 {
        summary.push_str(&format!(" +{} more", changes.len() - 3));
    }
    Some(truncate_unicode(&summary, TOOL_PARAM_MAX_CHARS))
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_unicode(s: &str, max_chars: usize) -> String {
    match s.char_indices().nth(max_chars) {
        None => s.to_string(),
        Some((idx, _)) => format!("{}...", &s[..idx]),
    }
}

async fn record_tool(
    name: &str,
    tool_calls_count: &Arc<Mutex<u32>>,
    tools_used: &Arc<Mutex<Vec<String>>>,
) {
    *tool_calls_count.lock().await += 1;
    let mut tools = tools_used.lock().await;
    if !tools.iter().any(|n| n == name) {
        tools.push(name.to_string());
    }
}

async fn send_progress(
    progress_slot: &Arc<Mutex<Option<mpsc::Sender<CliProgress>>>>,
    ev: CliProgress,
) {
    let tx = progress_slot.lock().await.clone();
    if let Some(tx) = tx {
        let _ = tx.send(ev).await;
    }
}

async fn fail_pending(pending: &PendingResponses, error: &str) {
    let mut guard = pending.lock().await;
    for (_, tx) in guard.drain() {
        let _ = tx.send(Err(error.to_string()));
    }
}

async fn mark_process_failed(
    progress_slot: &Arc<Mutex<Option<mpsc::Sender<CliProgress>>>>,
    active_turn_id: &Arc<Mutex<Option<String>>>,
    last_error: &Arc<Mutex<Option<String>>>,
    turn_failed: &Arc<Mutex<bool>>,
    approval_id: &Arc<Mutex<Option<String>>>,
    error: &str,
) {
    *last_error.lock().await = Some(error.to_string());
    *turn_failed.lock().await = true;
    *active_turn_id.lock().await = None;
    *approval_id.lock().await = None;
    *progress_slot.lock().await = None;
}

async fn stderr_drainer_task(stderr: tokio::process::ChildStderr, cancel: CancellationToken) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) if !line.trim().is_empty() => {
                        tracing::debug!(line = %line, "codex app-server stderr");
                    }
                    Ok(None) | Err(_) => break,
                    _ => {}
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_start_uses_codex_profile_fields() {
        let profile = CliProfile::Codex {
            bin: "codex".into(),
            model: Some("gpt-5.5".into()),
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: true,
        };
        let params = thread_start_params(
            &profile,
            Some(Path::new("/tmp/project")),
            Some("system prompt"),
            None,
        );
        assert_eq!(params["cwd"], "/tmp/project");
        assert_eq!(params["model"], "gpt-5.5");
        assert!(params.get("sandbox").is_none());
        assert_eq!(params["ephemeral"], true);
        assert_eq!(params["developerInstructions"], "system prompt");
        assert_eq!(params["approvalPolicy"], "never");
    }

    #[test]
    fn astra_profile_uses_app_server_pool() {
        let profile = CliProfile::Astra {
            bin: "astra".into(),
            model: Some("gpt-5.5".into()),
            permission_mode: "auto".into(),
            app_server_url: None,
        };
        assert!(CodexAppPool::supports_persistent(&profile));

        let params = thread_start_params(
            &profile,
            Some(Path::new("/tmp/project")),
            Some("system"),
            None,
        );
        assert_eq!(params["cwd"], "/tmp/project");
        assert_eq!(params["model"], "gpt-5.5");
        assert_eq!(params["permissionMode"], "auto");
        assert_eq!(params["developerInstructions"], "system");
    }

    #[test]
    fn astra_app_server_command_uses_serve_stdio() {
        let profile = CliProfile::Astra {
            bin: "astra".into(),
            model: None,
            permission_mode: "auto".into(),
            app_server_url: None,
        };
        let cmd = build_app_server_command(&profile, Some(Path::new("/tmp/project")), None, None)
            .expect("astra should support app-server command");
        assert_eq!(cmd.as_std().get_program(), "astra");
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, ["serve", "stdio"]);
        assert_eq!(
            cmd.as_std().get_current_dir(),
            Some(Path::new("/tmp/project"))
        );
    }

    #[test]
    fn astra_app_server_command_sets_api_url_from_profile() {
        let profile = CliProfile::Astra {
            bin: "astra".into(),
            model: None,
            permission_mode: "auto".into(),
            app_server_url: Some("http://10.222.1.50:28000".into()),
        };
        let cmd = build_app_server_command(&profile, None, None, None)
            .expect("astra should support app-server command");
        let envs: std::collections::HashMap<_, _> = cmd.as_std().get_envs().collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("ASTRA_API_URL"))
                .and_then(|value| *value)
                .and_then(|value| value.to_str()),
            Some("http://10.222.1.50:28000")
        );
    }

    #[test]
    fn codex_app_server_command_uses_plain_app_server_args() {
        let profile = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: true,
        };

        let cmd = build_app_server_command(&profile, None, None, None)
            .expect("codex should support app-server command");
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect();

        assert_eq!(args, ["app-server", "--listen", "stdio://"]);
        assert!(!args.contains(&"-c".to_string()));
    }

    #[test]
    fn codex_thread_start_includes_gateway_mcp_config() {
        let profile = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: true,
        };
        let config = CodexMcpConfig {
            command: "/tmp/astra-gateway".into(),
            args: vec!["mcp-serve".into()],
            env: [("GW_MCP_CHAT_ID".to_string(), "chat-1".to_string())].into(),
        };

        let params = thread_start_params(&profile, None, None, Some(&config));

        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["command"],
            "/tmp/astra-gateway"
        );
        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["args"],
            serde_json::json!(["mcp-serve"])
        );
        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["env"]["GW_MCP_CHAT_ID"],
            "chat-1"
        );
    }

    #[test]
    fn codex_thread_resume_includes_gateway_mcp_config() {
        let profile = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: true,
        };
        let config = CodexMcpConfig {
            command: "/tmp/astra-gateway".into(),
            args: vec!["mcp-serve".into()],
            env: [("GW_MCP_CHAT_ID".to_string(), "chat-1".to_string())].into(),
        };

        let params = thread_resume_params("thread-1", &profile, None, None, Some(&config));

        assert_eq!(params["threadId"], "thread-1");
        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["command"],
            "/tmp/astra-gateway"
        );
        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["args"],
            serde_json::json!(["mcp-serve"])
        );
        assert_eq!(
            params["config"]["mcp_servers"]["gateway"]["env"]["GW_MCP_CHAT_ID"],
            "chat-1"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn live_astra_app_server_pool_streams_turn() {
        let astra_bin =
            std::env::var("ASTRA_BIN").unwrap_or_else(|_| "target/debug/astra".to_string());
        let profile = CliProfile::Astra {
            bin: astra_bin,
            model: std::env::var("ASTRA_MODEL").ok(),
            permission_mode: "auto".into(),
            app_server_url: None,
        };
        let workspace = tempfile::tempdir().unwrap();
        let mut pool = CodexAppPool::new();
        let mut progress = pool
            .begin_turn(
                "live-astra-app-server-e2e",
                "只回复 ok 两个字母，不要调用工具。",
                &profile,
                None,
                Some(workspace.path()),
                Some("你是一个端到端验证助手，回答必须尽量简短。"),
                None,
                None,
                None,
            )
            .await
            .expect("astra app-server turn should start");

        let deadline = tokio::time::sleep(std::time::Duration::from_secs(180));
        tokio::pin!(deadline);
        let mut saw_progress = false;
        loop {
            tokio::select! {
                event = progress.recv() => {
                    match event {
                        Some(CliProgress::Token(delta)) => {
                            saw_progress |= !delta.is_empty();
                        }
                        Some(CliProgress::ReasoningBlock { text, .. }) => {
                            saw_progress |= !text.is_empty();
                        }
                        Some(_) => {
                            saw_progress = true;
                        }
                        None => break,
                    }
                }
                _ = &mut deadline => {
                    pool.kill("live-astra-app-server-e2e");
                    panic!("timed out waiting for astra app-server turn");
                }
            }
        }

        let result = pool
            .result("live-astra-app-server-e2e")
            .await
            .expect("pool result should exist");
        pool.kill("live-astra-app-server-e2e");

        assert!(saw_progress, "should receive streaming progress");
        assert!(result.success, "turn should succeed");
        assert!(result.session_id.is_some(), "session id should be captured");
        assert!(
            result.text.as_deref().is_some_and(|text| !text.is_empty()),
            "final text should be captured"
        );
    }

    #[tokio::test]
    async fn failed_turn_notification_sets_error_state() {
        let (progress_tx, mut progress_rx) = mpsc::channel(4);
        let progress_slot = Arc::new(Mutex::new(Some(progress_tx)));
        let thread_id = Arc::new(Mutex::new(None));
        let active_turn_id = Arc::new(Mutex::new(Some("turn-1".to_string())));
        let last_text = Arc::new(Mutex::new("partial text".to_string()));
        let tokens_prompt = Arc::new(Mutex::new(None));
        let tokens_completion = Arc::new(Mutex::new(None));
        let cached_input_tokens = Arc::new(Mutex::new(None));
        let reasoning_output_tokens = Arc::new(Mutex::new(None));
        let total_tokens = Arc::new(Mutex::new(None));
        let context_window = Arc::new(Mutex::new(None));
        let raw_usage_json = Arc::new(Mutex::new(None));
        let tool_calls_count = Arc::new(Mutex::new(0));
        let tools_used = Arc::new(Mutex::new(Vec::new()));
        let last_error = Arc::new(Mutex::new(None));
        let turn_failed = Arc::new(Mutex::new(false));
        let approval_id = Arc::new(Mutex::new(None));

        handle_notification(
            serde_json::json!({
                "method": "error",
                "params": {"message": "model failed"}
            }),
            &progress_slot,
            &thread_id,
            &active_turn_id,
            &last_text,
            &tokens_prompt,
            &tokens_completion,
            &cached_input_tokens,
            &reasoning_output_tokens,
            &total_tokens,
            &context_window,
            &raw_usage_json,
            &tool_calls_count,
            &tools_used,
            &last_error,
            &turn_failed,
            &approval_id,
        )
        .await;
        handle_notification(
            serde_json::json!({
                "method": "turn/completed",
                "params": {"turn": {"id": "turn-1"}, "status": "failed"}
            }),
            &progress_slot,
            &thread_id,
            &active_turn_id,
            &last_text,
            &tokens_prompt,
            &tokens_completion,
            &cached_input_tokens,
            &reasoning_output_tokens,
            &total_tokens,
            &context_window,
            &raw_usage_json,
            &tool_calls_count,
            &tools_used,
            &last_error,
            &turn_failed,
            &approval_id,
        )
        .await;

        assert_eq!(last_error.lock().await.as_deref(), Some("model failed"));
        assert!(*turn_failed.lock().await);
        assert!(active_turn_id.lock().await.is_none());
        assert!(progress_slot.lock().await.is_none());
        assert!(matches!(
            progress_rx.recv().await,
            Some(CliProgress::Status(status)) if status.contains("model failed")
        ));
    }

    #[tokio::test]
    async fn process_failure_marks_turn_failed_and_clears_pending_state() {
        let (progress_tx, _progress_rx) = mpsc::channel(4);
        let progress_slot = Arc::new(Mutex::new(Some(progress_tx)));
        let active_turn_id = Arc::new(Mutex::new(Some("turn-1".to_string())));
        let last_error = Arc::new(Mutex::new(None));
        let turn_failed = Arc::new(Mutex::new(false));
        let approval_id = Arc::new(Mutex::new(Some("approval-1".to_string())));

        mark_process_failed(
            &progress_slot,
            &active_turn_id,
            &last_error,
            &turn_failed,
            &approval_id,
            "codex app-server stdout closed",
        )
        .await;

        assert_eq!(
            last_error.lock().await.as_deref(),
            Some("codex app-server stdout closed")
        );
        assert!(*turn_failed.lock().await);
        assert!(active_turn_id.lock().await.is_none());
        assert!(progress_slot.lock().await.is_none());
        assert!(approval_id.lock().await.is_none());
    }

    #[tokio::test]
    async fn approval_requested_notification_records_and_forwards_progress() {
        let (progress_tx, mut progress_rx) = mpsc::channel(4);
        let progress_slot = Arc::new(Mutex::new(Some(progress_tx)));
        let thread_id = Arc::new(Mutex::new(None));
        let active_turn_id = Arc::new(Mutex::new(None));
        let last_text = Arc::new(Mutex::new(String::new()));
        let tokens_prompt = Arc::new(Mutex::new(None));
        let tokens_completion = Arc::new(Mutex::new(None));
        let cached_input_tokens = Arc::new(Mutex::new(None));
        let reasoning_output_tokens = Arc::new(Mutex::new(None));
        let total_tokens = Arc::new(Mutex::new(None));
        let context_window = Arc::new(Mutex::new(None));
        let raw_usage_json = Arc::new(Mutex::new(None));
        let tool_calls_count = Arc::new(Mutex::new(0));
        let tools_used = Arc::new(Mutex::new(Vec::new()));
        let last_error = Arc::new(Mutex::new(None));
        let turn_failed = Arc::new(Mutex::new(false));
        let approval_id = Arc::new(Mutex::new(None));

        handle_notification(
            serde_json::json!({
                "method": "approval/requested",
                "params": {
                    "approval": {
                        "id": "approval-123456",
                        "tool": "shell",
                        "header": "Run command?",
                        "detail": "make build-debug",
                        "reason": "requires confirmation"
                    }
                }
            }),
            &progress_slot,
            &thread_id,
            &active_turn_id,
            &last_text,
            &tokens_prompt,
            &tokens_completion,
            &cached_input_tokens,
            &reasoning_output_tokens,
            &total_tokens,
            &context_window,
            &raw_usage_json,
            &tool_calls_count,
            &tools_used,
            &last_error,
            &turn_failed,
            &approval_id,
        )
        .await;

        assert_eq!(approval_id.lock().await.as_deref(), Some("approval-123456"));
        assert!(matches!(
            progress_rx.recv().await,
            Some(CliProgress::ApprovalRequested { id, tool, detail, .. })
                if id == "approval-123456"
                    && tool == "shell"
                    && detail.as_deref() == Some("make build-debug")
        ));
    }

    #[test]
    fn thread_resume_includes_existing_thread_id() {
        let profile = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            sandbox: "read-only".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: false,
        };
        let params = thread_resume_params("thread-1", &profile, None, None, None);
        assert_eq!(params["threadId"], "thread-1");
        assert_eq!(params["excludeTurns"], true);
        assert_eq!(params["persistExtendedHistory"], false);
        assert!(params.get("sandbox").is_none());
    }

    #[test]
    fn codex_command_execution_summary_moves_command_to_params() {
        let item = serde_json::json!({
            "type": "commandExecution",
            "command": "printf 'hello world' && cargo test -p astra-gateway --lib -- --nocapture\n"
        });
        let summary = summarize_codex_tool_item(&item);
        assert_eq!(summary.name, "shell");
        assert_eq!(
            summary.params.as_deref(),
            Some("printf 'hello world' && cargo test -p astra-gateway --lib -- --nocapture")
        );
    }

    #[test]
    fn codex_command_execution_summary_truncates_long_commands() {
        let long_command = format!("echo {}", "x".repeat(300));
        let item = serde_json::json!({
            "type": "commandExecution",
            "command": long_command
        });
        let summary = summarize_codex_tool_item(&item);
        let params = summary.params.unwrap();
        assert_eq!(summary.name, "shell");
        assert!(params.len() < 190, "params should be compact: {params}");
        assert!(
            params.ends_with("..."),
            "params should be truncated: {params}"
        );
    }

    #[test]
    fn codex_mcp_summary_includes_server_tool_and_primary_argument() {
        let item = serde_json::json!({
            "type": "mcpToolCall",
            "server": "docs",
            "tool": "search",
            "arguments": {
                "query": "codex app-server tool events",
                "limit": 5
            }
        });
        let summary = summarize_codex_tool_item(&item);
        assert_eq!(summary.name, "docs/search");
        assert_eq!(
            summary.params.as_deref(),
            Some("query: codex app-server tool events")
        );
    }

    #[test]
    fn codex_dynamic_summary_handles_generic_arguments() {
        let item = serde_json::json!({
            "type": "dynamicToolCall",
            "namespace": "gateway",
            "tool": "inspect",
            "arguments": {
                "trace_id": "abc123",
                "mode": "full",
                "include_events": true,
                "unused": "ignored"
            }
        });
        let summary = summarize_codex_tool_item(&item);
        assert_eq!(summary.name, "gateway/inspect");
        assert_eq!(
            summary.params.as_deref(),
            Some("include_events: true, mode: full, trace_id: abc123 +1 more")
        );
    }

    #[test]
    fn codex_file_change_summary_lists_paths() {
        let item = serde_json::json!({
            "type": "fileChange",
            "changes": [
                {"path": "src/main.rs"},
                {"path": "src/lib.rs"},
                {"path": "README.md"},
                {"path": "Cargo.toml"}
            ]
        });
        let summary = summarize_codex_tool_item(&item);
        assert_eq!(summary.name, "file_change");
        assert_eq!(
            summary.params.as_deref(),
            Some("src/main.rs, src/lib.rs, README.md +1 more")
        );
    }
}
