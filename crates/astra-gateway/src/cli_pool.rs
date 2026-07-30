//! Long-lived Claude CLI process pool.
//!
//! Normal messages are sent via stdin to an existing process.
//! Special operations (model change, rewind, clear) kill and respawn.

use crate::cli_bridge::{self, ClaudeUsageSnapshot, CliProfile, CliProgress, CliResult};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

type ConversationKey = String;
type ControlWaiters = Arc<Mutex<HashMap<String, ControlWaiter>>>;

pub(crate) const CLAUDE_INTERRUPTED_ERROR_KIND: &str = "claude_interrupted";

struct ControlWaiter {
    turn_generation: u64,
    response_tx: oneshot::Sender<Result<(), String>>,
}

pub(crate) struct CliProcessPool {
    processes: HashMap<ConversationKey, ProcessHandle>,
}

struct ProcessHandle {
    stdin_tx: mpsc::Sender<StdinCommand>,
    /// Shared slot: the stdout reader pushes events here.
    /// The runner swaps in a fresh sender before each turn.
    /// Tuple: (generation, sender). Reader only clears if generation matches.
    progress_slot: Arc<Mutex<(u64, Option<mpsc::Sender<CliProgress>>)>>,
    cancel: CancellationToken,
    session_id: Arc<Mutex<Option<String>>>,
    /// Last `result` event JSON — reader stores it here for the runner to extract stats.
    last_result: Arc<Mutex<Option<serde_json::Value>>>,
    /// Status from a Claude system command event emitted before the result frame.
    last_system_status_output: Arc<Mutex<Option<String>>>,
    /// Claude persistent mode reports cumulative usage for the process.
    /// Keep the previous snapshot so each gateway turn receives only its delta.
    last_usage_snapshot: Arc<Mutex<Option<ClaudeUsageSnapshot>>>,
    /// Current turn generation — incremented on each begin_turn.
    generation: Arc<AtomicU64>,
    /// Generation currently executing inside Claude, or zero while idle.
    active_generation: Arc<AtomicU64>,
    interrupted_generation: Arc<AtomicU64>,
    next_control_request_id: AtomicU64,
    control_waiters: ControlWaiters,
    /// Stderr hint: drainer stores notable errors (e.g. stale session) for the runner to check.
    stderr_hint: Arc<Mutex<Option<String>>>,
}

enum StdinCommand {
    UserMessage(String),
    Interrupt { request_id: String },
}

struct StdoutState {
    progress_slot: Arc<Mutex<(u64, Option<mpsc::Sender<CliProgress>>)>>,
    session_id: Arc<Mutex<Option<String>>>,
    last_result: Arc<Mutex<Option<serde_json::Value>>>,
    last_system_status_output: Arc<Mutex<Option<String>>>,
    generation: Arc<AtomicU64>,
    active_generation: Arc<AtomicU64>,
    interrupted_generation: Arc<AtomicU64>,
    control_waiters: ControlWaiters,
}

impl CliProcessPool {
    pub fn new() -> Self {
        Self {
            processes: HashMap::new(),
        }
    }

    pub fn supports_persistent(profile: &CliProfile) -> bool {
        matches!(
            profile,
            CliProfile::Claude {
                stream_json: true,
                ..
            }
        )
    }

    /// Begin a turn: ensure process exists, register progress channel, send message.
    /// Returns a receiver that will get CliProgress events for this turn.
    /// The turn ends when a `None` is received (process sent result event or died).
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_turn(
        &mut self,
        key: &str,
        message: &str,
        profile: &CliProfile,
        session_id: Option<&str>,
        working_dir: Option<&Path>,
        system_prompt: Option<&str>,
        access_token: Option<&str>,
        github_token: Option<&str>,
        mcp_config: Option<&Path>,
        provider_config: Option<&crate::config::ProviderConfig>,
    ) -> Result<mpsc::Receiver<CliProgress>, String> {
        if !self.processes.contains_key(key) || !self.is_alive(key) {
            self.processes.remove(key);
            self.spawn(
                key,
                profile,
                session_id,
                working_dir,
                system_prompt,
                access_token,
                github_token,
                mcp_config,
                provider_config,
            )
            .await?;
        }

        let handle = self.processes.get(key).unwrap();

        // Clear stale result from previous turn (e.g. after interrupt)
        *handle.last_result.lock().await = None;
        *handle.last_system_status_output.lock().await = None;

        // Increment generation and register new progress channel
        let turn_gen = handle.generation.fetch_add(1, Ordering::Relaxed) + 1;
        handle
            .active_generation
            .compare_exchange(0, turn_gen, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|active| format!("Claude turn {active} is still active"))?;
        let (progress_tx, progress_rx) = mpsc::channel(256);
        *handle.progress_slot.lock().await = (turn_gen, Some(progress_tx));

        // Send user message via stdin
        let payload = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{"type": "text", "text": message}]
            }
        });
        let json_line = format!("{}\n", serde_json::to_string(&payload).unwrap());

        if handle
            .stdin_tx
            .send(StdinCommand::UserMessage(json_line))
            .await
            .is_err()
        {
            let _ = handle.active_generation.compare_exchange(
                turn_gen,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            let mut slot = handle.progress_slot.lock().await;
            if slot.0 == turn_gen {
                slot.1 = None;
            }
            return Err("process stdin closed (process may have died)".to_string());
        }

        Ok(progress_rx)
    }

    pub async fn interrupt(&self, key: &str) -> Result<(), String> {
        let handle = self
            .processes
            .get(key)
            .ok_or("no process for conversation")?;
        let turn_generation = handle.active_generation.load(Ordering::Acquire);
        if turn_generation == 0 {
            return Err("no active Claude turn".to_string());
        }
        let request_id = format!(
            "astra_interrupt_{}",
            handle
                .next_control_request_id
                .fetch_add(1, Ordering::Relaxed)
        );
        let (response_tx, response_rx) = oneshot::channel();
        handle.control_waiters.lock().await.insert(
            request_id.clone(),
            ControlWaiter {
                turn_generation,
                response_tx,
            },
        );
        if handle
            .stdin_tx
            .send(StdinCommand::Interrupt {
                request_id: request_id.clone(),
            })
            .await
            .is_err()
        {
            handle.control_waiters.lock().await.remove(&request_id);
            return Err("stdin closed".to_string());
        }
        match tokio::time::timeout(std::time::Duration::from_secs(5), response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                handle.control_waiters.lock().await.remove(&request_id);
                Err("Claude interrupt response channel closed".to_string())
            }
            // Keep the waiter after a timeout. A late acknowledgement still
            // means Claude will emit an interrupted result that must not be
            // reported as an upstream provider failure.
            Err(_) => Err("Claude interrupt response timed out".to_string()),
        }
    }

    pub fn kill(&mut self, key: &str) {
        if let Some(handle) = self.processes.remove(key) {
            handle.cancel.cancel();
        }
        crate::mcp::config::cleanup_mcp_config(key);
    }

    pub async fn session_id(&self, key: &str) -> Option<String> {
        self.processes.get(key)?.session_id.lock().await.clone()
    }

    /// Take and normalize the last `result` from the process (consumed once per turn).
    pub async fn take_last_result(&self, key: &str) -> Option<CliResult> {
        let handle = self.processes.get(key)?;
        let value = handle.last_result.lock().await.take()?;
        let mut result = cli_bridge::parse_claude_result_value(&value, 0);
        let turn_generation = handle.generation.load(Ordering::Acquire);
        apply_acknowledged_interrupt(&mut result, turn_generation, &handle.interrupted_generation);
        let system_status_output = handle.last_system_status_output.lock().await.take();
        cli_bridge::apply_claude_system_status_output(&mut result, system_status_output);
        // Preserve the existing persistent-mode interpretation: a single
        // model turn is not a tool call, while values above one indicate the
        // extra agent turns previously shown in the footer.
        if result.tool_calls_count == Some(1) {
            result.tool_calls_count = None;
        }
        let mut previous = handle.last_usage_snapshot.lock().await;
        cli_bridge::normalize_claude_pool_usage(&mut result, &mut previous);
        Some(result)
    }

    /// Take a stderr hint (e.g. "No conversation found") stored by the drainer.
    pub async fn take_stderr_hint(&self, key: &str) -> Option<String> {
        self.processes.get(key)?.stderr_hint.lock().await.take()
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
        access_token: Option<&str>,
        github_token: Option<&str>,
        mcp_config: Option<&Path>,
        provider_config: Option<&crate::config::ProviderConfig>,
    ) -> Result<(), String> {
        let mut cmd =
            build_persistent_command(profile, session_id, working_dir, system_prompt, mcp_config)
                .ok_or("profile does not support persistent mode")?;

        if let Some(token) = access_token {
            cmd.env("ASTRA_ACCESS_TOKEN", token);
        }
        profile
            .apply_runtime_environment(&mut cmd)
            .map_err(|e| format!("failed to prepare CLI environment: {e}"))?;

        if let Some(pc) = provider_config {
            crate::cli_bridge::apply_provider_environment(&mut cmd, pc)
                .map_err(|e| format!("failed to prepare provider environment: {e}"))?;
        }
        if let Some(token) = github_token {
            cmd.env("GH_TOKEN", token);
            cmd.env("GITHUB_TOKEN", token);
        }

        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn persistent claude: {e}"))?;

        let pid = child.id().unwrap_or(0);
        let stdin = child.stdin.take().ok_or("no stdin")?;
        let stdout = child.stdout.take().ok_or("no stdout")?;
        let stderr = child.stderr.take().ok_or("no stderr")?;

        let cancel = CancellationToken::new();
        let (stdin_tx, stdin_rx) = mpsc::channel::<StdinCommand>(32);
        let progress_slot: Arc<Mutex<(u64, Option<mpsc::Sender<CliProgress>>)>> =
            Arc::new(Mutex::new((0, None)));
        let session_id: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let last_result: Arc<Mutex<Option<serde_json::Value>>> = Arc::new(Mutex::new(None));
        let last_system_status_output: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let last_usage_snapshot: Arc<Mutex<Option<ClaudeUsageSnapshot>>> =
            Arc::new(Mutex::new(None));
        let generation = Arc::new(AtomicU64::new(0));
        let active_generation = Arc::new(AtomicU64::new(0));
        let interrupted_generation = Arc::new(AtomicU64::new(0));
        let control_waiters = Arc::new(Mutex::new(HashMap::new()));
        let stderr_hint: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        // Spawn stdin writer
        tokio::spawn(stdin_writer_task(stdin, stdin_rx, cancel.clone()));

        // Spawn stderr drainer
        tokio::spawn(stderr_drainer_task(
            stderr,
            stderr_hint.clone(),
            cancel.clone(),
        ));

        // Spawn stdout reader — routes events to progress_slot
        tokio::spawn(stdout_reader_task(
            stdout,
            StdoutState {
                progress_slot: progress_slot.clone(),
                session_id: session_id.clone(),
                last_result: last_result.clone(),
                last_system_status_output: last_system_status_output.clone(),
                generation: generation.clone(),
                active_generation: active_generation.clone(),
                interrupted_generation: interrupted_generation.clone(),
                control_waiters: control_waiters.clone(),
            },
            cancel.clone(),
        ));

        // Spawn child reaper — kills on cancel, waits to avoid zombies
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    // Process exited on its own
                    tracing::debug!(?status, "persistent claude process exited");
                    cancel_clone.cancel();
                }
                _ = cancel_clone.cancelled() => {
                    // Kill requested — send SIGKILL
                    let _ = child.kill().await;
                }
            }
        });

        let handle = ProcessHandle {
            stdin_tx,
            progress_slot,
            cancel,
            session_id,
            last_result,
            last_system_status_output,
            last_usage_snapshot,
            generation,
            active_generation,
            interrupted_generation,
            next_control_request_id: AtomicU64::new(1),
            control_waiters,
            stderr_hint,
        };

        self.processes.insert(key.to_string(), handle);
        tracing::info!(pid, key, "spawned persistent claude process");
        Ok(())
    }
}

fn build_persistent_command(
    profile: &CliProfile,
    session_id: Option<&str>,
    working_dir: Option<&Path>,
    system_prompt: Option<&str>,
    mcp_config: Option<&Path>,
) -> Option<Command> {
    match profile {
        CliProfile::Claude {
            bin,
            model,
            extra_args,
            ..
        } => {
            let mut cmd = Command::new(bin);
            let mut skip_next = false;
            for (i, arg) in extra_args.iter().enumerate() {
                if skip_next {
                    skip_next = false;
                    continue;
                }
                if arg == "--settings"
                    && !extra_args
                        .get(i + 1)
                        .is_some_and(|p| std::path::Path::new(p).exists())
                {
                    skip_next = true;
                    continue;
                }
                cmd.arg(arg);
            }
            // Resume previous session if available — preserves context across pool restarts
            if let Some(sid) = session_id.filter(|s| !s.is_empty()) {
                cmd.arg("--resume").arg(sid);
            }
            cmd.arg("--input-format").arg("stream-json");
            cmd.arg("--output-format").arg("stream-json");
            cmd.arg("--verbose");
            cmd.arg("--include-partial-messages");
            cmd.arg("--include-hook-events");
            cmd.arg("--dangerously-skip-permissions");
            if let Some(m) = model {
                cmd.arg("--model").arg(m);
            }
            if let Some(sp) = system_prompt {
                cmd.arg("--append-system-prompt").arg(sp);
            }
            if let Some(mcp) = mcp_config {
                cmd.arg("--mcp-config").arg(mcp);
            }
            if let Some(dir) = working_dir {
                cmd.current_dir(dir);
            }
            Some(cmd)
        }
        _ => None,
    }
}

async fn stdin_writer_task(
    mut stdin: tokio::process::ChildStdin,
    mut rx: mpsc::Receiver<StdinCommand>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    Some(StdinCommand::UserMessage(line)) => {
                        if stdin.write_all(line.as_bytes()).await.is_err() {
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                    Some(StdinCommand::Interrupt { request_id }) => {
                        let line = claude_interrupt_control_request(&request_id);
                        if let Err(e) = stdin.write_all(line.as_bytes()).await {
                            tracing::warn!(error = %e, "failed to write interrupt to stdin");
                            break;
                        }
                        let _ = stdin.flush().await;
                    }
                    None => break,
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}

fn claude_interrupt_control_request(request_id: &str) -> String {
    format!(
        "{}\n",
        serde_json::json!({
            "type": "control_request",
            "request_id": request_id,
            "request": {"subtype": "interrupt"},
        })
    )
}

fn claude_control_response(value: &serde_json::Value) -> Option<(&str, Result<(), String>)> {
    if value["type"].as_str() != Some("control_response") {
        return None;
    }
    let response = &value["response"];
    let request_id = response["request_id"].as_str()?;
    let result = if response["subtype"].as_str() == Some("success") {
        Ok(())
    } else {
        Err(response["error"]
            .as_str()
            .unwrap_or("Claude rejected the control request")
            .to_string())
    };
    Some((request_id, result))
}

fn mark_result_interrupted(result: &mut CliResult) {
    result.provider_error = None;
    result.success = false;
    result.error_kind = Some(CLAUDE_INTERRUPTED_ERROR_KIND.to_string());
}

fn apply_acknowledged_interrupt(
    result: &mut CliResult,
    turn_generation: u64,
    interrupted_generation: &AtomicU64,
) {
    let was_interrupted = interrupted_generation
        .compare_exchange(turn_generation, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok();
    if was_interrupted && !result.success {
        mark_result_interrupted(result);
    }
}

fn acknowledge_interrupt(
    result: Result<(), String>,
    turn_generation: u64,
    active_generation: &AtomicU64,
    interrupted_generation: &AtomicU64,
) -> Result<(), String> {
    result?;
    active_generation
        .compare_exchange(turn_generation, 0, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| "no matching active Claude turn".to_string())?;
    interrupted_generation.store(turn_generation, Ordering::Release);
    Ok(())
}

async fn stdout_reader_task(
    stdout: tokio::process::ChildStdout,
    state: StdoutState,
    cancel: CancellationToken,
) {
    let StdoutState {
        progress_slot,
        session_id,
        last_result,
        last_system_status_output,
        generation,
        active_generation,
        interrupted_generation,
        control_waiters,
    } = state;
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    #[allow(unused_assignments)]
    let mut current_gen = 0u64;

    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        // Track latest generation before processing
                        current_gen = generation.load(Ordering::Relaxed);

                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
                            if let Some((request_id, result)) = claude_control_response(&v) {
                                if let Some(waiter) =
                                    control_waiters.lock().await.remove(request_id)
                                {
                                    let result = acknowledge_interrupt(
                                        result,
                                        waiter.turn_generation,
                                        &active_generation,
                                        &interrupted_generation,
                                    );
                                    let _ = waiter.response_tx.send(result);
                                }
                                continue;
                            }
                            // Check for result event (turn complete)
                            if v["type"].as_str() == Some("result") {
                                let _ = active_generation.compare_exchange(
                                    current_gen,
                                    0,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                );
                                if let Some(sid) = v["session_id"].as_str() {
                                    *session_id.lock().await = Some(sid.to_string());
                                }
                                *last_result.lock().await = Some(v);
                                // Only clear slot if generation hasn't advanced
                                let mut slot = progress_slot.lock().await;
                                if slot.0 == current_gen {
                                    slot.1 = None;
                                }
                                continue;
                            }
                            if let Some(output) = cli_bridge::claude_system_status_output(&v) {
                                *last_system_status_output.lock().await = Some(output);
                                continue;
                            }
                        }

                        // Parse as CliProgress and forward to current turn
                        if let Some(ev) = cli_bridge::parse_stdout_jsonl_line(trimmed, "claude") {
                            let tx = {
                                let slot = progress_slot.lock().await;
                                slot.1.clone()
                            };
                            if let Some(tx) = tx {
                                let _ = tx.send(ev).await;
                            }
                        }
                    }
                    Ok(None) => {
                        let mut slot = progress_slot.lock().await;
                        slot.1 = None;
                        drop(slot);
                        cancel.cancel();
                        break;
                    }
                    Err(_) => {
                        cancel.cancel();
                        break;
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
    for (_, waiter) in control_waiters.lock().await.drain() {
        let _ = waiter
            .response_tx
            .send(Err("Claude process closed".to_string()));
    }
}

async fn stderr_drainer_task(
    stderr: tokio::process::ChildStderr,
    stderr_hint: Arc<Mutex<Option<String>>>,
    cancel: CancellationToken,
) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) if !line.trim().is_empty() => {
                        tracing::debug!(line = %line, "persistent claude stderr");
                        if line.contains("No conversation found")
                            || line.contains("session not found")
                        {
                            *stderr_hint.lock().await = Some(line.clone());
                        }
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
    fn interrupt_uses_claude_control_protocol_envelope() {
        let value: serde_json::Value =
            serde_json::from_str(claude_interrupt_control_request("request-7").trim()).unwrap();

        assert_eq!(value["type"], "control_request");
        assert_eq!(value["request_id"], "request-7");
        assert_eq!(value["request"]["subtype"], "interrupt");
    }

    #[test]
    fn claude_control_response_preserves_acknowledgement() {
        let success = serde_json::json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": "request-7",
                "response": {},
            },
        });
        let rejected = serde_json::json!({
            "type": "control_response",
            "response": {
                "subtype": "error",
                "request_id": "request-8",
                "error": "no active turn",
            },
        });

        assert_eq!(
            claude_control_response(&success),
            Some(("request-7", Ok(())))
        );
        assert_eq!(
            claude_control_response(&rejected),
            Some(("request-8", Err("no active turn".to_string())))
        );
    }

    #[test]
    fn acknowledged_interrupt_is_not_a_provider_error() {
        let active_generation = AtomicU64::new(7);
        let interrupted_generation = AtomicU64::new(0);
        assert_eq!(
            acknowledge_interrupt(Ok(()), 7, &active_generation, &interrupted_generation),
            Ok(())
        );
        assert_eq!(active_generation.load(Ordering::Acquire), 0);
        assert_eq!(interrupted_generation.load(Ordering::Acquire), 7);

        let mut result = cli_bridge::parse_claude_result_value(
            &serde_json::json!({
                "type": "result",
                "subtype": "error_during_execution",
                "is_error": true,
                "session_id": "session-1",
            }),
            0,
        );
        assert!(result.provider_error.is_some());

        apply_acknowledged_interrupt(&mut result, 7, &interrupted_generation);

        assert!(result.provider_error.is_none());
        assert!(!result.success);
        assert_eq!(
            result.error_kind.as_deref(),
            Some(CLAUDE_INTERRUPTED_ERROR_KIND)
        );
    }

    #[test]
    fn acknowledged_idle_race_preserves_successful_result() {
        let interrupted_generation = AtomicU64::new(7);
        let mut result = cli_bridge::parse_claude_result_value(
            &serde_json::json!({
                "type": "result",
                "subtype": "success",
                "is_error": false,
                "result": "completed normally",
                "session_id": "session-1",
            }),
            0,
        );

        apply_acknowledged_interrupt(&mut result, 7, &interrupted_generation);

        assert!(result.success);
        assert!(result.error_kind.is_none());
        assert!(result.provider_error.is_none());
        assert_eq!(interrupted_generation.load(Ordering::Acquire), 0);
    }

    #[test]
    fn idle_interrupt_ack_is_rejected_and_cannot_mark_a_result() {
        let active_generation = AtomicU64::new(0);
        let interrupted_generation = AtomicU64::new(0);

        assert_eq!(
            acknowledge_interrupt(Ok(()), 7, &active_generation, &interrupted_generation),
            Err("no matching active Claude turn".to_string())
        );
        assert_eq!(interrupted_generation.load(Ordering::Acquire), 0);
    }
}
