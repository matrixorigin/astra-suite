//! Long-lived Claude CLI process pool.
//!
//! Normal messages are sent via stdin to an existing process.
//! Special operations (model change, rewind, clear) kill and respawn.

use crate::cli_bridge::{self, CliProfile, CliProgress};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

type ConversationKey = String;

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
    /// Current turn generation — incremented on each begin_turn.
    generation: Arc<std::sync::atomic::AtomicU64>,
}

enum StdinCommand {
    UserMessage(String),
    Interrupt,
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
    pub async fn begin_turn(
        &mut self,
        key: &str,
        message: &str,
        profile: &CliProfile,
        working_dir: Option<&Path>,
        system_prompt: Option<&str>,
        access_token: Option<&str>,
    ) -> Result<mpsc::Receiver<CliProgress>, String> {
        if !self.processes.contains_key(key) || !self.is_alive(key) {
            self.processes.remove(key);
            self.spawn(key, profile, working_dir, system_prompt, access_token)
                .await?;
        }

        let handle = self.processes.get(key).unwrap();

        // Clear stale result from previous turn (e.g. after interrupt)
        *handle.last_result.lock().await = None;

        // Increment generation and register new progress channel
        let turn_gen = handle
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
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

        handle
            .stdin_tx
            .send(StdinCommand::UserMessage(json_line))
            .await
            .map_err(|_| "process stdin closed (process may have died)".to_string())?;

        Ok(progress_rx)
    }

    pub async fn interrupt(&self, key: &str) -> Result<(), String> {
        let handle = self
            .processes
            .get(key)
            .ok_or("no process for conversation")?;
        handle
            .stdin_tx
            .send(StdinCommand::Interrupt)
            .await
            .map_err(|_| "stdin closed".to_string())
    }

    pub fn kill(&mut self, key: &str) {
        if let Some(handle) = self.processes.remove(key) {
            handle.cancel.cancel();
        }
    }

    pub async fn session_id(&self, key: &str) -> Option<String> {
        self.processes.get(key)?.session_id.lock().await.clone()
    }

    /// Take the last `result` JSON from the process (consumed once per turn).
    pub async fn take_last_result(&self, key: &str) -> Option<serde_json::Value> {
        self.processes.get(key)?.last_result.lock().await.take()
    }

    fn is_alive(&self, key: &str) -> bool {
        self.processes
            .get(key)
            .map(|h| !h.cancel.is_cancelled())
            .unwrap_or(false)
    }

    async fn spawn(
        &mut self,
        key: &str,
        profile: &CliProfile,
        working_dir: Option<&Path>,
        system_prompt: Option<&str>,
        access_token: Option<&str>,
    ) -> Result<(), String> {
        let mut cmd = build_persistent_command(profile, working_dir, system_prompt)
            .ok_or("profile does not support persistent mode")?;

        if let Some(token) = access_token {
            cmd.env("ASTRA_ACCESS_TOKEN", token);
        }
        profile
            .apply_runtime_environment(&mut cmd)
            .map_err(|e| format!("failed to prepare CLI environment: {e}"))?;

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
        let generation = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Spawn stdin writer
        tokio::spawn(stdin_writer_task(stdin, stdin_rx, cancel.clone()));

        // Spawn stderr drainer
        tokio::spawn(stderr_drainer_task(stderr, cancel.clone()));

        // Spawn stdout reader — routes events to progress_slot
        tokio::spawn(stdout_reader_task(
            stdout,
            progress_slot.clone(),
            session_id.clone(),
            last_result.clone(),
            generation.clone(),
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
            generation,
        };

        self.processes.insert(key.to_string(), handle);
        tracing::info!(pid, key, "spawned persistent claude process");
        Ok(())
    }
}

fn build_persistent_command(
    profile: &CliProfile,
    working_dir: Option<&Path>,
    system_prompt: Option<&str>,
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
                    Some(StdinCommand::Interrupt) => {
                        if let Err(e) = stdin.write_all(b"{\"subtype\":\"interrupt\"}\n").await {
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

async fn stdout_reader_task(
    stdout: tokio::process::ChildStdout,
    progress_slot: Arc<Mutex<(u64, Option<mpsc::Sender<CliProgress>>)>>,
    session_id: Arc<Mutex<Option<String>>>,
    last_result: Arc<Mutex<Option<serde_json::Value>>>,
    generation: Arc<std::sync::atomic::AtomicU64>,
    cancel: CancellationToken,
) {
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
                        current_gen = generation.load(std::sync::atomic::Ordering::Relaxed);

                        // Check for result event (turn complete)
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed)
                            && v["type"].as_str() == Some("result")
                        {
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
}

async fn stderr_drainer_task(stderr: tokio::process::ChildStderr, cancel: CancellationToken) {
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();
    loop {
        tokio::select! {
            line = lines.next_line() => {
                match line {
                    Ok(Some(line)) if !line.trim().is_empty() => {
                        tracing::debug!(line = %line, "persistent claude stderr");
                    }
                    Ok(None) | Err(_) => break,
                    _ => {}
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
}
