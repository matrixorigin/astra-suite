//! CLI bridge — spawn any coding agent CLI per message.
//!
//! Supports multiple CLI backends (astra, claude, copilot, codex, custom) via CliProfile.
//! Each profile defines how to construct the command, parse the output,
//! and extract session/text/metadata.

use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

/// Kill guard: sends SIGKILL to a child process on drop. Defuse with
/// `.defuse()` when the process exits normally. Without this, an async
/// cancellation (outer task abort) would orphan the child process since
/// tokio's `Child::drop` does NOT kill the process.
pub(crate) struct ChildKillGuard {
    pid: Option<u32>,
}

impl ChildKillGuard {
    pub(crate) fn new(child: &tokio::process::Child) -> Self {
        Self { pid: child.id() }
    }

    pub(crate) fn defuse(&mut self) {
        self.pid = None;
    }

    #[cfg(test)]
    pub(crate) fn with_pid(pid: u32) -> Self {
        Self { pid: Some(pid) }
    }

    #[cfg(test)]
    pub(crate) fn is_defused(&self) -> bool {
        self.pid.is_none()
    }
}

impl Drop for ChildKillGuard {
    fn drop(&mut self) {
        if let Some(pid) = self.pid
            && let Ok(pid_i32) = i32::try_from(pid)
            && pid_i32 > 1
        {
            unsafe {
                libc::kill(pid_i32, libc::SIGKILL);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum CliProgress {
    Status(String),
    ToolCall(String),
    Stderr(String),
    /// Streamed text token from LLM (via --stream-events JSONL).
    Token(String),
    /// Tool execution started.
    ToolStarted {
        name: String,
        /// Brief description of tool params (extracted from assistant message).
        params: Option<String>,
    },
    /// Tool execution completed.
    ToolDone {
        name: String,
        /// Duration in milliseconds. `None` when unknown (e.g. stream-json
        /// where completion events don't carry timing).
        duration_ms: Option<u64>,
    },
    /// Thinking state changed.
    Thinking(bool),
    /// Reasoning/thinking text explicitly emitted by the underlying CLI.
    ReasoningBlock {
        kind: ReasoningKind,
        text: String,
    },
    /// Tool approval requested by a persistent app-server backend.
    ApprovalRequested {
        id: String,
        tool: String,
        header: String,
        detail: Option<String>,
        reason: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningKind {
    Raw,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningDisplay {
    Off,
    RawIfAvailable,
}

pub const REASONING_PREF_KEY: &str = "reasoning_display";

impl ReasoningDisplay {
    pub fn from_pref(value: Option<&str>) -> Self {
        match value
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "on" | "raw" | "raw-if-available" | "raw_if_available" | "thinking"
            | "thinking-chain" | "thinking_chain" | "reasoning" => Self::RawIfAvailable,
            _ => Self::Off,
        }
    }

    pub fn from_command_arg(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "on" | "raw" | "raw-if-available" | "raw_if_available" | "thinking"
            | "thinking-chain" | "thinking_chain" | "reasoning" => Some(Self::RawIfAvailable),
            "off" | "none" | "false" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn as_pref(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::RawIfAvailable => "raw-if-available",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::RawIfAvailable => "raw-if-available",
        }
    }

    pub fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderError {
    pub kind: String,
    pub status: Option<u16>,
    pub code: Option<String>,
    pub message: String,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ClaudeUsageSnapshot {
    tokens_prompt: Option<u64>,
    tokens_completion: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cost_usd: Option<f64>,
}

#[derive(Debug)]
pub struct CliResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub success: bool,
    pub error_kind: Option<String>,
    pub provider_error: Option<ProviderError>,
    pub trace_id: Option<String>,
    pub request_id: Option<String>,
    pub run_id: Option<String>,
    pub session_id: Option<String>,
    pub text: Option<String>,
    pub tool_calls_count: Option<u32>,
    pub tools_used: Vec<String>,
    pub tokens_prompt: Option<u64>,
    pub tokens_completion: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
    pub reasoning_output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub context_window: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub cost_usd: Option<f64>,
    pub raw_usage_json: Option<String>,
}

// ─── CLI Profile ────────────────────────────────────────────────────────────

/// Defines how to invoke a specific CLI agent.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(tag = "type")]
pub enum CliProfile {
    #[serde(rename = "astra")]
    Astra {
        #[serde(default = "default_astra_bin")]
        bin: String,
        model: Option<String>,
        #[serde(default = "default_permission_mode")]
        permission_mode: String,
        #[serde(default)]
        app_server_url: Option<String>,
    },
    #[serde(rename = "claude")]
    Claude {
        #[serde(default = "default_claude_bin")]
        bin: String,
        model: Option<String>,
        /// Use `--output-format stream-json` for real-time token/tool events on stdout.
        /// Defaults to true; set to false to fall back to a single end-of-run JSON blob.
        #[serde(default = "default_true")]
        stream_json: bool,
        /// Extra args appended to the claude invocation before the prompt flag.
        /// Example: ["--settings", "/path/to/hooks.json"]
        #[serde(default)]
        extra_args: Vec<String>,
        /// Environment variables injected into the spawned `claude` process.
        /// Useful for Bedrock / Anthropic-compat backends without leaking
        /// secrets into the parent shell. Values from `env` override `env_file`.
        #[serde(default)]
        env: std::collections::BTreeMap<String, String>,
        /// Optional dotenv-style KEY=VALUE file. Loaded before `env` overrides.
        #[serde(default)]
        env_file: Option<String>,
    },
    #[serde(rename = "copilot")]
    Copilot {
        #[serde(default = "default_copilot_bin")]
        bin: String,
        model: Option<String>,
        /// Environment variables injected into this CLI process. These are
        /// explicit and reproducible; prefer them over shell startup files
        /// when only env is needed.
        #[serde(default)]
        env: std::collections::BTreeMap<String, String>,
        /// Optional dotenv-style KEY=VALUE file. Values from `env` override
        /// values from this file.
        env_file: Option<String>,
        /// Optional launcher for non-binary commands, such as shell functions.
        /// Default is direct binary execution.
        launcher: Option<CliLauncher>,
        /// Use Copilot's JSONL output so gateway can extract session, text,
        /// usage, and tool events. Disable only for troubleshooting.
        #[serde(default = "default_true")]
        stream_json: bool,
        /// Non-interactive Copilot needs pre-approved tools or it may block.
        #[serde(default = "default_true")]
        allow_all_tools: bool,
        /// Extra args appended after gateway defaults.
        #[serde(default)]
        extra_args: Vec<String>,
    },
    #[serde(rename = "codex")]
    Codex {
        #[serde(default = "default_codex_bin")]
        bin: String,
        model: Option<String>,
        /// Optional default reasoning effort. A `/model` selection can override
        /// this per conversation.
        #[serde(default)]
        reasoning_effort: Option<String>,
        /// Sandbox policy: "read-only" | "workspace-write" | "danger-full-access".
        #[serde(default = "default_codex_sandbox")]
        sandbox: String,
        /// Use `--json` for JSONL streaming output in exec mode.
        /// Codex `--json` is inherently streaming (each event prints as it occurs).
        #[serde(default)]
        stream_json: bool,
        /// Extra args appended to the codex invocation before the prompt.
        #[serde(default)]
        extra_args: Vec<String>,
        /// Skip git repo check (useful for non-git directories).
        #[serde(default)]
        skip_git_repo_check: bool,
        /// Ephemeral mode: don't persist session files to disk.
        #[serde(default)]
        ephemeral: bool,
    },
    #[serde(rename = "custom")]
    Custom {
        bin: String,
        #[serde(default)]
        args_template: Vec<String>,
        #[serde(default)]
        json_output: bool,
        session_id_field: Option<String>,
        text_field: Option<String>,
    },
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CliLauncher {
    Script {
        /// Script path. The script receives `<bin>` as argv[1], then the
        /// gateway-generated CLI args. It may ignore `<bin>` and exec any
        /// provider-specific command.
        path: String,
        /// Optional launcher-specific args inserted before `<bin>`.
        #[serde(default)]
        args: Vec<String>,
    },
}

fn default_astra_bin() -> String {
    "astra".into()
}
fn default_claude_bin() -> String {
    "claude".into()
}
fn default_copilot_bin() -> String {
    "copilot".into()
}
fn default_codex_bin() -> String {
    "codex".into()
}
fn default_true() -> bool {
    true
}
fn default_permission_mode() -> String {
    "auto".into()
}
fn default_codex_sandbox() -> String {
    "workspace-write".into()
}

fn build_command_launcher(bin: &str, launcher: Option<&CliLauncher>) -> Command {
    match launcher {
        Some(CliLauncher::Script { path, args }) => build_script_launcher(bin, path, args),
        None => Command::new(bin),
    }
}

fn build_script_launcher(bin: &str, path: &str, args: &[String]) -> Command {
    let mut cmd = Command::new(expand_config_path(path));
    for arg in args {
        cmd.arg(arg);
    }
    cmd.arg(bin);
    cmd
}

fn expand_config_path(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return std::path::PathBuf::from(home)
            .join(rest)
            .to_string_lossy()
            .to_string();
    }
    path.to_string()
}

impl Default for CliProfile {
    fn default() -> Self {
        Self::Astra {
            bin: default_astra_bin(),
            model: None,
            permission_mode: default_permission_mode(),
            app_server_url: None,
        }
    }
}

/// What this CLI can do — gateway adapts behavior accordingly.
#[derive(Debug, Clone)]
pub struct CliCapabilities {
    pub supports_session: bool,
    pub supports_model_switch: bool,
    pub supports_json_output: bool,
    pub supports_harness: bool,
    pub supports_tools: bool,
}

impl CliProfile {
    pub fn capabilities(&self) -> CliCapabilities {
        match self {
            Self::Astra { .. } => CliCapabilities {
                supports_session: true,
                supports_model_switch: true,
                supports_json_output: true,
                supports_harness: true,
                supports_tools: true,
            },
            Self::Claude { .. } => CliCapabilities {
                supports_session: true,
                supports_model_switch: true,
                supports_json_output: true,
                supports_harness: false,
                supports_tools: true,
            },
            Self::Copilot { .. } => CliCapabilities {
                supports_session: true,
                supports_model_switch: true,
                supports_json_output: true,
                supports_harness: false,
                supports_tools: true,
            },
            Self::Codex { .. } => CliCapabilities {
                supports_session: true,
                supports_model_switch: true,
                supports_json_output: true,
                supports_harness: false,
                supports_tools: true,
            },
            Self::Custom { json_output, .. } => CliCapabilities {
                supports_session: false,
                supports_model_switch: false,
                supports_json_output: *json_output,
                supports_harness: false,
                supports_tools: false,
            },
        }
    }

    /// Build the command to execute for a given message.
    pub fn build_command(
        &self,
        message: &str,
        session_id: Option<&str>,
        working_dir: Option<&std::path::Path>,
    ) -> Command {
        self.build_command_with_context(message, session_id, working_dir, None)
    }

    /// Build the command with optional gateway context injected as system prompt.
    pub fn build_command_with_context(
        &self,
        message: &str,
        session_id: Option<&str>,
        working_dir: Option<&std::path::Path>,
        system_prompt: Option<&str>,
    ) -> Command {
        match self {
            Self::Astra {
                bin,
                model,
                permission_mode,
                app_server_url,
            } => {
                let mut cmd = Command::new(bin);
                // Ensure astra CLI connects directly to local server, not via HTTP proxy
                cmd.env("no_proxy", "127.0.0.1,localhost");
                if let Some(url) = app_server_url
                    .as_deref()
                    .filter(|url| !url.trim().is_empty())
                {
                    cmd.env("ASTRA_API_URL", url);
                }
                cmd.arg("chat")
                    .arg("-m")
                    .arg(message)
                    .arg("--json")
                    .arg("--quiet")
                    .arg("--stream-events")
                    .arg("--permission-mode")
                    .arg(permission_mode);
                if let Some(sid) = session_id {
                    cmd.arg("--session-id").arg(sid);
                }
                if let Some(m) = model {
                    let m = m.strip_prefix("@taas/").unwrap_or(m);
                    cmd.arg("--model").arg(m);
                }
                if let Some(sp) = system_prompt {
                    cmd.arg("--append-system-prompt").arg(sp);
                }
                if let Some(dir) = working_dir {
                    cmd.current_dir(dir);
                }
                cmd
            }
            Self::Claude {
                bin,
                model,
                stream_json,
                extra_args,
                env: _,
                env_file: _,
            } => {
                let mut cmd = Command::new(bin);
                // Extra args (e.g. --settings for hook injection).
                // Skip --settings if the referenced file doesn't exist.
                let mut skip_next = false;
                for (i, arg) in extra_args.iter().enumerate() {
                    if skip_next {
                        skip_next = false;
                        continue;
                    }
                    if arg == "--settings"
                        && let Some(path) = extra_args.get(i + 1)
                        && !std::path::Path::new(path).exists()
                    {
                        tracing::warn!(path = %path, "skipping --settings: file not found");
                        skip_next = true;
                        continue;
                    }
                    cmd.arg(arg);
                }
                cmd.arg("-p").arg(message);
                if *stream_json {
                    cmd.arg("--output-format")
                        .arg("stream-json")
                        .arg("--verbose")
                        .arg("--include-partial-messages")
                        .arg("--include-hook-events");
                } else {
                    cmd.arg("--output-format").arg("json");
                }
                cmd.arg("--dangerously-skip-permissions");
                if let Some(sid) = session_id {
                    cmd.arg("--resume").arg(sid);
                }
                if let Some(m) = model {
                    cmd.arg("--model").arg(m);
                }
                if let Some(sp) = system_prompt {
                    cmd.arg("--append-system-prompt").arg(sp);
                }
                if let Some(dir) = working_dir {
                    cmd.current_dir(dir);
                }
                self.apply_inline_env(&mut cmd);
                cmd
            }
            Self::Copilot {
                bin,
                model,
                env: _,
                env_file: _,
                launcher,
                stream_json,
                allow_all_tools,
                extra_args,
            } => {
                let mut cmd = build_command_launcher(bin, launcher.as_ref());
                let prompt = if let Some(sp) = system_prompt {
                    format!("{sp}\n\nUser message:\n{message}")
                } else {
                    message.to_string()
                };
                cmd.arg("-p").arg(prompt).arg("--silent").arg("--no-color");
                if *stream_json {
                    cmd.arg("--output-format").arg("json");
                } else {
                    cmd.arg("--output-format")
                        .arg("text")
                        .arg("--stream")
                        .arg("off");
                }
                if *allow_all_tools {
                    cmd.arg("--allow-all-tools");
                }
                if let Some(sid) = session_id {
                    cmd.arg(format!("--resume={sid}"));
                }
                if let Some(m) = model {
                    cmd.arg("--model").arg(m);
                }
                for arg in extra_args {
                    cmd.arg(arg);
                }
                if let Some(dir) = working_dir {
                    cmd.current_dir(dir);
                }
                self.apply_inline_env(&mut cmd);
                cmd
            }
            Self::Codex {
                bin,
                model,
                sandbox: _,
                stream_json,
                extra_args,
                skip_git_repo_check,
                ephemeral,
                ..
            } => {
                let mut cmd = Command::new(bin);
                if let Some(sid) = session_id {
                    cmd.arg("exec").arg("resume").arg(sid);
                    if !message.is_empty() {
                        cmd.arg(message);
                    }
                } else {
                    cmd.arg("exec").arg(message);
                }
                if *stream_json {
                    cmd.arg("--json");
                }
                if *skip_git_repo_check {
                    cmd.arg("--skip-git-repo-check");
                }
                if *ephemeral {
                    cmd.arg("--ephemeral");
                }
                if let Some(m) = model {
                    cmd.arg("--model").arg(m);
                }
                for arg in extra_args {
                    cmd.arg(arg);
                }
                if let Some(effort) = self.reasoning_effort() {
                    cmd.arg("-c")
                        .arg(format!("model_reasoning_effort={effort:?}"));
                }
                if let Some(dir) = working_dir {
                    cmd.current_dir(dir);
                }
                cmd
            }
            Self::Custom {
                bin, args_template, ..
            } => {
                let mut cmd = Command::new(bin);
                for arg in args_template {
                    let replaced = arg
                        .replace("{message}", message)
                        .replace("{session_id}", session_id.unwrap_or(""));
                    cmd.arg(replaced);
                }
                if let Some(dir) = working_dir {
                    cmd.current_dir(dir);
                }
                cmd
            }
        }
    }

    /// Parse stdout into structured result.
    pub fn parse_output(&self, stdout: &str, exit_code: i32) -> CliResult {
        match self {
            Self::Astra { .. } => parse_astra_json(stdout, exit_code),
            Self::Claude { stream_json, .. } => {
                if *stream_json {
                    parse_claude_stream_json_stdout(stdout, exit_code)
                } else {
                    parse_claude_json(stdout, exit_code)
                }
            }
            Self::Copilot { stream_json, .. } => {
                if *stream_json {
                    parse_copilot_jsonl(stdout, exit_code)
                } else {
                    plain_result(stdout, exit_code)
                }
            }
            Self::Codex { stream_json, .. } => {
                if *stream_json {
                    parse_codex_stream_json_stdout(stdout, exit_code)
                } else {
                    parse_generic_json(stdout, exit_code, "result", "session_id")
                }
            }
            Self::Custom {
                json_output,
                text_field,
                session_id_field,
                ..
            } => {
                if *json_output {
                    parse_generic_json(
                        stdout,
                        exit_code,
                        text_field.as_deref().unwrap_or("text"),
                        session_id_field.as_deref().unwrap_or("session_id"),
                    )
                } else {
                    CliResult {
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code,
                        success: exit_code == 0,
                        error_kind: default_error_kind(exit_code),
                        provider_error: None,
                        trace_id: None,
                        request_id: None,
                        run_id: None,
                        session_id: None,
                        text: Some(stdout.to_string()),
                        tool_calls_count: None,
                        tools_used: Vec::new(),
                        tokens_prompt: None,
                        tokens_completion: None,
                        cached_input_tokens: None,
                        cache_creation_input_tokens: None,
                        cache_read_input_tokens: None,
                        reasoning_output_tokens: None,
                        total_tokens: None,
                        context_window: None,
                        max_output_tokens: None,
                        cost_usd: None,
                        raw_usage_json: None,
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Astra { .. } => "astra",
            Self::Claude { .. } => "claude",
            Self::Copilot { .. } => "copilot",
            Self::Codex { .. } => "codex",
            Self::Custom { bin, .. } => bin,
        }
    }

    pub fn is_stale_session_error(&self, stderr: &str) -> bool {
        match self {
            Self::Claude { .. } => {
                stderr.contains("No conversation found with session ID")
                    || stderr.contains("No conversation found")
                    || stderr.contains("session not found")
            }
            Self::Codex { .. } => stderr.contains("no rollout found for thread id"),
            _ => false,
        }
    }

    fn probe_cache_key(&self) -> String {
        match self {
            Self::Astra {
                bin,
                permission_mode,
                ..
            } => format!("astra:{bin}:permission={permission_mode}"),
            Self::Claude {
                bin,
                stream_json,
                extra_args,
                env,
                env_file,
                ..
            } => {
                let env_keys: Vec<&String> = env.keys().collect();
                format!(
                    "claude:{bin}:stream={stream_json}:extra={extra_args:?}:\
                     env_file={env_file:?}:env_keys={env_keys:?}"
                )
            }
            Self::Copilot {
                bin,
                env,
                env_file,
                launcher,
                stream_json,
                allow_all_tools,
                extra_args,
                ..
            } => {
                let env_keys: Vec<&String> = env.keys().collect();
                format!(
                    "copilot:{bin}:stream={stream_json}:allow_all={allow_all_tools}:\
                     extra={extra_args:?}:env_file={env_file:?}:env_keys={env_keys:?}:\
                     launcher={launcher:?}"
                )
            }
            Self::Codex {
                bin,
                sandbox,
                stream_json,
                ..
            } => {
                format!("codex:{bin}:sandbox={sandbox}:stream={stream_json}")
            }
            Self::Custom {
                bin,
                args_template,
                json_output,
                ..
            } => format!("custom:{bin}:json={json_output}:args={args_template:?}"),
        }
    }

    pub fn model_name(&self) -> Option<&str> {
        match self {
            Self::Astra { model, .. }
            | Self::Claude { model, .. }
            | Self::Copilot { model, .. }
            | Self::Codex { model, .. } => model.as_deref(),
            _ => None,
        }
    }

    pub fn set_model_override(&mut self, model_name: String) {
        match self {
            Self::Astra { model, .. }
            | Self::Claude { model, .. }
            | Self::Copilot { model, .. }
            | Self::Codex { model, .. } => {
                *model = Some(model_name);
            }
            _ => {}
        }
    }

    pub fn set_reasoning_effort_override(&mut self, effort: Option<String>) {
        if let Self::Codex {
            reasoning_effort, ..
        } = self
        {
            *reasoning_effort = effort;
        }
    }

    pub fn reasoning_effort(&self) -> Option<&str> {
        match self {
            Self::Codex {
                reasoning_effort, ..
            } => reasoning_effort.as_deref(),
            _ => None,
        }
    }

    pub fn app_server_url(&self) -> Option<&str> {
        match self {
            Self::Astra { app_server_url, .. } => app_server_url.as_deref(),
            _ => None,
        }
    }

    fn apply_inline_env(&self, cmd: &mut Command) {
        match self {
            Self::Copilot { env, .. } | Self::Claude { env, .. } => {
                for (key, value) in env {
                    cmd.env(key, value);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn apply_runtime_environment(&self, cmd: &mut Command) -> Result<(), String> {
        let (env_file, env) = match self {
            Self::Copilot { env_file, env, .. } | Self::Claude { env_file, env, .. } => {
                (env_file, env)
            }
            _ => return Ok(()),
        };
        if let Some(path) = env_file.as_deref().filter(|p| !p.trim().is_empty()) {
            for (key, value) in load_env_file(path)? {
                cmd.env(key, value);
            }
        }
        for (key, value) in env {
            cmd.env(key, value);
        }
        Ok(())
    }
}

fn load_env_file(path: &str) -> Result<Vec<(String, String)>, String> {
    let expanded = expand_config_path(path);
    let iter = dotenvy::from_path_iter(&expanded)
        .map_err(|e| format!("load env_file `{expanded}`: {e}"))?;
    let mut vars = Vec::new();
    for entry in iter {
        let (key, value) = entry.map_err(|e| format!("parse env_file `{expanded}`: {e}"))?;
        vars.push((key, value));
    }
    Ok(vars)
}

/// Apply provider-specific environment variables on top of the CLI profile env.
pub(crate) fn apply_provider_environment(
    cmd: &mut Command,
    pc: &crate::config::ProviderConfig,
) -> Result<(), String> {
    if let Some(path) = pc.env_file.as_deref().filter(|p| !p.trim().is_empty()) {
        for (key, value) in load_env_file(path)? {
            cmd.env(key, value);
        }
    }
    for (key, value) in &pc.env {
        cmd.env(key, value);
    }
    Ok(())
}

pub(crate) fn provider_for_cli_profile(
    config: &crate::config::GatewayConfig,
    profile: &CliProfile,
) -> Option<crate::config::ProviderConfig> {
    if !matches!(profile, CliProfile::Claude { .. }) || profile_has_own_provider_env(profile) {
        return None;
    }

    const PREFERRED_ANTHROPIC_PROVIDERS: &[&str] = &["dashscope", "bedrock"];

    PREFERRED_ANTHROPIC_PROVIDERS
        .iter()
        .filter_map(|name| config.providers.get(*name))
        .chain(
            config
                .providers
                .iter()
                .filter(|(name, _)| !PREFERRED_ANTHROPIC_PROVIDERS.contains(&name.as_str()))
                .map(|(_, provider)| provider),
        )
        .find(|provider| provider.enabled && provider_has_anthropic_env(provider))
        .cloned()
}

fn profile_has_own_provider_env(profile: &CliProfile) -> bool {
    match profile {
        CliProfile::Claude { env, env_file, .. } | CliProfile::Copilot { env, env_file, .. } => {
            env_file
                .as_deref()
                .is_some_and(|path| !path.trim().is_empty())
                || env.keys().any(|key| is_anthropic_env_key(key))
        }
        _ => false,
    }
}

fn provider_has_anthropic_env(provider: &crate::config::ProviderConfig) -> bool {
    provider.env.keys().any(|key| is_anthropic_env_key(key))
        || provider
            .env_file
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty())
}

fn is_anthropic_env_key(key: &str) -> bool {
    matches!(
        key,
        "ANTHROPIC_BASE_URL"
            | "ANTHROPIC_AUTH_TOKEN"
            | "ANTHROPIC_API_KEY"
            | "ANTHROPIC_MODEL"
            | "ANTHROPIC_DEFAULT_OPUS_MODEL"
            | "ANTHROPIC_DEFAULT_SONNET_MODEL"
            | "ANTHROPIC_DEFAULT_HAIKU_MODEL"
            | "ANTHROPIC_DEFAULT_FABLE_MODEL"
            | "CLAUDE_CODE_USE_BEDROCK"
            | "AWS_REGION"
            | "AWS_BEARER_TOKEN_BEDROCK"
    )
}

// ─── JSON parsers ───────────────────────────────────────────────────────────

const ASTRA_REQUIRED_FIELDS: &[&str] = &[
    "trace_id",
    "request_id",
    "run_id",
    "session_id",
    "text",
    "prompt_tokens",
    "completion_tokens",
    "tool_calls_count",
    "tools_used",
    "exit_code",
    "success",
    "error_kind",
];

fn parse_astra_json(stdout: &str, exit_code: i32) -> CliResult {
    match serde_json::from_str::<serde_json::Value>(stdout) {
        Ok(v) => parse_strict_astra_envelope(&v, exit_code)
            .unwrap_or_else(|reason| malformed_astra_result(exit_code, reason)),
        Err(e) => malformed_astra_result(exit_code, format!("invalid JSON: {e}")),
    }
}

fn parse_strict_astra_envelope(
    v: &serde_json::Value,
    fallback_exit_code: i32,
) -> Result<CliResult, String> {
    let obj = v
        .as_object()
        .ok_or_else(|| "envelope must be a JSON object".to_string())?;
    for field in ASTRA_REQUIRED_FIELDS {
        if !obj.contains_key(*field) {
            return Err(format!("missing required field `{field}`"));
        }
    }

    let exit_code = required_i32(v, "exit_code")?.unwrap_or(fallback_exit_code);
    let tools_used = v["tools_used"]
        .as_array()
        .ok_or_else(|| "`tools_used` must be an array".to_string())?
        .iter()
        .map(|tool| {
            tool.as_str()
                .map(String::from)
                .ok_or_else(|| "`tools_used` entries must be strings".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;

    let success = required_bool(v, "success")?;
    let error_kind = required_nullable_string(v, "error_kind")?;
    if success && error_kind.is_some() {
        return Err("`error_kind` must be null when `success` is true".to_string());
    }
    if !success && error_kind.is_none() {
        return Err("`error_kind` must be a string when `success` is false".to_string());
    }
    if success != (exit_code == 0) {
        return Err("`success` must match whether `exit_code` is zero".to_string());
    }

    Ok(CliResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code,
        success,
        error_kind,
        provider_error: None,
        trace_id: required_nullable_string(v, "trace_id")?,
        request_id: required_nullable_string(v, "request_id")?,
        run_id: required_nullable_string(v, "run_id")?,
        session_id: required_nullable_string(v, "session_id")?,
        text: Some(required_string(v, "text")?),
        tool_calls_count: Some(required_u32(v, "tool_calls_count")?),
        tools_used,
        tokens_prompt: Some(required_u64(v, "prompt_tokens")?),
        tokens_completion: Some(required_u64(v, "completion_tokens")?),
        cached_input_tokens: v["cached_input_tokens"].as_u64(),
        cache_creation_input_tokens: v["cache_creation_input_tokens"].as_u64(),
        cache_read_input_tokens: v["cache_read_input_tokens"].as_u64(),
        reasoning_output_tokens: v["reasoning_output_tokens"].as_u64(),
        total_tokens: v["total_tokens"].as_u64(),
        context_window: v["context_window"].as_u64(),
        max_output_tokens: v["max_output_tokens"].as_u64(),
        cost_usd: v["cost_usd"].as_f64(),
        raw_usage_json: v.get("usage").and_then(raw_json),
    })
}

fn required_nullable_string(v: &serde_json::Value, field: &str) -> Result<Option<String>, String> {
    match &v[field] {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(s) => Ok(Some(s.clone())),
        _ => Err(format!("`{field}` must be a string or null")),
    }
}

fn required_string(v: &serde_json::Value, field: &str) -> Result<String, String> {
    v[field]
        .as_str()
        .map(String::from)
        .ok_or_else(|| format!("`{field}` must be a string"))
}

fn required_bool(v: &serde_json::Value, field: &str) -> Result<bool, String> {
    v[field]
        .as_bool()
        .ok_or_else(|| format!("`{field}` must be a boolean"))
}

fn required_i32(v: &serde_json::Value, field: &str) -> Result<Option<i32>, String> {
    let raw = v[field]
        .as_i64()
        .ok_or_else(|| format!("`{field}` must be an integer"))?;
    i32::try_from(raw)
        .map(Some)
        .map_err(|_| format!("`{field}` is outside i32 range"))
}

fn required_u64(v: &serde_json::Value, field: &str) -> Result<u64, String> {
    v[field]
        .as_u64()
        .ok_or_else(|| format!("`{field}` must be an unsigned integer"))
}

fn required_u32(v: &serde_json::Value, field: &str) -> Result<u32, String> {
    let raw = required_u64(v, field)?;
    u32::try_from(raw).map_err(|_| format!("`{field}` is outside u32 range"))
}

fn raw_json(value: &serde_json::Value) -> Option<String> {
    serde_json::to_string(value).ok()
}

fn sum_tokens(parts: &[Option<u64>]) -> Option<u64> {
    let mut total = 0u64;
    let mut any = false;
    for value in parts.iter().flatten() {
        total = total.saturating_add(*value);
        any = true;
    }
    any.then_some(total)
}

fn first_model_usage(v: &serde_json::Value) -> Option<&serde_json::Value> {
    v["modelUsage"].as_object()?.values().next()
}

fn cumulative_model_usage(v: &serde_json::Value) -> Option<ClaudeUsageSnapshot> {
    let models = v["modelUsage"].as_object()?;
    if models.is_empty() {
        return None;
    }

    let sum = |field: &str| {
        let values = models
            .values()
            .map(|usage| usage[field].as_u64())
            .collect::<Vec<_>>();
        sum_tokens(&values)
    };

    Some(ClaudeUsageSnapshot {
        tokens_prompt: sum("inputTokens"),
        tokens_completion: sum("outputTokens"),
        cache_creation_input_tokens: sum("cacheCreationInputTokens"),
        cache_read_input_tokens: sum("cacheReadInputTokens"),
        cost_usd: v["total_cost_usd"].as_f64().or_else(|| {
            let costs = models
                .values()
                .filter_map(|usage| usage["costUSD"].as_f64())
                .collect::<Vec<_>>();
            (!costs.is_empty()).then(|| costs.into_iter().sum())
        }),
    })
}

fn parse_api_error_payload(text: &str) -> Option<serde_json::Value> {
    let data = text
        .lines()
        .find_map(|line| line.trim().strip_prefix("data:"))?;
    serde_json::from_str(data.trim()).ok()
}

fn detect_claude_provider_error(
    v: &serde_json::Value,
    text: Option<&str>,
) -> Option<ProviderError> {
    let payload = text.and_then(parse_api_error_payload);
    let status = v["api_error_status"]
        .as_u64()
        .and_then(|value| u16::try_from(value).ok())
        .or_else(|| {
            v["api_error_status"]
                .as_str()
                .and_then(|value| value.parse::<u16>().ok())
        })
        .or_else(|| {
            text.and_then(|value| {
                value
                    .strip_prefix("API Error:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u16>()
                    .ok()
            })
        });
    let code = payload
        .as_ref()
        .and_then(|value| value["code"].as_str())
        .or_else(|| v["error"]["code"].as_str())
        .map(String::from);
    let payload_message = payload
        .as_ref()
        .and_then(|value| value["message"].as_str())
        .or_else(|| v["error"]["message"].as_str())
        .map(String::from);
    let request_id = payload
        .as_ref()
        .and_then(|value| value["request_id"].as_str())
        .or_else(|| v["request_id"].as_str())
        .map(String::from);
    let subtype_is_error = v["subtype"]
        .as_str()
        .is_some_and(|subtype| subtype.contains("error"));
    let text_is_error = text.is_some_and(|value| value.trim_start().starts_with("API Error:"));
    let is_error = v["is_error"].as_bool().unwrap_or(false)
        || status.is_some()
        || subtype_is_error
        || text_is_error;

    if !is_error {
        return None;
    }

    let message = payload_message
        .or_else(|| {
            text.map(str::trim)
                .filter(|value| !value.is_empty())
                .map(String::from)
        })
        .unwrap_or_else(|| "provider API request failed".to_string());
    let context_overflow =
        code.as_deref() == Some("InvalidParameter") && message.contains("Range of input length");

    Some(ProviderError {
        kind: if context_overflow {
            "context_overflow".to_string()
        } else {
            "provider_api_error".to_string()
        },
        status,
        code,
        message,
        request_id,
    })
}

fn claude_audit_usage_json(
    v: &serde_json::Value,
    provider_error: Option<&ProviderError>,
) -> Option<String> {
    let mut audit = serde_json::Map::new();
    for field in [
        "type",
        "subtype",
        "is_error",
        "api_error_status",
        "duration_ms",
        "duration_api_ms",
        "num_turns",
        "stop_reason",
        "session_id",
        "request_id",
        "total_cost_usd",
        "terminal_reason",
        "fast_mode_state",
        "uuid",
    ] {
        if let Some(value) = v.get(field)
            && (value.is_boolean() || value.is_number() || value.is_string())
        {
            audit.insert(field.to_string(), value.clone());
        }
    }
    if let Some(usage) = v["usage"].as_object() {
        let mut safe_usage = serde_json::Map::new();
        for field in [
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ] {
            if let Some(value) = usage.get(field).filter(|value| value.is_number()) {
                safe_usage.insert(field.to_string(), value.clone());
            }
        }
        if !safe_usage.is_empty() {
            audit.insert("usage".to_string(), serde_json::Value::Object(safe_usage));
        }
    }
    if let Some(models) = v["modelUsage"].as_object() {
        let mut safe_models = serde_json::Map::new();
        for (model, usage) in models {
            let Some(usage) = usage.as_object() else {
                continue;
            };
            let mut safe_usage = serde_json::Map::new();
            for field in [
                "inputTokens",
                "outputTokens",
                "cacheCreationInputTokens",
                "cacheReadInputTokens",
                "webSearchRequests",
                "costUSD",
                "contextWindow",
                "maxOutputTokens",
            ] {
                if let Some(value) = usage.get(field).filter(|value| value.is_number()) {
                    safe_usage.insert(field.to_string(), value.clone());
                }
            }
            if !safe_usage.is_empty() {
                safe_models.insert(model.clone(), serde_json::Value::Object(safe_usage));
            }
        }
        if !safe_models.is_empty() {
            audit.insert(
                "modelUsage".to_string(),
                serde_json::Value::Object(safe_models),
            );
        }
    }
    if let Some(error) = provider_error {
        audit.insert(
            "provider_error".to_string(),
            serde_json::json!({
                "kind": error.kind,
                "status": error.status,
                "code": error.code,
                "request_id": error.request_id,
            }),
        );
    }
    raw_json(&serde_json::Value::Object(audit))
}

pub(crate) fn parse_claude_result_value(v: &serde_json::Value, exit_code: i32) -> CliResult {
    let usage = &v["usage"];
    let model_usage = first_model_usage(v);
    let tokens_prompt = usage["input_tokens"].as_u64();
    let tokens_completion = usage["output_tokens"].as_u64();
    let cache_creation_input_tokens = usage["cache_creation_input_tokens"].as_u64();
    let cache_read_input_tokens = usage["cache_read_input_tokens"].as_u64();
    let text = v["result"].as_str().map(String::from);
    let provider_error = detect_claude_provider_error(v, text.as_deref());
    let success = exit_code == 0 && provider_error.is_none();
    let error_kind = provider_error
        .as_ref()
        .map(|error| error.kind.clone())
        .or_else(|| default_error_kind(exit_code));
    let raw_usage_json = claude_audit_usage_json(v, provider_error.as_ref());

    CliResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code,
        success,
        error_kind,
        provider_error,
        trace_id: None,
        request_id: None,
        run_id: None,
        session_id: v["session_id"].as_str().map(String::from),
        text,
        tool_calls_count: v["num_turns"].as_u64().map(|n| n as u32),
        tools_used: Vec::new(),
        tokens_prompt,
        tokens_completion,
        cached_input_tokens: cache_read_input_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        reasoning_output_tokens: None,
        total_tokens: sum_tokens(&[
            tokens_prompt,
            cache_creation_input_tokens,
            cache_read_input_tokens,
            tokens_completion,
        ]),
        context_window: model_usage.and_then(|m| m["contextWindow"].as_u64()),
        max_output_tokens: model_usage.and_then(|m| m["maxOutputTokens"].as_u64()),
        cost_usd: v["total_cost_usd"]
            .as_f64()
            .or_else(|| model_usage.and_then(|m| m["costUSD"].as_f64())),
        // Retain only accounting and request metadata. The upstream envelope
        // also contains the model response and permission details, which do
        // not belong in the usage table.
        raw_usage_json,
    }
}

pub(crate) fn claude_system_status_output(v: &serde_json::Value) -> Option<String> {
    if v["type"].as_str() != Some("system") {
        return None;
    }
    let content = v["content"].as_str()?.trim();
    let output = match v["subtype"].as_str() {
        Some("local_command") => content
            .strip_prefix("<local-command-stdout>")
            .and_then(|value| value.strip_suffix("</local-command-stdout>"))
            .unwrap_or(content)
            .trim(),
        Some("compact_boundary") => content,
        _ => return None,
    };
    (!output.is_empty()).then(|| output.to_string())
}

pub(crate) fn apply_claude_system_status_output(result: &mut CliResult, output: Option<String>) {
    if result
        .text
        .as_deref()
        .is_none_or(|text| text.trim().is_empty())
        && let Some(output) = output
    {
        result.text = Some(output);
    }
}

fn usage_delta(current: Option<u64>, previous: Option<u64>) -> Option<u64> {
    current.map(|value| {
        previous
            .filter(|old| value >= *old)
            .map_or(value, |old| value - old)
    })
}

fn cost_delta(current: Option<f64>, previous: Option<f64>) -> Option<f64> {
    current.map(|value| {
        previous
            .filter(|old| value >= *old)
            .map_or(value, |old| value - old)
    })
}

pub(crate) fn normalize_claude_pool_usage(
    result: &mut CliResult,
    previous: &mut Option<ClaudeUsageSnapshot>,
) {
    // Persistent Claude's top-level `usage` is already per turn, while
    // `modelUsage` and `total_cost_usd` are cumulative for the process. Build
    // the baseline from the cumulative fields so we do not difference a
    // per-turn value twice.
    let current = result
        .raw_usage_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .as_ref()
        .and_then(cumulative_model_usage)
        .unwrap_or(ClaudeUsageSnapshot {
            tokens_prompt: result.tokens_prompt,
            tokens_completion: result.tokens_completion,
            cache_creation_input_tokens: result.cache_creation_input_tokens,
            cache_read_input_tokens: result.cache_read_input_tokens,
            cost_usd: result.cost_usd,
        });
    let provider_failed_without_usage = result.provider_error.is_some()
        && sum_tokens(&[
            result.tokens_prompt,
            result.tokens_completion,
            result.cache_creation_input_tokens,
            result.cache_read_input_tokens,
        ])
        .unwrap_or(0)
            == 0;

    if provider_failed_without_usage {
        result.tokens_prompt = Some(0);
        result.tokens_completion = Some(0);
        result.cache_creation_input_tokens = Some(0);
        result.cache_read_input_tokens = Some(0);
        result.cached_input_tokens = Some(0);
        result.cost_usd = current.cost_usd.map(|_| 0.0);
    } else if let Some(old) = previous.as_ref() {
        // Counter resets can be field-specific. Difference counters that are
        // still monotonic, and use only the reset field's current value as its
        // new-process baseline. Treating the whole snapshot as absolute when
        // one field rolls back would duplicate lifetime usage in every other
        // field.
        result.tokens_prompt = usage_delta(current.tokens_prompt, old.tokens_prompt);
        result.tokens_completion = usage_delta(current.tokens_completion, old.tokens_completion);
        result.cache_creation_input_tokens = usage_delta(
            current.cache_creation_input_tokens,
            old.cache_creation_input_tokens,
        );
        result.cache_read_input_tokens =
            usage_delta(current.cache_read_input_tokens, old.cache_read_input_tokens);
        result.cached_input_tokens = result.cache_read_input_tokens;
        result.cost_usd = cost_delta(current.cost_usd, old.cost_usd);
    } else {
        result.tokens_prompt = current.tokens_prompt;
        result.tokens_completion = current.tokens_completion;
        result.cache_creation_input_tokens = current.cache_creation_input_tokens;
        result.cache_read_input_tokens = current.cache_read_input_tokens;
        result.cached_input_tokens = current.cache_read_input_tokens;
        result.cost_usd = current.cost_usd;
    }

    result.total_tokens = sum_tokens(&[
        result.tokens_prompt,
        result.cache_creation_input_tokens,
        result.cache_read_input_tokens,
        result.tokens_completion,
    ]);

    match previous {
        // Error-only frames frequently repeat cumulative totals. Keep an
        // established baseline unchanged so the next successful turn is not
        // charged twice. With no baseline yet, retain the frame as the initial
        // cumulative snapshot for the same reason.
        Some(_) if provider_failed_without_usage => {}
        Some(old) => {
            if current.tokens_prompt.is_some() {
                old.tokens_prompt = current.tokens_prompt;
            }
            if current.tokens_completion.is_some() {
                old.tokens_completion = current.tokens_completion;
            }
            if current.cache_creation_input_tokens.is_some() {
                old.cache_creation_input_tokens = current.cache_creation_input_tokens;
            }
            if current.cache_read_input_tokens.is_some() {
                old.cache_read_input_tokens = current.cache_read_input_tokens;
            }
            if current.cost_usd.is_some() {
                old.cost_usd = current.cost_usd;
            }
        }
        slot => *slot = Some(current),
    }
}

fn malformed_astra_result(exit_code: i32, reason: String) -> CliResult {
    CliResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code: if exit_code == 0 { 1 } else { exit_code },
        success: false,
        error_kind: Some("malformed_envelope".to_string()),
        provider_error: None,
        trace_id: None,
        request_id: None,
        run_id: None,
        session_id: None,
        text: Some(format!("malformed Astra JSON envelope: {reason}")),
        tool_calls_count: None,
        tools_used: Vec::new(),
        tokens_prompt: None,
        tokens_completion: None,
        cached_input_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        reasoning_output_tokens: None,
        total_tokens: None,
        context_window: None,
        max_output_tokens: None,
        cost_usd: None,
        raw_usage_json: None,
    }
}

fn parse_claude_json(stdout: &str, exit_code: i32) -> CliResult {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(stdout) {
        parse_claude_result_value(&v, exit_code)
    } else {
        parse_claude_plain_result(stdout, exit_code)
    }
}

fn parse_claude_plain_result(stdout: &str, exit_code: i32) -> CliResult {
    let mut result = plain_result(stdout, exit_code);
    if let Some(provider_error) =
        detect_claude_provider_error(&serde_json::Value::Null, result.text.as_deref())
    {
        result.success = false;
        result.error_kind = Some(provider_error.kind.clone());
        result.provider_error = Some(provider_error);
    }
    result
}

/// Parse the accumulated stdout of a `--output-format stream-json` run.
/// Walks every JSONL line to accumulate tool usage (since the final `result`
/// frame only carries `num_turns`, not tool metadata).
fn parse_claude_stream_json_stdout(stdout: &str, exit_code: i32) -> CliResult {
    let mut result_value: Option<serde_json::Value> = None;
    let mut system_status_output: Option<String> = None;
    let mut tools_used: Vec<String> = Vec::new();
    let mut tool_use_count: u32 = 0;

    for line in stdout.lines() {
        let line = strip_leading_osc(line.trim()).trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match v["type"].as_str() {
            Some("assistant") => {
                if let Some(content) = v["message"]["content"].as_array() {
                    for block in content {
                        if block["type"].as_str() == Some("tool_use") {
                            tool_use_count += 1;
                            if let Some(name) = block["name"].as_str()
                                && !tools_used.iter().any(|n| n == name)
                            {
                                tools_used.push(name.to_string());
                            }
                        }
                    }
                }
            }
            Some("result") => {
                result_value = Some(v);
            }
            Some("system") => {
                if let Some(output) = claude_system_status_output(&v) {
                    system_status_output = Some(output);
                }
            }
            _ => {}
        }
    }

    if let Some(value) = result_value {
        let mut result = parse_claude_result_value(&value, exit_code);
        apply_claude_system_status_output(&mut result, system_status_output);
        result.tool_calls_count = if tool_use_count == 0 {
            None
        } else {
            Some(tool_use_count)
        };
        result.tools_used = tools_used;
        result
    } else {
        parse_claude_plain_result(stdout, exit_code)
    }
}
/// Parse a single stdout JSONL line from `--output-format stream-json` into a
/// progress event. Returns `None` for lines that don't map to a user-visible event.
///
/// Claude stream-json emits these top-level types:
///   - `system` (init): tools, model, session info
///   - `assistant`: message with content blocks (text, tool_use, thinking)
///   - `result`: final answer with usage stats — carries session_id, result text, and token usage
fn truncate_unicode(s: &str, max_chars: usize) -> String {
    match s.char_indices().nth(max_chars) {
        None => s.to_string(),
        Some((idx, _)) => format!("{}…", &s[..idx]),
    }
}

fn parse_claude_stream_json_line(line: &str) -> Option<CliProgress> {
    let v = serde_json::from_str::<serde_json::Value>(line).ok()?;
    match v["type"].as_str()? {
        // Assistant message — used for tool_use/tool_result detection.
        // Extract tool_use params from the complete assistant message.
        // Text/thinking/tool tracking already handled via stream_event deltas.
        "assistant" => {
            let content = v["message"]["content"].as_array()?;
            for block in content {
                if block["type"].as_str() == Some("tool_use") {
                    let name = block["name"].as_str().unwrap_or("tool").to_string();
                    let input = &block["input"];
                    let description = input["description"].as_str().unwrap_or("");
                    let arg = input["command"]
                        .as_str()
                        .or_else(|| input["file_path"].as_str())
                        .or_else(|| input["query"].as_str())
                        .unwrap_or("");
                    let params = match (description, arg) {
                        ("", "") => None,
                        (desc, "") => Some(desc.to_string()),
                        ("", arg) => Some(truncate_unicode(arg, 40)),
                        (desc, arg) => Some(format!("{} ({})", desc, truncate_unicode(arg, 30))),
                    };
                    return Some(CliProgress::ToolStarted { name, params });
                }
            }
            None
        }
        // Real-time streaming events from --include-partial-messages.
        // content_block_delta carries incremental text tokens as they're generated.
        "stream_event" => {
            let event = &v["event"];
            match event["type"].as_str() {
                Some("content_block_delta") => {
                    let delta = &event["delta"];
                    match delta["type"].as_str() {
                        Some("text_delta") => {
                            if let Some(text) = delta["text"].as_str()
                                && !text.is_empty()
                            {
                                return Some(CliProgress::Token(text.to_string()));
                            }
                            None
                        }
                        Some("thinking_delta") => {
                            if let Some(text) = delta["thinking"].as_str()
                                && !text.is_empty()
                            {
                                return Some(CliProgress::ReasoningBlock {
                                    kind: ReasoningKind::Raw,
                                    text: text.to_string(),
                                });
                            }
                            None
                        }
                        _ => None,
                    }
                }
                Some("content_block_start") => None,
                _ => None,
            }
        }
        // Hook lifecycle events forwarded via --include-hook-events.
        // Claude emits the event name as `hook_event_name`
        // (e.g. PreToolUse, PostToolUse, UserPromptSubmit).
        "hook" => {
            let hook_name = v["hook_event_name"].as_str().unwrap_or("hook");
            Some(CliProgress::Status(format!("[hook:{hook_name}]")))
        }
        // Final result — no progress event (handled by parse_claude_stream_json_stdout).
        "result" | "system" => None,
        _ => None,
    }
}

fn parse_copilot_jsonl(stdout: &str, exit_code: i32) -> CliResult {
    let mut session_id: Option<String> = None;
    let mut final_text: Option<String> = None;
    let mut delta_text = String::new();
    let mut error_text: Option<String> = None;
    let mut tokens_prompt: Option<u64> = None;
    let mut tokens_completion: Option<u64> = None;
    let mut message_output_tokens: Option<u64> = None;
    let mut tools_used: Vec<String> = Vec::new();
    let mut seen_tool_calls: Vec<String> = Vec::new();
    let mut tool_calls_count: u32 = 0;
    let mut non_json_lines: Vec<String> = Vec::new();

    for line in stdout.lines() {
        let line = strip_leading_osc(line.trim()).trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            non_json_lines.push(line.to_string());
            continue;
        };
        match v["type"].as_str() {
            Some("session.start") => {
                session_id = v["data"]["sessionId"].as_str().map(String::from);
            }
            Some("session.resume") if session_id.is_none() => {
                session_id = v["data"]["sessionId"].as_str().map(String::from);
            }
            Some("session.error") => {
                error_text = v["data"]["message"].as_str().map(String::from);
            }
            Some("assistant.message_delta") => {
                if let Some(delta) = v["data"]["deltaContent"].as_str() {
                    delta_text.push_str(delta);
                }
            }
            Some("assistant.message") => {
                if let Some(content) = v["data"]["content"].as_str()
                    && !content.trim().is_empty()
                {
                    final_text = Some(content.to_string());
                }
                if let Some(requests) = v["data"]["toolRequests"].as_array() {
                    for request in requests {
                        record_copilot_tool(
                            request["toolCallId"].as_str(),
                            request["name"].as_str(),
                            &mut seen_tool_calls,
                            &mut tools_used,
                            &mut tool_calls_count,
                        );
                    }
                }
                if let Some(output) = v["data"]["outputTokens"].as_u64() {
                    message_output_tokens = Some(message_output_tokens.unwrap_or(0) + output);
                }
            }
            Some("assistant.usage") => {
                if let Some(input) = v["data"]["inputTokens"].as_u64() {
                    tokens_prompt = Some(tokens_prompt.unwrap_or(0) + input);
                }
                if let Some(output) = v["data"]["outputTokens"].as_u64() {
                    tokens_completion = Some(tokens_completion.unwrap_or(0) + output);
                }
            }
            Some("tool.execution_start") => {
                record_copilot_tool(
                    v["data"]["toolCallId"].as_str(),
                    v["data"]["toolName"].as_str(),
                    &mut seen_tool_calls,
                    &mut tools_used,
                    &mut tool_calls_count,
                );
            }
            Some("session.task_complete") => {
                if final_text.is_none()
                    && let Some(summary) = v["data"]["summary"].as_str()
                    && !summary.trim().is_empty()
                {
                    final_text = Some(summary.to_string());
                }
            }
            Some("result") if session_id.is_none() => {
                session_id = v["sessionId"].as_str().map(String::from);
            }
            _ => {}
        }
    }

    let text = final_text
        .or_else(|| (!delta_text.trim().is_empty()).then(|| delta_text.trim().to_string()))
        .or(error_text)
        .or_else(|| (!non_json_lines.is_empty()).then(|| non_json_lines.join("\n")));
    let tokens_completion = tokens_completion.or(message_output_tokens);

    if text.is_some()
        || session_id.is_some()
        || tokens_prompt.is_some()
        || tokens_completion.is_some()
    {
        CliResult {
            stdout: String::new(),
            stderr: String::new(),
            exit_code,
            success: exit_code == 0,
            error_kind: default_error_kind(exit_code),
            provider_error: None,
            trace_id: None,
            request_id: None,
            run_id: None,
            session_id,
            text,
            tool_calls_count: (tool_calls_count > 0).then_some(tool_calls_count),
            tools_used,
            tokens_prompt,
            tokens_completion,
            cached_input_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_output_tokens: None,
            total_tokens: sum_tokens(&[tokens_prompt, tokens_completion]),
            context_window: None,
            max_output_tokens: None,
            cost_usd: None,
            raw_usage_json: None,
        }
    } else {
        plain_result(stdout, exit_code)
    }
}

fn record_copilot_tool(
    call_id: Option<&str>,
    name: Option<&str>,
    seen_tool_calls: &mut Vec<String>,
    tools_used: &mut Vec<String>,
    tool_calls_count: &mut u32,
) {
    if let Some(name) = name
        && !name.is_empty()
        && !tools_used.iter().any(|n| n == name)
    {
        tools_used.push(name.to_string());
    }

    if let Some(call_id) = call_id
        && !call_id.is_empty()
    {
        if !seen_tool_calls.iter().any(|id| id == call_id) {
            seen_tool_calls.push(call_id.to_string());
            *tool_calls_count += 1;
        }
    } else {
        *tool_calls_count += 1;
    }
}

fn parse_copilot_jsonl_line(line: &str) -> Option<CliProgress> {
    let v = serde_json::from_str::<serde_json::Value>(line).ok()?;
    match v["type"].as_str()? {
        "assistant.message_delta" => v["data"]["deltaContent"]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(|text| CliProgress::Token(text.to_string())),
        "assistant.intent" => v["data"]["intent"]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(|text| CliProgress::Status(text.to_string())),
        "assistant.reasoning_delta" => v["data"]["deltaContent"]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(|text| CliProgress::ReasoningBlock {
                kind: ReasoningKind::Raw,
                text: text.to_string(),
            }),
        "assistant.reasoning_summary" => v["data"]["summary"]
            .as_str()
            .or_else(|| v["data"]["content"].as_str())
            .filter(|text| !text.is_empty())
            .map(|text| CliProgress::ReasoningBlock {
                kind: ReasoningKind::Summary,
                text: text.to_string(),
            }),
        "session.warning" | "session.info" => v["data"]["message"]
            .as_str()
            .filter(|text| !text.is_empty())
            .map(|text| CliProgress::Status(text.to_string())),
        "tool.execution_start" => {
            let name = v["data"]["toolName"].as_str().unwrap_or("tool").to_string();
            Some(CliProgress::ToolStarted { name, params: None })
        }
        "tool.execution_complete" => {
            let name = v["data"]["toolName"].as_str().unwrap_or("tool").to_string();
            Some(CliProgress::ToolDone {
                name,
                duration_ms: None,
            })
        }
        _ => None,
    }
}

/// Parse accumulated stdout of a Codex `--json` (JSONL streaming) run.
///
/// Codex exec --json emits these event types:
///   - `thread.started`: session init with thread_id
///   - `turn.started` / `turn.completed`: turn lifecycle with usage
///   - `item.started` / `item.updated` / `item.completed`: content items
///   - `error`: error events
///
/// Item types: agent_message, reasoning, command_execution, file_change, mcp_tool_call
fn parse_codex_stream_json_stdout(stdout: &str, exit_code: i32) -> CliResult {
    let mut session_id: Option<String> = None;
    let mut text = String::new();
    let mut tokens_prompt: Option<u64> = None;
    let mut tokens_completion: Option<u64> = None;
    let mut cached_input_tokens: Option<u64> = None;
    let mut reasoning_output_tokens: Option<u64> = None;
    let mut total_tokens: Option<u64> = None;
    let mut raw_usage_json: Option<String> = None;
    let mut tools_used: Vec<String> = Vec::new();
    let mut tool_use_count: u32 = 0;

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        match v["type"].as_str() {
            Some("thread.started") => {
                session_id = v["thread_id"].as_str().map(String::from);
            }
            Some("turn.completed") => {
                let usage = &v["usage"];
                tokens_prompt = usage["input_tokens"].as_u64();
                cached_input_tokens = usage["cached_input_tokens"].as_u64();
                tokens_completion = usage["output_tokens"].as_u64();
                reasoning_output_tokens = usage["reasoning_output_tokens"].as_u64();
                total_tokens = usage["total_tokens"].as_u64().or_else(|| {
                    sum_tokens(&[tokens_prompt, tokens_completion, reasoning_output_tokens])
                });
                raw_usage_json = raw_json(usage);
            }
            Some("item.completed") => {
                let item = &v["item"];
                match item["type"].as_str() {
                    Some("agent_message") => {
                        if let Some(t) = item["text"].as_str() {
                            if !text.is_empty() {
                                text.push('\n');
                            }
                            text.push_str(t);
                        }
                    }
                    Some("command_execution") => {
                        tool_use_count += 1;
                        if !tools_used.iter().any(|n| n == "command_execution") {
                            tools_used.push("command_execution".to_string());
                        }
                    }
                    Some("file_change") => {
                        tool_use_count += 1;
                        if !tools_used.iter().any(|n| n == "file_change") {
                            tools_used.push("file_change".to_string());
                        }
                    }
                    Some("mcp_tool_call") => {
                        tool_use_count += 1;
                        let tool_name = item["tool"].as_str().unwrap_or("mcp_tool").to_string();
                        if !tools_used.iter().any(|n| n == &tool_name) {
                            tools_used.push(tool_name);
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    if !text.is_empty() || session_id.is_some() {
        let tool_calls_count = if tool_use_count == 0 {
            None
        } else {
            Some(tool_use_count)
        };
        CliResult {
            stdout: String::new(),
            stderr: String::new(),
            exit_code,
            success: exit_code == 0,
            error_kind: default_error_kind(exit_code),
            provider_error: None,
            trace_id: None,
            request_id: None,
            run_id: None,
            session_id,
            text: if text.is_empty() { None } else { Some(text) },
            tool_calls_count,
            tools_used,
            tokens_prompt,
            tokens_completion,
            cached_input_tokens,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_output_tokens,
            total_tokens,
            context_window: None,
            max_output_tokens: None,
            cost_usd: None,
            raw_usage_json,
        }
    } else {
        plain_result(stdout, exit_code)
    }
}

/// Parse a single stdout JSONL line from Codex `--json` into a progress event.
///
/// Codex events: thread.started, turn.started, turn.completed, item.started,
/// item.updated, item.completed, error.
fn parse_codex_stream_json_line(line: &str) -> Option<CliProgress> {
    let v = serde_json::from_str::<serde_json::Value>(line).ok()?;
    match v["type"].as_str()? {
        "item.started" | "item.updated" => {
            let item = &v["item"];
            match item["type"].as_str() {
                Some("agent_message") => {
                    if let Some(t) = item["text"].as_str()
                        && !t.is_empty()
                    {
                        return Some(CliProgress::Token(t.to_string()));
                    }
                }
                Some("reasoning") => {
                    return Some(CliProgress::Thinking(true));
                }
                Some("command_execution") => {
                    let cmd = item["command"].as_str().unwrap_or("shell").to_string();
                    return Some(CliProgress::ToolStarted {
                        name: cmd,
                        params: None,
                    });
                }
                Some("file_change") => {
                    return Some(CliProgress::ToolStarted {
                        name: "file_change".to_string(),
                        params: None,
                    });
                }
                Some("mcp_tool_call") => {
                    let tool = item["tool"].as_str().unwrap_or("mcp_tool").to_string();
                    return Some(CliProgress::ToolStarted {
                        name: tool,
                        params: None,
                    });
                }
                _ => {}
            }
            None
        }
        "item.completed" => {
            let item = &v["item"];
            match item["type"].as_str() {
                Some("command_execution") => {
                    let cmd = item["command"].as_str().unwrap_or("shell").to_string();
                    return Some(CliProgress::ToolDone {
                        name: cmd,
                        duration_ms: None,
                    });
                }
                Some("file_change") => {
                    return Some(CliProgress::ToolDone {
                        name: "file_change".to_string(),
                        duration_ms: None,
                    });
                }
                Some("mcp_tool_call") => {
                    let tool = item["tool"].as_str().unwrap_or("mcp_tool").to_string();
                    return Some(CliProgress::ToolDone {
                        name: tool,
                        duration_ms: None,
                    });
                }
                Some("reasoning") => {
                    return Some(CliProgress::Thinking(false));
                }
                _ => {}
            }
            None
        }
        "turn.started" => Some(CliProgress::Status("codex turn started".to_string())),
        "error" => {
            let msg = v["message"].as_str().unwrap_or("unknown error").to_string();
            Some(CliProgress::Status(format!("[error] {msg}")))
        }
        _ => None,
    }
}

pub(crate) fn parse_stdout_jsonl_line(line: &str, cli_name: &str) -> Option<CliProgress> {
    let line = strip_leading_osc(line.trim()).trim();
    match cli_name {
        "codex" => parse_codex_stream_json_line(line),
        _ => parse_claude_stream_json_line(line).or_else(|| parse_copilot_jsonl_line(line)),
    }
}

fn strip_leading_osc(mut s: &str) -> &str {
    while let Some(rest) = s.strip_prefix("\x1b]") {
        let Some(end) = rest.find('\x07') else {
            break;
        };
        s = rest[end + 1..].trim_start();
    }
    s
}

fn parse_generic_json(
    stdout: &str,
    exit_code: i32,
    text_field: &str,
    session_field: &str,
) -> CliResult {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(stdout) {
        CliResult {
            stdout: String::new(),
            stderr: String::new(),
            exit_code,
            success: exit_code == 0,
            error_kind: default_error_kind(exit_code),
            provider_error: None,
            trace_id: None,
            request_id: None,
            run_id: None,
            session_id: v[session_field].as_str().map(String::from),
            text: v[text_field].as_str().map(String::from),
            tool_calls_count: None,
            tools_used: Vec::new(),
            tokens_prompt: None,
            tokens_completion: None,
            cached_input_tokens: None,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_output_tokens: None,
            total_tokens: None,
            context_window: None,
            max_output_tokens: None,
            cost_usd: None,
            raw_usage_json: None,
        }
    } else {
        plain_result(stdout, exit_code)
    }
}

fn plain_result(stdout: &str, exit_code: i32) -> CliResult {
    CliResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code,
        success: exit_code == 0,
        error_kind: default_error_kind(exit_code),
        provider_error: None,
        trace_id: None,
        request_id: None,
        run_id: None,
        session_id: None,
        text: if stdout.trim().is_empty() {
            None
        } else {
            Some(stdout.trim().to_string())
        },
        tool_calls_count: None,
        tools_used: Vec::new(),
        tokens_prompt: None,
        tokens_completion: None,
        cached_input_tokens: None,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        reasoning_output_tokens: None,
        total_tokens: None,
        context_window: None,
        max_output_tokens: None,
        cost_usd: None,
        raw_usage_json: None,
    }
}

fn default_error_kind(exit_code: i32) -> Option<String> {
    (exit_code != 0).then(|| "process_exit".to_string())
}

/// Parse a stderr line as a structured JSONL event (from --stream-events)
/// or fall back to heuristic classification.
fn parse_stderr_line(line: &str) -> CliProgress {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
        match v.get("type").and_then(|t| t.as_str()) {
            Some("token") => {
                let text = v["text"].as_str().unwrap_or_default().to_string();
                return CliProgress::Token(text);
            }
            Some("thinking") => {
                let active = v["active"].as_bool().unwrap_or(false);
                return CliProgress::Thinking(active);
            }
            Some("thinking_chunk") => {
                return CliProgress::ReasoningBlock {
                    kind: ReasoningKind::Raw,
                    text: v["text"].as_str().unwrap_or_default().to_string(),
                };
            }
            Some("tool_started") => {
                let name = v["name"].as_str().unwrap_or_default().to_string();
                return CliProgress::ToolStarted { name, params: None };
            }
            Some("tool_completed") => {
                let name = v["name"].as_str().unwrap_or_default().to_string();
                let duration_ms = v["duration_ms"].as_u64();
                return CliProgress::ToolDone { name, duration_ms };
            }
            Some("status") => {
                return CliProgress::Status(v["text"].as_str().unwrap_or_default().to_string());
            }
            Some("waiting_for_model" | "model_responding") => {
                return CliProgress::Status(line.to_string());
            }
            _ => {}
        }
    }
    if line.contains('⚡') || line.contains("tool") {
        CliProgress::ToolCall(line.to_string())
    } else {
        CliProgress::Status(line.to_string())
    }
}

// ─── Run ────────────────────────────────────────────────────────────────────

pub async fn run_cli(
    profile: &CliProfile,
    message: &str,
    session_id: Option<&str>,
    working_dir: Option<&std::path::Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
) -> Result<CliResult, String> {
    run_cli_with_context(
        profile,
        message,
        session_id,
        working_dir,
        progress_tx,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cli_with_context(
    profile: &CliProfile,
    message: &str,
    session_id: Option<&str>,
    working_dir: Option<&std::path::Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    system_prompt: Option<&str>,
    access_token: Option<&str>,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Result<CliResult, String> {
    run_cli_with_context_and_timeout(
        profile,
        message,
        session_id,
        working_dir,
        progress_tx,
        system_prompt,
        None,
        access_token,
        provider_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cli_with_context_and_timeout(
    profile: &CliProfile,
    message: &str,
    session_id: Option<&str>,
    working_dir: Option<&std::path::Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    system_prompt: Option<&str>,
    timeout: Option<Duration>,
    access_token: Option<&str>,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Result<CliResult, String> {
    run_cli_with_context_trace_and_timeout(
        profile,
        message,
        session_id,
        working_dir,
        progress_tx,
        system_prompt,
        None,
        None,
        timeout,
        access_token,
        provider_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cli_with_context_trace_and_timeout(
    profile: &CliProfile,
    message: &str,
    session_id: Option<&str>,
    working_dir: Option<&std::path::Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    system_prompt: Option<&str>,
    trace_id: Option<&str>,
    request_id: Option<&str>,
    timeout: Option<Duration>,
    access_token: Option<&str>,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Result<CliResult, String> {
    run_cli_with_cancel(
        profile,
        message,
        session_id,
        working_dir,
        progress_tx,
        system_prompt,
        trace_id,
        request_id,
        timeout,
        access_token,
        None,
        None,
        provider_config,
    )
    .await
}

/// Full CLI spawn with cancellation token support. When `cancel` fires,
/// the child process is killed (SIGKILL) immediately — no zombie.
#[allow(clippy::too_many_arguments)]
pub async fn run_cli_with_cancel(
    profile: &CliProfile,
    message: &str,
    session_id: Option<&str>,
    working_dir: Option<&std::path::Path>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    system_prompt: Option<&str>,
    trace_id: Option<&str>,
    request_id: Option<&str>,
    timeout: Option<Duration>,
    access_token: Option<&str>,
    github_token: Option<&str>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Result<CliResult, String> {
    let mut cmd =
        profile.build_command_with_context(message, session_id, working_dir, system_prompt);
    profile.apply_runtime_environment(&mut cmd).map_err(|e| {
        format!(
            "failed to prepare `{}` CLI environment: {e}",
            profile.name()
        )
    })?;
    if let Some(pc) = provider_config {
        apply_provider_environment(&mut cmd, pc).map_err(|e| {
            format!(
                "failed to prepare provider environment for `{}`: {e}",
                profile.name()
            )
        })?;
    }
    if let Some(trace_id) = trace_id {
        cmd.env("ASTRA_GATEWAY_TRACE_ID", trace_id);
    }
    if let Some(request_id) = request_id {
        cmd.env("ASTRA_GATEWAY_REQUEST_ID", request_id);
    }
    if let Some(token) = access_token {
        cmd.env("ASTRA_ACCESS_TOKEN", token);
    }
    if let Some(token) = github_token {
        cmd.env("GH_TOKEN", token);
        cmd.env("GITHUB_TOKEN", token);
    }
    let name = profile.name().to_string();
    let stream_stdout = matches!(
        profile,
        CliProfile::Claude {
            stream_json: true,
            ..
        } | CliProfile::Copilot {
            stream_json: true,
            ..
        } | CliProfile::Codex {
            stream_json: true,
            ..
        }
    );
    let (stdout_text, stderr_text, exit_code) = if stream_stdout {
        run_child_with_cancel_streaming(cmd, progress_tx, timeout, cancel, &name).await?
    } else {
        run_child_with_cancel(cmd, progress_tx, timeout, cancel, &name).await?
    };

    let mut result = profile.parse_output(&stdout_text, exit_code);
    result.stdout = stdout_text;
    result.stderr = stderr_text;
    Ok(result)
}

/// Core subprocess lifecycle: spawn, stream output, handle timeout/cancel.
/// Separated from `run_cli_with_cancel` for testability — tests can pass
/// any `Command` directly without going through `build_command_with_context`.
/// Kill a child process and abort its I/O tasks. Used by cancel paths.
async fn abort_child(
    kill_guard: ChildKillGuard,
    mut child: tokio::process::Child,
    stderr_task: tokio::task::JoinHandle<String>,
    stdout_task: tokio::task::JoinHandle<String>,
) {
    // Defuse so drop doesn't redundantly SIGKILL after we explicitly kill.
    let mut guard = kill_guard;
    guard.defuse();
    let _ = child.kill().await;
    stderr_task.abort();
    stdout_task.abort();
}

pub(crate) async fn run_child_with_cancel(
    cmd: Command,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    timeout: Option<Duration>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    name: &str,
) -> Result<(String, String, i32), String> {
    run_child_with_cancel_inner(cmd, progress_tx, timeout, cancel, name, false).await
}

/// Like `run_child_with_cancel` but also parses stdout as a JSONL progress
/// stream. Each stdout line is dispatched as a `CliProgress` event; the full
/// stdout text is still returned for `parse_output` to extract the final result.
pub(crate) async fn run_child_with_cancel_streaming(
    cmd: Command,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    timeout: Option<Duration>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    name: &str,
) -> Result<(String, String, i32), String> {
    run_child_with_cancel_inner(cmd, progress_tx, timeout, cancel, name, true).await
}

async fn run_child_with_cancel_inner(
    mut cmd: Command,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
    timeout: Option<Duration>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    name: &str,
    stream_stdout: bool,
) -> Result<(String, String, i32), String> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn {name}: {e}"))?;

    let mut kill_guard = ChildKillGuard::new(&child);

    let stdout = child.stdout.take().ok_or("no stdout")?;
    let stderr = child.stderr.take().ok_or("no stderr")?;

    // In JSONL stream mode, stdout carries progress events.
    // In normal mode, stderr carries --stream-events JSONL progress events.
    let (stderr_progress_tx, stdout_progress_tx) = if stream_stdout {
        (None, progress_tx)
    } else {
        (progress_tx, None)
    };

    let stderr_task = tokio::spawn(async move {
        let reader = BufReader::new(stderr);
        let mut lines = reader.lines();
        let mut collected = String::new();
        while let Ok(Some(line)) = lines.next_line().await {
            let line = line.trim().to_string();
            if line.is_empty() {
                continue;
            }
            if !collected.is_empty() {
                collected.push('\n');
            }
            collected.push_str(&line);
            if let Some(ref tx) = stderr_progress_tx {
                let event = parse_stderr_line(&line);
                let _ = tx.send(event).await;
            }
        }
        collected
    });

    let cli_name = name.to_string();
    let stdout_task = tokio::spawn(async move {
        let reader = BufReader::new(stdout);
        let mut lines = reader.lines();
        let mut output = String::new();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::trace!(cli = %cli_name, raw = %line, "cli stdout");
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&line);
            if let Some(ref tx) = stdout_progress_tx
                && let Some(ev) = parse_stdout_jsonl_line(&line, &cli_name)
            {
                let _ = tx.send(ev).await;
            }
        }
        output
    });

    let cancel_future = async {
        match cancel.as_ref() {
            Some(t) => t.cancelled().await,
            None => std::future::pending().await,
        }
    };

    let status = if let Some(timeout) = timeout {
        tokio::select! {
            status = child.wait() => status.map_err(|e| format!("wait failed: {e}"))?,
            _ = tokio::time::sleep(timeout) => {
                // Timeout — kill and collect stderr for diagnostics.
                kill_guard.defuse();
                let _ = child.kill().await;
                let stderr_text = stderr_task.await.unwrap_or_default();
                let _stdout_text = stdout_task.await.unwrap_or_default();
                return Err(format!(
                    "{name} timed out after {}s\n{}{}",
                    timeout.as_secs(),
                    if stderr_text.is_empty() { "" } else { "stderr: " },
                    stderr_text.lines().take(10).collect::<Vec<_>>().join("\n")
                ).trim().to_string());
            }
            _ = cancel_future => {
                abort_child(kill_guard, child, stderr_task, stdout_task).await;
                return Err(format!("{name} killed by user"));
            }
        }
    } else {
        tokio::select! {
            status = child.wait() => status.map_err(|e| format!("wait failed: {e}"))?,
            _ = cancel_future => {
                abort_child(kill_guard, child, stderr_task, stdout_task).await;
                return Err(format!("{name} killed by user"));
            }
        }
    };
    // Normal exit — defuse so Drop doesn't send SIGKILL.
    kill_guard.defuse();

    let stderr_text = stderr_task.await.unwrap_or_default();
    let stdout_text = stdout_task.await.unwrap_or_default();
    let exit_code = status.code().unwrap_or(-1);
    Ok((stdout_text, stderr_text, exit_code))
}

// ─── CLI Availability ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliAvailability {
    Available { version: Option<String> },
    NotInstalled,
    NotExecutable(String),
}

impl CliAvailability {
    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available { .. })
    }
}

use std::sync::LazyLock;
use std::time::Instant;

static CLI_PROBE_CACHE: LazyLock<dashmap::DashMap<String, (CliAvailability, Instant)>> =
    LazyLock::new(dashmap::DashMap::new);

const PROBE_CACHE_TTL_SECS: u64 = 300;

pub async fn probe_cli(profile: &CliProfile) -> CliAvailability {
    let cache_key = profile.probe_cache_key();
    if let Some(entry) = CLI_PROBE_CACHE.get(&cache_key) {
        let (ref avail, created) = *entry;
        if created.elapsed().as_secs() < PROBE_CACHE_TTL_SECS {
            return avail.clone();
        }
    }
    let result = probe_cli_uncached(profile).await;
    CLI_PROBE_CACHE.insert(cache_key, (result.clone(), Instant::now()));
    result
}

async fn probe_cli_uncached(profile: &CliProfile) -> CliAvailability {
    let version_arg = match profile {
        CliProfile::Astra { .. } => "--version",
        CliProfile::Claude { .. } => "--version",
        CliProfile::Copilot { .. } => "--version",
        CliProfile::Codex { .. } => "--version",
        CliProfile::Custom { .. } => "--version",
    };

    let mut cmd = match profile {
        CliProfile::Astra { bin, .. }
        | CliProfile::Claude { bin, .. }
        | CliProfile::Codex { bin, .. }
        | CliProfile::Custom { bin, .. } => tokio::process::Command::new(bin),
        CliProfile::Copilot { bin, launcher, .. } => build_command_launcher(bin, launcher.as_ref()),
    };
    if let Err(e) = profile.apply_runtime_environment(&mut cmd) {
        return CliAvailability::NotExecutable(e);
    }

    match cmd
        .arg(version_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => CliAvailability::NotInstalled,
        Err(e) => CliAvailability::NotExecutable(e.to_string()),
        Ok(child) => match child.wait_with_output().await {
            Ok(output) => {
                let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
                CliAvailability::Available {
                    version: if version.is_empty() {
                        None
                    } else {
                        Some(version)
                    },
                }
            }
            Err(e) => CliAvailability::NotExecutable(e.to_string()),
        },
    }
}

pub fn onboarding_message(profile: &CliProfile, availability: &CliAvailability) -> String {
    let name = profile.name();
    match availability {
        CliAvailability::Available { .. } => String::new(),
        CliAvailability::NotInstalled => format!(
            "⚠️ CLI `{name}` 未安装\n\n\
             请先安装对应的 CLI 工具:\n\
             - **astra**: `cargo install astra-cli`\n\
             - **claude**: `npm install -g @anthropic-ai/claude-code`\n\
             - **copilot**: `npm install -g @github/copilot`\n\
             - **codex**: `npm install -g @openai/codex`\n\n\
             安装完成后发送任意消息即可开始对话。\n\
             或使用 `/cli` 切换到其他已安装的 CLI。"
        ),
        CliAvailability::NotExecutable(err) => format!(
            "⚠️ CLI `{name}` 无法执行: {err}\n\n\
             请检查文件权限或 PATH 配置。\n\
             使用 `/cli` 查看可用的 CLI 选项。"
        ),
    }
}

/// Check whether stderr output indicates an authentication / credentials failure.
pub fn is_auth_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("could not validate credentials")
        || lower.contains("invalid_api_key")
        || lower.contains("invalid api key")
        || lower.contains("401 unauthorized")
        || lower.contains("authentication failed")
        || lower.contains("not logged in")
        || lower.contains("token expired")
        || lower.contains("token has expired")
        || lower.contains("access denied by policy settings")
        || lower.contains("copilot subscription does not include this feature")
        // Match bare "401" only when it looks like an HTTP status, not a random number.
        // We check for "401" preceded by a space, start-of-line, or common prefix.
        || lower.contains("status: 401")
        || lower.contains("http 401")
        || lower.contains("error 401")
}

/// Invalidate the CLI probe cache so the next `probe_cli` call re-checks.
pub fn invalidate_probe_cache() {
    CLI_PROBE_CACHE.clear();
}

pub fn translate_cli_error(profile: &CliProfile, exit_code: i32, stderr: &str) -> String {
    let name = profile.name();
    if is_auth_error(stderr) {
        if matches!(profile, CliProfile::Copilot { .. }) {
            return format!(
                "🔑 `{name}` 认证或策略校验失败\n\n\
                 请尝试:\n\
                 1. 运行 `copilot login` 重新登录\n\
                 2. 检查 GitHub Copilot 策略/订阅是否允许 Copilot CLI\n\
                 3. 或切换到其他 CLI: `/cli astra`、`/cli claude`"
            );
        }
        return format!(
            "🔑 `{name}` 认证失败\n\n\
             请尝试:\n\
             1. 发送 `/auth` 重置认证\n\
             2. 运行 `astra /login` 重新登录\n\
             3. 或 `/cli claude` 切换到其他 CLI"
        );
    }
    if stderr.contains("rate limit") || stderr.contains("429") {
        return format!("⏳ `{name}` 请求过于频繁，请稍后再试。");
    }
    if stderr.contains("waiting_for_model") {
        return format!("⏳ `{name}` 模型繁忙，请稍后重试。");
    }
    if stderr.contains("timeout") || stderr.contains("timed out") {
        return format!("⏰ `{name}` 响应超时，请重试。");
    }
    format!("⚠️ `{name}` 执行失败 (exit={exit_code})")
}

pub fn translate_provider_error(profile: &CliProfile, error: &ProviderError) -> String {
    let upstream_request = error
        .request_id
        .as_deref()
        .map(|request_id| format!("\n上游 request_id: `{request_id}`"))
        .unwrap_or_default();
    if error.kind == "context_overflow" {
        let model = profile.model_name().unwrap_or("当前模型");
        return format!(
            "⚠️ `{model}` 会话上下文已超过上游输入上限。请先发送 `/compact`；如果压缩仍失败，请使用 `/new` 开始新会话。{upstream_request}"
        );
    }

    let status = error
        .status
        .map(|value| format!(" HTTP {value}"))
        .unwrap_or_default();
    let code = error
        .code
        .as_deref()
        .map(|value| format!(" `{value}`"))
        .unwrap_or_default();
    format!("⚠️ 上游模型服务返回错误{status}{code}。请稍后重试。{upstream_request}")
}

// Legacy compat
pub type CliBridgeConfig = CliProfile;

pub async fn run_astra_chat(
    config: &CliBridgeConfig,
    message: &str,
    session_id: Option<&str>,
    progress_tx: Option<mpsc::Sender<CliProgress>>,
) -> Result<CliResult, String> {
    run_cli(config, message, session_id, None, progress_tx).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_is_astra() {
        let p = CliProfile::default();
        assert_eq!(p.name(), "astra");
    }

    // ── Astra JSON parsing ──────────────────────────────────────────

    #[test]
    fn parse_astra_valid() {
        let r = parse_astra_json(
            r#"{"trace_id":"trace-1","request_id":"req-1","run_id":"run-1","session_id":"ses-1","text":"Hello","tool_calls_count":2,"tools_used":["bash"],"prompt_tokens":100,"completion_tokens":50,"exit_code":0,"success":true,"error_kind":null}"#,
            0,
        );
        assert!(r.success);
        assert_eq!(r.trace_id.as_deref(), Some("trace-1"));
        assert_eq!(r.request_id.as_deref(), Some("req-1"));
        assert_eq!(r.run_id.as_deref(), Some("run-1"));
        assert_eq!(r.session_id.as_deref(), Some("ses-1"));
        assert_eq!(r.text.as_deref(), Some("Hello"));
        assert_eq!(r.tool_calls_count, Some(2));
        assert_eq!(r.tools_used, vec!["bash"]);
    }

    #[test]
    fn parse_astra_real_output() {
        let json = r#"{
            "trace_id": "trace-real",
            "request_id": "req-real",
            "run_id": "run-real",
            "session_id": "a0fc41a0-3176-480d-99fd-d52007cdb2ce",
            "text": "\nHello! 👋",
            "tool_calls_count": 0,
            "tools_used": [],
            "prompt_tokens": 7367,
            "completion_tokens": 44,
            "exit_code": 0,
            "success": true,
            "error_kind": null
        }"#;
        let r = parse_astra_json(json, 0);
        assert!(r.text.as_ref().unwrap().contains("Hello!"));
        assert_eq!(r.tokens_prompt, Some(7367));
        assert_eq!(r.error_kind, None);
    }

    #[test]
    fn parse_astra_malformed() {
        let r = parse_astra_json("not json", 1);
        assert!(!r.success);
        assert_eq!(r.error_kind.as_deref(), Some("malformed_envelope"));
        assert!(
            r.text
                .as_deref()
                .unwrap_or_default()
                .contains("invalid JSON")
        );
    }

    #[test]
    fn parse_astra_missing_required_field_is_typed_failure() {
        let r = parse_astra_json(
            r#"{"trace_id":"trace-1","request_id":"req-1","run_id":"run-1","session_id":"ses-1","text":"Hello","tool_calls_count":2,"tools_used":[],"prompt_tokens":100,"completion_tokens":50,"exit_code":0,"success":true}"#,
            0,
        );
        assert!(!r.success);
        assert_eq!(r.exit_code, 1);
        assert_eq!(r.error_kind.as_deref(), Some("malformed_envelope"));
        assert!(r.text.as_deref().unwrap_or_default().contains("error_kind"));
    }

    #[test]
    fn parse_astra_failure_envelope_preserves_error_kind() {
        let r = parse_astra_json(
            r#"{"trace_id":"trace-1","request_id":"req-1","run_id":"run-1","session_id":"ses-1","text":"tool failed","tool_calls_count":1,"tools_used":["bash"],"prompt_tokens":100,"completion_tokens":50,"exit_code":1,"success":false,"error_kind":"tool_failure"}"#,
            1,
        );
        assert!(!r.success);
        assert_eq!(r.error_kind.as_deref(), Some("tool_failure"));
        assert_eq!(r.exit_code, 1);
    }

    // ── Claude JSON parsing ─────────────────────────────────────────

    #[test]
    fn parse_claude_real_output() {
        let json = r#"{
            "type": "result",
            "subtype": "success",
            "result": "Hello!",
            "session_id": "28246761-888a-4d37-a694-c740f843f49d",
            "num_turns": 1,
            "duration_ms": 3322,
            "total_cost_usd": 0.15,
            "usage": {
                "input_tokens": 3,
                "cache_creation_input_tokens": 24984,
                "output_tokens": 5
            }
        }"#;
        let r = parse_claude_json(json, 0);
        assert_eq!(r.text.as_deref(), Some("Hello!"));
        assert_eq!(
            r.session_id.as_deref(),
            Some("28246761-888a-4d37-a694-c740f843f49d")
        );
        assert_eq!(r.tool_calls_count, Some(1));
        assert_eq!(r.tokens_prompt, Some(3));
        assert_eq!(r.tokens_completion, Some(5));
    }

    #[test]
    fn parse_claude_plain_text() {
        let r = parse_claude_json("Just plain text output", 0);
        assert_eq!(r.text.as_deref(), Some("Just plain text output"));
    }

    // ── Copilot JSONL parsing ───────────────────────────────────────

    #[test]
    fn parse_copilot_jsonl_realistic_events() {
        let jsonl = r#"{"type":"session.start","data":{"sessionId":"0cb916db-26aa-40f2-86b5-1ba81b225fd2","version":1,"producer":"copilot-agent","copilotVersion":"1.0.41","startTime":"2026-05-06T08:41:00Z"},"id":"e1","timestamp":"2026-05-06T08:41:00Z","parentId":null}
{"type":"assistant.message_delta","data":{"messageId":"m1","deltaContent":"Hel"},"id":"e2","timestamp":"2026-05-06T08:41:01Z","parentId":"e1","ephemeral":true}
{"type":"assistant.message_delta","data":{"messageId":"m1","deltaContent":"lo"},"id":"e3","timestamp":"2026-05-06T08:41:01Z","parentId":"e2","ephemeral":true}
{"type":"assistant.message","data":{"messageId":"m1","content":"Hello!","toolRequests":[{"toolCallId":"tc1","name":"shell"}],"outputTokens":5},"id":"e4","timestamp":"2026-05-06T08:41:02Z","parentId":"e3"}
{"type":"tool.execution_start","data":{"toolCallId":"tc1","toolName":"shell"},"id":"e5","timestamp":"2026-05-06T08:41:03Z","parentId":"e4"}
{"type":"assistant.usage","data":{"model":"gpt-5.2","inputTokens":10,"outputTokens":5},"id":"e6","timestamp":"2026-05-06T08:41:04Z","parentId":"e5","ephemeral":true}"#;
        let r = parse_copilot_jsonl(jsonl, 0);
        assert_eq!(
            r.session_id.as_deref(),
            Some("0cb916db-26aa-40f2-86b5-1ba81b225fd2")
        );
        assert_eq!(r.text.as_deref(), Some("Hello!"));
        assert_eq!(r.tool_calls_count, Some(1));
        assert_eq!(r.tools_used, vec!["shell"]);
        assert_eq!(r.tokens_prompt, Some(10));
        assert_eq!(r.tokens_completion, Some(5));
    }

    #[test]
    fn parse_copilot_policy_error_text() {
        let output = r#"{"type":"session.warning","data":{"warningType":"policy","message":"Third-party MCP servers are disabled"},"id":"e1","timestamp":"2026-05-06T08:41:00Z","parentId":null,"ephemeral":true}
Error: Access denied by policy settings

Your Copilot CLI policy setting may be preventing access."#;
        let r = parse_copilot_jsonl(output, 1);
        assert!(!r.success);
        assert!(
            r.text
                .as_deref()
                .unwrap_or_default()
                .contains("Access denied")
        );
    }

    #[test]
    fn parse_copilot_result_session_with_leading_osc() {
        let output = concat!(
            "\x1b]2;copilot \"$@\"\x07\x1b]1;\x07",
            r#"{"type":"assistant.message","data":{"messageId":"m1","content":"收到","toolRequests":[],"outputTokens":2},"id":"e1","timestamp":"2026-05-06T09:08:34Z","parentId":null}"#,
            "\n",
            r#"{"type":"result","timestamp":"2026-05-06T09:08:34Z","sessionId":"5de42873-32fa-4856-b97e-2193d225d265","exitCode":0}"#,
        );
        let r = parse_copilot_jsonl(output, 0);
        assert_eq!(r.text.as_deref(), Some("收到"));
        assert_eq!(
            r.session_id.as_deref(),
            Some("5de42873-32fa-4856-b97e-2193d225d265")
        );
    }

    // ── Generic JSON parsing ────────────────────────────────────────

    #[test]
    fn parse_generic() {
        let r = parse_generic_json(
            r#"{"result":"ok","session_id":"s2"}"#,
            0,
            "result",
            "session_id",
        );
        assert_eq!(r.text.as_deref(), Some("ok"));
        assert_eq!(r.session_id.as_deref(), Some("s2"));
    }

    // ── Command building ────────────────────────────────────────────

    #[test]
    fn build_astra_command() {
        let p = CliProfile::Astra {
            bin: "astra".into(),
            model: Some("MiniMax-M2.7".into()),
            permission_mode: "auto".into(),
            app_server_url: None,
        };
        let cmd = p.build_command("hello", Some("ses-1"), None);
        let prog = cmd.as_std().get_program().to_str().unwrap();
        assert_eq!(prog, "astra");
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("--json")));
        assert!(args.contains(&std::ffi::OsStr::new("--session-id")));
        assert!(args.contains(&std::ffi::OsStr::new("MiniMax-M2.7")));
    }

    #[test]
    fn build_astra_command_sets_api_url_from_app_server_url() {
        let p = CliProfile::Astra {
            bin: "astra".into(),
            model: None,
            permission_mode: "auto".into(),
            app_server_url: Some("http://127.0.0.1:28000".into()),
        };
        let cmd = p.build_command("hello", None, None);
        let envs: std::collections::HashMap<_, _> = cmd.as_std().get_envs().collect();
        assert_eq!(
            envs.get(std::ffi::OsStr::new("ASTRA_API_URL"))
                .and_then(|value| *value)
                .and_then(|value| value.to_str()),
            Some("http://127.0.0.1:28000")
        );
    }

    #[test]
    fn build_claude_command() {
        let p = CliProfile::Claude {
            bin: "claude".into(),
            model: Some("claude-sonnet-4-6".into()),
            stream_json: false,
            extra_args: vec![],
            env: Default::default(),
            env_file: None,
        };
        let cmd = p.build_command("fix bug", None, None);
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("-p")));
        assert!(args.contains(&std::ffi::OsStr::new("fix bug")));
        assert!(args.contains(&std::ffi::OsStr::new("--output-format")));
    }

    #[test]
    fn claude_uses_append_system_prompt() {
        let p = CliProfile::Claude {
            bin: "claude".into(),
            model: None,
            stream_json: false,
            extra_args: vec![],
            env: Default::default(),
            env_file: None,
        };
        let cmd = p.build_command_with_context("hi", None, None, Some("gateway context"));
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("--append-system-prompt")));
        assert!(args.contains(&std::ffi::OsStr::new("gateway context")));
        assert!(!args.contains(&std::ffi::OsStr::new("--system-prompt")));
    }

    #[test]
    fn astra_uses_append_system_prompt() {
        let p = CliProfile::Astra {
            bin: "astra".into(),
            model: None,
            permission_mode: "auto".into(),
            app_server_url: None,
        };
        let cmd = p.build_command_with_context("hi", None, None, Some("gateway context"));
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("--append-system-prompt")));
        assert!(args.contains(&std::ffi::OsStr::new("gateway context")));
    }

    #[test]
    fn build_copilot_command() {
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: Some("gpt-5.2".into()),
            env: std::collections::BTreeMap::new(),
            env_file: None,
            launcher: None,
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec!["--disable-builtin-mcps".into()],
        };
        let cmd = p.build_command_with_context("fix bug", Some("0cb916d"), None, Some("gateway"));
        assert_eq!(cmd.as_std().get_program(), std::ffi::OsStr::new("copilot"));
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("-p")));
        assert!(args.contains(&std::ffi::OsStr::new("--output-format")));
        assert!(args.contains(&std::ffi::OsStr::new("json")));
        assert!(args.contains(&std::ffi::OsStr::new("--allow-all-tools")));
        assert!(args.contains(&std::ffi::OsStr::new("--resume=0cb916d")));
        assert!(args.contains(&std::ffi::OsStr::new("gpt-5.2")));
        assert!(args.contains(&std::ffi::OsStr::new("--disable-builtin-mcps")));
        let prompt = args
            .iter()
            .find_map(|arg| arg.to_str().filter(|s| s.contains("User message")));
        assert!(
            prompt.is_some(),
            "gateway context should be folded into prompt"
        );
    }

    #[test]
    fn build_copilot_script_launcher_command() {
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: None,
            env: std::collections::BTreeMap::new(),
            env_file: None,
            launcher: Some(CliLauncher::Script {
                path: "~/bin/copilot-launcher".into(),
                args: vec!["--profile".into(), "bedrock".into()],
            }),
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec![],
        };
        let cmd = p.build_command("hello", Some("sid-1"), None);
        assert!(
            cmd.as_std()
                .get_program()
                .to_str()
                .is_some_and(|s| s.ends_with("/bin/copilot-launcher"))
        );
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args.first().and_then(|arg| arg.to_str()), Some("--profile"));
        assert_eq!(args.get(1).and_then(|arg| arg.to_str()), Some("bedrock"));
        assert_eq!(args.get(2).and_then(|arg| arg.to_str()), Some("copilot"));
        assert!(args.contains(&std::ffi::OsStr::new("-p")));
        assert!(args.contains(&std::ffi::OsStr::new("--resume=sid-1")));
    }

    #[test]
    fn copilot_env_file_is_loaded_with_inline_override() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("copilot.env");
        std::fs::write(&env_path, "COPILOT_PROVIDER=file\nAWS_REGION=us-east-1\n").unwrap();
        let mut env = std::collections::BTreeMap::new();
        env.insert("COPILOT_PROVIDER".to_string(), "inline".to_string());
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: None,
            env,
            env_file: Some(env_path.to_string_lossy().to_string()),
            launcher: None,
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec![],
        };
        let mut cmd = Command::new("env");
        p.apply_runtime_environment(&mut cmd).unwrap();
        let envs: std::collections::BTreeMap<_, _> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| Some((key.to_str()?, value?.to_str()?)))
            .collect();
        assert_eq!(envs.get("COPILOT_PROVIDER"), Some(&"inline"));
        assert_eq!(envs.get("AWS_REGION"), Some(&"us-east-1"));
    }

    #[test]
    fn claude_env_file_is_loaded_with_inline_override() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("claude.env");
        std::fs::write(
            &env_path,
            "CLAUDE_CODE_OAUTH_TOKEN=file-token\nHTTPS_PROXY=http://proxy\n",
        )
        .unwrap();
        let mut env = std::collections::BTreeMap::new();
        env.insert(
            "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
            "inline-token".to_string(),
        );
        let p = CliProfile::Claude {
            bin: "claude".into(),
            model: None,
            stream_json: true,
            extra_args: vec![],
            env,
            env_file: Some(env_path.to_string_lossy().to_string()),
        };
        let mut cmd = Command::new("env");
        p.apply_runtime_environment(&mut cmd).unwrap();
        let envs: std::collections::BTreeMap<_, _> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(key, value)| Some((key.to_str()?, value?.to_str()?)))
            .collect();
        assert_eq!(envs.get("CLAUDE_CODE_OAUTH_TOKEN"), Some(&"inline-token"));
        assert_eq!(envs.get("HTTPS_PROXY"), Some(&"http://proxy"));
    }

    #[test]
    fn copilot_missing_env_file_is_traceable() {
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: None,
            env: std::collections::BTreeMap::new(),
            env_file: Some("/tmp/astra-gateway-missing-copilot.env".into()),
            launcher: None,
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec![],
        };
        let mut cmd = Command::new("env");
        let err = p.apply_runtime_environment(&mut cmd).unwrap_err();
        assert!(err.contains("env_file"));
        assert!(err.contains("/tmp/astra-gateway-missing-copilot.env"));
    }

    #[test]
    fn build_custom_command() {
        let p = CliProfile::Custom {
            bin: "my-agent".into(),
            args_template: vec![
                "--msg".into(),
                "{message}".into(),
                "--sid".into(),
                "{session_id}".into(),
            ],
            json_output: true,
            text_field: Some("output".into()),
            session_id_field: Some("id".into()),
        };
        let cmd = p.build_command("hello", Some("s1"), None);
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("hello")));
        assert!(args.contains(&std::ffi::OsStr::new("s1")));
    }

    // ── Profile deserialization ──────────────────────────────────────

    #[test]
    fn deserialize_astra_profile() {
        let yaml = r#"type: astra
bin: /usr/local/bin/astra
model: MiniMax-M2.7"#;
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.name(), "astra");
    }

    #[test]
    fn deserialize_claude_profile() {
        let yaml = r#"type: claude
model: claude-sonnet-4-6"#;
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.name(), "claude");
    }

    #[test]
    fn deserialize_copilot_profile() {
        let yaml = r#"type: copilot
model: gpt-5.2
env:
  AWS_REGION: us-east-1
env_file: ~/.astra-gateway/copilot.env
extra_args:
  - --disable-builtin-mcps"#;
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.name(), "copilot");
        assert_eq!(p.model_name(), Some("gpt-5.2"));
        match p {
            CliProfile::Copilot {
                env,
                env_file,
                launcher,
                ..
            } => {
                assert_eq!(env.get("AWS_REGION").map(String::as_str), Some("us-east-1"));
                assert_eq!(env_file.as_deref(), Some("~/.astra-gateway/copilot.env"));
                assert_eq!(launcher, None);
            }
            other => panic!("expected copilot profile, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_copilot_script_launcher_profile() {
        let yaml = r#"type: copilot
launcher:
  type: script
  path: ~/.astra-gateway/copilot-launcher
  args:
    - --provider
    - bedrock"#;
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        match p {
            CliProfile::Copilot { launcher, .. } => {
                assert_eq!(
                    launcher,
                    Some(CliLauncher::Script {
                        path: "~/.astra-gateway/copilot-launcher".into(),
                        args: vec!["--provider".into(), "bedrock".into()],
                    })
                );
            }
            other => panic!("expected copilot profile, got {other:?}"),
        }
    }

    // ── Run tests ───────────────────────────────────────────────────

    #[tokio::test]
    async fn run_echo() {
        let p = CliProfile::Custom {
            bin: "echo".into(),
            args_template: vec!["hello".into()],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli(&p, "ignored", None, None, None).await.unwrap();
        assert_eq!(r.exit_code, 0);
        assert!(r.text.as_ref().unwrap().contains("hello"));
    }

    #[tokio::test]
    async fn run_captures_stderr() {
        // sh -c writes to stderr and exits non-zero
        let p = CliProfile::Custom {
            bin: "sh".into(),
            args_template: vec!["-c".into(), "echo errmsg >&2; exit 3".into()],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli(&p, "ignored", None, None, None).await.unwrap();
        assert_eq!(r.exit_code, 3);
        assert!(r.stderr.contains("errmsg"), "stderr={}", r.stderr);
    }

    #[tokio::test]
    async fn run_kills_cli_on_timeout() {
        // Use `sleep 30` directly (not via sh -c) so kill is immediate.
        let p = CliProfile::Custom {
            bin: "sleep".into(),
            args_template: vec!["30".into()],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let err = run_cli_with_context_and_timeout(
            &p,
            "ignored",
            None,
            None,
            None,
            None,
            Some(Duration::from_millis(50)),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.contains("timed out"), "{err}");
    }

    #[tokio::test]
    async fn run_injects_gateway_trace_env() {
        let p = CliProfile::Custom {
            bin: "sh".into(),
            args_template: vec![
                "-c".into(),
                "printf '%s/%s' \"$ASTRA_GATEWAY_TRACE_ID\" \"$ASTRA_GATEWAY_REQUEST_ID\"".into(),
            ],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli_with_context_trace_and_timeout(
            &p,
            "ignored",
            None,
            None,
            None,
            None,
            Some("trace-1"),
            Some("req-1"),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(r.text.as_deref(), Some("trace-1/req-1"));
    }

    #[tokio::test]
    async fn run_injects_access_token_env() {
        let p = CliProfile::Custom {
            bin: "sh".into(),
            args_template: vec!["-c".into(), "printf '%s' \"$ASTRA_ACCESS_TOKEN\"".into()],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli_with_context_trace_and_timeout(
            &p,
            "ignored",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("test-token-xyz"),
            None,
        )
        .await
        .unwrap();
        assert_eq!(r.text.as_deref(), Some("test-token-xyz"));
    }

    #[tokio::test]
    async fn run_no_access_token_when_none() {
        let p = CliProfile::Custom {
            bin: "sh".into(),
            args_template: vec![
                "-c".into(),
                "printf '%s' \"${ASTRA_ACCESS_TOKEN:-unset}\"".into(),
            ],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli_with_context_trace_and_timeout(
            &p, "ignored", None, None, None, None, None, None, None, None, None,
        )
        .await
        .unwrap();
        assert_eq!(r.text.as_deref(), Some("unset"));
    }

    #[tokio::test]
    async fn run_copilot_script_launcher_invokes_script() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("copilot-launcher");
        std::fs::write(
            &script_path,
            r#"#!/bin/sh
bin="$1"
shift
if [ "$bin" != "copilot" ]; then
  echo "unexpected bin: $bin" >&2
  exit 2
fi
printf '%s\n' '{"type":"session.start","data":{"sessionId":"script-session"},"id":"e1"}'
printf '%s\n' '{"type":"assistant.message_delta","data":{"deltaContent":"from script"},"id":"e2"}'
"#,
        )
        .unwrap();
        let mut perms = std::fs::metadata(&script_path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
            std::fs::set_permissions(&script_path, perms).unwrap();
        }
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: None,
            env: std::collections::BTreeMap::new(),
            env_file: None,
            launcher: Some(CliLauncher::Script {
                path: script_path.to_string_lossy().to_string(),
                args: vec![],
            }),
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec![],
        };
        let r = run_cli_with_context_trace_and_timeout(
            &p, "ignored", None, None, None, None, None, None, None, None, None,
        )
        .await
        .unwrap();
        assert_eq!(r.session_id.as_deref(), Some("script-session"));
        assert_eq!(r.text.as_deref(), Some("from script"));
    }

    #[tokio::test]
    async fn run_nonexistent() {
        let p = CliProfile::Custom {
            bin: "/nonexistent/xyz".into(),
            args_template: vec![],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let r = run_cli(&p, "test", None, None, None).await;
        assert!(r.is_err());
    }

    // ── CLI availability ──────────────────────────────────────────

    #[tokio::test]
    async fn probe_existing_binary() {
        let p = CliProfile::Custom {
            bin: "echo".into(),
            args_template: vec![],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let avail = probe_cli(&p).await;
        assert!(avail.is_available());
    }

    #[tokio::test]
    async fn probe_nonexistent_binary() {
        let p = CliProfile::Custom {
            bin: "/nonexistent/no-such-cli-12345".into(),
            args_template: vec![],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let avail = probe_cli(&p).await;
        assert_eq!(avail, CliAvailability::NotInstalled);
    }

    #[test]
    fn onboarding_not_installed() {
        let p = CliProfile::default();
        let msg = onboarding_message(&p, &CliAvailability::NotInstalled);
        assert!(msg.contains("未安装"));
        assert!(msg.contains("/cli"));
    }

    #[test]
    fn onboarding_available_is_empty() {
        let p = CliProfile::default();
        let msg = onboarding_message(
            &p,
            &CliAvailability::Available {
                version: Some("1.0".into()),
            },
        );
        assert!(msg.is_empty());
    }

    #[test]
    fn stale_session_error_is_backend_specific() {
        let claude = CliProfile::Claude {
            bin: "claude".into(),
            model: None,
            stream_json: true,
            extra_args: vec![],
            env: std::collections::BTreeMap::new(),
            env_file: None,
        };
        assert!(claude.is_stale_session_error(
            "No conversation found with session ID: f22599c8-9c3b-44b0-bb96-613c4db552d9"
        ));

        let codex = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            reasoning_effort: None,
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: false,
            ephemeral: false,
        };
        assert!(codex.is_stale_session_error(
            "Error: thread/resume: thread/resume failed: no rollout found for thread id 00000000-0000-0000-0000-000000000000 (code -32600)"
        ));
        assert!(!codex.is_stale_session_error(
            "Error: thread/resume: thread/resume failed: capability mismatch"
        ));

        assert!(!CliProfile::default().is_stale_session_error("session not found"));
    }

    #[test]
    fn translate_auth_error() {
        let p = CliProfile::default();
        let msg = translate_cli_error(&p, 1, "Error: Could not validate credentials");
        assert!(msg.contains("认证失败"));
        assert!(msg.contains("/auth"));
    }

    // ── is_auth_error tests ──────────────────────────────────────

    #[test]
    fn is_auth_error_validates_credentials() {
        assert!(is_auth_error("Could not validate credentials"));
        assert!(is_auth_error("could not validate credentials"));
    }

    #[test]
    fn is_auth_error_invalid_api_key() {
        assert!(is_auth_error("Error: invalid_api_key"));
        assert!(is_auth_error("invalid api key provided"));
    }

    #[test]
    fn is_auth_error_401_status() {
        assert!(is_auth_error("status: 401"));
        assert!(is_auth_error("HTTP 401 response"));
        assert!(is_auth_error("error 401"));
    }

    #[test]
    fn is_auth_error_token_expired() {
        assert!(is_auth_error("token expired"));
        assert!(is_auth_error("Token has expired"));
    }

    #[test]
    fn is_auth_error_copilot_policy() {
        assert!(is_auth_error("Access denied by policy settings"));
        let p = CliProfile::Copilot {
            bin: "copilot".into(),
            model: None,
            env: std::collections::BTreeMap::new(),
            env_file: None,
            launcher: None,
            stream_json: true,
            allow_all_tools: true,
            extra_args: vec![],
        };
        let msg = translate_cli_error(&p, 1, "Access denied by policy settings");
        assert!(msg.contains("copilot login"));
        assert!(msg.contains("策略"));
    }

    #[test]
    fn is_auth_error_false_positives_avoided() {
        assert!(!is_auth_error("port 4010 is in use"));
        assert!(!is_auth_error("some random error"));
        assert!(!is_auth_error("timeout after 30s"));
        assert!(!is_auth_error("rate limit exceeded"));
    }

    #[test]
    fn translate_rate_limit() {
        let p = CliProfile::default();
        let msg = translate_cli_error(&p, 1, "429 rate limit exceeded");
        assert!(msg.contains("频繁"));
    }

    #[test]
    fn translate_generic_error() {
        let p = CliProfile::default();
        let msg = translate_cli_error(&p, 42, "some random error");
        assert!(msg.contains("exit=42"));
    }

    #[tokio::test]
    async fn progress_channel_closed_after_exit() {
        let p = CliProfile::Custom {
            bin: "echo".into(),
            args_template: vec!["done".into()],
            json_output: false,
            text_field: None,
            session_id_field: None,
        };
        let (tx, mut rx) = mpsc::channel(64);
        let _ = run_cli(&p, "", None, None, Some(tx)).await;
        while rx.try_recv().is_ok() {}
        assert!(rx.recv().await.is_none());
    }

    // ── Stream events JSONL parsing ──

    #[test]
    fn parse_token_event() {
        let line = r#"{"type":"token","text":"hello"}"#;
        assert!(matches!(parse_stderr_line(line), CliProgress::Token(t) if t == "hello"));
    }

    #[test]
    fn parse_thinking_event() {
        let line = r#"{"type":"thinking","active":true}"#;
        assert!(matches!(
            parse_stderr_line(line),
            CliProgress::Thinking(true)
        ));
    }

    #[test]
    fn parse_tool_started_event() {
        let line = r#"{"type":"tool_started","name":"bash","description":"ls"}"#;
        assert!(
            matches!(parse_stderr_line(line), CliProgress::ToolStarted { name, .. } if name == "bash")
        );
    }

    #[test]
    fn parse_tool_completed_event() {
        let line = r#"{"type":"tool_completed","name":"read_file","description":"x","status":"ok","duration_ms":42,"output_summary":null}"#;
        assert!(
            matches!(parse_stderr_line(line), CliProgress::ToolDone { name, duration_ms } if name == "read_file" && duration_ms == Some(42))
        );
    }

    #[test]
    fn parse_fallback_for_plain_stderr() {
        let line = "some random log line";
        assert!(matches!(parse_stderr_line(line), CliProgress::Status(_)));
    }

    #[test]
    fn parse_fallback_for_tool_heuristic() {
        let line = "⚡ running tool bash";
        assert!(matches!(parse_stderr_line(line), CliProgress::ToolCall(_)));
    }

    #[test]
    fn parse_malformed_json_falls_back() {
        let line = r#"{"type":"token","text":"he"#; // truncated JSON
        assert!(matches!(parse_stderr_line(line), CliProgress::Status(_)));
    }

    #[test]
    fn parse_unknown_type_falls_back() {
        let line = r#"{"type":"future_event","data":42}"#;
        assert!(matches!(parse_stderr_line(line), CliProgress::Status(_)));
    }

    #[test]
    fn parse_empty_token_text() {
        let line = r#"{"type":"token","text":""}"#;
        assert!(matches!(parse_stderr_line(line), CliProgress::Token(t) if t.is_empty()));
    }

    #[test]
    fn parse_unicode_tool_name() {
        let line = r#"{"type":"tool_started","name":"读取文件","description":"src/main.rs"}"#;
        assert!(
            matches!(parse_stderr_line(line), CliProgress::ToolStarted { name, .. } if name == "读取文件")
        );
    }

    #[test]
    fn parse_null_fields_handled() {
        let line = r#"{"type":"tool_completed","name":"bash","description":null,"status":"ok","duration_ms":0,"output_summary":null}"#;
        assert!(
            matches!(parse_stderr_line(line), CliProgress::ToolDone { name, .. } if name == "bash")
        );
    }

    #[test]
    fn parse_thinking_chunk_event() {
        let line = r#"{"type":"thinking_chunk","text":"let me consider..."}"#;
        assert!(matches!(
            parse_stderr_line(line),
            CliProgress::ReasoningBlock { kind: ReasoningKind::Raw, text } if text == "let me consider..."
        ));
    }

    #[test]
    fn parse_copilot_reasoning_delta_progress() {
        let line = r#"{"type":"assistant.reasoning_delta","data":{"deltaContent":"checking context"},"id":"r1"}"#;
        assert!(matches!(
            parse_stdout_jsonl_line(line, "copilot"),
            Some(CliProgress::ReasoningBlock { kind: ReasoningKind::Raw, text }) if text == "checking context"
        ));
    }

    #[test]
    fn parse_claude_thinking_block_skipped_in_assistant() {
        // thinking blocks in assistant messages are now skipped — handled via stream_event deltas
        let line = r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"considering options"}]}}"#;
        assert!(parse_stdout_jsonl_line(line, "claude").is_none());
    }

    #[test]
    fn reasoning_display_aliases() {
        assert_eq!(
            ReasoningDisplay::from_command_arg("thinking-chain"),
            Some(ReasoningDisplay::RawIfAvailable)
        );
        assert_eq!(
            ReasoningDisplay::from_command_arg("off"),
            Some(ReasoningDisplay::Off)
        );
        assert_eq!(ReasoningDisplay::from_command_arg("bogus"), None);
    }

    #[test]
    fn parse_waiting_for_model() {
        let line = r#"{"type":"waiting_for_model"}"#;
        assert!(matches!(parse_stderr_line(line), CliProgress::Status(_)));
    }

    #[test]
    fn parse_status_event() {
        let line = r#"{"type":"status","text":"compiling..."}"#;
        assert!(matches!(parse_stderr_line(line), CliProgress::Status(t) if t == "compiling..."));
    }

    // ── Codex JSONL parsing ──────────────────────────────────────────

    #[test]
    fn parse_codex_stream_thread_started() {
        let stdout = r#"{"type":"thread.started","thread_id":"th-abc123"}"#;
        let r = parse_codex_stream_json_stdout(stdout, 0);
        assert_eq!(r.session_id.as_deref(), Some("th-abc123"));
    }

    #[test]
    fn parse_codex_stream_full_session() {
        let stdout = concat!(
            r#"{"type":"thread.started","thread_id":"th-001"}"#,
            "\n",
            r#"{"type":"turn.started","turn_id":"t1"}"#,
            "\n",
            r#"{"type":"item.started","item":{"type":"agent_message","text":""}}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"Hello from Codex!"}}"#,
            "\n",
            r#"{"type":"item.started","item":{"type":"command_execution","command":"ls -la"}}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"command_execution","command":"ls -la","exit_code":0}}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"file_change","path":"src/main.rs"}}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"mcp_tool_call","tool":"search"}}"#,
            "\n",
            r#"{"type":"turn.completed","usage":{"input_tokens":500,"output_tokens":120}}"#,
            "\n",
        );
        let r = parse_codex_stream_json_stdout(stdout, 0);
        assert!(r.success);
        assert_eq!(r.session_id.as_deref(), Some("th-001"));
        assert_eq!(r.text.as_deref(), Some("Hello from Codex!"));
        assert_eq!(r.tool_calls_count, Some(3));
        assert_eq!(
            r.tools_used,
            vec!["command_execution", "file_change", "search"]
        );
        assert_eq!(r.tokens_prompt, Some(500));
        assert_eq!(r.tokens_completion, Some(120));
    }

    #[test]
    fn parse_codex_stream_line_item_started_message() {
        let line =
            r#"{"type":"item.started","item":{"type":"agent_message","text":"thinking..."}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::Token(t)) if t == "thinking..."
        ));
    }

    #[test]
    fn parse_codex_stream_line_reasoning() {
        let line = r#"{"type":"item.started","item":{"type":"reasoning"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::Thinking(true))
        ));
    }

    #[test]
    fn parse_codex_stream_line_reasoning_completed() {
        let line = r#"{"type":"item.completed","item":{"type":"reasoning"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::Thinking(false))
        ));
    }

    #[test]
    fn parse_codex_stream_line_command_started() {
        let line =
            r#"{"type":"item.started","item":{"type":"command_execution","command":"cargo test"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::ToolStarted { name, params: None }) if name == "cargo test"
        ));
    }

    #[test]
    fn parse_codex_stream_line_command_completed() {
        let line = r#"{"type":"item.completed","item":{"type":"command_execution","command":"cargo test"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::ToolDone { name, duration_ms: None }) if name == "cargo test"
        ));
    }

    #[test]
    fn parse_codex_stream_line_file_change() {
        let line = r#"{"type":"item.started","item":{"type":"file_change","path":"src/lib.rs"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::ToolStarted { name, params: None }) if name == "file_change"
        ));
    }

    #[test]
    fn parse_codex_stream_line_mcp_tool() {
        let line = r#"{"type":"item.started","item":{"type":"mcp_tool_call","tool":"web_search"}}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::ToolStarted { name, params: None }) if name == "web_search"
        ));
    }

    #[test]
    fn parse_codex_stream_line_turn_started() {
        let line = r#"{"type":"turn.started","turn_id":"t1"}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::Status(s)) if s.contains("codex turn started")
        ));
    }

    #[test]
    fn parse_codex_stream_line_error() {
        let line = r#"{"type":"error","message":"rate limit exceeded"}"#;
        assert!(matches!(
            parse_codex_stream_json_line(line),
            Some(CliProgress::Status(s)) if s.contains("rate limit exceeded")
        ));
    }

    #[test]
    fn parse_codex_non_streaming_fallback() {
        let stdout = r#"{"result":"done","session_id":"sid-xyz"}"#;
        let r = parse_generic_json(stdout, 0, "result", "session_id");
        assert_eq!(r.text.as_deref(), Some("done"));
        assert_eq!(r.session_id.as_deref(), Some("sid-xyz"));
    }

    // ── Codex command building ─────────────────────────────────────

    #[test]
    fn build_codex_exec_command() {
        let p = CliProfile::Codex {
            bin: "codex".into(),
            model: Some("o3".into()),
            reasoning_effort: Some("high".into()),
            sandbox: "workspace-write".into(),
            stream_json: true,
            extra_args: vec!["-c".into(), "model_reasoning_effort=\"low\"".into()],
            skip_git_repo_check: false,
            ephemeral: false,
        };
        let cmd = p.build_command("fix the bug", None, None);
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(cmd.as_std().get_program(), std::ffi::OsStr::new("codex"));
        assert!(args.contains(&std::ffi::OsStr::new("exec")));
        assert!(args.contains(&std::ffi::OsStr::new("fix the bug")));
        assert!(!args.contains(&std::ffi::OsStr::new("--sandbox")));
        assert!(!args.contains(&std::ffi::OsStr::new("workspace-write")));
        assert!(args.contains(&std::ffi::OsStr::new("--json")));
        assert!(args.contains(&std::ffi::OsStr::new("--model")));
        assert!(args.contains(&std::ffi::OsStr::new("o3")));
        assert!(args.contains(&std::ffi::OsStr::new("model_reasoning_effort=\"high\"")));
        assert_eq!(
            args.iter()
                .rposition(|arg| *arg == std::ffi::OsStr::new("model_reasoning_effort=\"high\"")),
            Some(args.len() - 1)
        );
    }

    #[test]
    fn build_codex_resume_command() {
        let p = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            reasoning_effort: None,
            sandbox: "read-only".into(),
            stream_json: true,
            extra_args: vec![],
            skip_git_repo_check: true,
            ephemeral: true,
        };
        let cmd = p.build_command("follow up", Some("th-abc123"), None);
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(args.contains(&std::ffi::OsStr::new("exec")));
        assert!(args.contains(&std::ffi::OsStr::new("resume")));
        assert!(args.contains(&std::ffi::OsStr::new("th-abc123")));
        assert!(args.contains(&std::ffi::OsStr::new("follow up")));
        assert!(args.contains(&std::ffi::OsStr::new("--skip-git-repo-check")));
        assert!(args.contains(&std::ffi::OsStr::new("--ephemeral")));
    }

    #[test]
    fn build_codex_no_json_without_stream() {
        let p = CliProfile::Codex {
            bin: "codex".into(),
            model: None,
            reasoning_effort: None,
            sandbox: "workspace-write".into(),
            stream_json: false,
            extra_args: vec!["--quiet".into()],
            skip_git_repo_check: false,
            ephemeral: false,
        };
        let cmd = p.build_command("hello", None, None);
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert!(!args.contains(&std::ffi::OsStr::new("--json")));
        assert!(args.contains(&std::ffi::OsStr::new("--quiet")));
    }

    // ── Codex profile deserialization ──────────────────────────────

    #[test]
    fn deserialize_codex_profile_full() {
        let yaml = r#"type: codex
bin: /usr/local/bin/codex
model: o3
sandbox: danger-full-access
stream_json: true
skip_git_repo_check: true
ephemeral: true
extra_args:
  - --quiet"#;
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.name(), "codex");
        assert_eq!(p.model_name(), Some("o3"));
        match p {
            CliProfile::Codex {
                sandbox,
                stream_json,
                skip_git_repo_check,
                ephemeral,
                extra_args,
                ..
            } => {
                assert_eq!(sandbox, "danger-full-access");
                assert!(stream_json);
                assert!(skip_git_repo_check);
                assert!(ephemeral);
                assert_eq!(extra_args, vec!["--quiet"]);
            }
            other => panic!("expected codex profile, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_codex_profile_minimal() {
        let yaml = "type: codex";
        let p: CliProfile = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(p.name(), "codex");
        match p {
            CliProfile::Codex {
                bin,
                model,
                sandbox,
                stream_json,
                ..
            } => {
                assert_eq!(bin, "codex");
                assert_eq!(model, None);
                assert_eq!(sandbox, "workspace-write");
                assert!(!stream_json);
            }
            other => panic!("expected codex profile, got {other:?}"),
        }
    }

    #[test]
    fn codex_dispatch_via_parse_stdout_jsonl_line() {
        let line = r#"{"type":"item.started","item":{"type":"agent_message","text":"hi"}}"#;
        assert!(matches!(
            parse_stdout_jsonl_line(line, "codex"),
            Some(CliProgress::Token(t)) if t == "hi"
        ));
        // Same line should NOT parse via claude/copilot path
        assert!(parse_stdout_jsonl_line(line, "claude").is_none());
    }

    // ── Cancellation tests ─────────────────────────────────────────────
    //
    // Test `run_child_with_cancel` DIRECTLY — bypasses build_command_with_context
    // so we can pass a real blocking command (plain `cat` with no args = blocks
    // forever on stdin). This avoids the false-positive bug where `cat` received
    // `-m msg --json` args and exited immediately.

    #[tokio::test]
    async fn cancel_pre_fired_kills_immediately() {
        use tokio_util::sync::CancellationToken;

        // Pre-cancelled token: function must return Err without blocking.
        let token = CancellationToken::new();
        token.cancel();

        // `sleep 30` would block 30s, but pre-fired cancel kills immediately.
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let result = run_child_with_cancel(
            cmd,
            None,
            Some(Duration::from_secs(30)),
            Some(token),
            "test",
        )
        .await;

        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("killed by user"),
            "pre-fired token must produce killed error"
        );
    }

    #[tokio::test]
    async fn cancel_kills_blocking_process() {
        use tokio_util::sync::CancellationToken;

        let token = CancellationToken::new();
        let token_clone = token.clone();

        // `sleep 30` blocks for 30s — guaranteed to still be running when cancel fires.
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let handle = tokio::spawn(async move {
            run_child_with_cancel(
                cmd,
                None,
                Some(Duration::from_secs(60)),
                Some(token_clone),
                "test",
            )
            .await
        });

        // Cancel fires. The select! picks it up and kills the child.
        token.cancel();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("task must complete promptly after cancel")
            .expect("join");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("killed by user"),
            "expected 'killed by user', got: {err}"
        );
    }

    #[tokio::test]
    async fn no_cancel_completes_normally() {
        // `true` exits 0 immediately; no cancel token.
        let cmd = Command::new("true");
        let result =
            run_child_with_cancel(cmd, None, Some(Duration::from_secs(5)), None, "test").await;
        assert!(result.is_ok(), "true must exit 0: {result:?}");
        let (stdout, _stderr, code) = result.unwrap();
        assert_eq!(code, 0);
        assert!(stdout.is_empty());
    }

    #[tokio::test]
    async fn timeout_kills_blocking_process() {
        use tokio_util::sync::CancellationToken;

        // `sleep 30` blocks for 30s; token NOT cancelled; 100ms timeout fires.
        let token = CancellationToken::new();
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let result = run_child_with_cancel(
            cmd,
            None,
            Some(Duration::from_millis(100)),
            Some(token.clone()),
            "test",
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("timed out"), "expected timeout, got: {err}");
        assert!(
            !token.is_cancelled(),
            "token must NOT be cancelled by timeout"
        );
    }

    #[test]
    fn stream_event_parsing_matches_real_cli_output() {
        use super::*;

        // Simulate the exact sequence from a real Claude CLI session:
        // thinking → text → tool_use → text(final)
        let lines = vec![
            // system (ignored)
            r#"{"type":"system","subtype":"init","session_id":"abc"}"#,
            // message_start (ignored)
            r#"{"type":"stream_event","event":{"type":"message_start","message":{"model":"claude"}}}"#,
            // thinking block
            r#"{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me think about this"}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_stop","index":0}}"#,
            // text block
            r#"{"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Let me "}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"check."}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_stop","index":1}}"#,
            // tool_use block
            r#"{"type":"stream_event","event":{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"t1","name":"Bash","input":{}}}}"#,
            r#"{"type":"stream_event","event":{"type":"content_block_stop","index":2}}"#,
            // assistant complete message (should NOT duplicate events)
            r#"{"type":"assistant","message":{"content":[{"type":"thinking","thinking":"let me think"},{"type":"text","text":"Let me check."},{"type":"tool_use","name":"Bash","id":"t1","input":{}}]}}"#,
            // message end
            r#"{"type":"stream_event","event":{"type":"message_delta","delta":{"stop_reason":"tool_use"}}}"#,
            r#"{"type":"stream_event","event":{"type":"message_stop"}}"#,
            // result
            r#"{"type":"result","result":"final","session_id":"abc"}"#,
        ];

        let events: Vec<Option<CliProgress>> = lines
            .iter()
            .map(|l| parse_claude_stream_json_line(l))
            .collect();

        // system → None
        assert!(events[0].is_none());
        // message_start → None
        assert!(events[1].is_none());
        // thinking content_block_start → None
        assert!(events[2].is_none());
        // thinking_delta → ReasoningBlock
        assert!(
            matches!(&events[3], Some(CliProgress::ReasoningBlock { kind: ReasoningKind::Raw, text }) if text == "let me think about this")
        );
        // thinking content_block_stop → None
        assert!(events[4].is_none());
        // text content_block_start → None
        assert!(events[5].is_none());
        // text_delta "Let me " → Token
        assert!(matches!(&events[6], Some(CliProgress::Token(t)) if t == "Let me "));
        // text_delta "check." → Token
        assert!(matches!(&events[7], Some(CliProgress::Token(t)) if t == "check."));
        // text content_block_stop → None
        assert!(events[8].is_none());
        // tool_use content_block_start → None (params not available yet)
        assert!(events[9].is_none());
        // tool_use content_block_stop → None
        assert!(events[10].is_none());
        // assistant complete message → ToolStarted with params (extracted from input)
        assert!(
            matches!(&events[11], Some(CliProgress::ToolStarted { name, .. }) if name == "Bash")
        );
        // message_delta → None
        assert!(events[12].is_none());
        // message_stop → None
        assert!(events[13].is_none());
        // result → None
        assert!(events[14].is_none());
    }

    #[test]
    fn claude_system_status_output_is_preserved_with_empty_result() {
        let local_command = serde_json::json!({
            "type": "system",
            "subtype": "local_command",
            "content": "<local-command-stdout>Not enough messages to compact.</local-command-stdout>"
        });
        assert_eq!(
            claude_system_status_output(&local_command).as_deref(),
            Some("Not enough messages to compact.")
        );

        let result = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "session_id": "compact-session",
            "result": "",
            "usage": {"input_tokens": 0, "output_tokens": 0},
            "total_cost_usd": 0.0
        });
        let stdout = format!("{local_command}\n{result}\n");
        let parsed = parse_claude_stream_json_stdout(&stdout, 0);
        assert!(parsed.success);
        assert_eq!(
            parsed.text.as_deref(),
            Some("Not enough messages to compact.")
        );

        let compact_boundary = serde_json::json!({
            "type": "system",
            "subtype": "compact_boundary",
            "content": "Conversation compacted",
            "compactMetadata": {"trigger": "manual"}
        });
        assert_eq!(
            claude_system_status_output(&compact_boundary).as_deref(),
            Some("Conversation compacted")
        );
        let stdout = format!("{compact_boundary}\n{result}\n");
        let parsed = parse_claude_stream_json_stdout(&stdout, 0);
        assert_eq!(parsed.text.as_deref(), Some("Conversation compacted"));
    }

    #[test]
    fn claude_exit_zero_api_error_is_structured_failure() {
        let value = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "is_error": false,
            "api_error_status": 400,
            "session_id": "2e8c8b02-5c96-45e8-85b3-b74f0bc2f101",
            "result": "API Error: 400 event:error\ndata:{\"code\":\"InvalidParameter\",\"message\":\"Range of input length should be [1, 983616]\",\"request_id\":\"upstream-123\"}",
            "usage": {"input_tokens": 0, "output_tokens": 0},
            "total_cost_usd": 77.5227695
        });

        let result = parse_claude_result_value(&value, 0);
        assert!(!result.success);
        assert_eq!(result.error_kind.as_deref(), Some("context_overflow"));
        let raw: serde_json::Value =
            serde_json::from_str(result.raw_usage_json.as_deref().expect("audit usage JSON"))
                .unwrap();
        assert!(raw.get("result").is_none());
        assert_eq!(raw["provider_error"]["status"], 400);
        assert_eq!(raw["provider_error"]["request_id"], "upstream-123");
        let error = result.provider_error.expect("structured provider error");
        assert_eq!(error.status, Some(400));
        assert_eq!(error.code.as_deref(), Some("InvalidParameter"));
        assert_eq!(error.request_id.as_deref(), Some("upstream-123"));
    }

    #[test]
    fn claude_api_error_text_fallback_and_generic_error() {
        let result = parse_claude_json("API Error: 503 service unavailable", 0);
        assert!(!result.success);
        assert_eq!(
            result.provider_error.as_ref().map(|error| error.status),
            Some(Some(503))
        );
        assert_eq!(result.error_kind.as_deref(), Some("provider_api_error"));

        let structured = serde_json::json!({
            "type": "result",
            "subtype": "error_during_execution",
            "is_error": true,
            "error": {"code": "ServiceUnavailable", "message": "try again"},
            "request_id": "req-generic",
            "result": "",
            "usage": {}
        });
        let result = parse_claude_result_value(&structured, 0);
        let error = result.provider_error.expect("generic provider error");
        assert_eq!(error.kind, "provider_api_error");
        assert_eq!(error.code.as_deref(), Some("ServiceUnavailable"));
        assert_eq!(error.request_id.as_deref(), Some("req-generic"));
    }

    #[test]
    fn claude_audit_usage_excludes_response_and_permission_details() {
        let value = serde_json::json!({
            "type": "result",
            "subtype": "success",
            "result": "sensitive model response",
            "permission_denials": [{"tool": "Read", "path": "/secret/path"}],
            "session_id": "session-1",
            "usage": {
                "input_tokens": 3,
                "output_tokens": 4,
                "iterations": [{"prompt": "secret iteration prompt"}],
                "provider_extension": {"secret": "opaque usage data"}
            },
            "modelUsage": {"model": {
                "inputTokens": 3,
                "outputTokens": 4,
                "debug": {"prompt": "secret model debug data"}
            }},
            "total_cost_usd": 0.25
        });

        let result = parse_claude_result_value(&value, 0);
        assert_eq!(result.text.as_deref(), Some("sensitive model response"));
        let raw: serde_json::Value =
            serde_json::from_str(result.raw_usage_json.as_deref().unwrap()).unwrap();
        assert!(raw.get("result").is_none());
        assert!(raw.get("permission_denials").is_none());
        assert!(raw["usage"].get("iterations").is_none());
        assert!(raw["usage"].get("provider_extension").is_none());
        assert!(raw["modelUsage"]["model"].get("debug").is_none());
        assert!(!raw.to_string().contains("secret"));
        assert_eq!(raw["usage"]["input_tokens"], 3);
        assert_eq!(raw["modelUsage"]["model"]["outputTokens"], 4);
    }

    #[test]
    fn persistent_claude_usage_is_per_turn_delta() {
        let first = serde_json::json!({
            "result": "first",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 20,
                "cache_creation_input_tokens": 10,
                "cache_read_input_tokens": 40
            },
            "modelUsage": {"model": {
                "inputTokens": 100,
                "outputTokens": 20,
                "cacheCreationInputTokens": 10,
                "cacheReadInputTokens": 40,
                "costUSD": 1.5
            }},
            "total_cost_usd": 1.5
        });
        let mut baseline = None;
        let mut first_result = parse_claude_result_value(&first, 0);
        normalize_claude_pool_usage(&mut first_result, &mut baseline);
        assert_eq!(first_result.tokens_prompt, Some(100));
        assert_eq!(first_result.cost_usd, Some(1.5));

        let second = serde_json::json!({
            "result": "second",
            "usage": {
                "input_tokens": 60,
                "output_tokens": 12,
                "cache_creation_input_tokens": 4,
                "cache_read_input_tokens": 35
            },
            "modelUsage": {"model": {
                "inputTokens": 160,
                "outputTokens": 32,
                "cacheCreationInputTokens": 14,
                "cacheReadInputTokens": 75,
                "costUSD": 2.1
            }},
            "total_cost_usd": 2.1
        });
        let mut second_result = parse_claude_result_value(&second, 0);
        normalize_claude_pool_usage(&mut second_result, &mut baseline);
        assert_eq!(second_result.tokens_prompt, Some(60));
        assert_eq!(second_result.tokens_completion, Some(12));
        assert_eq!(second_result.cache_creation_input_tokens, Some(4));
        assert_eq!(second_result.cache_read_input_tokens, Some(35));
        assert!((second_result.cost_usd.unwrap() - 0.6).abs() < 1e-9);

        let repeated_error = serde_json::json!({
            "api_error_status": 400,
            "result": "API Error: 400 event:error\ndata:{\"code\":\"InvalidParameter\",\"message\":\"Range of input length should be [1, 983616]\"}",
            "usage": {"input_tokens": 0, "output_tokens": 0},
            "modelUsage": {"model": {
                "inputTokens": 160,
                "outputTokens": 32,
                "cacheCreationInputTokens": 14,
                "cacheReadInputTokens": 75,
                "costUSD": 2.1
            }},
            "total_cost_usd": 2.1
        });
        let mut error_result = parse_claude_result_value(&repeated_error, 0);
        normalize_claude_pool_usage(&mut error_result, &mut baseline);
        assert_eq!(error_result.total_tokens, Some(0));
        assert_eq!(error_result.cost_usd, Some(0.0));

        let after_error = serde_json::json!({
            "result": "after",
            "usage": {
                "input_tokens": 30,
                "output_tokens": 8,
                "cache_creation_input_tokens": 1,
                "cache_read_input_tokens": 15
            },
            "modelUsage": {"model": {
                "inputTokens": 190,
                "outputTokens": 40,
                "cacheCreationInputTokens": 15,
                "cacheReadInputTokens": 90,
                "costUSD": 2.5
            }},
            "total_cost_usd": 2.5
        });
        let mut after_result = parse_claude_result_value(&after_error, 0);
        normalize_claude_pool_usage(&mut after_result, &mut baseline);
        assert_eq!(after_result.tokens_prompt, Some(30));
        assert_eq!(after_result.tokens_completion, Some(8));
        assert!((after_result.cost_usd.unwrap() - 0.4).abs() < 1e-9);
    }

    #[test]
    fn persistent_claude_counter_rollback_starts_new_baseline() {
        let mut baseline = Some(ClaudeUsageSnapshot {
            tokens_prompt: Some(500),
            tokens_completion: Some(100),
            cache_creation_input_tokens: Some(50),
            cache_read_input_tokens: Some(200),
            cost_usd: Some(5.0),
        });
        let restarted = serde_json::json!({
            "result": "fresh process",
            "modelUsage": {"claude": {
                "inputTokens": 600,
                "outputTokens": 5,
                "cacheCreationInputTokens": 60,
                "cacheReadInputTokens": 7,
                "costUSD": 5.2
            }},
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 2,
                "cache_read_input_tokens": 7
            },
            "total_cost_usd": 5.2
        });
        let mut result = parse_claude_result_value(&restarted, 0);
        normalize_claude_pool_usage(&mut result, &mut baseline);
        assert_eq!(result.tokens_prompt, Some(100));
        assert_eq!(result.tokens_completion, Some(5));
        assert_eq!(result.cache_creation_input_tokens, Some(10));
        assert!((result.cost_usd.unwrap() - 0.2).abs() < 1e-9);
        assert_eq!(baseline.unwrap().tokens_prompt, Some(600));
    }
}
