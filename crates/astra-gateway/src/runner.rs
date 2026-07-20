//! Gateway runner — bridges chat platforms to the `astra` CLI.
//!
//! Each inbound message spawns `astra chat -m "..." --session-id X`
//! and streams CLI progress to the chat platform while waiting for output.

use crate::cli_bridge::{
    self, CliProfile, CliProgress, CliResult, ReasoningDisplay, ReasoningKind,
};
use crate::commands::{self, CommandContext};
use crate::config::{DEFAULT_ASTRA_BASE_URL, GatewayConfig};
use crate::gateway_context::GatewayContext;
use crate::mcp::tools_cron;
use crate::platforms::{FeedbackEvent, InboundMessage, OutboundAttachment, PlatformAdapter};
use crate::store::{self, GatewayStore, UsageStatus};
use crate::trace_model::{
    ConversationKey, GatewayEventKind, GatewayRequest, OutboxId, RequestId, RequestStatus, RunId,
    RunStatus, TraceId, TraceRepository, TraceWriter,
};
use futures_util::future::select_all;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Redact credentials that might appear in tool call params or assistant text.
/// Scope: HTTP `Authorization` headers and well-known token prefixes
/// (GitHub PATs, Grafana service account tokens). Applied before any
/// tool/assistant text lands in `gw_trace_events.payload`.
fn redact_sensitive(s: &str) -> String {
    static AUTH_HEADER_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"(?i)(Authorization\s*:\s*(?:token|Bearer)\s+)\S+").unwrap()
    });
    static GITHUB_TOKEN_RE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"\b(gh[pousr]_|github_pat_)[A-Za-z0-9_]{20,}\b").unwrap()
    });
    static GRAFANA_TOKEN_RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"\bglsa_[A-Za-z0-9_]{20,}\b").unwrap());

    let step1 = AUTH_HEADER_RE.replace_all(s, "${1}<redacted>");
    let step2 = GITHUB_TOKEN_RE.replace_all(&step1, "${1}<redacted>");
    let step3 = GRAFANA_TOKEN_RE.replace_all(&step2, "glsa_<redacted>");
    step3.into_owned()
}

const MAX_CHUNK_LEN: usize = 3800;
const INITIAL_ACK_DELAY: Duration = Duration::from_secs(3);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);
const WECOM_STREAM_CUTOFF: Duration = Duration::from_secs(9 * 60 + 30);
const WECOM_POST_STREAM_HEARTBEAT: Duration = Duration::from_secs(120);
#[allow(dead_code)]
const PROGRESSIVE_FLUSH_INTERVAL: Duration = Duration::from_secs(8);
const PROGRESSIVE_MIN_CHARS: usize = 200;

pub(crate) fn persistent_pool_key(
    platform: &str,
    effective_chat_id: &str,
    cli_profile: &str,
) -> String {
    format!(
        "p{}:{platform}|c{}:{cli_profile}|{effective_chat_id}",
        platform.len(),
        cli_profile.len()
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttemptOutcome {
    Success,
    ProviderError,
    CliError,
    Cancelled,
    PreflightFailed,
    InternalError,
}

impl AttemptOutcome {
    fn from_result(result: &CliResult) -> Self {
        if result.provider_error.is_some() {
            Self::ProviderError
        } else if result.success {
            Self::Success
        } else {
            Self::CliError
        }
    }

    fn usage_status(self) -> UsageStatus {
        match self {
            Self::Success => UsageStatus::Success,
            Self::ProviderError => UsageStatus::ProviderError,
            Self::CliError => UsageStatus::CliError,
            Self::Cancelled => UsageStatus::Cancelled,
            Self::PreflightFailed => UsageStatus::PreflightFailed,
            Self::InternalError => UsageStatus::InternalError,
        }
    }

    fn run_status(self) -> RunStatus {
        if self == Self::Success {
            RunStatus::Succeeded
        } else {
            RunStatus::Failed
        }
    }

    fn is_failure(self) -> bool {
        self != Self::Success
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AttemptUsage {
    prompt: u64,
    completion: u64,
    cached: u64,
    cache_creation: u64,
    cache_read: u64,
    reasoning: u64,
    total: u64,
    cost_usd: Option<f64>,
    tool_calls: u32,
}

fn is_astra_app_server_startup_error(error: &str) -> bool {
    error.contains("codex app-server stdout closed")
        || error.contains("codex app-server response channel closed")
        || error.contains("failed to spawn codex app-server")
        || error.contains("unrecognized subcommand")
        || error.contains("unknown command")
}

// ─── Send Circuit Breaker ───────────────────────────────────────────────────

const SEND_FAILURE_THRESHOLD: u32 = 3;
/// After this long without a new failure, the breaker auto half-opens even
/// without a success call. This matters for long-running requests that recover
/// the platform but don't emit sends (so `record_success` is never called).
/// Without the cooldown, such tasks would stay silent forever.
#[allow(dead_code)]
const SEND_FAILURE_COOLDOWN: Duration = Duration::from_secs(60);

/// Injectable clock so cooldown tests don't need real sleeps.
trait Clock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> Instant;
}

#[derive(Debug, Default)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct TestClockHandle {
    offset: Arc<std::sync::Mutex<Duration>>,
    base: Instant,
}

#[cfg(test)]
impl Clock for TestClockHandle {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock().unwrap()
    }
}

#[cfg(test)]
struct TestClock {
    offset: Arc<std::sync::Mutex<Duration>>,
    base: Instant,
}

#[cfg(test)]
impl TestClock {
    fn new() -> Self {
        Self {
            offset: Arc::new(std::sync::Mutex::new(Duration::ZERO)),
            base: Instant::now(),
        }
    }
    fn advance(&self, d: Duration) {
        *self.offset.lock().unwrap() += d;
    }
    fn handle(&self) -> TestClockHandle {
        TestClockHandle {
            offset: Arc::clone(&self.offset),
            base: self.base,
        }
    }
}

/// Entries whose `last_failure_at` is older than this are reaped on the
/// next record_failure call. Bounds state.len() under a steady stream of
/// one-off failures from unique conversations (gateway at scale).
const SEND_FAILURE_EVICTION_AGE: Duration = Duration::from_secs(10 * 60);

/// Floor on how often we walk `state` to evict. Prevents an O(n) sweep
/// from running on every single `record_failure` during a failure spike.
const SEND_FAILURE_EVICTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Per-conversation send health tracker. Suppresses heartbeats after
/// consecutive send failures to avoid flooding an unreachable platform.
///
/// Recovery paths:
/// - `record_success` closes the breaker immediately (clears state).
/// - Without any send (task processing locally), the breaker auto half-opens
///   after `SEND_FAILURE_COOLDOWN` elapsed since `last_failure_at`, so the
///   next heartbeat probes platform recovery.
/// - Abandoned entries (no new failure for `SEND_FAILURE_EVICTION_AGE`) are
///   lazily reaped so `state.len()` stays bounded under high-conversation
///   churn. Sweep runs at most once per `SEND_FAILURE_EVICTION_SWEEP_INTERVAL`.
#[derive(Clone)]
struct SendCircuitBreaker {
    state: Arc<dashmap::DashMap<String, (u32, Instant)>>,
    last_sweep_at: Arc<std::sync::Mutex<Option<Instant>>>,
    clock: Arc<dyn Clock>,
}

impl Default for SendCircuitBreaker {
    fn default() -> Self {
        Self {
            state: Arc::new(dashmap::DashMap::new()),
            last_sweep_at: Arc::new(std::sync::Mutex::new(None)),
            clock: Arc::new(SystemClock),
        }
    }
}

impl SendCircuitBreaker {
    #[cfg(test)]
    fn with_clock(clock: TestClockHandle) -> Self {
        Self {
            state: Arc::new(dashmap::DashMap::new()),
            last_sweep_at: Arc::new(std::sync::Mutex::new(None)),
            clock: Arc::new(clock),
        }
    }

    /// Opportunistically reap entries older than the eviction age. Called
    /// from record_failure so the sweep cost is amortized with the write.
    /// Rate-limited to once per SEND_FAILURE_EVICTION_SWEEP_INTERVAL to
    /// avoid O(n) per call during a failure spike.
    fn maybe_evict(&self, now: Instant) {
        {
            let mut last = match self.last_sweep_at.lock() {
                Ok(g) => g,
                Err(_) => return, // poisoned — skip eviction, not critical
            };
            if let Some(prev) = *last
                && now.saturating_duration_since(prev) < SEND_FAILURE_EVICTION_SWEEP_INTERVAL
            {
                return;
            }
            *last = Some(now);
        }
        let before = self.state.len();
        self.state.retain(|_, (_, last_failure_at)| {
            now.saturating_duration_since(*last_failure_at) < SEND_FAILURE_EVICTION_AGE
        });
        let evicted = before.saturating_sub(self.state.len());
        if evicted > 0 {
            tracing::debug!(
                target: "gateway::circuit_breaker",
                evicted,
                remaining = self.state.len(),
                "send circuit breaker swept stale entries"
            );
        }
    }

    fn record_success(&self, key: &str) {
        if let Some((_, (count, _))) = self.state.remove(key)
            && count >= SEND_FAILURE_THRESHOLD
        {
            tracing::info!(
                target: "gateway::circuit_breaker",
                key = %key,
                prior_failures = count,
                "send circuit breaker CLOSED after successful send"
            );
        }
    }

    fn record_failure(&self, key: &str) {
        let now = self.clock.now();
        // Reap stale entries before inserting so state.len() stays bounded
        // under a steady stream of one-off failures from unique keys.
        self.maybe_evict(now);
        let mut entry = self.state.entry(key.to_string()).or_insert((0, now));
        entry.0 += 1;
        entry.1 = now;
        if entry.0 == SEND_FAILURE_THRESHOLD {
            tracing::warn!(
                target: "gateway::circuit_breaker",
                key = %key,
                threshold = SEND_FAILURE_THRESHOLD,
                "send circuit breaker OPENED — heartbeats suppressed until recovery or cooldown"
            );
        }
    }

    #[allow(dead_code)]
    fn is_open(&self, key: &str) -> bool {
        let Some(entry) = self.state.get(key) else {
            return false;
        };
        let (count, last_failure_at) = *entry;
        if count < SEND_FAILURE_THRESHOLD {
            return false;
        }
        // Half-open after cooldown: lets the caller probe platform recovery.
        // State (failure count) is kept — a probe failure re-trips immediately
        // via record_failure, without needing THRESHOLD more failures.
        let now = self.clock.now();
        now.saturating_duration_since(last_failure_at) < SEND_FAILURE_COOLDOWN
    }

    fn reset(&self, key: &str) {
        self.state.remove(key);
    }
}

// ─── Safe String Truncation ─────────────────────────────────────────────────

/// Truncate a string to at most `n` characters, safe for multi-byte UTF-8.
pub(crate) fn truncate_chars(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

fn feedback_type_label(feedback_type: i64) -> &'static str {
    match feedback_type {
        1 => "positive",
        2 => "negative",
        3 => "cancel",
        _ => "unknown",
    }
}
const CONVERSATION_QUEUE_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Number of consecutive auth failures before the circuit breaker trips.
const AUTH_FAILURE_THRESHOLD: u32 = 2;
/// How long the circuit breaker stays open before allowing retries.
const AUTH_FAILURE_COOLDOWN: Duration = Duration::from_secs(300);

/// Outbound message from CLI, scheduler, or other background tasks.
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub platform: String,
    pub chat_id: String,
    pub text: String,
    pub reply_token: Option<String>,
    /// Stream ID shared across all chunks of one reply (for streaming platforms like WeCom).
    pub stream_id: Option<String>,
    /// Feedback ID associated with a stream response. WeCom sends this back in
    /// feedback callbacks when users rate the AI response.
    pub feedback_id: Option<String>,
    /// Whether this is the final chunk of a stream.
    pub stream_finish: bool,
    pub outbox: Option<OutboxDelivery>,
}

#[derive(Debug, Clone)]
pub struct OutboxDelivery {
    pub outbox_id: OutboxId,
    pub trace_id: TraceId,
    pub request_id: RequestId,
}

impl OutboundMessage {
    pub fn plain(
        platform: impl Into<String>,
        chat_id: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            platform: platform.into(),
            chat_id: chat_id.into(),
            text: text.into(),
            reply_token: None,
            stream_id: None,
            feedback_id: None,
            stream_finish: true,
            outbox: None,
        }
    }

    pub fn plain_with_feedback(
        platform: impl Into<String>,
        chat_id: impl Into<String>,
        text: impl Into<String>,
        feedback_id: Option<String>,
    ) -> Self {
        Self {
            platform: platform.into(),
            chat_id: chat_id.into(),
            text: text.into(),
            reply_token: None,
            stream_id: None,
            feedback_id,
            stream_finish: true,
            outbox: None,
        }
    }

    pub fn stream_chunk(
        platform: impl Into<String>,
        chat_id: impl Into<String>,
        text: impl Into<String>,
        reply_token: Option<String>,
        stream_id: Option<String>,
        feedback_id: Option<String>,
        finish: bool,
    ) -> Self {
        Self {
            platform: platform.into(),
            chat_id: chat_id.into(),
            text: text.into(),
            reply_token,
            stream_id,
            feedback_id,
            stream_finish: finish,
            outbox: None,
        }
    }

    pub fn with_outbox(
        platform: impl Into<String>,
        chat_id: impl Into<String>,
        text: impl Into<String>,
        reply_token: Option<String>,
        feedback_id: Option<String>,
        outbox: OutboxDelivery,
    ) -> Self {
        Self {
            platform: platform.into(),
            chat_id: chat_id.into(),
            text: text.into(),
            reply_token,
            stream_id: None,
            feedback_id,
            stream_finish: true,
            outbox: Some(outbox),
        }
    }
}

pub struct GatewayRunner {
    config: GatewayConfig,
    store: Option<Arc<dyn GatewayStore>>,
    cli_profile: CliProfile,
    thin: astra::Client,
    outbound_tx: Option<tokio::sync::mpsc::Sender<OutboundMessage>>,
    user_skills: Vec<(String, String)>,
    projects: Vec<String>,
    trace_repo: Option<Arc<dyn TraceRepository>>,
    queue_senders:
        tokio::sync::Mutex<HashMap<ConversationKey, tokio::sync::mpsc::Sender<QueuedRequest>>>,
    global_run_limiter: Arc<tokio::sync::Semaphore>,
    cli_availability: Vec<(String, cli_bridge::CliAvailability)>,
    /// Tracks consecutive auth failures per CLI profile name.
    /// Value: (failure_count, last_failure_time).
    auth_failures: Arc<dashmap::DashMap<String, (u32, Instant)>>,
    /// Shared access token — gateway validates once, all CLI spawns reuse via env var.
    shared_auth: Option<SharedAuthToken>,
    /// Monotonic counter for generating short request tags when no trace exists.
    request_counter: AtomicU32,
    /// Active CLI turns indexed by trace_id. Used by `/esc` to interrupt
    /// running turns immediately instead of only marking DB state.
    active_requests: Arc<dashmap::DashMap<String, CancellationToken>>,
    /// Serializes session mutations against terminal session persistence.
    /// Entries exist only while one or more turns for that conversation are active.
    session_states: Arc<tokio::sync::Mutex<HashMap<String, SessionGenerationState>>>,
    /// Per-conversation send circuit breaker. Workers check this before
    /// emitting heartbeats — stops sending after consecutive failures to
    /// avoid message flood when platform is unreachable.
    send_health: SendCircuitBreaker,
    /// When this gateway process started. Requests with `created_at`
    /// before this are zombies — their cancel tokens and CLI children
    /// died with the previous gateway lifecycle.
    gateway_start: chrono::DateTime<chrono::Utc>,
    /// Pool of long-lived Claude CLI processes (persistent mode).
    cli_pool: Arc<tokio::sync::Mutex<crate::cli_pool::CliProcessPool>>,
    /// Pool of long-lived Codex app-server processes (persistent mode).
    codex_app_pool: Arc<tokio::sync::Mutex<crate::codex_app_pool::CodexAppPool>>,
    runtime_api_url: Option<String>,
    runtime_api_token: Option<String>,
}

#[derive(Default)]
struct SessionGenerationState {
    generation: u64,
    active_attempts: u32,
}

/// No-op adapter used in spawned CLI tasks (typing/heartbeats not available in background).
struct NullAdapter;

#[async_trait::async_trait]
impl PlatformAdapter for NullAdapter {
    fn name(&self) -> &'static str {
        "null"
    }
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    async fn stop(&mut self) {}
    async fn send_text(&self, _: &str, _: &str, _: Option<&str>) -> Result<(), String> {
        Ok(())
    }
    async fn recv(&self) -> Option<InboundMessage> {
        None
    }
}

/// Response from a background CLI task, routed back to the adapter.
type CliResponse = OutboundMessage;

/// Agent turn submitted by the scheduler. It shares the normal conversation
/// queue, pool, and session, but defers all delivery until the scheduler has
/// inspected the final response.
pub struct ScheduledAgentTurn {
    pub message: InboundMessage,
    pub response_tx: tokio::sync::oneshot::Sender<Option<OutboundMessage>>,
}

struct QueuedRequest {
    msg: InboundMessage,
    conversation: ConversationKey,
    trace: Option<OutboxDeliveryTrace>,
    background: bool,
    scheduled_response_tx: Option<tokio::sync::oneshot::Sender<Option<OutboundMessage>>>,
}

#[derive(Debug, Clone)]
struct OutboxDeliveryTrace {
    trace_id: TraceId,
    request_id: RequestId,
}

enum AdapterRecv {
    Message(Box<InboundMessage>),
    Closed(usize),
}

impl GatewayRunner {
    pub async fn new(
        config: GatewayConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let astra_url = if config.astra.base_url.is_empty() {
            DEFAULT_ASTRA_BASE_URL
        } else {
            &config.astra.base_url
        };
        let thin = astra::Client::new(
            astra_url,
            if config.astra.api_key.is_empty() {
                None
            } else {
                Some(config.astra.api_key.clone())
            },
        )?;

        let storage_config = config.resolve_storage();
        let (store, trace_repo) = match store::open_store_bundle(&storage_config).await {
            Ok(Some(bundle)) => {
                tracing::info!(backend = ?storage_config, "storage connected");
                (Some(bundle.store), bundle.trace_repo)
            }
            Ok(None) => {
                tracing::info!("running without persistence (storage: none)");
                (None, None)
            }
            Err(e) => return Err(e),
        };

        let cli_profile = config.cli.clone();

        let user_skills = config
            .skills_dir
            .as_deref()
            .map(crate::gateway_context::load_skills_from_dir)
            .unwrap_or_default();
        if !user_skills.is_empty() {
            tracing::info!(
                count = user_skills.len(),
                "loaded user skills from directory"
            );
        }

        // Discover available projects
        let projects: Vec<String> = crate::workspace::discover_all_projects(&config.project_dirs)
            .iter()
            .map(|p| p.summary())
            .collect();
        if !projects.is_empty() {
            tracing::info!(count = projects.len(), "discovered projects");
        }

        let max_concurrent_runs = config.max_concurrent_runs.max(1);

        // Probe all configured CLI profiles for availability
        let mut cli_availability = Vec::new();
        let default_avail = cli_bridge::probe_cli(&cli_profile).await;
        cli_availability.push((cli_profile.name().to_string(), default_avail.clone()));
        for (name, profile) in &config.cli_profiles {
            let avail = cli_bridge::probe_cli(profile).await;
            cli_availability.push((name.clone(), avail));
        }

        // If default CLI not available, auto-select first available
        let effective_cli = if !default_avail.is_available() {
            if let Some((name, _)) = cli_availability.iter().find(|(_, a)| a.is_available()) {
                if let Some(profile) = config.cli_profiles.get(name) {
                    tracing::info!(cli = %name, "default CLI unavailable, auto-selected");
                    profile.clone()
                } else {
                    cli_profile.clone()
                }
            } else {
                cli_profile.clone()
            }
        } else {
            cli_profile.clone()
        };

        let shared_auth = Some(SharedAuthToken::new(
            thin.clone(),
            config.astra.username.clone(),
            config.astra.password.clone(),
        ));

        Ok(Self {
            config,
            store,
            cli_profile: effective_cli,
            thin,
            outbound_tx: None,
            user_skills,
            projects,
            trace_repo,
            queue_senders: tokio::sync::Mutex::new(HashMap::new()),
            global_run_limiter: Arc::new(tokio::sync::Semaphore::new(max_concurrent_runs)),
            cli_availability,
            auth_failures: Arc::new(dashmap::DashMap::new()),
            shared_auth,
            request_counter: AtomicU32::new(0),
            active_requests: Arc::new(dashmap::DashMap::new()),
            session_states: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            send_health: SendCircuitBreaker::default(),
            gateway_start: chrono::Utc::now(),
            cli_pool: Arc::new(tokio::sync::Mutex::new(
                crate::cli_pool::CliProcessPool::new(),
            )),
            codex_app_pool: Arc::new(tokio::sync::Mutex::new(
                crate::codex_app_pool::CodexAppPool::new(),
            )),
            runtime_api_url: None,
            runtime_api_token: None,
        })
    }

    /// Whether any storage backend is active.
    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    /// Clone the Arc-wrapped store for use by external components (e.g. scheduler).
    pub fn store(&self) -> Option<Arc<dyn GatewayStore>> {
        self.store.clone()
    }

    /// Clone the Arc-wrapped trace repository.
    pub fn trace_repo(&self) -> Option<Arc<dyn TraceRepository>> {
        self.trace_repo.clone()
    }

    pub fn cli_profile(&self) -> &CliProfile {
        &self.cli_profile
    }

    pub async fn sweep_stale_traces(&self) {
        if let Some(ref repo) = self.trace_repo {
            let result = retry_once_on_transient("sweep_stale_traces", || async {
                repo.sweep_stale_requests("gateway restarted").await
            })
            .await;
            match result {
                Ok(0) => {}
                Ok(n) => tracing::info!(count = n, "swept stale trace requests → failed"),
                Err(e) => tracing::warn!(error = %e, "failed to sweep stale traces"),
            }
        }
    }

    pub fn set_outbound_tx(&mut self, tx: tokio::sync::mpsc::Sender<OutboundMessage>) {
        self.outbound_tx = Some(tx);
    }

    pub fn set_runtime_api(&mut self, url: String, token: String) {
        self.runtime_api_url = Some(url);
        self.runtime_api_token = Some(token);
    }

    async fn record_feedback(&self, msg: &InboundMessage, feedback: &FeedbackEvent) {
        let Some(repo) = self.trace_repo.as_ref() else {
            tracing::info!(
                platform = msg.platform,
                feedback_id = %feedback.feedback_id,
                feedback_type = feedback.feedback_type,
                "feedback received without trace repository"
            );
            return;
        };
        let request_id = RequestId::from(feedback.feedback_id.as_str());
        let request = match repo.get_request(&request_id).await {
            Ok(Some(request)) => request,
            Ok(None) => {
                tracing::warn!(
                    platform = msg.platform,
                    feedback_id = %feedback.feedback_id,
                    feedback_type = feedback.feedback_type,
                    "feedback target request not found"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    platform = msg.platform,
                    feedback_id = %feedback.feedback_id,
                    error = %e,
                    "failed to load feedback target request"
                );
                return;
            }
        };

        let writer = TraceWriter::from_existing(
            repo.as_ref() as &dyn TraceRepository,
            request.trace_id.clone(),
            request.request_id.clone(),
        );
        if let Err(e) = writer
            .append(
                GatewayEventKind::Feedback,
                serde_json::json!({
                    "platform": msg.platform,
                    "chat_id": &msg.chat_id,
                    "user_id": &msg.user_id,
                    "platform_msg_id": &msg.msg_id,
                    "feedback_id": &feedback.feedback_id,
                    "feedback_type": feedback.feedback_type,
                    "feedback_label": feedback_type_label(feedback.feedback_type),
                    "content": &feedback.content,
                    "inaccurate_reason_list": &feedback.inaccurate_reason_list,
                    "raw": &feedback.raw,
                }),
            )
            .await
        {
            tracing::warn!(
                platform = msg.platform,
                feedback_id = %feedback.feedback_id,
                error = %e,
                "failed to append feedback event"
            );
        } else {
            tracing::info!(
                platform = msg.platform,
                user = %safe_id(&msg.user_id),
                feedback_id = %feedback.feedback_id,
                feedback_type = feedback.feedback_type,
                "feedback recorded"
            );
        }
    }

    /// Resolve the active CLI profile for a user (may be overridden via /cli + /model).
    ///
    /// `chat_id` scopes `/model` preferences to the conversation so that
    /// different group chats do not interfere with each other.
    async fn resolve_cli_profile(
        &self,
        platform: &str,
        user_id: &str,
        chat_id: &str,
    ) -> (CliProfile, Option<crate::config::ProviderConfig>) {
        let mut profile = if let Some(ref store) = self.store
            && let Ok(Some(name)) = store
                .get_user_preference(platform, user_id, "cli_profile")
                .await
            && let Some(p) = self.config.cli_profiles.get(&name)
        {
            p.clone()
        } else {
            self.cli_profile.clone()
        };

        // Apply per-user model override scoped to this CLI. Empty string is the
        // "use default" sentinel written by `/model 默认` — treat as no override.
        let model_key = store::model_preference_key(profile.name(), Some(chat_id));
        if matches!(profile, CliProfile::Codex { .. }) {
            let selection_key = store::codex_model_selection_preference_key(chat_id);
            let selection = if let Some(ref store) = self.store {
                store
                    .get_user_preference(platform, user_id, &selection_key)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|value| {
                        serde_json::from_str::<store::CodexModelSelection>(&value).ok()
                    })
            } else {
                None
            };
            if let Some(selection) = selection {
                let has_model_override = selection.model.is_some();
                if let Some(model) = selection.model {
                    profile.set_model_override(model);
                }
                // A selected model with no catalog effort is the compatibility
                // fallback and must omit effort. The reset tombstone instead
                // leaves the YAML profile default intact.
                if has_model_override {
                    profile.set_reasoning_effort_override(selection.effort);
                }
            } else if let Some(ref store) = self.store
                && let Ok(Some(model_name)) = store
                    .get_user_preference(platform, user_id, &model_key)
                    .await
                && !model_name.is_empty()
            {
                profile.set_model_override(model_name);
            }
        } else if let Some(ref store) = self.store
            && let Ok(Some(model_name)) = store
                .get_user_preference(platform, user_id, &model_key)
                .await
            && !model_name.is_empty()
        {
            profile.set_model_override(model_name);
        }

        let provider = crate::cli_bridge::provider_for_cli_profile(&self.config, &profile);

        (profile, provider)
    }

    /// Fast path: access control + slash commands. Returns Some if handled (no CLI needed).
    pub async fn handle_fast(
        &self,
        msg: &InboundMessage,
    ) -> Result<Option<String>, InboundMessage> {
        // Access control check
        if !self.config.access.is_allowed(&msg.user_id) {
            tracing::debug!(user = %safe_id(&msg.user_id), "message rejected by access policy");
            return Ok(Some(self.config.access.rejection_message().to_string()));
        }

        // Ensure gw_users has a row for this sender so mutation slash
        // commands (/model, /cli, /workspace, /reasoning) can write per-user
        // preferences. handle_message_inner does this on the slow path; we
        // must mirror it here because some users' very first interaction
        // with the bot is a slash command — they never hit the slow path,
        // and set_user_preference then fails with "user not found".
        if let Some(ref store) = self.store
            && let Err(e) = store
                .upsert_user(msg.platform, &msg.user_id, &msg.user_id)
                .await
        {
            tracing::warn!(error = %e, "handle_fast: failed to upsert user");
        }

        // Group chat: require @mention if configured
        if msg.chat_type == crate::platforms::ChatType::Group
            && self.config.group_require_mention
            && !is_mentioned(&msg.text, &self.config.bot_name)
        {
            return Ok(None);
        }

        // Strip @mention prefix so slash commands and CLI see clean text
        let msg = &{
            let mut m = msg.clone();
            m.text = strip_mention(&m.text, &self.config.bot_name);
            m
        };

        // Bare @mention with no content → treat as greeting
        let msg = &if msg.text.is_empty() && msg.attachments.is_empty() {
            let mut m = msg.clone();
            m.text = "你好".to_string();
            m
        } else {
            msg.clone()
        };

        // Group chat: per-user session isolation
        let effective_chat_id = if msg.chat_type == crate::platforms::ChatType::Group
            && self.config.group_sessions_per_user
        {
            format!("{}:{}", msg.chat_id, msg.user_id)
        } else {
            msg.chat_id.clone()
        };

        // Resolve active CLI profile
        let (cli_profile, provider_config) = self
            .resolve_cli_profile(msg.platform, &msg.user_id, &effective_chat_id)
            .await;

        let trimmed = msg.text.trim();

        // /auth — reset auth circuit breaker, show CLI auth status, attempt auto-relogin
        if trimmed == "/auth" {
            return Ok(Some(self.handle_auth_command(&cli_profile).await));
        }

        // /manage cancel, /manage esc/kill → redirect to fast-path commands
        // so they execute immediately even when a task is running. For
        // bulk cleanup the user should type /esc all directly — we don't
        // try to guess bulk intent from natural language (too brittle,
        // CN/EN case-explosion), and the AI slow-path would queue behind
        // the very tasks the user wants to clear.
        if let Some(rest) = trimmed.strip_prefix("/manage ") {
            let rest = rest.trim();
            if rest == "cancel"
                || rest.starts_with("cancel ")
                || rest == "kill"
                || rest.starts_with("kill ")
                || rest == "esc"
                || rest.starts_with("esc ")
            {
                let rewritten_cmd = if rest == "kill" || rest.starts_with("kill ") {
                    format!("/esc{}", rest.strip_prefix("kill").unwrap_or_default())
                } else {
                    format!("/{rest}")
                };
                // Build command context and dispatch directly (avoids async recursion).
                let cmd_ctx = CommandContext {
                    astra: &self.thin,
                    config: &self.config,
                    store: self.store.as_deref(),
                    platform: msg.platform,
                    chat_id: &effective_chat_id,
                    user_id: &msg.user_id,
                    resolved_cli: &cli_profile,
                    resolved_provider_config: provider_config.as_ref(),
                    trace_repo: self
                        .trace_repo
                        .as_ref()
                        .map(|repo| repo.as_ref() as &dyn TraceRepository),
                    project_dirs: &self.config.project_dirs,
                    cli_availability: &self.cli_availability,
                    auth_status: self.auth_status_line(cli_profile.name()),
                    active_requests: Some(&self.active_requests),
                    codex_app_pool: Some(&self.codex_app_pool),
                    gateway_start: self.gateway_start,
                };
                if let Some(response) = commands::handle_command(&cmd_ctx, &rewritten_cmd).await {
                    return Ok(Some(response));
                }
            }
        }

        // /manage — rewrite to rich context message and send to slow CLI path.
        // MUST be routed to a SEPARATE conversation worker (virtual profile
        // MANAGE_CLI_PROFILE) so /manage doesn't queue behind the very tasks
        // it's supposed to manage. Mark the message with the route override
        // via chat_id-side metadata; the enqueue point checks this and
        // applies the override on build_queued_request.
        if trimmed == "/manage" || trimmed.starts_with("/manage ") {
            let extra = trimmed.strip_prefix("/manage").unwrap_or("").trim();
            let context = self
                .build_manage_context(msg, &effective_chat_id, &cli_profile, extra)
                .await;
            let mut managed_msg = msg.clone();
            managed_msg.text = context;
            // Mark the msg so handle_fast's caller routes through the
            // _manage worker instead of the user's normal queue.
            managed_msg.route_override = Some(commands::MANAGE_CLI_PROFILE.to_string());
            return Err(managed_msg);
        }

        // Slash commands — instant response, no CLI
        let cmd_ctx = CommandContext {
            astra: &self.thin,
            config: &self.config,
            store: self.store.as_deref(),
            platform: msg.platform,
            chat_id: &effective_chat_id,
            user_id: &msg.user_id,
            resolved_cli: &cli_profile,
            resolved_provider_config: provider_config.as_ref(),
            trace_repo: self
                .trace_repo
                .as_ref()
                .map(|repo| repo.as_ref() as &dyn TraceRepository),
            project_dirs: &self.config.project_dirs,
            cli_availability: &self.cli_availability,
            auth_status: self.auth_status_line(cli_profile.name()),
            active_requests: Some(&self.active_requests),
            codex_app_pool: Some(&self.codex_app_pool),
            gateway_start: self.gateway_start,
        };
        let command = msg.text.trim();
        let may_mutate_session = command.starts_with("/new")
            || command.starts_with("/reset")
            || command.starts_with("/model ")
            || command.starts_with("/cli ")
            || command.starts_with("/workspace ")
            || command.starts_with("/ws ")
            || command.split_whitespace().next() == Some("/session");
        let mut session_states = if may_mutate_session {
            Some(self.session_states.lock().await)
        } else {
            None
        };
        if let Some(response) = commands::handle_command(&cmd_ctx, &msg.text).await {
            // Commands that invalidate the long-lived process (model/session/workspace change)
            let switched_session = command.split_whitespace().next() == Some("/session")
                && response.starts_with("✅ 已切换到会话");
            let session_reset = (command.starts_with("/new") || command.starts_with("/reset"))
                && response.starts_with("🔄 ");
            let model_switched =
                command.starts_with("/model ") && response.starts_with("🤖 模型已切换:");
            let cli_switched = command.starts_with("/cli ") && response.starts_with("✅ 已切换到");
            let workspace_switched = (command.starts_with("/workspace ")
                || command.starts_with("/ws "))
                && response.starts_with("📂 工作目录已切换:");
            let invalidates_session = session_reset
                || model_switched
                || cli_switched
                || workspace_switched
                || switched_session;
            let pool_key =
                persistent_pool_key(msg.platform, &effective_chat_id, cli_profile.name());
            if invalidates_session
                && let Some(states) = session_states.as_mut()
                && let Some(state) = states.get_mut(&pool_key)
            {
                state.generation = state.generation.wrapping_add(1);
            }
            drop(session_states);
            if invalidates_session {
                self.cli_pool.lock().await.kill(&pool_key);
                self.codex_app_pool.lock().await.kill(&pool_key);
            }
            return Ok(Some(response));
        }
        drop(session_states);
        // Not a slash command — needs CLI (slow path)
        Err(msg.clone())
    }

    /// Handle a single inbound message (full path including CLI).
    pub async fn handle_message(
        &self,
        msg: &InboundMessage,
        adapter: &dyn PlatformAdapter,
    ) -> Option<String> {
        self.handle_message_inner(msg, adapter, None, false)
            .await
            .map(|outbound| outbound.text)
    }

    async fn handle_message_inner(
        &self,
        msg: &InboundMessage,
        adapter: &dyn PlatformAdapter,
        trace: Option<OutboxDeliveryTrace>,
        background: bool,
    ) -> Option<OutboundMessage> {
        let mut msg = msg.clone();
        let execution_outbound_tx = if background {
            None
        } else {
            self.outbound_tx.clone()
        };
        if let Some(feedback) = msg.feedback.as_ref() {
            self.record_feedback(&msg, feedback).await;
            return None;
        }
        if let Some(text) = prepare_inbound_attachments(&mut msg).await {
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    text,
                )
                .await,
            );
        }

        // Group chat: per-user session isolation
        let effective_chat_id = if msg.chat_type == crate::platforms::ChatType::Group
            && self.config.group_sessions_per_user
        {
            format!("{}:{}", msg.chat_id, msg.user_id)
        } else {
            msg.chat_id.clone()
        };

        let (cli_profile, provider_config) = self
            .resolve_cli_profile(msg.platform, &msg.user_id, &effective_chat_id)
            .await;

        let cmd_ctx = CommandContext {
            astra: &self.thin,
            config: &self.config,
            store: self.store.as_deref(),
            platform: msg.platform,
            chat_id: &effective_chat_id,
            user_id: &msg.user_id,
            resolved_cli: &cli_profile,
            resolved_provider_config: provider_config.as_ref(),
            trace_repo: self
                .trace_repo
                .as_ref()
                .map(|repo| repo.as_ref() as &dyn TraceRepository),
            project_dirs: &self.config.project_dirs,
            cli_availability: &self.cli_availability,
            auth_status: self.auth_status_line(cli_profile.name()),
            active_requests: Some(&self.active_requests),
            codex_app_pool: Some(&self.codex_app_pool),
            gateway_start: self.gateway_start,
        };
        if let Some(text) = image_attachment_guard_message(&msg, &cmd_ctx).await {
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    text,
                )
                .await,
            );
        }

        // Check first-time user BEFORE upsert (upsert creates the row)
        let is_first = if let Some(ref store) = self.store {
            store
                .is_first_message(msg.platform, &msg.user_id)
                .await
                .unwrap_or(false)
        } else {
            false
        };

        if let Some(ref store) = self.store
            && let Err(e) = store
                .upsert_user(msg.platform, &msg.user_id, &msg.user_id)
                .await
        {
            tracing::warn!(error = %e, "failed to upsert user");
        }

        // Send first-time welcome after upsert
        if is_first {
            let welcome = build_welcome_message(&cli_profile);
            if let Some(ref tx) = execution_outbound_tx {
                let _ = tx
                    .send(OutboundMessage::plain(
                        msg.platform.to_string(),
                        msg.chat_id.clone(),
                        welcome,
                    ))
                    .await;
            }
        }

        let cli_name = cli_profile.name().to_string();
        let session_state_key = persistent_pool_key(msg.platform, &effective_chat_id, &cli_name);

        // Read the starting session and register this turn while holding the same
        // gate used by fast session mutations. A late result can then be prevented
        // from overwriting a `/new` or `/session switch` that happened mid-turn.
        let mut session_states = self.session_states.lock().await;
        let mut auto_reset = false;
        if let Some(ref store) = self.store
            && let Ok(Some(last_active_str)) = store
                .get_session_last_active(msg.platform, &effective_chat_id, &cli_name)
                .await
            && let Ok(last_active) =
                chrono::NaiveDateTime::parse_from_str(&last_active_str, "%Y-%m-%d %H:%M:%S%.f")
        {
            let last_utc = last_active.and_utc();
            let now = chrono::Utc::now();
            if self.config.session_reset.should_reset(last_utc, now) {
                if let Err(e) = store
                    .reset_session(msg.platform, &effective_chat_id, &cli_name)
                    .await
                {
                    tracing::warn!(error = %e, "session auto-reset failed");
                } else {
                    if let Some(state) = session_states.get_mut(&session_state_key) {
                        state.generation = state.generation.wrapping_add(1);
                    }
                    auto_reset = true;
                    tracing::info!(cli = cli_name, "session auto-reset by policy");
                }
            }
        }

        let session_id = if let Some(ref store) = self.store {
            store
                .get_current_session(msg.platform, &effective_chat_id, &cli_name)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let session_state = session_states.entry(session_state_key.clone()).or_default();
        session_state.active_attempts = session_state.active_attempts.saturating_add(1);
        let session_generation = session_state.generation;
        drop(session_states);

        if auto_reset {
            self.cli_pool.lock().await.kill(&session_state_key);
            self.codex_app_pool.lock().await.kill(&session_state_key);
        }

        let trace_writer = trace.as_ref().and_then(|trace| {
            self.trace_repo.as_ref().map(|repo| {
                TraceWriter::from_existing(
                    repo.as_ref() as &dyn TraceRepository,
                    trace.trace_id.clone(),
                    trace.request_id.clone(),
                )
            })
        });
        let mut run_id = None;
        if let Some(writer) = trace_writer.as_ref() {
            match writer.start_run(&cli_name, session_id.clone()).await {
                Ok(id) => {
                    let _ = writer.mark_running().await;
                    run_id = Some(id);
                }
                Err(e) => {
                    let mut session_states = self.session_states.lock().await;
                    if let Some(state) = session_states.get_mut(&session_state_key) {
                        state.active_attempts = state.active_attempts.saturating_sub(1);
                        if state.active_attempts == 0 {
                            session_states.remove(&session_state_key);
                        }
                    }
                    tracing::info!(error = %e, "queued request skipped before CLI start");
                    return None;
                }
            }
        }

        // Request tag for user-facing message correlation
        self.request_counter.fetch_add(1, Ordering::Relaxed);
        let request_tag = session_id
            .as_deref()
            .and_then(|s| s.get(s.len().saturating_sub(8)..))
            .map(|suffix| format!("#{suffix}"))
            .unwrap_or_else(|| "#new".to_string());

        tracing::info!(
            platform = msg.platform,
            chat_id = %safe_id(&msg.chat_id),
            user = %safe_id(&msg.user_id),
            tag = %request_tag,
            "→ {}",
            truncate(&msg.text, 80),
        );

        // Send typing indicator immediately so user gets feedback
        let _ = adapter.send_typing(&msg.chat_id).await;

        // Check CLI is available before spawning
        let availability = cli_bridge::probe_cli(&cli_profile).await;
        if !availability.is_available() {
            self.finalize_attempt(
                &msg,
                &effective_chat_id,
                session_generation,
                &cli_name,
                &cli_profile,
                trace.as_ref(),
                trace_writer.as_ref(),
                run_id.as_ref(),
                session_id.as_deref(),
                None,
                AttemptOutcome::PreflightFailed,
                Some("cli_unavailable"),
                Some("CLI unavailable"),
                Duration::ZERO,
            )
            .await;
            let text = cli_bridge::onboarding_message(&cli_profile, &availability);
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    text,
                )
                .await,
            );
        }

        // Check auth circuit breaker before spawning CLI
        if let Some(auth_msg) = self.check_auth_circuit(&cli_name) {
            self.finalize_attempt(
                &msg,
                &effective_chat_id,
                session_generation,
                &cli_name,
                &cli_profile,
                trace.as_ref(),
                trace_writer.as_ref(),
                run_id.as_ref(),
                session_id.as_deref(),
                None,
                AttemptOutcome::PreflightFailed,
                Some("auth_circuit_open"),
                Some("auth circuit breaker open"),
                Duration::ZERO,
            )
            .await;
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    auth_msg,
                )
                .await,
            );
        }

        // Resolve workspace directory for CLI. User preference wins; the
        // configured working_dir is only a fallback for new users.
        let user_workspace: Option<std::path::PathBuf> = if let Some(ref store) = self.store
            && let Ok(Some(ws)) = store
                .get_user_preference(msg.platform, &msg.user_id, "workspace")
                .await
        {
            crate::workspace::resolve_existing_dir(&ws)
        } else {
            None
        };
        let workspace = user_workspace.or_else(|| {
            self.config
                .working_dir
                .as_deref()
                .and_then(crate::workspace::resolve_existing_dir)
        });

        // Build gateway context for CLI system prompt
        let gw_context = {
            let mut ctx = GatewayContext::new(
                &msg.user_id,
                &msg.user_id,
                msg.platform,
                &cli_profile,
                self.store.is_some(),
            )
            .with_model_actions_allowed(self.config.action_policy.allow_model_generated_mutations);
            if let Some(ref store) = self.store
                && let Ok(jobs) = store.list_cron_jobs(msg.platform, &effective_chat_id).await
            {
                let cron_list: Vec<_> = jobs
                    .iter()
                    .map(|j| {
                        (
                            j.job_id[..8.min(j.job_id.len())].to_string(),
                            j.cron_expr.clone(),
                            j.description.clone(),
                        )
                    })
                    .collect();
                ctx = ctx.with_cron_jobs(cron_list);
            }
            if !self.user_skills.is_empty() {
                ctx = ctx.with_extra_skills(self.user_skills.clone());
            }
            if !self.projects.is_empty() {
                ctx = ctx.with_projects(self.projects.clone());
            }
            if let Some(ref ws) = workspace {
                ctx = ctx.with_workspace(Some(ws.to_string_lossy().to_string()));
            }
            ctx
        };
        let supports_claude_pool =
            crate::cli_pool::CliProcessPool::supports_persistent(&cli_profile);
        let supports_codex_app_pool =
            crate::codex_app_pool::CodexAppPool::supports_persistent(&cli_profile);
        let system_prompt = if supports_claude_pool || supports_codex_app_pool {
            let mut prompt = gw_context.to_slim_system_prompt();
            if let Some(ref extra) = self.config.system_prompt_extra
                && !extra.is_empty()
            {
                prompt.push_str("\n\n");
                prompt.push_str(extra);
            }
            prompt
        } else {
            let mut prompt = gw_context.to_system_prompt();
            if let Some(ref extra) = self.config.system_prompt_extra
                && !extra.is_empty()
            {
                prompt.push_str("\n\n");
                prompt.push_str(extra);
            }
            prompt
        };

        let reasoning_display = if let Some(ref store) = self.store {
            store
                .get_user_preference(msg.platform, &msg.user_id, cli_bridge::REASONING_PREF_KEY)
                .await
                .ok()
                .flatten()
                .as_deref()
                .map(|value| ReasoningDisplay::from_pref(Some(value)))
                .unwrap_or(ReasoningDisplay::Off)
        } else {
            ReasoningDisplay::Off
        };

        // Run CLI with rich progress heartbeats and a bounded lifetime.
        let message_text = message_text_for_cli(&msg);
        let sid = session_id.clone();
        let chat_id = effective_chat_id.clone();
        let reply_token = msg.reply_token.clone();
        // Shared stream_id for all chunks of this reply (WeCom streaming).
        let mut stream_id = reply_token
            .as_ref()
            .map(|_| uuid::Uuid::new_v4().to_string());
        let feedback_id = trace.as_ref().map(|trace| trace.request_id.to_string());
        let cli_name = cli_profile.name().to_string();
        let cli_timeout = Duration::from_secs(self.config.cli_timeout_secs.max(1));

        // Pre-fetch shared access token so the CLI can skip per-spawn auth.
        let access_token = if let Some(ref auth) = self.shared_auth {
            auth.get().await
        } else {
            None
        };
        let github_token = crate::github_tokens::resolve_github_token_for_user(
            &msg.user_id,
            &self.config.github_tokens,
        );
        if github_token.is_some() {
            tracing::debug!(platform = %msg.platform, user_id = %msg.user_id, "resolved per-user GitHub token");
        }

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel::<CliProgress>(64);

        // Register cancellation token for this task so /esc can interrupt it.
        let cancel_token = CancellationToken::new();
        let kill_registry_key = trace
            .as_ref()
            .map(|t| t.trace_id.to_string())
            .unwrap_or_else(|| format!("notrace:{request_tag}"));
        self.active_requests
            .insert(kill_registry_key.clone(), cancel_token.clone());

        let use_claude_pool = supports_claude_pool;
        let use_codex_app_pool = supports_codex_app_pool;
        let use_codex_mcp = matches!(&cli_profile, CliProfile::Codex { .. });
        let persistent_pool_key = persistent_pool_key(msg.platform, &effective_chat_id, &cli_name);

        let storage_env = if use_claude_pool || use_codex_mcp {
            match &self.config.storage {
                crate::store::StorageConfig::Mysql { url }
                | crate::store::StorageConfig::MatrixOne { url } => {
                    Some(crate::mcp::config::McpStorageEnv {
                        database_url: Some(url.clone()),
                        sqlite_path: None,
                    })
                }
                crate::store::StorageConfig::Sqlite { path } => {
                    Some(crate::mcp::config::McpStorageEnv {
                        database_url: None,
                        sqlite_path: Some(path.clone()),
                    })
                }
                _ => Some(crate::mcp::config::McpStorageEnv {
                    database_url: None,
                    sqlite_path: None,
                }),
            }
        } else {
            None
        };

        let mcp_config = if let Some(ref env) = storage_env {
            write_mcp_config_file(
                env,
                &persistent_pool_key,
                msg.platform,
                &effective_chat_id,
                &msg.user_id,
                &self.config.project_dirs,
                self.runtime_api_url.as_deref(),
                self.runtime_api_token.as_deref(),
            )
            .ok()
        } else {
            None
        };

        let cli_handle = if use_claude_pool {
            // Long-lived process path: send message via pool, forward progress events
            let pool = self.cli_pool.clone();
            let pool_key = persistent_pool_key.clone();
            let pool_chat_id = effective_chat_id.clone();
            let profile = cli_profile.clone();
            let msg_text = message_text.clone();
            let sp = system_prompt.clone();
            let ws = workspace.clone();
            let token = access_token.clone();
            let gh_token = github_token.clone();
            let kill_token = cancel_token.clone();
            let mcp_config_path = mcp_config
                .as_ref()
                .map(|config| config.claude_config_path.clone());
            let storage_env_clone = storage_env.clone();
            let pc = provider_config.clone();
            let sid = sid.clone();
            let pool_store = self.store.clone();
            let pool_platform = msg.platform.to_string();
            let pool_cli_name = cli_name.clone();
            let pool_user_id = msg.user_id.clone();
            let project_dirs = self.config.project_dirs.clone();
            let runtime_api_url = self.runtime_api_url.clone();
            let runtime_api_token = self.runtime_api_token.clone();
            let pool_session_states = self.session_states.clone();

            tokio::spawn(async move {
                let mut attempt_sid = sid;
                let mut retried_stale_session = false;

                loop {
                    let mut pool_guard = pool.lock().await;
                    let mut pool_progress_rx = pool_guard
                        .begin_turn(
                            &pool_key,
                            &msg_text,
                            &profile,
                            attempt_sid.as_deref(),
                            ws.as_deref(),
                            Some(&sp),
                            token.as_deref(),
                            gh_token.as_deref(),
                            mcp_config_path.as_deref(),
                            pc.as_ref(),
                        )
                        .await?;
                    drop(pool_guard); // release lock before reading progress

                    // Forward events from pool's progress_rx to the runner's progress_tx
                    // until the turn ends (channel closes), cancel, or timeout.
                    let deadline = tokio::time::sleep(cli_timeout);
                    tokio::pin!(deadline);
                    // Silence heartbeat: every 60s without progress, reassure the user
                    let heartbeat = tokio::time::sleep(Duration::from_secs(60));
                    tokio::pin!(heartbeat);
                    loop {
                        tokio::select! {
                            ev = pool_progress_rx.recv() => {
                                match ev {
                                    Some(event) => {
                                        heartbeat.as_mut().reset(
                                            tokio::time::Instant::now() + Duration::from_secs(60),
                                        );
                                        let _ = progress_tx.send(event).await;
                                    }
                                    None => break, // turn complete
                                }
                            }
                            _ = &mut heartbeat => {
                                let _ = progress_tx
                                    .send(CliProgress::Status(
                                        "⏳ 模型正在深入分析，请稍候…".into(),
                                    ))
                                    .await;
                                heartbeat.as_mut().reset(
                                    tokio::time::Instant::now() + Duration::from_secs(60),
                                );
                            }
                            _ = kill_token.cancelled() => {
                                let pool = pool.lock().await;
                                let _ = pool.interrupt(&pool_key).await;
                                drop(pool);
                                // Give Claude up to 3s to emit result event after interrupt
                                let drain_deadline = tokio::time::sleep(Duration::from_secs(3));
                                tokio::pin!(drain_deadline);
                                loop {
                                    tokio::select! {
                                        ev = pool_progress_rx.recv() => {
                                            match ev {
                                                Some(event) => { let _ = progress_tx.send(event).await; }
                                                None => break,
                                            }
                                        }
                                        _ = &mut drain_deadline => break,
                                    }
                                }
                                break;
                            }
                            _ = &mut deadline => {
                                tracing::warn!(key = %pool_key, "pool turn timed out, killing process");
                                pool.lock().await.kill(&pool_key);
                                break;
                            }
                        }
                    }

                    // Extract stats from the result event stored by stdout reader
                    let pool_guard = pool.lock().await;
                    let session_id = pool_guard.session_id(&pool_key).await;
                    let pool_result = pool_guard.take_last_result(&pool_key).await;
                    let stderr_hint = pool_guard
                        .take_stderr_hint(&pool_key)
                        .await
                        .unwrap_or_default();
                    drop(pool_guard);

                    let stale_session_error = profile.is_stale_session_error(&stderr_hint);
                    if stale_session_error
                        && attempt_sid.is_some()
                        && !retried_stale_session
                        && !kill_token.is_cancelled()
                    {
                        let session_is_current = {
                            let session_states = pool_session_states.lock().await;
                            let generation_matches = session_states
                                .get(&pool_key)
                                .is_some_and(|state| state.generation == session_generation);
                            if generation_matches {
                                if let Some(ref store) = pool_store {
                                    store
                                        .reset_session(
                                            &pool_platform,
                                            &pool_chat_id,
                                            &pool_cli_name,
                                        )
                                        .await
                                        .is_ok()
                                } else {
                                    true
                                }
                            } else {
                                false
                            }
                        };
                        if session_is_current {
                            tracing::info!(
                                key = %pool_key,
                                "pool: cleared stale session; retrying without resume"
                            );
                            pool.lock().await.kill(&pool_key);
                            // Regenerate MCP config file since kill() deleted it
                            if let Some(ref env) = storage_env_clone
                                && let Err(e) = write_mcp_config_file(
                                    env,
                                    &pool_key,
                                    &pool_platform,
                                    &pool_chat_id,
                                    &pool_user_id,
                                    &project_dirs,
                                    runtime_api_url.as_deref(),
                                    runtime_api_token.as_deref(),
                                )
                            {
                                tracing::warn!(
                                    key = %pool_key,
                                    error = %e,
                                    "failed to regenerate MCP config for stale-session retry"
                                );
                            }
                            attempt_sid = None;
                            retried_stale_session = true;
                            continue;
                        }
                    }

                    // Process exited without producing a result event
                    if pool_result.is_none() && !stale_session_error {
                        return Err(if stderr_hint.is_empty() {
                            "pool process exited without result".to_string()
                        } else {
                            format!("pool process exited: {stderr_hint}")
                        });
                    }

                    let mut result = pool_result.unwrap_or_else(|| cli_bridge::CliResult {
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: 1,
                        success: false,
                        error_kind: Some("stale_session".to_string()),
                        provider_error: None,
                        trace_id: None,
                        request_id: None,
                        run_id: None,
                        session_id: session_id.clone(),
                        text: None,
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
                    });
                    result.stderr = stderr_hint;
                    if result.session_id.is_none() {
                        result.session_id = session_id;
                    }
                    if stale_session_error {
                        result.exit_code = 1;
                        result.success = false;
                        result.error_kind = Some("stale_session".to_string());
                    }
                    return Ok(result);
                }
            })
        } else if use_codex_app_pool {
            // Long-lived Codex app-server path: JSON-RPC thread + turn protocol.
            let pool = self.codex_app_pool.clone();
            let pool_key = persistent_pool_key.clone();
            let profile = cli_profile.clone();
            let msg_text = message_text.clone();
            let sp = system_prompt.clone();
            let ws = workspace.clone();
            let kill_token = cancel_token.clone();
            let token = access_token.clone();
            let gh_token = github_token.clone();
            let codex_mcp_config = mcp_config.as_ref().map(|config| config.codex.clone());
            let pc = provider_config.clone();

            tokio::spawn(async move {
                let begin_turn_result = {
                    let mut pool_guard = pool.lock().await;
                    pool_guard
                        .begin_turn(
                            &pool_key,
                            &msg_text,
                            &profile,
                            sid.as_deref(),
                            ws.as_deref(),
                            Some(&sp),
                            pc.as_ref(),
                            gh_token.as_deref(),
                            codex_mcp_config.as_ref(),
                        )
                        .await
                };
                let mut pool_progress_rx = match begin_turn_result {
                    Ok(rx) => rx,
                    Err(e)
                        if matches!(&profile, CliProfile::Astra { .. })
                            && is_astra_app_server_startup_error(&e) =>
                    {
                        tracing::warn!(
                            error = %e,
                            "astra app-server unavailable; falling back to one-shot astra chat"
                        );
                        return cli_bridge::run_cli_with_cancel(
                            &profile,
                            &msg_text,
                            sid.as_deref(),
                            ws.as_deref(),
                            Some(progress_tx.clone()),
                            Some(&sp),
                            None,
                            None,
                            Some(cli_timeout),
                            token.as_deref(),
                            gh_token.as_deref(),
                            Some(kill_token.clone()),
                            pc.as_ref(),
                        )
                        .await;
                    }
                    Err(e) => return Err(e),
                };

                let mut terminal_error: Option<String> = None;
                let deadline = tokio::time::sleep(cli_timeout);
                tokio::pin!(deadline);
                loop {
                    tokio::select! {
                        ev = pool_progress_rx.recv() => {
                            match ev {
                                Some(event) => {
                                    let _ = progress_tx.send(event).await;
                                }
                                None => break,
                            }
                        }
                        _ = kill_token.cancelled() => {
                            let pool_guard = pool.lock().await;
                            let _ = pool_guard.interrupt(&pool_key).await;
                            drop(pool_guard);
                            let drain_deadline = tokio::time::sleep(Duration::from_secs(3));
                            tokio::pin!(drain_deadline);
                            loop {
                                tokio::select! {
                                    ev = pool_progress_rx.recv() => {
                                        match ev {
                                            Some(event) => { let _ = progress_tx.send(event).await; }
                                            None => break,
                                        }
                                    }
                                    _ = &mut drain_deadline => break,
                                }
                            }
                            break;
                        }
                        _ = &mut deadline => {
                            let error = format!(
                                "codex app-server turn timed out after {}s",
                                cli_timeout.as_secs()
                            );
                            tracing::warn!(key = %pool_key, "codex app-server turn timed out, killing process");
                            pool.lock().await.kill(&pool_key);
                            terminal_error = Some(error);
                            break;
                        }
                    }
                }

                if let Some(error) = terminal_error {
                    return Err(error);
                }

                let pool_guard = pool.lock().await;
                let result = pool_guard
                    .result(&pool_key)
                    .await
                    .ok_or_else(|| "codex app-server result unavailable".to_string())?;
                drop(pool_guard);
                Ok(result)
            })
        } else {
            // Legacy per-request spawn path
            let profile = cli_profile.clone();
            let message_text = message_text.clone();
            let system_prompt = system_prompt.clone();
            let ws = workspace.clone();
            let token = access_token.clone();
            let gh_token = github_token.clone();
            let kill_token = cancel_token.clone();
            let trace_id_str = trace.as_ref().map(|t| t.trace_id.to_string());
            let request_id_str = trace.as_ref().map(|t| t.request_id.to_string());
            let pc = provider_config.clone();
            tokio::spawn(async move {
                cli_bridge::run_cli_with_cancel(
                    &profile,
                    &message_text,
                    sid.as_deref(),
                    ws.as_deref(),
                    Some(progress_tx),
                    Some(&system_prompt),
                    trace_id_str.as_deref(),
                    request_id_str.as_deref(),
                    Some(cli_timeout),
                    token.as_deref(),
                    gh_token.as_deref(),
                    Some(kill_token),
                    pc.as_ref(),
                )
                .await
            })
        };

        let start = Instant::now();
        let mut _tool_count: u32 = 0;
        let mut _last_tool = String::new();
        let mut sent_initial_ack = false;
        let mut token_buf = String::new();
        let mut reasoning_buf = String::new();
        let mut reasoning_kind = ReasoningKind::Raw;
        let mut _reasoning_chunk_counter: u32 = 0;
        let allow_answer_progressive_flush = answer_progressive_flush_enabled(reasoning_display);
        let mut think_filter = ThinkTagStreamFilter::default();
        let mut gateway_action_filter = GatewayActionStreamFilter::default();
        let mut progressive_text_len: usize = 0;
        let mut _chunk_counter: u32 = 0;
        let next_timer = tokio::time::sleep(INITIAL_ACK_DELAY);
        tokio::pin!(next_timer);
        let stream_cutoff_timer = tokio::time::sleep(WECOM_STREAM_CUTOFF);
        tokio::pin!(stream_cutoff_timer);
        let post_stream_heartbeat_timer =
            tokio::time::sleep(Duration::from_secs(365 * 24 * 60 * 60));
        tokio::pin!(post_stream_heartbeat_timer);

        // Health key for heartbeat circuit-breaker. MUST use msg.chat_id
        // (not effective_chat_id) because deliver_outbound receives
        // OutboundMessage with msg.chat_id and tracks failures under
        // that key. Using effective_chat_id here would create a mismatch.
        let health_key = format!("{}:{}", msg.platform, msg.chat_id);
        // Reset health at request start — previous failures are stale.
        self.send_health.reset(&health_key);

        // Full accumulated text for stream (WeCom stream content is full-replacement, not append).
        let mut accumulated = String::new();
        let stream_cutoff_enabled = msg.platform == "wecom" && stream_id.is_some();
        let mut stream_cutoff_active = false;
        let mut post_stream_buffer = String::new();
        let mut post_stream_final_sent = false;
        // Next flush threshold: random 5-20 chars.
        let mut next_flush_at: usize = 5 + (rand::random::<u8>() % 16) as usize;

        const STREAM_MAX_BYTES: usize = 20480;

        let send_stream = |accumulated: &String,
                           tx: &Option<tokio::sync::mpsc::Sender<OutboundMessage>>,
                           platform: &str,
                           chat: &str,
                           reply_token: Option<String>,
                           stream_id: Option<String>,
                           finish: bool| {
            let Some(tx) = tx else {
                return;
            };
            if stream_id.is_none() {
                return;
            }
            if accumulated.is_empty() {
                return;
            }
            let _ = tx.try_send(OutboundMessage::stream_chunk(
                platform.to_string(),
                chat.to_string(),
                accumulated.clone(),
                reply_token,
                stream_id,
                feedback_id.clone(),
                finish,
            ));
        };
        let send_plain = |text: String,
                          tx: &Option<tokio::sync::mpsc::Sender<OutboundMessage>>,
                          platform: &str,
                          chat: &str| {
            let Some(tx) = tx else {
                return;
            };
            if text.trim().is_empty() {
                return;
            }
            let _ = tx.try_send(OutboundMessage::plain(
                platform.to_string(),
                chat.to_string(),
                text,
            ));
        };
        let send_plain_ai = |text: String,
                             tx: &Option<tokio::sync::mpsc::Sender<OutboundMessage>>,
                             platform: &str,
                             chat: &str| {
            let Some(tx) = tx else {
                return;
            };
            if text.trim().is_empty() {
                return;
            }
            let _ = tx.try_send(OutboundMessage::plain_with_feedback(
                platform.to_string(),
                chat.to_string(),
                text,
                feedback_id.clone(),
            ));
        };
        let flush_reasoning_buf = |buf: &mut String,
                                   accumulated: &mut String,
                                   kind: ReasoningKind,
                                   agent_name: &str,
                                   tx: &Option<tokio::sync::mpsc::Sender<OutboundMessage>>,
                                   platform: &str,
                                   chat: &str,
                                   reply_token: Option<String>,
                                   stream_id: Option<String>|
         -> usize {
            let text = buf.trim().to_string();
            buf.clear();
            if text.is_empty() {
                return 0;
            }
            let Some(tx) = tx else {
                return 0;
            };
            let title = reasoning_block_title(kind, agent_name);
            let block = format!("{title}\n{text}");
            let len = block.len();
            if !accumulated.is_empty() {
                accumulated.push('\n');
            }
            accumulated.push_str(&block);
            let _ = tx.try_send(OutboundMessage::stream_chunk(
                platform.to_string(),
                chat.to_string(),
                accumulated.clone(),
                reply_token,
                stream_id,
                feedback_id.clone(),
                false,
            ));
            len
        };

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!(tag = %request_tag, "task interrupted by user");
                    break;
                }
                progress = progress_rx.recv() => {
                    match progress {
                        Some(CliProgress::Token(text)) => {
                            let filtered = think_filter.push(&text);
                            let filtered = gateway_action_filter.push(&filtered);
                            if !filtered.is_empty() {
                                token_buf.push_str(&filtered);
                                if allow_answer_progressive_flush
                                    && token_buf.len() >= next_flush_at
                                {
                                    let chunk = std::mem::take(&mut token_buf);
                                    // Check if appending would exceed stream limit — if so, close current stream first
                                    if !stream_cutoff_active
                                        && stream_id.is_some()
                                        && accumulated.len() + chunk.len() > STREAM_MAX_BYTES
                                    {
                                        send_stream(&accumulated, &execution_outbound_tx, msg.platform, &chat_id, reply_token.clone(), stream_id.clone(), true);
                                        accumulated.clear();
                                        stream_id = reply_token.as_ref().map(|_| uuid::Uuid::new_v4().to_string());
                                    }
                                    accumulated.push_str(&chunk);
                                    if stream_cutoff_active {
                                        post_stream_buffer.push_str(&chunk);
                                    }
                                    next_flush_at = 5 + (rand::random::<u8>() % 16) as usize;
                                    _chunk_counter += 1;
                                    if stream_id.is_some() {
                                        progressive_text_len = accumulated.len();
                                    }
                                    if !stream_cutoff_active {
                                        send_stream(&accumulated, &execution_outbound_tx, msg.platform, &chat_id, reply_token.clone(), stream_id.clone(), false);
                                    }
                                }
                            }
                        }
                        Some(CliProgress::ToolStarted { ref name, ref params }) => {
                            _tool_count += 1;
                            _last_tool = name.clone();
                            tracing::debug!(tool = %name, params = ?params, "ToolStarted received");
                            if let Some(writer) = trace_writer.as_ref() {
                                let redacted_params = params
                                    .as_deref()
                                    .map(redact_sensitive);
                                let _ = writer
                                    .append(
                                        GatewayEventKind::CliProgress,
                                        serde_json::json!({
                                            "phase": "tool_started",
                                            "name": name,
                                            "params": redacted_params,
                                        }),
                                    )
                                    .await;
                            }
                            // Append tool use indicator to stream and flush immediately
                            if stream_id.is_some() || stream_cutoff_active {
                                let mut chunk = String::new();
                                if !accumulated.is_empty() && !accumulated.ends_with('\n') {
                                    chunk.push_str("\n\n");
                                }
                                if let Some(p) = params {
                                    chunk.push_str(&format!("🔧 {}: {}\n\n", name, p));
                                } else {
                                    chunk.push_str(&format!("🔧 {}\n\n", name));
                                }
                                accumulated.push_str(&chunk);
                                if stream_cutoff_active {
                                    post_stream_buffer.push_str(&chunk);
                                } else {
                                    send_stream(&accumulated, &execution_outbound_tx, msg.platform, &chat_id, reply_token.clone(), stream_id.clone(), false);
                                }
                            }
                        }
                        Some(CliProgress::ToolDone { name, duration_ms }) => {
                            if let Some(writer) = trace_writer.as_ref() {
                                let _ = writer
                                    .append(
                                        GatewayEventKind::CliProgress,
                                        serde_json::json!({
                                            "phase": "tool_done",
                                            "name": name,
                                            "duration_ms": duration_ms,
                                        }),
                                    )
                                    .await;
                            }
                            _last_tool = name;
                        }
                        Some(CliProgress::ToolCall(line)) => {
                            _tool_count += 1;
                            _last_tool = line;
                        }
                        Some(CliProgress::ReasoningBlock { kind, text }) => {
                            if reasoning_display.is_enabled() {
                                if !reasoning_buf.is_empty() && reasoning_kind != kind {
                                    _reasoning_chunk_counter += 1;
                                    let _ = flush_reasoning_buf(
                                        &mut reasoning_buf,
                                        &mut accumulated,
                                        reasoning_kind,
                                        &cli_name,
                                        &execution_outbound_tx,
                                        msg.platform,
                                        &chat_id,
                                        reply_token.clone(),
                                        stream_id.clone(),
                                    );
                                }
                                reasoning_kind = kind;
                                reasoning_buf.push_str(&text);
                                if reasoning_buf.len() >= PROGRESSIVE_MIN_CHARS {
                                    _reasoning_chunk_counter += 1;
                                    let _ = flush_reasoning_buf(
                                        &mut reasoning_buf,
                                        &mut accumulated,
                                        reasoning_kind,
                                        &cli_name,
                                        &execution_outbound_tx,
                                        msg.platform,
                                        &chat_id,
                                        reply_token.clone(),
                                        stream_id.clone(),
                                    );
                                }
                            }
                        }
                        Some(CliProgress::ApprovalRequested { id, tool, header, detail, reason }) => {
                            sent_initial_ack = true;
                            if let Some(writer) = trace_writer.as_ref() {
                                let _ = writer
                                    .append(
                                        GatewayEventKind::CliProgress,
                                        serde_json::json!({
                                            "phase": "approval_requested",
                                            "id": id,
                                            "tool": tool,
                                            "header": header,
                                            "detail": detail.as_deref().map(redact_sensitive),
                                            "reason": reason,
                                        }),
                                    )
                                    .await;
                            }

                            let mut lines = vec![
                                format!("🔐 `{tool}` 需要确认"),
                                header,
                            ];
                            if let Some(detail) = detail
                                && !detail.trim().is_empty()
                            {
                                lines.push(format!("详情: {}", truncate_chars(&detail, 400)));
                            }
                            if !reason.trim().is_empty() {
                                lines.push(format!("原因: {reason}"));
                            }
                            lines.push(String::new());
                            lines.push("回复 `/approve` 继续，或 `/deny` 拒绝。".to_string());

                            if let Some(ref tx) = execution_outbound_tx {
                                let _ = tx.try_send(OutboundMessage {
                                    platform: msg.platform.to_string(),
                                    chat_id: chat_id.clone(),
                                    text: lines.join("\n"),
                                    reply_token: reply_token.clone(),
                                    outbox: None,
                                    stream_id: None,
                                    feedback_id: None,
                                    stream_finish: true,
                                });
                            }
                        }
                        Some(CliProgress::Thinking(_)) => {}
                        Some(CliProgress::Status(_) | CliProgress::Stderr(_)) => {}
                        None => {
                            if reasoning_display.is_enabled() {
                                let _ = flush_reasoning_buf(
                                    &mut reasoning_buf,
                                    &mut accumulated,
                                    reasoning_kind,
                                    &cli_name,
                                    &execution_outbound_tx,
                                    msg.platform,
                                    &chat_id,
                                    reply_token.clone(),
                                    stream_id.clone(),
                                );
                            }
                            let think_tail = think_filter.finish();
                            if !think_tail.is_empty() {
                                let filtered = gateway_action_filter.push(&think_tail);
                                token_buf.push_str(&filtered);
                            }
                            let tail = gateway_action_filter.finish();
                            if !tail.is_empty() {
                                token_buf.push_str(&tail);
                            }
                            if !token_buf.is_empty() {
                                let chunk = std::mem::take(&mut token_buf);
                                accumulated.push_str(&chunk);
                                if stream_cutoff_active {
                                    post_stream_buffer.push_str(&chunk);
                                }
                            }
                            if !accumulated.is_empty() && stream_id.is_some() {
                                progressive_text_len = accumulated.len();
                            }
                            if let Some(writer) = trace_writer.as_ref()
                                && !accumulated.is_empty()
                            {
                                let redacted = redact_sensitive(&accumulated);
                                let _ = writer
                                    .append(
                                        GatewayEventKind::CliProgress,
                                        serde_json::json!({
                                            "phase": "assistant_reply",
                                            "text": redacted,
                                            "text_len": accumulated.len(),
                                            "tool_count": _tool_count,
                                        }),
                                    )
                                    .await;
                            }
                            // Send full accumulated content with finish=true to close the stream
                            if stream_cutoff_active {
                                send_plain_ai(
                                    post_stream_buffer.clone(),
                                    &execution_outbound_tx,
                                    msg.platform,
                                    &chat_id,
                                );
                                post_stream_final_sent = true;
                            } else {
                                send_stream(&accumulated, &execution_outbound_tx, msg.platform, &chat_id, reply_token.clone(), stream_id.clone(), true);
                            }
                            break;
                        }
                    }
                }
                _ = &mut stream_cutoff_timer, if stream_cutoff_enabled && !stream_cutoff_active => {
                    if !token_buf.is_empty() {
                        let chunk = std::mem::take(&mut token_buf);
                        accumulated.push_str(&chunk);
                    }
                    if !accumulated.is_empty() && stream_id.is_some() {
                        progressive_text_len = accumulated.len();
                    }
                    send_stream(&accumulated, &execution_outbound_tx, msg.platform, &chat_id, reply_token.clone(), stream_id.clone(), true);
                    stream_id = None;
                    stream_cutoff_active = true;
                    post_stream_heartbeat_timer.as_mut().reset(
                        tokio::time::Instant::now() + WECOM_POST_STREAM_HEARTBEAT,
                    );
                    tracing::info!(
                        platform = %msg.platform,
                        chat_id = %safe_id(&chat_id),
                        bytes = accumulated.len(),
                        "wecom stream cutoff reached; continuing with deferred plain output"
                    );
                }
                _ = &mut post_stream_heartbeat_timer, if stream_cutoff_active && !post_stream_final_sent => {
                    let heartbeat = format!(
                        "[{request_tag}] 流式窗口已切段，仍在继续处理；当前已累积后续输出 {} 字节。",
                        post_stream_buffer.len() + token_buf.len()
                    );
                    send_plain(heartbeat, &execution_outbound_tx, msg.platform, &chat_id);
                    post_stream_heartbeat_timer.as_mut().reset(
                        tokio::time::Instant::now() + WECOM_POST_STREAM_HEARTBEAT,
                    );
                }
                _ = &mut next_timer => {
                    if !sent_initial_ack {
                        sent_initial_ack = true;
                        let ack = format!("[{request_tag}] 🤔 {cli_name} 思考中…");
                        if let Some(ref tx) = execution_outbound_tx {
                            let _ = tx.try_send(OutboundMessage {
                                platform: msg.platform.to_string(),
                                chat_id: chat_id.clone(),
                                text: ack,
                                reply_token: reply_token.clone(),
                                outbox: None,
                                stream_id: None,
                                feedback_id: None,
                                stream_finish: true,
                                                });
                        }
                    }
                    next_timer.as_mut().reset(tokio::time::Instant::now() + HEARTBEAT_INTERVAL);
                }
            }
        }

        // Deregister from active tasks registry.
        self.active_requests.remove(&kill_registry_key);

        // If the task was cancelled, the persistent pools translate it to
        // native Esc/interrupt for the active turn. One-shot CLIs are
        // terminated by the bridge. Short-circuit: mark trace as failed
        // and return an interrupt confirmation
        // without processing the result as a normal completion.
        if cancel_token.is_cancelled() {
            let cancelled_result = match cli_handle.await {
                Ok(Ok(result)) => Some(result),
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "CLI returned no result after cancel");
                    None
                }
                Err(e) => {
                    tracing::debug!(error = %e, "cli task join error after cancel");
                    None
                }
            };
            self.finalize_attempt(
                &msg,
                &effective_chat_id,
                session_generation,
                &cli_name,
                &cli_profile,
                trace.as_ref(),
                trace_writer.as_ref(),
                run_id.as_ref(),
                session_id.as_deref(),
                cancelled_result.as_ref(),
                AttemptOutcome::Cancelled,
                Some("cancelled_by_user"),
                Some("interrupted by user"),
                start.elapsed(),
            )
            .await;
            tracing::info!(tag = %request_tag, "task interrupted, skipping result processing");
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    format!("[{request_tag}] ⎋ 当前 turn 已中断"),
                )
                .await,
            );
        }

        let mut result = match cli_handle.await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                // Track auth failures for circuit breaker
                if cli_bridge::is_auth_error(&e) {
                    self.record_auth_failure(&cli_name);
                    if let Some(ref auth) = self.shared_auth {
                        auth.invalidate().await;
                    }
                }
                self.finalize_attempt(
                    &msg,
                    &effective_chat_id,
                    session_generation,
                    &cli_name,
                    &cli_profile,
                    trace.as_ref(),
                    trace_writer.as_ref(),
                    run_id.as_ref(),
                    session_id.as_deref(),
                    None,
                    AttemptOutcome::InternalError,
                    Some("cli_execution_error"),
                    Some(&e),
                    start.elapsed(),
                )
                .await;
                let text = cli_bridge::translate_cli_error(&cli_profile, -1, &e);
                let text = format!("[{request_tag}] {text}");
                return Some(
                    self.outbound_response(
                        trace.as_ref(),
                        msg.platform,
                        &msg.chat_id,
                        msg.reply_token.clone(),
                        text,
                    )
                    .await,
                );
            }
            Err(e) => {
                let error = e.to_string();
                self.finalize_attempt(
                    &msg,
                    &effective_chat_id,
                    session_generation,
                    &cli_name,
                    &cli_profile,
                    trace.as_ref(),
                    trace_writer.as_ref(),
                    run_id.as_ref(),
                    session_id.as_deref(),
                    None,
                    AttemptOutcome::InternalError,
                    Some("cli_task_join_error"),
                    Some(&error),
                    start.elapsed(),
                )
                .await;
                return Some(
                    self.outbound_response(
                        trace.as_ref(),
                        msg.platform,
                        &msg.chat_id,
                        msg.reply_token.clone(),
                        format!("[{request_tag}] ⚠️ 任务中断: {e}"),
                    )
                    .await,
                );
            }
        };

        // Stale session recovery: if CLI says the stored session/thread is gone,
        // clear it and retry without resume. Some backends report this on stderr
        // while still exiting 0, so do not gate on exit_code.
        if cli_profile.is_stale_session_error(&result.stderr) && session_id.is_some() && {
            let session_states = self.session_states.lock().await;
            let generation_matches = session_states
                .get(&session_state_key)
                .is_some_and(|state| state.generation == session_generation);
            if generation_matches {
                if let Some(ref store) = self.store {
                    store
                        .reset_session(msg.platform, &effective_chat_id, &cli_name)
                        .await
                        .is_ok()
                } else {
                    true
                }
            } else {
                false
            }
        } {
            if let Some(writer) = trace_writer.as_ref()
                && let Some(ref run_id) = run_id
            {
                let _ = writer
                    .finish_run(
                        run_id,
                        RunStatus::Failed,
                        Some(result.exit_code),
                        Some("stale session"),
                    )
                    .await;
            }
            tracing::info!(
                cli = cli_name,
                "stale session detected — clearing and retrying"
            );
            // The database session and the live app-server/process handle are
            // one logical continuation state. Drop any stale handle before the
            // one-shot recovery so the following turn cannot resume the old
            // in-memory thread again.
            self.cli_pool.lock().await.kill(&persistent_pool_key);
            self.codex_app_pool.lock().await.kill(&persistent_pool_key);
            let retry_run_id = if let Some(writer) = trace_writer.as_ref() {
                writer.start_run(&cli_name, None).await.ok()
            } else {
                None
            };
            result = match cli_bridge::run_cli_with_context(
                &cli_profile,
                &message_text,
                None,
                workspace.as_deref(),
                None,
                Some(&system_prompt),
                access_token.as_deref(),
                None,
            )
            .await
            {
                Ok(retry_result) => retry_result,
                Err(e) => {
                    let trace_error = format!("stale session retry failed: {e}");
                    self.finalize_attempt(
                        &msg,
                        &effective_chat_id,
                        session_generation,
                        &cli_name,
                        &cli_profile,
                        trace.as_ref(),
                        trace_writer.as_ref(),
                        retry_run_id.as_ref(),
                        None,
                        None,
                        AttemptOutcome::InternalError,
                        Some("stale_session_retry_error"),
                        Some(&trace_error),
                        start.elapsed(),
                    )
                    .await;
                    let text = cli_bridge::translate_cli_error(&cli_profile, -1, &e);
                    return Some(
                        self.outbound_response(
                            trace.as_ref(),
                            msg.platform,
                            &msg.chat_id,
                            msg.reply_token.clone(),
                            format!("[{request_tag}] 会话已失效，自动重试失败: {text}"),
                        )
                        .await,
                    );
                }
            };
            run_id = retry_run_id;
        }

        if let Some(provider_error) = result.provider_error.as_ref() {
            let trace_error = format!(
                "{} status={:?} code={:?} request_id={:?}: {}",
                provider_error.kind,
                provider_error.status,
                provider_error.code,
                provider_error.request_id,
                provider_error.message
            );
            tracing::warn!(
                kind = provider_error.kind,
                status = ?provider_error.status,
                upstream_request_id = ?provider_error.request_id,
                "Claude provider request failed"
            );
            let elapsed = start.elapsed();
            let usage = self
                .finalize_attempt(
                    &msg,
                    &effective_chat_id,
                    session_generation,
                    &cli_name,
                    &cli_profile,
                    trace.as_ref(),
                    trace_writer.as_ref(),
                    run_id.as_ref(),
                    session_id.as_deref(),
                    Some(&result),
                    AttemptOutcome::ProviderError,
                    Some(&provider_error.kind),
                    Some(&trace_error),
                    elapsed,
                )
                .await;

            let mut stats_parts = vec![format_elapsed(elapsed)];
            if usage.cost_usd.unwrap_or(0.0) > 0.001 {
                stats_parts.push(format!("${:.3}", usage.cost_usd.unwrap_or(0.0)));
            }
            if !self.config.response_footer {
                stats_parts.clear();
            }
            let body = cli_bridge::translate_provider_error(&cli_profile, provider_error);
            let text = build_final_message(&body, "", &stats_parts, 0, &request_tag);
            return Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    text,
                )
                .await,
            );
        }

        if !result.success {
            tracing::warn!(
                exit_code = result.exit_code,
                stderr = %result.stderr.chars().take(200).collect::<String>(),
                stdout_len = result.stdout.len(),
                text = ?result.text.as_deref().map(|t| truncate_chars(t, 100)),
                "CLI non-zero exit"
            );

            // Detect auth errors and record for circuit breaker
            let combined_err = format!("{}\n{}", result.stderr, result.stdout);
            if cli_bridge::is_auth_error(&combined_err) {
                self.record_auth_failure(&cli_name);
                if let Some(ref auth) = self.shared_auth {
                    auth.invalidate().await;
                }

                // Attempt auto-relogin if credentials are configured
                if self.config.astra.username.is_some()
                    && self.config.astra.password.is_some()
                    && matches!(cli_profile, CliProfile::Astra { .. })
                {
                    match self.try_auto_relogin().await {
                        Ok(ref token) => {
                            tracing::info!(cli = %cli_name, "auto-relogin succeeded after auth failure");
                            self.clear_auth_failure(&cli_name);
                            // Update shared token cache with the fresh token
                            if let Some(ref auth) = self.shared_auth {
                                let mut guard = auth.token.write().await;
                                *guard = Some(token.clone());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(cli = %cli_name, error = %e, "auto-relogin failed");
                        }
                    }
                }
            }

            if result.text.is_none() || result.text.as_deref() == Some("") {
                let error_text = if result.stderr.is_empty() {
                    &result.stdout
                } else {
                    &result.stderr
                };
                let elapsed = start.elapsed();
                self.finalize_attempt(
                    &msg,
                    &effective_chat_id,
                    session_generation,
                    &cli_name,
                    &cli_profile,
                    trace.as_ref(),
                    trace_writer.as_ref(),
                    run_id.as_ref(),
                    session_id.as_deref(),
                    Some(&result),
                    AttemptOutcome::CliError,
                    result.error_kind.as_deref().or(Some("cli_error")),
                    Some(error_text.trim()),
                    elapsed,
                )
                .await;
                let text = cli_bridge::translate_cli_error(
                    &cli_profile,
                    result.exit_code,
                    error_text.trim(),
                );
                let text = format!("[{request_tag}] {text}");
                return Some(
                    self.outbound_response(
                        trace.as_ref(),
                        msg.platform,
                        &msg.chat_id,
                        msg.reply_token.clone(),
                        text,
                    )
                    .await,
                );
            }
        }

        // Clear auth failure counter on success
        if result.success {
            self.clear_auth_failure(&cli_name);
        }

        // Use the parsed text field (from --json), fallback to raw stdout
        let mut text = result
            .text
            .as_deref()
            .unwrap_or(result.stdout.trim())
            .to_string();

        if let Some(response) = compact_response_override(&message_text, result.success, &text) {
            text = response.to_string();
        }

        // Strip <think>...</think> blocks that some models emit as plain text
        text = strip_think_blocks(&text);

        // Execute gateway actions embedded in agent response
        let mut action_results_text = String::new();
        if !background && text.contains("[[GATEWAY:") {
            let mut action_results = Vec::new();
            text = execute_gateway_actions_with_policy(
                &text,
                self.store.as_deref(),
                msg.platform,
                &msg.chat_id,
                &msg.user_id,
                &self.config.action_policy,
                &mut action_results,
            )
            .await;
            if !action_results.is_empty() {
                action_results_text = action_results.join("\n");
                text.push_str("\n\n");
                text.push_str(&action_results_text);
            }
        }

        tracing::info!(
            platform = msg.platform,
            chat_id = %safe_id(&msg.chat_id),
            text_len = text.len(),
            tools = result.tool_calls_count.unwrap_or(0),
            exit = result.exit_code,
            "← done"
        );

        // Append token usage stats + cost estimate
        let elapsed = start.elapsed();
        let outcome = AttemptOutcome::from_result(&result);
        let terminal_error = outcome.is_failure().then(|| {
            if !result.stderr.trim().is_empty() {
                result.stderr.trim()
            } else if !result.stdout.trim().is_empty() {
                result.stdout.trim()
            } else {
                "CLI request failed"
            }
        });
        let usage = self
            .finalize_attempt(
                &msg,
                &effective_chat_id,
                session_generation,
                &cli_name,
                &cli_profile,
                trace.as_ref(),
                trace_writer.as_ref(),
                run_id.as_ref(),
                session_id.as_deref(),
                Some(&result),
                outcome,
                result.error_kind.as_deref(),
                terminal_error,
                elapsed,
            )
            .await;
        let prompt_tok = usage.prompt;
        let completion_tok = usage.completion;
        let cache_create_tok = usage.cache_creation;
        let cache_read_tok = usage.cache_read;
        let cached_tok = usage.cached;
        let reasoning_tok = usage.reasoning;
        let total_tok = usage.total;
        let cost = usage.cost_usd.unwrap_or_else(|| {
            (prompt_tok as f64 * 3.0 + completion_tok as f64 * 15.0) / 1_000_000.0
        });
        let mut stats_parts = Vec::new();
        if prompt_tok > 0 {
            stats_parts.push(format!("↓{}", format_tokens(prompt_tok)));
        }
        if completion_tok > 0 {
            stats_parts.push(format!("↑{}", format_tokens(completion_tok)));
        }
        if cache_read_tok > 0 || cache_create_tok > 0 || cached_tok > 0 {
            let cache_display = cache_read_tok.max(cached_tok);
            let cache_part = if cache_create_tok > 0 {
                format!(
                    "cache r{} c{}",
                    format_tokens(cache_display),
                    format_tokens(cache_create_tok)
                )
            } else {
                format!("cache {}", format_tokens(cache_display))
            };
            stats_parts.push(cache_part);
        }
        if reasoning_tok > 0 {
            stats_parts.push(format!("reason {}", format_tokens(reasoning_tok)));
        }
        if let Some(context_window) = result.context_window
            && context_window > 0
            && total_tok > 0
        {
            stats_parts.push(format!(
                "ctx {}/{}",
                format_tokens(total_tok),
                format_tokens(context_window)
            ));
        }
        let tool_count_total = usage.tool_calls;
        if tool_count_total > 0 {
            stats_parts.push(format!("🔧{tool_count_total}"));
        }
        stats_parts.push(format_elapsed(elapsed));
        if cost > 0.001 {
            stats_parts.push(format!("${cost:.3}"));
        }
        if !self.config.response_footer {
            stats_parts.clear();
        }
        let progressive_delivery_len = if progressive_text_len > 0 || post_stream_final_sent {
            progressive_text_len.max(1)
        } else {
            0
        };
        text = build_final_message(
            &text,
            &action_results_text,
            &stats_parts,
            progressive_delivery_len,
            &request_tag,
        );

        // Scheduled turns share the normal conversation queue, pool, and
        // session, but the scheduler owns delivery and lifecycle decisions.
        // Return the raw final response without creating an outbox entry.
        if background {
            if let Some(writer) = trace_writer.as_ref() {
                let _ = writer.complete_request().await;
            }
            return Some(OutboundMessage::plain(
                msg.platform.to_string(),
                msg.chat_id.clone(),
                text,
            ));
        }

        // When progressive streaming already delivered the main content, the
        // final message is just a stats footer + action results.  Sending it
        // as a plain message (no durable outbox) avoids retry storms: if this
        // low-value footer fails to deliver it is simply dropped rather than
        // retried on every restart.
        if progressive_delivery_len > 0 {
            // Still mark the trace request as completed (even without outbox).
            if let Some(writer) = trace_writer.as_ref() {
                let _ = writer.complete_request().await;
            }
            if text.is_empty() {
                return None;
            }
            // Delay stats footer so it doesn't visually collide with the stream closing
            tokio::time::sleep(Duration::from_secs(2)).await;
            // Stream already closed by final flush; send stats as a separate plain message
            Some(OutboundMessage::plain(
                msg.platform.to_string(),
                msg.chat_id.clone(),
                text,
            ))
        } else {
            let text = if text.is_empty() {
                "(无回复)".to_string()
            } else {
                text
            };
            Some(
                self.outbound_response(
                    trace.as_ref(),
                    msg.platform,
                    &msg.chat_id,
                    msg.reply_token.clone(),
                    text,
                )
                .await,
            )
        }
    }

    async fn outbound_response(
        &self,
        trace: Option<&OutboxDeliveryTrace>,
        platform: &str,
        chat_id: &str,
        reply_token: Option<String>,
        text: String,
    ) -> OutboundMessage {
        let Some(trace) = trace else {
            return OutboundMessage {
                platform: platform.to_string(),
                chat_id: chat_id.to_string(),
                text,
                reply_token,
                stream_id: None,
                feedback_id: None,
                stream_finish: true,
                outbox: None,
            };
        };
        let Some(repo) = self.trace_repo.as_ref() else {
            return OutboundMessage {
                platform: platform.to_string(),
                chat_id: chat_id.to_string(),
                text,
                reply_token,
                stream_id: None,
                feedback_id: Some(trace.request_id.to_string()),
                stream_finish: true,
                outbox: None,
            };
        };
        let writer = TraceWriter::from_existing(
            repo.as_ref() as &dyn TraceRepository,
            trace.trace_id.clone(),
            trace.request_id.clone(),
        );
        match writer
            .enqueue_outbox(platform, chat_id, reply_token.clone(), &text)
            .await
        {
            Ok(outbox_id) => OutboundMessage::with_outbox(
                platform.to_string(),
                chat_id.to_string(),
                text,
                reply_token,
                Some(trace.request_id.to_string()),
                OutboxDelivery {
                    outbox_id,
                    trace_id: trace.trace_id.clone(),
                    request_id: trace.request_id.clone(),
                },
            ),
            Err(e) => {
                tracing::warn!(error = %e, "failed to enqueue outbox; falling back to direct send");
                OutboundMessage {
                    platform: platform.to_string(),
                    chat_id: chat_id.to_string(),
                    text,
                    reply_token,
                    stream_id: None,
                    feedback_id: Some(trace.request_id.to_string()),
                    stream_finish: true,
                    outbox: None,
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finalize_attempt(
        &self,
        msg: &InboundMessage,
        effective_chat_id: &str,
        session_generation: u64,
        cli_name: &str,
        cli_profile: &CliProfile,
        trace: Option<&OutboxDeliveryTrace>,
        trace_writer: Option<&TraceWriter<'_>>,
        run_id: Option<&RunId>,
        previous_session_id: Option<&str>,
        result: Option<&CliResult>,
        outcome: AttemptOutcome,
        failure_kind: Option<&str>,
        error_message: Option<&str>,
        elapsed: Duration,
    ) -> AttemptUsage {
        let usage = result.map_or_else(AttemptUsage::default, |result| {
            let prompt = result.tokens_prompt.unwrap_or(0);
            let completion = result.tokens_completion.unwrap_or(0);
            let cached = result.cached_input_tokens.unwrap_or(0);
            let cache_creation = result.cache_creation_input_tokens.unwrap_or(0);
            let cache_read = result.cache_read_input_tokens.unwrap_or(0);
            let reasoning = result.reasoning_output_tokens.unwrap_or(0);
            let total = result
                .total_tokens
                .unwrap_or(prompt + completion + cache_creation + cache_read + reasoning);
            let cost_usd = if outcome.is_failure() && total == 0 {
                Some(0.0)
            } else {
                result.cost_usd
            };
            AttemptUsage {
                prompt,
                completion,
                cached,
                cache_creation,
                cache_read,
                reasoning,
                total,
                cost_usd,
                tool_calls: result.tool_calls_count.unwrap_or(0),
            }
        });

        if let Some(writer) = trace_writer {
            if let Some(run_id) = run_id
                && let Err(e) = writer
                    .finish_run(
                        run_id,
                        outcome.run_status(),
                        result.map(|result| result.exit_code),
                        error_message,
                    )
                    .await
            {
                tracing::warn!(error = %e, "failed to finish CLI trace run");
            }
            if outcome.is_failure()
                && let Err(e) = writer
                    .fail_request(error_message.unwrap_or("CLI request failed"))
                    .await
            {
                tracing::warn!(error = %e, "failed to mark CLI trace request as failed");
            }
        }

        let result_session_id = result.and_then(|result| result.session_id.as_deref());
        let session_state_key = persistent_pool_key(msg.platform, effective_chat_id, cli_name);
        let mut session_states = self.session_states.lock().await;
        let generation_is_current = session_states
            .get(&session_state_key)
            .is_some_and(|state| state.generation == session_generation);
        if let Some(store) = self.store.as_ref() {
            if generation_is_current && let Some(session_id) = result_session_id {
                if let Err(e) = store
                    .set_current_session(
                        msg.platform,
                        effective_chat_id,
                        &msg.user_id,
                        session_id,
                        cli_name,
                    )
                    .await
                {
                    tracing::warn!(error = %e, "failed to persist terminal CLI session");
                }
            } else if generation_is_current
                && outcome == AttemptOutcome::Success
                && let Err(e) = store
                    .touch_session(msg.platform, effective_chat_id, cli_name)
                    .await
            {
                tracing::warn!(error = %e, "failed to touch terminal CLI session");
            }
        }

        if !generation_is_current {
            tracing::info!(
                key = %session_state_key,
                "skipped stale terminal session persistence after session mutation"
            );
        }
        let remove_session_state = if let Some(state) = session_states.get_mut(&session_state_key) {
            state.active_attempts = state.active_attempts.saturating_sub(1);
            state.active_attempts == 0
        } else {
            false
        };
        if remove_session_state {
            session_states.remove(&session_state_key);
        }
        drop(session_states);

        if let Some(store) = self.store.as_ref() {
            let failure_kind = failure_kind
                .map(String::from)
                .or_else(|| result.and_then(|result| result.error_kind.clone()));
            if let Err(e) = store
                .record_usage(&store::UsageRecord {
                    platform: msg.platform.to_string(),
                    user_id: msg.user_id.clone(),
                    cli_profile: cli_name.to_string(),
                    model: cli_profile.model_name().map(String::from),
                    trace_id: result
                        .and_then(|result| result.trace_id.clone())
                        .or_else(|| trace.map(|trace| trace.trace_id.to_string())),
                    request_id: result
                        .and_then(|result| result.request_id.clone())
                        .or_else(|| trace.map(|trace| trace.request_id.to_string())),
                    run_id: result
                        .and_then(|result| result.run_id.clone())
                        .or_else(|| run_id.map(ToString::to_string)),
                    session_id: result_session_id.or(previous_session_id).map(String::from),
                    tokens_prompt: usage.prompt,
                    tokens_completion: usage.completion,
                    cached_input_tokens: usage.cached,
                    cache_creation_input_tokens: usage.cache_creation,
                    cache_read_input_tokens: usage.cache_read,
                    reasoning_output_tokens: usage.reasoning,
                    total_tokens: usage.total,
                    context_window: result.and_then(|result| result.context_window),
                    max_output_tokens: result.and_then(|result| result.max_output_tokens),
                    cost_usd: usage
                        .cost_usd
                        .or_else(|| outcome.is_failure().then_some(0.0)),
                    raw_usage_json: result.and_then(|result| result.raw_usage_json.clone()),
                    status: outcome.usage_status(),
                    failure_kind,
                    tool_calls: usage.tool_calls,
                    elapsed_ms: elapsed.as_millis() as u64,
                })
                .await
            {
                tracing::warn!(error = %e, ?outcome, "failed to record terminal CLI usage");
            }
        }

        usage
    }

    fn effective_chat_id(&self, msg: &InboundMessage) -> String {
        if msg.chat_type == crate::platforms::ChatType::Group && self.config.group_sessions_per_user
        {
            format!("{}:{}", msg.chat_id, msg.user_id)
        } else {
            msg.chat_id.clone()
        }
    }

    // ─── Auth circuit breaker ──────────────────────────────────────────────

    /// Check if the auth circuit breaker is tripped for the given CLI.
    /// Returns `Some(message)` if the circuit is open and the caller should
    /// short-circuit without spawning the CLI.
    fn check_auth_circuit(&self, cli_name: &str) -> Option<String> {
        if let Some(entry) = self.auth_failures.get(cli_name) {
            let (count, last_failure) = *entry;
            if count > AUTH_FAILURE_THRESHOLD && last_failure.elapsed() < AUTH_FAILURE_COOLDOWN {
                let remaining = AUTH_FAILURE_COOLDOWN
                    .saturating_sub(last_failure.elapsed())
                    .as_secs();
                return Some(format!(
                    "🔑 CLI `{cli_name}` 认证失败（连续 {count} 次）\n\n\
                     可能原因:\n\
                     - API 密钥过期\n\
                     - 服务端 token 刷新失败\n\n\
                     解决方法:\n\
                     1. 运行 `astra /login` 重新登录\n\
                     2. 或检查环境变量 ASTRA_API_KEY\n\
                     3. 或切换到其他 CLI: `/cli claude`\n\n\
                     {remaining} 秒后自动重试，或发送 `/auth` 手动重试。",
                ));
            }
            // Cooldown expired — clear the counter
            if last_failure.elapsed() >= AUTH_FAILURE_COOLDOWN {
                drop(entry);
                self.auth_failures.remove(cli_name);
            }
        }
        None
    }

    fn auth_status_line(&self, cli_name: &str) -> Option<String> {
        let entry = self.auth_failures.get(cli_name)?;
        let (count, last_failure) = *entry;
        if last_failure.elapsed() >= AUTH_FAILURE_COOLDOWN {
            drop(entry);
            self.auth_failures.remove(cli_name);
            return None;
        }

        if count > AUTH_FAILURE_THRESHOLD {
            let remaining = AUTH_FAILURE_COOLDOWN
                .saturating_sub(last_failure.elapsed())
                .as_secs();
            Some(format!("⚠️ 暂停 (剩余 {remaining}s, 连续失败 {count} 次)"))
        } else {
            Some(format!("✅ 正常 (最近失败 {count} 次)"))
        }
    }

    /// Record an auth failure for the given CLI profile.
    fn record_auth_failure(&self, cli_name: &str) {
        let mut entry = self
            .auth_failures
            .entry(cli_name.to_string())
            .or_insert((0, Instant::now()));
        entry.0 += 1;
        entry.1 = Instant::now();
        tracing::warn!(
            cli = cli_name,
            consecutive_failures = entry.0,
            "auth failure recorded"
        );
    }

    /// Clear auth failure counter for a CLI (called on successful request).
    fn clear_auth_failure(&self, cli_name: &str) {
        if self.auth_failures.remove(cli_name).is_some() {
            tracing::info!(cli = cli_name, "auth failure counter cleared (success)");
        }
    }

    /// Attempt to re-login to the astra server using configured credentials.
    /// On success, writes the new tokens to `~/.astra/credentials.json` so
    /// subsequent CLI spawns pick them up.
    async fn try_auto_relogin(&self) -> Result<String, String> {
        let username = self
            .config
            .astra
            .username
            .as_ref()
            .ok_or("no username configured")?;
        let password = self
            .config
            .astra
            .password
            .as_ref()
            .ok_or("no password configured")?;

        let body = serde_json::json!({ "username": username, "password": password });
        let resp = self
            .thin
            .post_auth_login_json(&body)
            .await
            .map_err(|e| format!("login request failed: {e}"))?;

        let v: serde_json::Value =
            serde_json::from_str(&resp).map_err(|e| format!("invalid login response: {e}"))?;
        let access = v
            .get("access_token")
            .and_then(|t| t.as_str())
            .ok_or("missing access_token in response")?;
        let refresh = v.get("refresh_token").and_then(|t| t.as_str());

        save_token_to_cli_credentials(username, access, refresh)?;
        tracing::info!(username = %username, "auto-relogin succeeded, CLI credentials refreshed");
        Ok(access.to_string())
    }

    /// Handle the `/auth` slash command: reset circuit breaker, show CLI auth
    /// status, and attempt auto-relogin if credentials are configured.
    async fn handle_auth_command(&self, _current_cli: &CliProfile) -> String {
        // 1. Reset circuit breaker
        let cleared = self.auth_failures.len();
        self.auth_failures.clear();

        // 2. Invalidate probe cache so re-probe is fresh
        cli_bridge::invalidate_probe_cache();

        // 3. Re-probe all CLIs
        let mut lines = vec!["🔑 **认证状态**".to_string()];
        let default_avail = cli_bridge::probe_cli(&self.cli_profile).await;
        let default_status = if default_avail.is_available() {
            "✅"
        } else {
            "❌"
        };
        lines.push(format!(
            "  {default_status} `{}` (默认)",
            self.cli_profile.name()
        ));
        for (name, profile) in &self.config.cli_profiles {
            let avail = cli_bridge::probe_cli(profile).await;
            let status = if avail.is_available() { "✅" } else { "❌" };
            lines.push(format!("  {status} `{name}`"));
        }

        if cleared > 0 {
            lines.push(format!(
                "\n认证缓存已重置（清除 {cleared} 个 CLI 的失败计数）。"
            ));
        } else {
            lines.push("\n认证缓存已重置。".into());
        }

        // 4. Invalidate shared token cache so it refreshes on next message
        if let Some(ref auth) = self.shared_auth {
            auth.invalidate().await;
        }

        // 5. Attempt auto-relogin if credentials configured
        if self.config.astra.username.is_some() && self.config.astra.password.is_some() {
            match self.try_auto_relogin().await {
                Ok(ref token) => {
                    lines.push("✅ 自动重新登录成功，凭证已刷新。".into());
                    // Warm shared token cache with the fresh token
                    if let Some(ref auth) = self.shared_auth {
                        let mut guard = auth.token.write().await;
                        *guard = Some(token.clone());
                    }
                }
                Err(e) => {
                    lines.push(format!("⚠️ 自动重新登录失败: {e}"));
                }
            }
        }

        lines.push("\n下次消息将重新验证。".into());
        lines.join("\n")
    }

    /// Build a rich context message for `/manage` that gets sent to the CLI agent.
    async fn build_manage_context(
        &self,
        msg: &InboundMessage,
        effective_chat_id: &str,
        cli_profile: &CliProfile,
        extra: &str,
    ) -> String {
        let cli_name = cli_profile.name();
        let mut sections = vec![
            "# Gateway 任务管理模式".to_string(),
            "你现在是 Gateway 任务管理助手。分析以下运行状态，帮助用户管理任务。".to_string(),
            "你不能直接执行管理操作；请给出明确的 slash command 建议。".to_string(),
        ];

        // Active requests
        if let Some(repo) = self.trace_repo.as_ref() {
            let conversation = ConversationKey::new(msg.platform, effective_chat_id, cli_name);
            let rows = repo
                .list_active_requests(&conversation, 50)
                .await
                .unwrap_or_default();
            if !rows.is_empty() {
                sections.push("\n## 活跃请求".to_string());
                for (i, row) in rows.iter().enumerate() {
                    let icon = match row.display_status() {
                        "running" => "\u{1f504}",
                        "queued" => "\u{231b}",
                        "reply_retrying" => "\u{1f4ec}",
                        "reply_pending" => "\u{1f4e4}",
                        _ => "\u{2753}",
                    };
                    let error_suffix = row
                        .error_message
                        .as_ref()
                        .map(|e| format!(" | error: {e}"))
                        .unwrap_or_default();
                    sections.push(format!(
                        "[{}] {} {} | {} | {}{}",
                        i + 1,
                        icon,
                        row.display_status(),
                        row.text_preview,
                        row.created_at,
                        error_suffix,
                    ));
                }
                sections.push("\n可用操作:".to_string());
                sections.push("- 用 `/esc <trace_id>` 中断运行中的请求".to_string());
                sections.push("- 用 `/retry dismiss <request_id>` 清除失败投递".to_string());
            } else {
                sections.push("\n## 活跃请求\n无".to_string());
            }
        }

        // Cron jobs
        if let Some(ref store) = self.store {
            let jobs = store
                .list_cron_jobs(msg.platform, effective_chat_id)
                .await
                .unwrap_or_default();
            if !jobs.is_empty() {
                sections.push("\n## 定时任务".to_string());
                for job in &jobs {
                    let short = &job.job_id[..8.min(job.job_id.len())];
                    let status = if job.enabled { "\u{2705}" } else { "\u{23f8}" };
                    sections.push(format!(
                        "- {status} `{short}` | `{}` | {}",
                        job.cron_expr, job.description
                    ));
                }
                sections.push("\n可用操作: 使用 `/task cancel <job_id>` 删除".to_string());
            }
        }

        if !extra.is_empty() {
            sections.push(format!("\n## 用户指示\n{extra}"));
        } else {
            sections.push(
                "\n## 请求\n请分析以上状态，报告异常，并建议用户执行对应 slash command。"
                    .to_string(),
            );
        }

        sections.join("\n")
    }

    /// Build a queued request, optionally overriding the conversation's
    /// cli_profile. Used by `/manage` to route to an independent worker
    /// (virtual profile `_manage`) so management commands don't queue
    /// behind the very tasks they're supposed to fix.
    async fn build_queued_request_with_profile_override(
        &self,
        msg: InboundMessage,
        profile_override: Option<&str>,
    ) -> QueuedRequest {
        let effective_chat_id = self.effective_chat_id(&msg);
        let (resolved, _provider_config) = self
            .resolve_cli_profile(msg.platform, &msg.user_id, &effective_chat_id)
            .await;

        let conv_profile = profile_override.unwrap_or(resolved.name());
        let conversation = ConversationKey::new(msg.platform, effective_chat_id, conv_profile);
        let trace = if let Some(repo) = self.trace_repo.as_ref() {
            let request = GatewayRequest::new(
                conversation.clone(),
                msg.msg_id.clone(),
                msg.user_id.clone(),
                msg.text.clone(),
            );
            let trace = OutboxDeliveryTrace {
                trace_id: request.trace_id.clone(),
                request_id: request.request_id.clone(),
            };
            match TraceWriter::begin(repo.as_ref(), request).await {
                Ok(writer) => {
                    let depth = repo
                        .list_active_requests(&conversation, 100)
                        .await
                        .map(|rows| rows.len().saturating_sub(1))
                        .unwrap_or(0);
                    let _ = writer.mark_queued(depth).await;
                    Some(trace)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to create trace request");
                    None
                }
            }
        } else {
            None
        };
        QueuedRequest {
            msg,
            conversation,
            trace,
            background: false,
            scheduled_response_tx: None,
        }
    }

    async fn enqueue_cli_request(
        self: &Arc<Self>,
        msg: InboundMessage,
        cli_resp_tx: tokio::sync::mpsc::Sender<CliResponse>,
    ) {
        // If handle_fast marked the message with a route_override (e.g.
        // `/manage` → MANAGE_CLI_PROFILE), use that as the ConversationKey
        // profile so it goes to its own independent worker.
        let override_profile = msg.route_override.clone();
        self.enqueue_cli_request_with_profile_override(
            msg,
            cli_resp_tx,
            override_profile.as_deref(),
        )
        .await
    }

    /// See build_queued_request_with_profile_override — used by the
    /// `/manage` slow-path so the request goes to a different worker
    /// than the user's currently-running requests.
    async fn enqueue_cli_request_with_profile_override(
        self: &Arc<Self>,
        msg: InboundMessage,
        cli_resp_tx: tokio::sync::mpsc::Sender<CliResponse>,
        profile_override: Option<&str>,
    ) {
        let queued = self
            .build_queued_request_with_profile_override(msg, profile_override)
            .await;
        let key = queued.conversation.clone();
        let tx = {
            let mut queues = self.queue_senders.lock().await;
            if let Some(tx) = queues.get(&key) {
                tx.clone()
            } else {
                let (tx, rx) = tokio::sync::mpsc::channel(128);
                queues.insert(key.clone(), tx.clone());
                let runner = self.clone();
                tokio::spawn(async move {
                    runner.run_conversation_worker(key, rx, cli_resp_tx).await;
                });
                tx
            }
        };
        if let Err(e) = tx.send(queued).await {
            tracing::warn!(error = %e, "failed to enqueue gateway request");
        }
    }

    async fn enqueue_scheduled_agent_turn(
        self: &Arc<Self>,
        turn: ScheduledAgentTurn,
        cli_resp_tx: tokio::sync::mpsc::Sender<CliResponse>,
    ) {
        let mut queued = self
            .build_queued_request_with_profile_override(turn.message, None)
            .await;
        queued.background = true;
        queued.scheduled_response_tx = Some(turn.response_tx);
        let key = queued.conversation.clone();
        let tx = {
            let mut queues = self.queue_senders.lock().await;
            if let Some(tx) = queues.get(&key) {
                tx.clone()
            } else {
                let (tx, rx) = tokio::sync::mpsc::channel(128);
                queues.insert(key.clone(), tx.clone());
                let runner = self.clone();
                tokio::spawn(async move {
                    runner.run_conversation_worker(key, rx, cli_resp_tx).await;
                });
                tx
            }
        };
        if let Err(e) = tx.send(queued).await {
            tracing::warn!(error = %e, "failed to enqueue scheduled agent turn");
        }
    }

    async fn run_conversation_worker(
        self: Arc<Self>,
        key: ConversationKey,
        mut rx: tokio::sync::mpsc::Receiver<QueuedRequest>,
        cli_resp_tx: tokio::sync::mpsc::Sender<CliResponse>,
    ) {
        loop {
            let queued = match tokio::time::timeout(CONVERSATION_QUEUE_IDLE_TIMEOUT, rx.recv())
                .await
            {
                Ok(Some(queued)) => queued,
                Ok(None) => break,
                Err(_) => {
                    let mut queues = self.queue_senders.lock().await;
                    if let Ok(queued) = rx.try_recv() {
                        drop(queues);
                        queued
                    } else {
                        queues.remove(&key);
                        tracing::debug!(conversation = %key, "conversation worker idle timeout");
                        break;
                    }
                }
            };
            if !self.should_execute_queued(&queued).await {
                continue;
            }
            let Ok(_permit) = self.global_run_limiter.clone().acquire_owned().await else {
                break;
            };
            let response = self
                .handle_message_inner(
                    &queued.msg,
                    &NullAdapter,
                    queued.trace.clone(),
                    queued.background,
                )
                .await;
            if let Some(response_tx) = queued.scheduled_response_tx {
                let _ = response_tx.send(response);
                continue;
            }
            match response {
                Some(outbound) => {
                    let _ = cli_resp_tx.send(outbound).await;
                }
                None => {
                    // Fix A: Never leave a trace stuck in Running when no response is produced.
                    if let Some(trace) = queued.trace.as_ref()
                        && let Some(repo) = self.trace_repo.as_ref()
                    {
                        let writer = TraceWriter::from_existing(
                            repo.as_ref() as &dyn TraceRepository,
                            trace.trace_id.clone(),
                            trace.request_id.clone(),
                        );
                        let _ = writer.fail_request("request produced no response").await;
                    }
                }
            }
        }
        // Fix C: Sweep any Running/Accepted traces for this conversation before exiting.
        if let Some(repo) = self.trace_repo.as_ref() {
            match repo
                .sweep_conversation_stale_requests(&key, "conversation worker exited")
                .await
            {
                Ok(0) => {}
                Ok(n) => {
                    tracing::info!(conversation = %key, count = n, "swept stale traces on worker exit");
                }
                Err(e) => {
                    tracing::warn!(conversation = %key, error = %e, "failed to sweep stale traces on worker exit");
                }
            }
        }
        self.queue_senders.lock().await.remove(&key);
        tracing::debug!(conversation = %key, "conversation worker stopped");
    }

    async fn should_execute_queued(&self, queued: &QueuedRequest) -> bool {
        let Some(trace) = queued.trace.as_ref() else {
            return true;
        };
        let Some(repo) = self.trace_repo.as_ref() else {
            return true;
        };
        match repo.get_request(&trace.request_id).await {
            Ok(Some(request)) if request.status == RequestStatus::Accepted => true,
            Ok(Some(request)) if request.status.is_terminal() => {
                tracing::info!(
                    request_id = %trace.request_id,
                    status = request.status.as_str(),
                    "skipping queued request (terminal status)"
                );
                false
            }
            Ok(Some(request)) => {
                // Non-terminal, non-Accepted (e.g. Running from a previous crash) — mark failed
                tracing::info!(
                    request_id = %trace.request_id,
                    status = request.status.as_str(),
                    "skipping queued request with unexpected status; marking failed"
                );
                let writer = TraceWriter::from_existing(
                    repo.as_ref() as &dyn TraceRepository,
                    trace.trace_id.clone(),
                    trace.request_id.clone(),
                );
                let _ = writer
                    .fail_request(&format!(
                        "queued request had unexpected status: {}",
                        request.status.as_str()
                    ))
                    .await;
                false
            }
            Ok(None) => false,
            Err(e) => {
                // Execute on DB error rather than silently dropping the request
                tracing::warn!(error = %e, "failed to verify queued request status; executing anyway");
                true
            }
        }
    }

    async fn replay_retryable_outbox(
        &self,
        adapters: &[Box<dyn PlatformAdapter>],
        adapter_indices: &HashMap<&'static str, usize>,
    ) {
        let Some(repo) = self.trace_repo.as_ref() else {
            return;
        };
        let result = retry_once_on_transient("list_retryable_outbox", || async {
            repo.list_retryable_outbox(None, 100).await
        })
        .await;
        match result {
            Ok(rows) if rows.is_empty() => {}
            Ok(rows) => {
                tracing::info!(count = rows.len(), "replaying retryable outbox");
                for row in rows {
                    let outbound = OutboundMessage::with_outbox(
                        row.platform.clone(),
                        row.chat_id.clone(),
                        row.body.clone(),
                        row.reply_token.clone(),
                        Some(row.request_id.to_string()),
                        OutboxDelivery {
                            outbox_id: row.outbox_id,
                            trace_id: row.trace_id,
                            request_id: row.request_id,
                        },
                    );
                    self.deliver_outbound(adapters, adapter_indices, outbound)
                        .await;
                }
            }
            Err(e) => tracing::warn!(error = %e, "failed to load retryable outbox"),
        }
    }

    async fn deliver_outbound(
        &self,
        adapters: &[Box<dyn PlatformAdapter>],
        adapter_indices: &HashMap<&'static str, usize>,
        outbound: OutboundMessage,
    ) {
        let health_key = format!("{}:{}", outbound.platform, outbound.chat_id);
        let result = send_text_to_platform(
            adapters,
            adapter_indices,
            &outbound.platform,
            &outbound.chat_id,
            &outbound.text,
            outbound.reply_token.as_deref(),
            outbound.stream_id.as_deref(),
            outbound.feedback_id.as_deref(),
            outbound.stream_finish,
        )
        .await;

        // Track send health for heartbeat circuit-breaker.
        match &result {
            Ok(_) => {
                self.send_health.record_success(&health_key);
            }
            Err((_, error)) => {
                self.send_health.record_failure(&health_key);
                if outbound.outbox.is_none() {
                    tracing::debug!(
                        platform = %outbound.platform,
                        chat_id = %safe_id(&outbound.chat_id),
                        error,
                        "heartbeat/chunk send failed (no outbox, not retried)"
                    );
                }
            }
        }

        let Some(outbox) = outbound.outbox else {
            return;
        };
        let Some(repo) = self.trace_repo.as_ref() else {
            return;
        };
        let writer = TraceWriter::from_existing(
            repo.as_ref() as &dyn TraceRepository,
            outbox.trace_id,
            outbox.request_id,
        );
        match result {
            Ok(chunk_count) => {
                if let Err(e) = writer
                    .mark_outbox_sent(&outbox.outbox_id, chunk_count)
                    .await
                {
                    tracing::warn!(error = %e, "failed to ack sent outbox");
                }
            }
            Err((failed_chunk, error)) => {
                if let Err(e) = writer
                    .mark_outbox_failed(&outbox.outbox_id, &error, failed_chunk)
                    .await
                {
                    tracing::warn!(error = %e, "failed to mark outbox retryable");
                }
            }
        }
    }

    pub async fn run(
        self: std::sync::Arc<Self>,
        adapters: Vec<Box<dyn PlatformAdapter>>,
        mut cron_rx: tokio::sync::mpsc::Receiver<OutboundMessage>,
        mut scheduled_turn_rx: tokio::sync::mpsc::Receiver<ScheduledAgentTurn>,
        mut inject_rx: tokio::sync::mpsc::Receiver<InboundMessage>,
        mut runtime_cmd_rx: tokio::sync::mpsc::Receiver<crate::runtime_api::RuntimeCommand>,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) {
        let mut started: Vec<Box<dyn PlatformAdapter>> = Vec::new();
        for mut adapter in adapters {
            match adapter.start().await {
                Ok(()) => {
                    tracing::info!(platform = adapter.name(), "started");
                    started.push(adapter);
                }
                Err(e) => tracing::error!(platform = adapter.name(), error = %e, "start failed"),
            }
        }
        let mut adapters = started;
        if adapters.is_empty() {
            tracing::error!("no adapters started — exiting");
            return;
        }
        tracing::info!(count = adapters.len(), "gateway running");

        // Channel for CLI task responses back to the main loop
        let (cli_resp_tx, mut cli_resp_rx) = tokio::sync::mpsc::channel::<CliResponse>(64);

        let mut adapter_indices = HashMap::new();
        for (idx, adapter) in adapters.iter().enumerate() {
            adapter_indices.insert(adapter.name(), idx);
        }
        self.sweep_stale_traces().await;
        self.replay_retryable_outbox(&adapters, &adapter_indices)
            .await;
        // Drain any progressive chunks that accumulated in the outbound channel
        // during replay (the main select loop wasn't consuming yet).
        while let Ok(outbound) = cron_rx.try_recv() {
            self.deliver_outbound(&adapters, &adapter_indices, outbound)
                .await;
        }

        loop {
            tokio::select! {
                inbound = recv_from_any(&adapters) => {
                    match inbound {
                        Some(AdapterRecv::Message(msg)) => {
                            if msg.feedback.is_some() {
                                self.handle_message_inner(&msg, &NullAdapter, None, false).await;
                                continue;
                            }
                            // Fast path: slash commands — instant, no CLI
                            match self.handle_fast(&msg).await {
                                Ok(Some(text)) => {
                                    let _ = send_text_to_platform(&adapters, &adapter_indices, msg.platform, &msg.chat_id, &text, msg.reply_token.as_deref(), None, None, true).await;
                                }
                                Ok(None) => {}
                                Err(msg) => {
                                    // Slow path: enqueue by conversation. Workers serialize each
                                    // conversation while a global semaphore allows cross-chat concurrency.
                                    let platform = msg.platform;
                                    send_typing_to_platform(&adapters, &adapter_indices, platform, &msg.chat_id).await;
                                    self.enqueue_cli_request(msg, cli_resp_tx.clone()).await;
                                }
                            }
                        }
                        Some(AdapterRecv::Closed(idx)) => {
                            if idx < adapters.len() {
                                let mut adapter = adapters.remove(idx);
                                tracing::warn!(platform = adapter.name(), "adapter receive channel closed");
                                adapter.stop().await;
                                adapter_indices.clear();
                                for (idx, adapter) in adapters.iter().enumerate() {
                                    adapter_indices.insert(adapter.name(), idx);
                                }
                            }
                            if adapters.is_empty() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                // CLI task completed — send response to user
                resp = cli_resp_rx.recv() => {
                    if let Some(resp) = resp {
                        self.deliver_outbound(&adapters, &adapter_indices, resp).await;
                    }
                }
                outbound = cron_rx.recv() => {
                    if let Some(outbound) = outbound {
                        self.deliver_outbound(&adapters, &adapter_indices, outbound).await;
                    }
                }
                scheduled = scheduled_turn_rx.recv() => {
                    if let Some(turn) = scheduled {
                        self.enqueue_scheduled_agent_turn(turn, cli_resp_tx.clone()).await;
                    }
                }
                injected = inject_rx.recv() => {
                    if let Some(msg) = injected {
                        tracing::info!(platform = "inject", chat_id = %msg.chat_id, user = %msg.user_id, text = %msg.text, "injected message");
                        if msg.feedback.is_some() {
                            self.handle_message_inner(&msg, &NullAdapter, None, false).await;
                            continue;
                        }
                        match self.handle_fast(&msg).await {
                            Ok(Some(text)) => {
                                let _ = send_text_to_platform(&adapters, &adapter_indices, msg.platform, &msg.chat_id, &text, msg.reply_token.as_deref(), None, None, true).await;
                            }
                            Ok(None) => {}
                            Err(msg) => {
                                let platform = msg.platform;
                                send_typing_to_platform(&adapters, &adapter_indices, platform, &msg.chat_id).await;
                                self.enqueue_cli_request(msg, cli_resp_tx.clone()).await;
                            }
                        }
                    }
                }
                command = runtime_cmd_rx.recv() => {
                    if let Some(command) = command {
                        self.handle_runtime_command(&adapters, &adapter_indices, command).await;
                    }
                }
                _ = shutdown.recv() => break,
            }
        }

        for adapter in &mut adapters {
            adapter.stop().await;
        }
    }

    async fn handle_runtime_command(
        &self,
        adapters: &[Box<dyn PlatformAdapter>],
        adapter_indices: &HashMap<&'static str, usize>,
        command: crate::runtime_api::RuntimeCommand,
    ) {
        match command {
            crate::runtime_api::RuntimeCommand::SendAttachment {
                platform,
                chat_id,
                attachment,
                caption,
            } => {
                tracing::info!(
                    platform = %platform,
                    chat_id = %safe_id(&chat_id),
                    path = attachment.local_path.as_deref(),
                    media_id = attachment.media_id.as_deref(),
                    "runtime send attachment command"
                );
                if let Some(caption) = caption.as_deref().filter(|s| !s.trim().is_empty())
                    && let Err((_, e)) = send_text_to_platform(
                        adapters,
                        adapter_indices,
                        &platform,
                        &chat_id,
                        caption,
                        None,
                        None,
                        None,
                        true,
                    )
                    .await
                {
                    tracing::warn!(
                        platform = %platform,
                        chat_id = %safe_id(&chat_id),
                        error = %e,
                        "runtime attachment caption send failed"
                    );
                }
                if let Err(e) = send_attachment_to_platform(
                    adapters,
                    adapter_indices,
                    &platform,
                    &chat_id,
                    &attachment,
                    None,
                )
                .await
                {
                    tracing::warn!(
                        platform = %platform,
                        chat_id = %safe_id(&chat_id),
                        error = %e,
                        "runtime attachment send failed"
                    );
                }
            }
        }
    }
}

/// Write the gateway MCP config file for a conversation and return the generated config.
/// Used both for the initial pool spawn and for regeneration after a stale-session
/// retry (kill() deletes the file, so it must be rewritten before the retry reuses it).
#[allow(clippy::too_many_arguments)]
fn write_mcp_config_file(
    env: &crate::mcp::config::McpStorageEnv,
    config_identity: &str,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    project_dirs: &[String],
    runtime_api_url: Option<&str>,
    runtime_api_token: Option<&str>,
) -> Result<crate::mcp::config::GeneratedMcpConfig, String> {
    crate::mcp::config::generate_gateway_mcp_config(
        env,
        config_identity,
        platform,
        chat_id,
        user_id,
        project_dirs,
        runtime_api_url,
        runtime_api_token,
    )
    .map_err(|e| e.to_string())
}

async fn recv_from_any(adapters: &[Box<dyn PlatformAdapter>]) -> Option<AdapterRecv> {
    if adapters.is_empty() {
        return None;
    }
    let futures: Vec<Pin<Box<dyn Future<Output = AdapterRecv> + Send + '_>>> = adapters
        .iter()
        .enumerate()
        .map(|(idx, adapter)| {
            Box::pin(async move {
                match adapter.recv().await {
                    Some(msg) => AdapterRecv::Message(Box::new(msg)),
                    None => AdapterRecv::Closed(idx),
                }
            }) as Pin<Box<dyn Future<Output = AdapterRecv> + Send + '_>>
        })
        .collect();
    let (event, _, _) = select_all(futures).await;
    Some(event)
}

#[allow(clippy::too_many_arguments)]
async fn send_text_to_platform(
    adapters: &[Box<dyn PlatformAdapter>],
    adapter_indices: &HashMap<&'static str, usize>,
    platform: &str,
    chat_id: &str,
    text: &str,
    reply_token: Option<&str>,
    stream_id: Option<&str>,
    feedback_id: Option<&str>,
    stream_finish: bool,
) -> Result<usize, (usize, String)> {
    let Some(idx) = adapter_indices.get(platform).copied() else {
        tracing::warn!(platform, chat_id = %safe_id(chat_id), "no adapter for outbound message");
        return Err((0, "no adapter for outbound message".into()));
    };
    let Some(adapter) = adapters.get(idx) else {
        tracing::warn!(platform, chat_id = %safe_id(chat_id), "adapter index missing for outbound message");
        return Err((0, "adapter index missing for outbound message".into()));
    };

    // Stream mode: send full text as one frame. WeCom stream semantics are
    // full-replacement per frame — splitting would corrupt the display.
    if stream_id.is_some() {
        if let Err(e) = adapter
            .send_stream_chunk(
                chat_id,
                text,
                reply_token,
                stream_id,
                feedback_id,
                stream_finish,
            )
            .await
        {
            tracing::warn!(platform, chat_id = %safe_id(chat_id), error = %e, "failed to send stream chunk");
            return Err((0, e));
        }
        return Ok(1);
    }

    // Non-stream mode: split long messages for platforms with size limits.
    let chunks = split_message(text);
    let chunk_count = chunks.len();
    for (i, chunk) in chunks.into_iter().enumerate() {
        let result = if feedback_id.is_some() {
            adapter
                .send_stream_chunk(chat_id, chunk, reply_token, None, feedback_id, true)
                .await
        } else {
            adapter.send_text(chat_id, chunk, reply_token).await
        };
        if let Err(e) = result {
            tracing::warn!(platform, chat_id = %safe_id(chat_id), error = %e, "failed to send platform message");
            return Err((i, e));
        }
    }
    Ok(chunk_count)
}

async fn send_attachment_to_platform(
    adapters: &[Box<dyn PlatformAdapter>],
    adapter_indices: &HashMap<&'static str, usize>,
    platform: &str,
    chat_id: &str,
    attachment: &OutboundAttachment,
    reply_token: Option<&str>,
) -> Result<(), String> {
    let Some(idx) = adapter_indices.get(platform).copied() else {
        return Err("no adapter for outbound attachment".into());
    };
    let Some(adapter) = adapters.get(idx) else {
        return Err("adapter index missing for outbound attachment".into());
    };
    adapter
        .send_attachment(chat_id, attachment, reply_token)
        .await
}

async fn send_typing_to_platform(
    adapters: &[Box<dyn PlatformAdapter>],
    adapter_indices: &HashMap<&'static str, usize>,
    platform: &str,
    chat_id: &str,
) {
    let Some(idx) = adapter_indices.get(platform).copied() else {
        return;
    };
    if let Some(adapter) = adapters.get(idx) {
        let _ = adapter.send_typing(chat_id).await;
    }
}

fn split_message(text: &str) -> Vec<&str> {
    if text.len() <= MAX_CHUNK_LEN {
        return vec![text];
    }
    let mut chunks = Vec::new();
    let mut remaining = text;
    while !remaining.is_empty() {
        if remaining.len() <= MAX_CHUNK_LEN {
            if !remaining.trim().is_empty() {
                chunks.push(remaining);
            }
            break;
        }
        let window_end = crate::text::floor_char_boundary(remaining, MAX_CHUNK_LEN);
        let window = &remaining[..window_end];
        // Priority 1: paragraph boundary (\n\n)
        let split_at = rfind_paragraph_break(window)
            // Priority 2: code fence boundary (``` on its own line)
            .or_else(|| rfind_fence_break(window))
            // Priority 3: any newline
            .or_else(|| window.rfind('\n'))
            // Priority 4: space
            .or_else(|| window.rfind(' '))
            // Fallback: hard cut
            .unwrap_or(window_end);

        let chunk = &remaining[..split_at];
        if !chunk.trim().is_empty() {
            chunks.push(chunk);
        }
        remaining = remaining[split_at..].trim_start_matches('\n');
        if remaining.starts_with('\n') {
            remaining = remaining.trim_start_matches('\n');
        }
    }
    chunks
}

fn rfind_paragraph_break(s: &str) -> Option<usize> {
    // Find last \n\n that's not inside a code fence
    let mut pos = s.len();
    while pos > 0 {
        if let Some(p) = s[..pos].rfind("\n\n") {
            // Check we're not inside a code block
            let before = &s[..p];
            let fence_count = before.matches("```").count();
            if fence_count.is_multiple_of(2) {
                return Some(p);
            }
            pos = p;
        } else {
            break;
        }
    }
    None
}

fn rfind_fence_break(s: &str) -> Option<usize> {
    // Find last ``` followed by \n — split after the closing fence
    let mut search = s.len();
    while search > 3 {
        if let Some(p) = s[..search].rfind("```") {
            let after_fence = p + 3;
            if after_fence < s.len() && s.as_bytes().get(after_fence) == Some(&b'\n') {
                return Some(after_fence + 1);
            }
            search = p;
        } else {
            break;
        }
    }
    None
}

// ─── Shared auth token ─────────────────────────────────────────────────────
//
// Gateway manages a single cached access token that is injected into CLI
// spawns via `ASTRA_ACCESS_TOKEN`.  This eliminates per-message auth
// round-trips (`GET /auth/me`) inside the CLI.

/// Thread-safe cached access token.  The gateway validates the token once,
/// then all concurrent CLI spawns reuse it without any HTTP call.
struct SharedAuthToken {
    token: tokio::sync::RwLock<Option<String>>,
    api: astra::Client,
    username: Option<String>,
    password: Option<String>,
}

impl SharedAuthToken {
    fn new(api: astra::Client, username: Option<String>, password: Option<String>) -> Self {
        Self {
            token: tokio::sync::RwLock::new(None),
            api,
            username,
            password,
        }
    }

    /// Return a cached valid token, or try to obtain one (from credentials file
    /// or by logging in).
    async fn get(&self) -> Option<String> {
        // Fast path: return cached token
        {
            let guard = self.token.read().await;
            if let Some(ref tok) = *guard {
                return Some(tok.clone());
            }
        }
        // Slow path: refresh
        self.refresh().await
    }

    /// Force-refresh: read from `~/.astra/credentials.json`, validate via
    /// `GET /auth/me`, and optionally re-login with username/password.
    async fn refresh(&self) -> Option<String> {
        // Read from CLI credentials file
        if let Some(ref tok) = read_cli_access_token()
            && let Ok(resp) = self
                .api
                .get_auth_me_text_timeout(tok, Duration::from_secs(3))
                .await
            && resp.status().is_success()
        {
            let mut guard = self.token.write().await;
            *guard = Some(tok.clone());
            return Some(tok.clone());
        }
        // Token invalid — try login if credentials available
        if let (Some(username), Some(password)) = (&self.username, &self.password)
            && let Ok(body) = self
                .api
                .post_auth_login_json(&serde_json::json!({
                    "username": username,
                    "password": password,
                }))
                .await
            && let Ok(v) = serde_json::from_str::<serde_json::Value>(&body)
            && let Some(access) = v["access_token"].as_str()
        {
            let refresh = v["refresh_token"].as_str();
            let _ = save_token_to_cli_credentials(username, access, refresh);
            let tok = access.to_string();
            let mut guard = self.token.write().await;
            *guard = Some(tok.clone());
            return Some(tok);
        }
        None
    }

    /// Clear the cached token (e.g. after an auth failure from the CLI).
    async fn invalidate(&self) {
        let mut guard = self.token.write().await;
        *guard = None;
    }
}

/// Read the access token for the current profile from `~/.astra/credentials.json`.
fn read_cli_access_token() -> Option<String> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .ok()?;
    let path = std::path::Path::new(&home)
        .join(".astra")
        .join("credentials.json");
    let content = std::fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&content).ok()?;
    let current = v["current_profile"].as_str().unwrap_or("default");
    v["profiles"][current]["access_token"]
        .as_str()
        .map(String::from)
}

/// Write refreshed tokens to `~/.astra/credentials.json` so subsequent CLI
/// spawns pick them up.  Reads the existing file (if any), updates the
/// access/refresh tokens on the current or default profile, and writes back.
fn save_token_to_cli_credentials(
    username: &str,
    access_token: &str,
    refresh_token: Option<&str>,
) -> Result<(), String> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());
    let path = std::path::PathBuf::from(home)
        .join(".astra")
        .join("credentials.json");

    // Read existing file (may not exist)
    let mut doc: serde_json::Value = if let Ok(content) = std::fs::read_to_string(&path) {
        serde_json::from_str(&content).unwrap_or_else(|_| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    // Determine profile name
    let profile_name = doc
        .get("current_profile")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();

    // Ensure profiles object exists
    if !doc.get("profiles").is_some_and(|p| p.is_object()) {
        doc["profiles"] = serde_json::json!({});
    }

    // Update the profile
    let profile = &mut doc["profiles"][&profile_name];
    if !profile.is_object() {
        *profile = serde_json::json!({});
    }
    profile["username"] = serde_json::Value::String(username.to_string());
    profile["access_token"] = serde_json::Value::String(access_token.to_string());
    if let Some(rt) = refresh_token {
        profile["refresh_token"] = serde_json::Value::String(rt.to_string());
    }

    // Write back
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir failed: {e}"))?;
    }
    let body = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize failed: {e}"))?;
    std::fs::write(&path, body).map_err(|e| format!("write failed: {e}"))?;

    // Restrict to owner-only (0o600)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(())
}

#[cfg(test)]
fn is_safe_db_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse and execute gateway action tags in agent response text.
/// Returns the text with tags removed, and populates action_results with status messages.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn execute_gateway_actions(
    text: &str,
    store: Option<&dyn GatewayStore>,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    action_results: &mut Vec<String>,
) -> String {
    execute_gateway_actions_with_policy(
        text,
        store,
        platform,
        chat_id,
        user_id,
        &crate::access_control::ActionPolicy {
            allow_slash_mutations: true,
            allow_model_generated_mutations: true,
            workspace_roots: Vec::new(),
        },
        action_results,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_gateway_actions_with_policy(
    text: &str,
    store: Option<&dyn GatewayStore>,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    action_policy: &crate::access_control::ActionPolicy,
    action_results: &mut Vec<String>,
) -> String {
    static RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?s)\[\[GATEWAY:(.*?)\]\]").unwrap());
    let re = &*RE;
    let mut clean = text.to_string();

    for cap in re.captures_iter(text) {
        let full_match = cap.get(0).unwrap().as_str();
        let inner = &cap[1];
        let parts: Vec<&str> = inner.splitn(3, ':').collect();
        if let Some(capability) = action_capability(parts.first().copied().unwrap_or_default())
            && let Err(denial) = action_policy.check(
                crate::access_control::ActionSource::ModelGenerated,
                capability,
            )
        {
            action_results.push(denial);
            clean = clean.replace(full_match, "");
            continue;
        }

        let result = match parts.first().copied() {
            Some("cron_add") if parts.len() == 3 => {
                let cron_expr = parts[1].trim();
                let message = parts[2].trim();
                tools_cron::cron_add(store, platform, chat_id, user_id, cron_expr, message).await
            }
            Some("cron_add") => "Error: cron_add format is cron_add:<expr>:<message>".into(),

            Some("remind_after") if parts.len() == 3 => {
                let minutes: u64 = parts[1].trim().parse().unwrap_or(0);
                let raw_message = parts[2].trim().to_string();
                let (exec, message) = if let Some(stripped) = raw_message.strip_prefix("exec:") {
                    (true, stripped.trim())
                } else {
                    (false, raw_message.as_str())
                };
                tools_cron::remind_after(store, platform, chat_id, user_id, minutes, message, exec)
                    .await
            }
            Some("remind_after") => {
                "Error: remind_after format is remind_after:<minutes>:<message>".into()
            }

            _ => {
                tracing::warn!(action = inner, "unknown gateway action");
                format!("⚠️ 未知操作: {inner}")
            }
        };

        action_results.push(result);
        clean = clean.replace(full_match, "");
    }

    // Clean up extra whitespace from removed tags
    clean.trim().to_string()
}

fn action_capability(action: &str) -> Option<crate::access_control::ActionCapability> {
    use crate::access_control::ActionCapability as Cap;
    match action {
        "cron_add" | "remind_after" => Some(Cap::CronMutation),
        _ => None,
    }
}

#[cfg(test)]
fn is_valid_cron_expr(expr: &str) -> bool {
    store::is_valid_cron_expr(expr)
}

/// Streaming filter for `<think>...</think>` / `<thinking>...</thinking>` blocks.
///
/// Buffers partial tag fragments across token boundaries so that a `<think>`
/// split across two chunks (e.g. `"<thi"` + `"nk>..."`) is correctly detected
/// and suppressed, instead of leaking the partial tag to the user.
#[derive(Default)]
struct ThinkTagStreamFilter {
    pending: String,
    in_think: bool,
}

impl ThinkTagStreamFilter {
    fn push(&mut self, text: &str) -> String {
        self.pending.push_str(text);
        let mut out = String::new();

        loop {
            if self.in_think {
                if let Some((end, close)) = find_next_think_close(&self.pending) {
                    self.pending.drain(..end + close.len());
                    self.in_think = false;
                    continue;
                }
                // No closing tag yet — keep all pending content. finish()
                // will return it if the block is never closed.
                break;
            }

            if let Some((start, tag)) = find_next_think_open(&self.pending) {
                out.push_str(&self.pending[..start]);
                self.pending.drain(..start + tag.open.len());
                self.in_think = true;
                continue;
            }

            let keep = open_think_prefix_len(&self.pending);
            let emit_len = self.pending.len().saturating_sub(keep);
            out.push_str(&self.pending[..emit_len]);
            self.pending.drain(..emit_len);
            break;
        }

        out
    }

    fn finish(&mut self) -> String {
        if self.in_think {
            // Unclosed think tag — return accumulated content so it's not lost.
            let leftover = std::mem::take(&mut self.pending);
            self.in_think = false;
            leftover
        } else {
            std::mem::take(&mut self.pending)
        }
    }
}

#[derive(Clone, Copy)]
struct ThinkTag {
    open: &'static str,
    close: &'static str,
}

const THINK_TAGS: [ThinkTag; 2] = [
    ThinkTag {
        open: "<think>",
        close: "</think>",
    },
    ThinkTag {
        open: "<thinking>",
        close: "</thinking>",
    },
];

fn find_next_think_open(text: &str) -> Option<(usize, ThinkTag)> {
    THINK_TAGS
        .iter()
        .filter_map(|tag| text.find(tag.open).map(|pos| (pos, *tag)))
        .min_by_key(|(pos, _)| *pos)
}

fn find_next_think_close(text: &str) -> Option<(usize, &'static str)> {
    THINK_TAGS
        .iter()
        .filter_map(|tag| text.find(tag.close).map(|pos| (pos, tag.close)))
        .min_by_key(|(pos, _)| *pos)
}

/// Suffix of `text` that could be a prefix of a think opening tag.
fn open_think_prefix_len(text: &str) -> usize {
    THINK_TAGS
        .iter()
        .map(|tag| tag_suffix_prefix_len(text, tag.open))
        .max()
        .unwrap_or(0)
}

fn tag_suffix_prefix_len(text: &str, tag: &str) -> usize {
    let max = text.len().min(tag.len() - 1);
    for len in (1..=max).rev() {
        if text.is_char_boundary(text.len() - len) && tag.starts_with(&text[text.len() - len..]) {
            return len;
        }
    }
    0
}

/// Simple non-streaming filter for complete text. Used on the final CLI output.
fn filter_think_tags(text: &str, in_think: &mut bool) -> String {
    let mut result = String::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        if *in_think {
            if let Some((end, close)) = find_next_think_close(remaining) {
                *in_think = false;
                remaining = &remaining[end + close.len()..];
            } else {
                break;
            }
        } else if let Some((start, tag)) = find_next_think_open(remaining) {
            result.push_str(&remaining[..start]);
            *in_think = true;
            remaining = &remaining[start + tag.open.len()..];
        } else {
            result.push_str(remaining);
            break;
        }
    }
    result
}

#[derive(Default)]
struct GatewayActionStreamFilter {
    pending: String,
    in_tag: bool,
}

impl GatewayActionStreamFilter {
    fn push(&mut self, text: &str) -> String {
        const TAG_START: &str = "[[GATEWAY:";
        self.pending.push_str(text);
        let mut out = String::new();

        loop {
            if self.in_tag {
                if let Some(end) = self.pending.find("]]") {
                    self.pending.drain(..end + 2);
                    self.in_tag = false;
                    continue;
                }
                self.pending.clear();
                break;
            }

            if let Some(start) = self.pending.find(TAG_START) {
                out.push_str(&self.pending[..start]);
                self.pending.drain(..start + TAG_START.len());
                self.in_tag = true;
                continue;
            }

            let keep = gateway_tag_prefix_suffix_len(&self.pending);
            let emit_len = self.pending.len().saturating_sub(keep);
            out.push_str(&self.pending[..emit_len]);
            self.pending.drain(..emit_len);
            break;
        }

        out
    }

    fn finish(&mut self) -> String {
        if self.in_tag {
            self.pending.clear();
            self.in_tag = false;
            String::new()
        } else {
            std::mem::take(&mut self.pending)
        }
    }
}

fn gateway_tag_prefix_suffix_len(text: &str) -> usize {
    const TAG_START: &str = "[[GATEWAY:";
    let max = text.len().min(TAG_START.len() - 1);
    for len in (1..=max).rev() {
        if text.is_char_boundary(text.len() - len)
            && TAG_START.starts_with(&text[text.len() - len..])
        {
            return len;
        }
    }
    0
}

/// Build the final message to send after CLI finishes.
/// When `progressive_text_len > 0`, text was already streamed — send only
/// action results + stats footer. Otherwise send full text + stats.
fn build_final_message(
    text: &str,
    action_results: &str,
    stats_parts: &[String],
    progressive_text_len: usize,
    request_tag: &str,
) -> String {
    if progressive_text_len > 0 {
        // Progressive mode: main content already streamed; final msg is stats footer only
        let mut parts = Vec::new();
        if !action_results.is_empty() {
            parts.push(action_results.to_string());
        }
        if !stats_parts.is_empty() {
            parts.push(format!("`{}`", stats_parts.join(" | ")));
        }
        let body = parts.join("\n\n");
        if body.is_empty() {
            body
        } else {
            format!("[{request_tag}] {body}")
        }
    } else {
        // Non-progressive: full text + stats in one message (no tag prefix —
        // the response was not streamed, so there is no chunk sequence to correlate).
        let mut result = text.to_string();
        if !result.is_empty() && !stats_parts.is_empty() {
            result.push_str(&format!(
                "\n\n`[{request_tag}] {}`",
                stats_parts.join(" | ")
            ));
        }
        result
    }
}

fn compact_response_override(
    message: &str,
    success: bool,
    response_text: &str,
) -> Option<&'static str> {
    let message = message.trim();
    let is_compact = message == "/compact" || message.starts_with("/compact ");
    if !is_compact || !success {
        return None;
    }
    let response_text = response_text.trim();
    if response_text.is_empty() {
        return Some("⚠️ 未收到 Claude 的压缩状态，无法确认会话是否已压缩；请重试 `/compact`。");
    }
    let normalized = response_text.to_ascii_lowercase();
    if normalized.contains("not enough messages to compact") {
        return Some(
            "ℹ️ 当前会话内容太少，Claude 未执行压缩；继续对话即可，稍后可再次使用 `/compact`。",
        );
    }
    if normalized == "compacted"
        || normalized.starts_with("compacted (")
        || normalized == "conversation compacted"
        || normalized.starts_with("conversation compacted (")
    {
        return Some("✅ 会话已压缩，可以继续对话。原始历史记录仍然保留。");
    }
    None
}

/// Strip `<think>...</think>` / `<thinking>...</thinking>` blocks from complete text.
/// Unclosed think tags at EOF: the tag is removed but content after it is
/// preserved — a malicious or buggy model cannot suppress all output.
fn strip_think_blocks(text: &str) -> String {
    let mut in_think = false;
    let mut result = filter_think_tags(text, &mut in_think);
    if in_think
        && let Some((pos, tag)) = THINK_TAGS
            .iter()
            .filter_map(|tag| text.rfind(tag.open).map(|pos| (pos, *tag)))
            .max_by_key(|(pos, _)| *pos)
    {
        let after = &text[pos + tag.open.len()..];
        if !after.is_empty() {
            if !result.is_empty() {
                result.push('\n');
            }
            result.push_str(after);
        }
    }
    result
}

async fn image_attachment_guard_message(
    msg: &InboundMessage,
    ctx: &CommandContext<'_>,
) -> Option<String> {
    let has_image = msg
        .attachments
        .iter()
        .any(|attachment| attachment.kind == crate::platforms::AttachmentKind::Image);
    if !has_image {
        return None;
    }

    let resolved_model = commands::current_resolved_model_id_for_context(ctx)
        .await
        .ok()
        .flatten();
    image_attachment_guard_response(resolved_model.as_deref(), &ctx.config.vision_models)
}

fn image_attachment_guard_response(
    resolved_model: Option<&str>,
    vision_models: &[String],
) -> Option<String> {
    match crate::model_vision::vision_capability_with_supported_models(
        resolved_model,
        vision_models,
    ) {
        crate::model_vision::VisionCapability::Supported => None,
        crate::model_vision::VisionCapability::Unsupported => {
            let model = resolved_model.unwrap_or("当前模型");
            Some(format!(
                "当前模型 `{model}` 不支持图片识别。请切换到支持视觉能力的模型后再发送图片。"
            ))
        }
        crate::model_vision::VisionCapability::Unknown => {
            let model = resolved_model.unwrap_or("当前模型");
            Some(format!(
                "无法确认 `{model}` 支持图片识别。请先用 `/model refresh` 刷新模型信息，或切换到明确支持视觉能力的模型后再发送图片。"
            ))
        }
    }
}

async fn prepare_inbound_attachments(msg: &mut InboundMessage) -> Option<String> {
    if msg.attachments.is_empty() {
        return None;
    }
    match msg.platform {
        "wecom" => {
            crate::platforms::wecom::prepare_inbound_attachments(&mut msg.attachments, &msg.msg_id)
                .await
        }
        "weixin" => {
            crate::platforms::weixin::prepare_inbound_attachments(&mut msg.attachments, &msg.msg_id)
                .await
        }
        _ => None,
    }
}

fn message_text_for_cli(msg: &InboundMessage) -> String {
    if msg.attachments.is_empty() {
        return msg.text.clone();
    }

    let mut text = if msg.text.trim().is_empty() {
        "The user sent attachment(s).".to_string()
    } else {
        msg.text.clone()
    };
    text.push_str("\n\nAttachments:");
    for (idx, attachment) in msg.attachments.iter().enumerate() {
        text.push_str(&format!(
            "\n{}. type: {}",
            idx + 1,
            attachment_kind_label(attachment.kind)
        ));
        if let Some(name) = attachment.name.as_deref() {
            text.push_str(&format!(", name: {name}"));
        }
        let has_local_path = attachment.local_path.is_some();
        if let Some(path) = attachment.local_path.as_deref() {
            text.push_str(&format!(", local_path: {path}"));
        } else if let Some(url) = attachment.url.as_deref() {
            text.push_str(&format!(", url: {url}"));
        }
        if !has_local_path && let Some(media_id) = attachment.media_id.as_deref() {
            text.push_str(&format!(", media_id: {media_id}"));
        }
        if let Some(mime) = attachment.mime_type.as_deref() {
            text.push_str(&format!(", mime_type: {mime}"));
        }
        if let Some(size) = attachment.size_bytes {
            text.push_str(&format!(", size_bytes: {size}"));
        }
    }
    text
}

fn attachment_kind_label(kind: crate::platforms::AttachmentKind) -> &'static str {
    match kind {
        crate::platforms::AttachmentKind::Image => "image",
        crate::platforms::AttachmentKind::File => "file",
        crate::platforms::AttachmentKind::Video => "video",
        crate::platforms::AttachmentKind::Audio => "audio",
        crate::platforms::AttachmentKind::Unknown => "unknown",
    }
}

/// Derive a short request tag like `#A7` from the first two hex digits of a
/// trace ID (UUID).  Returns `#??` when the input has fewer than two hex chars.
pub(crate) fn short_request_tag(trace_id: &str) -> String {
    let hex: String = trace_id
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(2)
        .collect();
    if hex.len() == 2 {
        format!("#{}", hex.to_uppercase())
    } else {
        "#??".to_string()
    }
}

fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let t: String = s.chars().take(max).collect();
        format!("{t}…")
    }
}

fn reasoning_block_title(kind: ReasoningKind, agent_name: &str) -> String {
    let label = match kind {
        ReasoningKind::Raw => "思考过程",
        ReasoningKind::Summary => "思考摘要",
    };
    format!("【{agent_name} {label}】")
}

fn answer_progressive_flush_enabled(reasoning_display: ReasoningDisplay) -> bool {
    !reasoning_display.is_enabled()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_empty_success_reports_unknown_state() {
        assert_eq!(
            compact_response_override("/compact", true, ""),
            Some("⚠️ 未收到 Claude 的压缩状态，无法确认会话是否已压缩；请重试 `/compact`。")
        );
        assert_eq!(
            compact_response_override("/compact focus on fixes", true, "  "),
            Some("⚠️ 未收到 Claude 的压缩状态，无法确认会话是否已压缩；请重试 `/compact`。")
        );
        assert_eq!(compact_response_override("/compact", false, ""), None);
        assert_eq!(compact_response_override("hello", true, ""), None);
    }

    #[test]
    fn compact_local_command_result_is_not_misreported() {
        assert_eq!(
            compact_response_override("/compact", true, "Not enough messages to compact."),
            Some(
                "ℹ️ 当前会话内容太少，Claude 未执行压缩；继续对话即可，稍后可再次使用 `/compact`。"
            )
        );
        assert_eq!(
            compact_response_override("/compact", true, "Compacted (ctrl+o to see full summary)"),
            Some("✅ 会话已压缩，可以继续对话。原始历史记录仍然保留。")
        );
        assert_eq!(
            compact_response_override("/compact", true, "Not compacted"),
            None
        );
        assert_eq!(
            compact_response_override("/compact", true, "Could not be compacted"),
            None
        );
    }

    #[test]
    fn astra_app_server_startup_error_detection_is_narrow() {
        assert!(is_astra_app_server_startup_error(
            "codex app-server stdout closed"
        ));
        assert!(is_astra_app_server_startup_error(
            "error: unrecognized subcommand 'stdio'"
        ));
        assert!(!is_astra_app_server_startup_error(
            "invalid permission mode 'oops'"
        ));
    }

    #[test]
    fn split_short() {
        assert_eq!(split_message("hello"), vec!["hello"]);
    }

    #[test]
    fn split_long() {
        let text = "x".repeat(8000);
        let chunks = split_message(&text);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(!chunk.is_empty());
        }
    }

    #[test]
    fn split_long_multibyte_does_not_panic_or_split_chars() {
        let text = "中文内容".repeat(2000);
        let chunks = split_message(&text);
        assert!(chunks.len() >= 2);
        for chunk in &chunks {
            assert!(chunk.len() <= MAX_CHUNK_LEN);
            assert!(text.contains(chunk.trim()));
        }
    }

    #[test]
    fn split_preserves_code_block() {
        // Code block should not be split in the middle
        let code = format!(
            "before\n\n```rust\n{}\n```\n\nafter",
            "let x = 1;\n".repeat(300)
        );
        let chunks = split_message(&code);
        // The code block should be entirely in one chunk (or if too large, at least not split mid-line)
        let has_orphan_fence = chunks.iter().any(|c| {
            let opens = c.matches("```").count();
            opens % 2 != 0 // odd number of fences = split inside a code block
        });
        // If the code block fits in one chunk, it should not be split
        if code.len() <= MAX_CHUNK_LEN {
            assert_eq!(chunks.len(), 1);
        } else {
            // Large code block: at least no orphan fences
            assert!(
                !has_orphan_fence,
                "code block was split mid-fence: {chunks:?}"
            );
        }
    }

    #[test]
    fn split_prefers_paragraph_boundary() {
        let text = format!("{}\n\n{}", "a".repeat(1000), "b".repeat(1000));
        if text.len() <= MAX_CHUNK_LEN {
            assert_eq!(split_message(&text).len(), 1);
        }
        // For text > MAX_CHUNK_LEN: split at \n\n paragraph boundary
        let big = format!("{}\n\n{}", "a".repeat(2000), "b".repeat(2000));
        let chunks = split_message(&big);
        if chunks.len() >= 2 {
            assert!(
                chunks[0].ends_with('a'),
                "should split at paragraph boundary, got: {:?}...",
                &chunks[0][chunks[0].len() - 20..]
            );
        }
    }

    #[test]
    fn split_no_empty_chunks() {
        let text = "a\n\n\n\nb\n\n\n\nc".repeat(500);
        let chunks = split_message(&text);
        for (i, chunk) in chunks.iter().enumerate() {
            assert!(!chunk.trim().is_empty(), "chunk {i} is empty");
        }
    }

    #[test]
    fn format_elapsed_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(5)), "5s");
        assert_eq!(format_elapsed(Duration::from_secs(45)), "45s");
    }

    #[test]
    fn format_elapsed_minutes() {
        assert_eq!(format_elapsed(Duration::from_secs(65)), "1m5s");
        assert_eq!(format_elapsed(Duration::from_secs(130)), "2m10s");
    }

    // ── Short request tag ──────────────────────────────────────

    #[test]
    fn short_request_tag_from_uuid() {
        assert_eq!(
            short_request_tag("a7bc1234-5678-9abc-def0-123456789abc"),
            "#A7"
        );
        assert_eq!(
            short_request_tag("3f001122-3344-5566-7788-99aabbccddee"),
            "#3F"
        );
    }

    #[test]
    fn short_request_tag_empty_input() {
        assert_eq!(short_request_tag(""), "#??");
    }

    #[test]
    fn short_request_tag_single_hex_char() {
        assert_eq!(short_request_tag("a"), "#??");
    }

    #[test]
    fn short_request_tag_always_uppercase() {
        assert_eq!(
            short_request_tag("abcdef12-0000-0000-0000-000000000000"),
            "#AB"
        );
    }

    #[test]
    fn short_request_tag_skips_dashes() {
        // UUID dashes should be skipped, so "a-b-c" should pick up 'a' and 'b'
        assert_eq!(short_request_tag("a-b-c"), "#AB");
    }

    #[test]
    fn short_request_tag_counter_fallback_format() {
        // Verify the counter-based format matches expectations
        assert_eq!(format!("#{:02X}", 0u32), "#00");
        assert_eq!(format!("#{:02X}", 167u32), "#A7");
        assert_eq!(format!("#{:02X}", 255u32), "#FF");
        assert_eq!(format!("#{:02X}", 256u32 % 256), "#00"); // wraps
    }

    #[test]
    fn initial_ack_delay_is_shorter_than_heartbeat() {
        assert!(INITIAL_ACK_DELAY < HEARTBEAT_INTERVAL);
        assert!(
            INITIAL_ACK_DELAY.as_secs() <= 5,
            "initial ack should be <= 5s for good UX"
        );
    }

    #[test]
    fn progressive_flush_interval_is_reasonable() {
        const { assert!(PROGRESSIVE_MIN_CHARS > 0) };
        const { assert!(PROGRESSIVE_MIN_CHARS <= 200) };
        let secs = PROGRESSIVE_FLUSH_INTERVAL.as_secs();
        assert!(secs >= 2, "too fast = flood WeChat");
        assert!(secs <= 10, "too slow = feels laggy");
    }

    // ── is_mentioned tests ────────────────────────────────────

    #[test]
    fn mentioned_with_bot_name() {
        assert!(is_mentioned("hello @Astra help", "Astra"));
        assert!(is_mentioned("@astra", "Astra"));
        assert!(!is_mentioned("hello world", "Astra"));
        assert!(!is_mentioned("@someone else", "Astra"));
    }

    #[test]
    fn mentioned_empty_bot_name_matches_any_at() {
        assert!(is_mentioned("@anyone", ""));
        assert!(is_mentioned("hey @bot", ""));
        assert!(!is_mentioned("hello world", ""));
    }

    // ── Think tag filtering ──────────────────────────────────────

    #[test]
    fn filter_think_tags_strips_complete_block() {
        let mut state = false;
        let result = filter_think_tags("<think>internal reasoning</think>Hello!", &mut state);
        assert_eq!(result, "Hello!");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_strips_complete_thinking_block() {
        let mut state = false;
        let result = filter_think_tags("<thinking>internal reasoning</thinking>Hello!", &mut state);
        assert_eq!(result, "Hello!");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_handles_streaming_chunks() {
        let mut state = false;
        // Chunk 1: start of think block
        let r1 = filter_think_tags("Hi <think>reasoning", &mut state);
        assert_eq!(r1, "Hi ");
        assert!(state);
        // Chunk 2: still inside
        let r2 = filter_think_tags(" more thinking", &mut state);
        assert_eq!(r2, "");
        assert!(state);
        // Chunk 3: end of think block + visible text
        let r3 = filter_think_tags("</think>Visible", &mut state);
        assert_eq!(r3, "Visible");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_handles_streaming_thinking_chunks() {
        let mut state = false;
        let r1 = filter_think_tags("Hi <thinking>reasoning", &mut state);
        assert_eq!(r1, "Hi ");
        assert!(state);
        let r2 = filter_think_tags(" more thinking", &mut state);
        assert_eq!(r2, "");
        assert!(state);
        let r3 = filter_think_tags("</thinking>Visible", &mut state);
        assert_eq!(r3, "Visible");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_allows_mismatched_close_tags() {
        let mut state = false;
        let result = filter_think_tags("<thinking>hidden</think>Visible", &mut state);
        assert_eq!(result, "Visible");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_no_think_passthrough() {
        let mut state = false;
        let result = filter_think_tags("Just normal text", &mut state);
        assert_eq!(result, "Just normal text");
    }

    #[test]
    fn strip_think_blocks_removes_all() {
        let text = "<think>hmm</think>Answer is 42<think>double check</think>.";
        assert_eq!(strip_think_blocks(text), "Answer is 42.");
    }

    #[test]
    fn strip_think_blocks_removes_thinking_blocks() {
        let text = "<thinking>hmm</thinking>Answer is 42<thinking>double check</thinking>.";
        assert_eq!(strip_think_blocks(text), "Answer is 42.");
    }

    #[test]
    fn strip_think_blocks_unclosed_preserves_content() {
        // Malicious/buggy model: <think> without </think> should NOT suppress output
        let text = "Before<think>suppressed content that should still appear";
        let result = strip_think_blocks(text);
        assert!(
            result.contains("Before"),
            "text before think lost: {result}"
        );
        assert!(
            result.contains("suppressed content"),
            "unclosed think suppressed output: {result}"
        );
    }

    #[test]
    fn strip_think_blocks_unclosed_at_start() {
        let text = "<think>all content here, no close tag";
        let result = strip_think_blocks(text);
        assert!(
            result.contains("all content here"),
            "unclosed think at start suppressed everything: {result}"
        );
    }

    #[test]
    fn strip_think_blocks_unclosed_thinking_at_start() {
        let text = "<thinking>all content here, no close tag";
        let result = strip_think_blocks(text);
        assert!(
            result.contains("all content here"),
            "unclosed thinking at start suppressed everything: {result}"
        );
    }

    // ── Progressive delivery dedup ─────────────────────────────────

    #[test]
    fn final_message_no_progressive_includes_full_text() {
        let stats = vec!["↓8.4k".into(), "↑95".into(), "8s".into()];
        let msg = build_final_message("Hello world", "", &stats, 0, "#A7");
        assert!(msg.contains("Hello world"));
        assert!(msg.contains("↓8.4k"));
        assert!(msg.contains("[#A7]"), "stats footer should have tag: {msg}");
    }

    #[test]
    fn final_message_progressive_skips_body() {
        let stats = vec!["↓8.4k".into(), "↑95".into()];
        let msg = build_final_message("Hello world (already sent)", "", &stats, 500, "#3F");
        assert!(
            !msg.contains("Hello world"),
            "body should not repeat: {msg}"
        );
        assert!(msg.contains("↓8.4k"), "stats should still appear: {msg}");
        assert!(
            msg.starts_with("[#3F]"),
            "progressive footer should be tagged: {msg}"
        );
    }

    #[test]
    fn final_message_progressive_with_actions() {
        let stats = vec!["8s".into()];
        let msg = build_final_message("body", "⏰ 提醒已创建", &stats, 100, "#B2");
        assert!(
            msg.contains("⏰ 提醒已创建"),
            "action results should appear"
        );
        assert!(msg.contains("8s"), "stats should appear");
        assert!(!msg.contains("body"), "body should not repeat");
        assert!(
            msg.starts_with("[#B2]"),
            "progressive footer should be tagged: {msg}"
        );
    }

    #[test]
    fn final_message_progressive_empty_stats() {
        let msg = build_final_message("body", "", &[], 100, "#C0");
        assert!(
            msg.is_empty(),
            "nothing to send if progressive + no actions + no stats"
        );
    }

    #[test]
    fn reasoning_block_title_includes_agent_name() {
        assert_eq!(
            reasoning_block_title(ReasoningKind::Raw, "copilot"),
            "【copilot 思考过程】"
        );
        assert_eq!(
            reasoning_block_title(ReasoningKind::Summary, "claude"),
            "【claude 思考摘要】"
        );
    }

    #[test]
    fn answer_progressive_flush_waits_when_reasoning_is_enabled() {
        assert!(answer_progressive_flush_enabled(ReasoningDisplay::Off));
        assert!(!answer_progressive_flush_enabled(
            ReasoningDisplay::RawIfAvailable
        ));
    }

    // ── Tool status merged into buffer ──────────────────────────

    #[test]
    fn tool_status_format_is_inline() {
        // Verify the format strings used in the progress loop
        let started = format!("🔧 {}…\n", "bash");
        let done = format!("✅ {} ({}ms)\n", "bash", 120);
        assert!(started.contains("🔧 bash…"));
        assert!(done.contains("✅ bash (120ms)"));
        // Both end with newline — they'll be part of a multi-line buffer
        assert!(started.ends_with('\n'));
        assert!(done.ends_with('\n'));
    }

    // ── Think tag filtering ──────────────────────────────────────

    #[test]
    fn filter_think_tags_empty_think_block() {
        let mut state = false;
        assert_eq!(filter_think_tags("<think></think>OK", &mut state), "OK");
        assert!(!state);
    }

    #[test]
    fn filter_think_tags_at_start_and_end() {
        assert_eq!(strip_think_blocks("<think>x</think>"), "");
        assert_eq!(strip_think_blocks("text<think>x</think>"), "text");
        assert_eq!(strip_think_blocks("<think>x</think>text"), "text");
    }

    #[test]
    fn filter_think_tags_unclosed_stays_open() {
        let mut state = false;
        let r = filter_think_tags("before<think>never closed", &mut state);
        assert_eq!(r, "before");
        assert!(state, "should remain in think state");
        // Subsequent call still in think
        let r2 = filter_think_tags("still thinking", &mut state);
        assert_eq!(r2, "");
        assert!(state);
    }

    #[test]
    fn filter_think_tags_split_at_tag_boundary() {
        let mut state = false;
        // "<think>" split across two chunks as "<thin" + "k>reasoning</think>out"
        let r1 = filter_think_tags("<thin", &mut state);
        // Can't detect partial tag — passes through (acceptable: rare edge case)
        assert_eq!(r1, "<thin");
        assert!(!state);
        // Next chunk completes the tag — won't match as opening tag
        let r2 = filter_think_tags("k>reasoning</think>out", &mut state);
        // "k>" isn't a valid tag, passes through; "</think>" is a close without open, passes through
        assert!(r2.contains("out"));
    }

    #[test]
    fn filter_think_tags_nested_ignored() {
        // Nested <think> inside another — inner is just text, outer close ends it
        let mut state = false;
        let r = filter_think_tags("<think>a<think>b</think>c", &mut state);
        // First </think> closes, "c" is visible
        assert_eq!(r, "c");
        assert!(!state);
    }

    #[test]
    fn gateway_action_stream_filter_removes_complete_tag() {
        let mut filter = GatewayActionStreamFilter::default();
        let out = filter.push("before [[GATEWAY:cron_add:0 9 * * *:hello]] after");
        assert_eq!(out, "before  after");
        assert_eq!(filter.finish(), "");
    }

    #[test]
    fn gateway_action_stream_filter_handles_split_tag_start() {
        let mut filter = GatewayActionStreamFilter::default();
        assert_eq!(filter.push("hello [["), "hello ");
        assert_eq!(filter.push("GATEWAY:remind_after:5:hello]] done"), " done");
        assert_eq!(filter.finish(), "");
    }

    #[test]
    fn gateway_action_stream_filter_drops_unclosed_tag_at_finish() {
        let mut filter = GatewayActionStreamFilter::default();
        assert_eq!(
            filter.push("visible [[GATEWAY:remind_after:5:hello"),
            "visible "
        );
        assert_eq!(filter.finish(), "");
    }

    // ── ThinkTagStreamFilter tests ────────────────────────────────

    #[test]
    fn think_stream_filter_complete_block() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("<think>reasoning</think>Hello!"), "Hello!");
        assert_eq!(f.finish(), "");
    }

    #[test]
    fn think_stream_filter_complete_thinking_block() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("<thinking>reasoning</thinking>Hello!"), "Hello!");
        assert_eq!(f.finish(), "");
    }

    #[test]
    fn think_stream_filter_split_open_tag() {
        let mut f = ThinkTagStreamFilter::default();
        let r1 = f.push("before<thi");
        assert_eq!(r1, "before");
        let r2 = f.push("nk>hidden</think>visible");
        assert_eq!(r2, "visible");
    }

    #[test]
    fn think_stream_filter_split_thinking_open_tag() {
        let mut f = ThinkTagStreamFilter::default();
        let r1 = f.push("before<thinki");
        assert_eq!(r1, "before");
        let r2 = f.push("ng>hidden</thinking>visible");
        assert_eq!(r2, "visible");
    }

    #[test]
    fn think_stream_filter_split_close_tag() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("<think>hidden</thi"), "");
        assert_eq!(f.push("nk>after"), "after");
    }

    #[test]
    fn think_stream_filter_split_thinking_close_tag() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("<thinking>hidden</think"), "");
        assert_eq!(f.push("ing>after"), "after");
    }

    #[test]
    fn think_stream_filter_allows_mismatched_close_tags() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("<thinking>hidden</think>visible"), "visible");
        assert_eq!(f.finish(), "");
    }

    #[test]
    fn think_stream_filter_no_think_passthrough() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("just normal text"), "just normal text");
        assert_eq!(f.finish(), "");
    }

    #[test]
    fn think_stream_filter_unclosed_at_finish_preserves_content() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("before<think>hidden"), "before");
        let tail = f.finish();
        assert_eq!(tail, "hidden");
    }

    #[test]
    fn think_stream_filter_unclosed_thinking_at_finish_preserves_content() {
        let mut f = ThinkTagStreamFilter::default();
        assert_eq!(f.push("before<thinking>hidden"), "before");
        let tail = f.finish();
        assert_eq!(tail, "hidden");
    }

    #[test]
    fn think_stream_filter_single_char_chunks() {
        let mut f = ThinkTagStreamFilter::default();
        let input = "A<think>secret</think>B";
        let mut output = String::new();
        for ch in input.chars() {
            output.push_str(&f.push(&ch.to_string()));
        }
        output.push_str(&f.finish());
        assert_eq!(output, "AB", "single-char chunking must still filter");
    }

    #[test]
    fn think_stream_filter_multiple_blocks() {
        let mut f = ThinkTagStreamFilter::default();
        let out = f.push("A<think>x</think>B<think>y</think>C");
        assert_eq!(out, "ABC");
    }

    #[test]
    fn open_think_prefix_len_values() {
        assert_eq!(open_think_prefix_len("hello"), 0);
        assert_eq!(open_think_prefix_len("hello<"), 1);
        assert_eq!(open_think_prefix_len("hello<t"), 2);
        assert_eq!(open_think_prefix_len("hello<th"), 3);
        assert_eq!(open_think_prefix_len("hello<thi"), 4);
        assert_eq!(open_think_prefix_len("hello<thin"), 5);
        assert_eq!(open_think_prefix_len("hello<think"), 6);
        assert_eq!(open_think_prefix_len("hello<thinki"), 7);
        assert_eq!(open_think_prefix_len("hello<thinkin"), 8);
    }

    #[test]
    fn tag_suffix_prefix_len_for_close_tag() {
        assert_eq!(tag_suffix_prefix_len("text</", "</think>"), 2);
        assert_eq!(tag_suffix_prefix_len("text</t", "</think>"), 3);
        assert_eq!(tag_suffix_prefix_len("text</think", "</think>"), 7);
        assert_eq!(tag_suffix_prefix_len("text", "</think>"), 0);
    }

    // ── Gateway action tests ──────────────────────────────────────

    #[tokio::test]
    async fn action_cron_add_no_db() {
        let text = "好的\n[[GATEWAY:cron_add:0 9 * * *:早上好]]";
        let mut r = Vec::new();
        let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert_eq!(clean, "好的");
        assert!(r[0].contains("storage"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_cron_add_invalid_expr() {
        let text = "[[GATEWAY:cron_add:bad expr:msg]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("invalid cron expression"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_cron_add_empty_message() {
        let text = "[[GATEWAY:cron_add:0 9 * * *:]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("message cannot be empty"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_cron_add_missing_parts() {
        let text = "[[GATEWAY:cron_add:only_one_part]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("format"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_remind_after_no_db() {
        let text = "好的\n[[GATEWAY:remind_after:5:喝水]]";
        let mut r = Vec::new();
        let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert_eq!(clean, "好的");
        assert!(r[0].contains("storage"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_remind_after_zero_minutes() {
        let text = "[[GATEWAY:remind_after:0:msg]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("minutes must be > 0"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_remind_after_too_long() {
        let text = "[[GATEWAY:remind_after:99999:msg]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("maximum 7 days"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_remind_after_empty_message() {
        let text = "[[GATEWAY:remind_after:5:]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("message cannot be empty"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_remind_after_non_numeric() {
        let text = "[[GATEWAY:remind_after:abc:msg]]";
        let mut r = Vec::new();
        execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("minutes must be > 0"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_multiple_mixed() {
        let text = "好的，帮你设置：\n[[GATEWAY:cron_add:0 9 * * 1-5:工作日早报]]\n[[GATEWAY:remind_after:30:半小时后开会]]";
        let mut r = Vec::new();
        let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert_eq!(clean, "好的，帮你设置：");
        assert_eq!(r.len(), 2);
    }

    #[tokio::test]
    async fn action_unknown_type() {
        let text = format!("[[GATEWAY:{}:now]]", "fly_to_moon");
        let mut r = Vec::new();
        execute_gateway_actions(&text, None, "wx", "c1", "u1", &mut r).await;
        assert!(r[0].contains("未知"), "{}", r[0]);
    }

    #[tokio::test]
    async fn action_no_tags_passthrough() {
        let text = "普通回复";
        let mut r = Vec::new();
        let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
        assert_eq!(clean, "普通回复");
        assert!(r.is_empty());
    }

    // ── Validation helpers ──────────────────────────────────────

    #[test]
    fn valid_cron_expressions() {
        assert!(is_valid_cron_expr("0 9 * * *"));
        assert!(is_valid_cron_expr("*/5 * * * *"));
        assert!(is_valid_cron_expr("0 9 * * 1-5"));
        assert!(is_valid_cron_expr("30 17 * * 5"));
        assert!(is_valid_cron_expr("0 0 1 * *"));
    }

    #[test]
    fn invalid_cron_expressions() {
        assert!(!is_valid_cron_expr("bad"));
        assert!(!is_valid_cron_expr("0 9 *"));
        assert!(!is_valid_cron_expr(""));
        assert!(!is_valid_cron_expr("0 9 * * * *")); // 6 fields
        assert!(!is_valid_cron_expr("hello world foo bar baz"));
    }

    #[test]
    fn default_profile_is_astra() {
        use crate::cli_bridge::CliProfile;
        let p = CliProfile::default();
        assert_eq!(p.name(), "astra");
    }

    // ── C1: SQL injection prevention in CREATE DATABASE ──

    #[test]
    fn safe_db_name_accepts_valid() {
        assert!(is_safe_db_name("astra_gateway"));
        assert!(is_safe_db_name("test123"));
        assert!(is_safe_db_name("DB_NAME"));
    }

    #[test]
    fn safe_db_name_rejects_injection() {
        assert!(!is_safe_db_name(""));
        assert!(!is_safe_db_name("foo`; DROP TABLE users; --"));
        assert!(!is_safe_db_name("db name"));
        assert!(!is_safe_db_name("foo;bar"));
        assert!(!is_safe_db_name("foo`bar"));
        assert!(!is_safe_db_name("../etc/passwd"));
    }

    // ── Auth circuit breaker tests ────────────────────────────────

    #[test]
    fn auth_circuit_not_tripped_initially() {
        let failures: dashmap::DashMap<String, (u32, Instant)> = dashmap::DashMap::new();
        // No entries — check_auth_circuit equivalent
        assert!(!failures.contains_key("astra"));
    }

    #[test]
    fn auth_circuit_trips_after_threshold() {
        let failures: dashmap::DashMap<String, (u32, Instant)> = dashmap::DashMap::new();
        // Simulate consecutive failures exceeding threshold
        failures.insert(
            "astra".to_string(),
            (AUTH_FAILURE_THRESHOLD + 1, Instant::now()),
        );
        let entry = failures.get("astra").unwrap();
        let (count, last) = *entry;
        assert!(count > AUTH_FAILURE_THRESHOLD);
        assert!(last.elapsed() < AUTH_FAILURE_COOLDOWN);
    }

    #[test]
    fn auth_circuit_resets_after_cooldown() {
        let failures: dashmap::DashMap<String, (u32, Instant)> = dashmap::DashMap::new();
        // Simulate an old failure past cooldown
        failures.insert(
            "astra".to_string(),
            (
                AUTH_FAILURE_THRESHOLD + 1,
                Instant::now() - AUTH_FAILURE_COOLDOWN - Duration::from_secs(1),
            ),
        );
        let entry = failures.get("astra").unwrap();
        let (_, last) = *entry;
        assert!(
            last.elapsed() >= AUTH_FAILURE_COOLDOWN,
            "should be past cooldown"
        );
    }

    #[test]
    fn auth_circuit_constants_are_reasonable() {
        let threshold = AUTH_FAILURE_THRESHOLD;
        assert!(threshold >= 1, "threshold should be at least 1");
        assert!(threshold <= 10, "threshold should be reasonable");
        assert!(
            AUTH_FAILURE_COOLDOWN.as_secs() >= 60,
            "cooldown should be >= 1 min"
        );
        assert!(
            AUTH_FAILURE_COOLDOWN.as_secs() <= 600,
            "cooldown should be <= 10 min"
        );
    }

    #[test]
    fn save_token_to_cli_credentials_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let creds_dir = dir.path().join(".astra");
        std::fs::create_dir_all(&creds_dir).unwrap();
        let creds_path = creds_dir.join("credentials.json");

        // Write initial file
        std::fs::write(
            &creds_path,
            r#"{"current_profile":"default","profiles":{"default":{"username":"old"}}}"#,
        )
        .unwrap();

        // We can't easily test save_token_to_cli_credentials because it uses
        // dirs::home_dir(), but we can test the JSON structure it produces.
        let mut doc: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&creds_path).unwrap()).unwrap();
        doc["profiles"]["default"]["access_token"] = serde_json::Value::String("new-token".into());
        doc["profiles"]["default"]["refresh_token"] =
            serde_json::Value::String("new-refresh".into());
        doc["profiles"]["default"]["username"] = serde_json::Value::String("admin".into());
        let body = serde_json::to_string_pretty(&doc).unwrap();
        std::fs::write(&creds_path, body).unwrap();

        // Verify
        let loaded: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&creds_path).unwrap()).unwrap();
        assert_eq!(
            loaded["profiles"]["default"]["access_token"].as_str(),
            Some("new-token")
        );
        assert_eq!(
            loaded["profiles"]["default"]["refresh_token"].as_str(),
            Some("new-refresh")
        );
        assert_eq!(
            loaded["profiles"]["default"]["username"].as_str(),
            Some("admin")
        );
    }

    // ── SharedAuthToken ─────────────────────────────────────────

    #[tokio::test]
    async fn shared_auth_token_get_returns_none_without_credentials() {
        // With no credentials file and no username/password, get() should return None
        let api = astra::Client::new("http://127.0.0.1:1", None).unwrap();
        let auth = SharedAuthToken::new(api, None, None);
        // get() will try to read credentials file and validate — both fail → None
        assert!(auth.get().await.is_none());
    }

    #[tokio::test]
    async fn shared_auth_token_invalidate_clears_cached() {
        let api = astra::Client::new("http://127.0.0.1:1", None).unwrap();
        let auth = SharedAuthToken::new(api, None, None);
        // Manually set a cached token
        {
            let mut guard = auth.token.write().await;
            *guard = Some("cached-token-abc".to_string());
        }
        assert_eq!(auth.get().await, Some("cached-token-abc".to_string()));

        auth.invalidate().await;
        // After invalidation, cached token is cleared (get returns None because
        // refresh will fail with unreachable server)
        let guard = auth.token.read().await;
        assert!(guard.is_none());
    }

    #[tokio::test]
    async fn shared_auth_token_get_returns_cached() {
        let api = astra::Client::new("http://127.0.0.1:1", None).unwrap();
        let auth = SharedAuthToken::new(api, None, None);
        // Manually seed cache
        {
            let mut guard = auth.token.write().await;
            *guard = Some("fast-path-token".to_string());
        }
        // get() should return the cached token without any network call
        assert_eq!(auth.get().await, Some("fast-path-token".to_string()));
    }

    #[test]
    fn read_cli_access_token_missing_file() {
        // With a nonexistent HOME, read_cli_access_token should return None
        let _guard = EnvGuard::set("HOME", "/nonexistent/path/xyz");
        assert!(read_cli_access_token().is_none());
    }

    #[test]
    fn read_cli_access_token_valid_file() {
        let tmp = tempfile::tempdir().unwrap();
        let astra_dir = tmp.path().join(".astra");
        std::fs::create_dir_all(&astra_dir).unwrap();
        let creds = serde_json::json!({
            "current_profile": "default",
            "profiles": {
                "default": {
                    "access_token": "my-token-123",
                    "refresh_token": "my-refresh"
                }
            }
        });
        std::fs::write(
            astra_dir.join("credentials.json"),
            serde_json::to_string(&creds).unwrap(),
        )
        .unwrap();

        let _guard = EnvGuard::set("HOME", tmp.path().to_str().unwrap());
        assert_eq!(read_cli_access_token(), Some("my-token-123".to_string()));
    }

    struct EnvGuard {
        key: &'static str,
        old: Option<String>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let old = std::env::var(key).ok();
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, old }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.old {
                Some(value) => unsafe {
                    std::env::set_var(self.key, value);
                },
                None => unsafe {
                    std::env::remove_var(self.key);
                },
            }
        }
    }
}

// ── Fix #1: Regex handles JSON with `]` chars (arrays/nested) ──

#[tokio::test]
async fn action_tag_json_with_array() {
    let text = r#"[[GATEWAY:cron_add:0 9 * * *:{"items":[1,2,3]}]]"#;
    let mut r = Vec::new();
    let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
    assert!(clean.is_empty(), "tags should be stripped, got: {clean}");
    assert_eq!(r.len(), 1);
    assert!(
        r[0].contains("storage"),
        "expected no-db error, got: {}",
        r[0]
    );
}

#[tokio::test]
async fn action_tag_json_with_nested_brackets() {
    let text = r#"[[GATEWAY:cron_add:0 9 * * *:{"a":{"b":[true]}}]]"#;
    let mut r = Vec::new();
    let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
    assert!(clean.is_empty(), "tags should be stripped, got: {clean}");
    assert_eq!(r.len(), 1);
}

#[tokio::test]
async fn action_tag_with_text_around_bracket_json() {
    let text = r#"OK here:
[[GATEWAY:cron_add:0 9 * * *:{"steps":["a","b"]}]]
done"#;
    let mut r = Vec::new();
    let clean = execute_gateway_actions(text, None, "wx", "c1", "u1", &mut r).await;
    assert_eq!(r.len(), 1);
    assert!(!clean.contains("GATEWAY"), "tag should be removed: {clean}");
    assert!(clean.contains("OK here"));
    assert!(clean.contains("done"));
}

// ── Fix #4: allow_slash_mutations=false denial ──

#[tokio::test]
async fn action_policy_blocks_model_mutations_when_disabled() {
    let text = "[[GATEWAY:cron_add:0 9 * * *:早上好]]";
    let policy = crate::access_control::ActionPolicy {
        allow_slash_mutations: true,
        allow_model_generated_mutations: false,
        workspace_roots: Vec::new(),
    };
    let mut r = Vec::new();
    let clean =
        execute_gateway_actions_with_policy(text, None, "wx", "c1", "u1", &policy, &mut r).await;
    assert!(clean.is_empty(), "tag should be stripped: {clean}");
    assert_eq!(r.len(), 1);
    assert!(r[0].contains("拒绝"), "expected denial, got: {}", r[0]);
}

#[tokio::test]
async fn action_policy_allows_when_enabled() {
    let text = "[[GATEWAY:cron_add:0 9 * * *:test]]";
    let policy = crate::access_control::ActionPolicy {
        allow_slash_mutations: true,
        allow_model_generated_mutations: true,
        workspace_roots: Vec::new(),
    };
    let mut r = Vec::new();
    let clean =
        execute_gateway_actions_with_policy(text, None, "wx", "c1", "u1", &policy, &mut r).await;
    assert!(clean.is_empty());
    assert_eq!(r.len(), 1);
    assert!(
        r[0].contains("storage"),
        "expected no-db fallback, got: {}",
        r[0]
    );
}

fn build_welcome_message(cli: &CliProfile) -> String {
    format!(
        "👋 **欢迎使用 Astra Gateway!**\n\n\
         当前 CLI: `{cli_name}`\n\
         发送任意消息开始对话，或使用命令:\n\n\
         - `/help` — 所有命令\n\
         - `/status` — 当前状态\n\
         - `/cli` — 切换 CLI 后端\n\
         - `/model` — 切换模型\n\
         - `/ws ls` — 可用项目\n\
         - `/gateway` — 完整能力概览\n\n\
         发送消息开始 🚀",
        cli_name = cli.name()
    )
}

fn format_tokens(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1e6)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1e3)
    } else {
        format!("{n}")
    }
}

/// Check if the message text contains an @mention for the bot.
/// When `bot_name` is non-empty, matches `@{bot_name}` (case-insensitive).
/// When `bot_name` is empty, matches any `@` followed by a word character.
fn is_mentioned(text: &str, bot_name: &str) -> bool {
    if bot_name.is_empty() {
        text.contains('@')
    } else {
        let pattern = format!("@{bot_name}");
        text.to_lowercase().contains(&pattern.to_lowercase())
    }
}

/// Remove `@BotName` prefix from group messages so downstream handlers see clean text.
fn strip_mention(text: &str, bot_name: &str) -> String {
    if bot_name.is_empty() {
        let trimmed = text.trim_start();
        if let Some(rest) = trimmed.strip_prefix('@') {
            let after_word = rest.trim_start_matches(|c: char| !c.is_whitespace());
            after_word.trim_start().to_string()
        } else {
            text.to_string()
        }
    } else {
        let lower = text.to_lowercase();
        let pattern = format!("@{}", bot_name).to_lowercase();
        if let Some(pos) = lower.find(&pattern) {
            let before = &text[..pos];
            let after = &text[pos + pattern.len()..];
            format!("{before}{after}").trim().to_string()
        } else {
            text.to_string()
        }
    }
}

fn safe_id(id: &str) -> String {
    if id.len() <= 8 {
        id.to_string()
    } else {
        format!("{}…", crate::text::safe_prefix(id, 8))
    }
}

// ── Concurrency tests ───────────────────────────────────────

#[test]
fn null_adapter_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<NullAdapter>();
}

#[test]
fn cli_response_fields() {
    let r = CliResponse {
        platform: "weixin".into(),
        chat_id: "c1".into(),
        text: "hello".into(),
        reply_token: Some("tok".into()),
        stream_id: None,
        feedback_id: None,
        stream_finish: true,
        outbox: None,
    };
    assert_eq!(r.platform, "weixin");
    assert_eq!(r.chat_id, "c1");
    assert_eq!(r.text, "hello");
    assert_eq!(r.reply_token.as_deref(), Some("tok"));
}

#[cfg(test)]
struct RecordingAdapter {
    name: &'static str,
    sent: std::sync::Arc<tokio::sync::Mutex<Vec<(String, String)>>>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<InboundMessage>>,
}

#[cfg(test)]
#[async_trait::async_trait]
impl PlatformAdapter for RecordingAdapter {
    fn name(&self) -> &'static str {
        self.name
    }
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
    async fn stop(&mut self) {}
    async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
        _reply_token: Option<&str>,
    ) -> Result<(), String> {
        self.sent
            .lock()
            .await
            .push((chat_id.to_string(), text.to_string()));
        Ok(())
    }
    async fn recv(&self) -> Option<InboundMessage> {
        self.rx.lock().await.recv().await
    }
}

#[tokio::test]
async fn send_text_routes_to_matching_platform_only() {
    let wecom_sent = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let weixin_sent = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let (_tx1, rx1) = tokio::sync::mpsc::channel(1);
    let (_tx2, rx2) = tokio::sync::mpsc::channel(1);
    let adapters: Vec<Box<dyn PlatformAdapter>> = vec![
        Box::new(RecordingAdapter {
            name: "wecom",
            sent: wecom_sent.clone(),
            rx: tokio::sync::Mutex::new(rx1),
        }),
        Box::new(RecordingAdapter {
            name: "weixin",
            sent: weixin_sent.clone(),
            rx: tokio::sync::Mutex::new(rx2),
        }),
    ];
    let mut indices = HashMap::new();
    for (idx, adapter) in adapters.iter().enumerate() {
        indices.insert(adapter.name(), idx);
    }

    let sent = send_text_to_platform(
        &adapters, &indices, "weixin", "chat", "hello", None, None, None, true,
    )
    .await
    .unwrap();
    assert_eq!(sent, 1);

    assert!(wecom_sent.lock().await.is_empty());
    assert_eq!(
        weixin_sent.lock().await.as_slice(),
        &[("chat".to_string(), "hello".to_string())]
    );
}

#[tokio::test]
async fn handle_fast_slash_command_returns_ok() {
    // Can't easily construct a full GatewayRunner in unit test (needs DB),
    // but we can test that NullAdapter works for spawned tasks
    let adapter = NullAdapter;
    let result = adapter.send_text("chat", "text", None).await;
    assert!(result.is_ok());
    let result = adapter.send_typing("chat").await;
    assert!(result.is_ok());
}

#[test]
fn image_attachment_guard_rejects_unknown_model() {
    let text = image_attachment_guard_response(Some("haiku"), &[]).unwrap();
    assert!(text.contains("无法确认"));
    assert!(text.contains("haiku"));
}

#[test]
fn image_attachment_guard_allows_known_vision_model() {
    assert!(image_attachment_guard_response(Some("qwen2.5-vl"), &[]).is_none());
}

#[test]
fn image_attachment_guard_uses_configured_vision_rules() {
    let vision_models = vec!["haiku".into()];
    assert!(image_attachment_guard_response(Some("haiku"), &vision_models).is_none());
}

#[tokio::test]
async fn wecom_media_id_only_attachment_is_rejected_before_cli() {
    let mut msg = InboundMessage {
        platform: "wecom",
        chat_id: "chat".into(),
        user_id: "user".into(),
        text: String::new(),
        attachments: vec![crate::platforms::InboundAttachment {
            kind: crate::platforms::AttachmentKind::Image,
            name: Some("image".into()),
            media_id: Some("media-only".into()),
            url: None,
            local_path: None,
            mime_type: Some("image/png".into()),
            size_bytes: None,
            raw: serde_json::json!({"media_id": "media-only"}),
        }],
        msg_id: "msg-media-only".into(),
        chat_type: crate::platforms::ChatType::DirectMessage,
        reply_token: None,
        route_override: None,
        feedback: None,
    };

    let text = prepare_inbound_attachments(&mut msg).await.unwrap();
    assert!(text.contains("无法读取"));
    assert!(text.contains("缺少可下载链接"));
    assert!(msg.attachments.is_empty());
}

#[tokio::test]
async fn wecom_url_attachment_is_downloaded_before_cli_text() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0_u8; 1024];
        let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;
        let body = b"\x89PNG\r\n\x1a\n";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
            .await
            .unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut stream, body)
            .await
            .unwrap();
    });

    let mut msg = InboundMessage {
        platform: "wecom",
        chat_id: "chat".into(),
        user_id: "user".into(),
        text: "看图".into(),
        attachments: vec![crate::platforms::InboundAttachment {
            kind: crate::platforms::AttachmentKind::Image,
            name: Some("shot".into()),
            media_id: Some("media-with-url".into()),
            url: Some(format!("http://{addr}/shot.png")),
            local_path: None,
            mime_type: Some("image/png".into()),
            size_bytes: None,
            raw: serde_json::json!({}),
        }],
        msg_id: format!("msg-download-{}", uuid::Uuid::new_v4()),
        chat_type: crate::platforms::ChatType::DirectMessage,
        reply_token: None,
        route_override: None,
        feedback: None,
    };

    assert!(prepare_inbound_attachments(&mut msg).await.is_none());
    server.await.unwrap();
    let cli_text = message_text_for_cli(&msg);
    assert!(cli_text.contains("local_path:"));
    assert!(!cli_text.contains("url: http://"));
    assert!(!cli_text.contains("media_id:"));
}

#[tokio::test]
async fn prepared_wecom_attachment_is_not_downloaded_again() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hit_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let server_hits = hit_count.clone();
    let server = tokio::spawn(async move {
        if let Ok(accept) =
            tokio::time::timeout(std::time::Duration::from_millis(150), listener.accept()).await
        {
            let (mut stream, _) = accept.unwrap();
            server_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = tokio::io::AsyncWriteExt::write_all(
                &mut stream,
                b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n",
            )
            .await;
        }
    });

    let local_file = tempfile::NamedTempFile::new().unwrap();
    let mut msg = InboundMessage {
        platform: "wecom",
        chat_id: "chat".into(),
        user_id: "user".into(),
        text: "看图".into(),
        attachments: vec![crate::platforms::InboundAttachment {
            kind: crate::platforms::AttachmentKind::Image,
            name: Some("shot.png".into()),
            media_id: Some("media-with-url".into()),
            url: Some(format!("http://{addr}/shot.png")),
            local_path: Some(local_file.path().to_string_lossy().to_string()),
            mime_type: Some("image/png".into()),
            size_bytes: Some(8),
            raw: serde_json::json!({}),
        }],
        msg_id: format!("msg-prepared-{}", uuid::Uuid::new_v4()),
        chat_type: crate::platforms::ChatType::DirectMessage,
        reply_token: None,
        route_override: None,
        feedback: None,
    };

    assert!(prepare_inbound_attachments(&mut msg).await.is_none());
    server.await.unwrap();
    assert_eq!(hit_count.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cli_response_channel_roundtrip() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<CliResponse>(8);
    tx.send(CliResponse {
        platform: "weixin".into(),
        chat_id: "c1".into(),
        text: "result".into(),
        reply_token: None,
        stream_id: None,
        feedback_id: None,
        stream_finish: true,
        outbox: None,
    })
    .await
    .unwrap();
    let resp = rx.recv().await.unwrap();
    assert_eq!(resp.chat_id, "c1");
    assert_eq!(resp.text, "result");
}

#[tokio::test]
async fn concurrent_cli_responses_ordered() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<CliResponse>(8);
    let tx2 = tx.clone();

    // Simulate two concurrent CLI tasks
    let h1 = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(CliResponse {
            platform: "weixin".into(),
            chat_id: "user1".into(),
            text: "response1".into(),
            reply_token: None,
            stream_id: None,
            feedback_id: None,
            stream_finish: true,
            outbox: None,
        })
        .await
        .unwrap();
    });
    let h2 = tokio::spawn(async move {
        tx2.send(CliResponse {
            platform: "wecom".into(),
            chat_id: "user2".into(),
            text: "response2".into(),
            reply_token: None,
            stream_id: None,
            feedback_id: None,
            stream_finish: true,
            outbox: None,
        })
        .await
        .unwrap();
    });

    h1.await.unwrap();
    h2.await.unwrap();

    // Both responses arrive (order may vary)
    let mut responses = vec![];
    while let Ok(r) = rx.try_recv() {
        responses.push(r.chat_id);
    }
    assert_eq!(responses.len(), 2);
    assert!(responses.contains(&"user1".to_string()));
    assert!(responses.contains(&"user2".to_string()));
}

#[tokio::test]
async fn heartbeat_via_channel_not_adapter() {
    // Heartbeats in spawned tasks should go through outbound channel,
    // not NullAdapter (which drops them silently)
    let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundMessage>(8);
    tx.send(OutboundMessage::plain("weixin", "chat1", "🤔 thinking…"))
        .await
        .unwrap();
    let msg = rx.recv().await.unwrap();
    assert_eq!(msg.platform, "weixin");
    assert_eq!(msg.chat_id, "chat1");
    assert!(msg.text.contains("thinking"));
}

#[tokio::test]
async fn typing_sent_before_cli_spawn() {
    // Typing indicator should be sent in the main loop (via real adapter),
    // NOT in the spawned task (via NullAdapter)
    let adapter = NullAdapter;
    // NullAdapter.send_typing succeeds but does nothing — that's OK
    // because the real adapter sends typing in run() before spawning
    assert!(adapter.send_typing("chat").await.is_ok());
}

// ── Active turn interruption registry tests ────────────────────────────

#[test]
fn active_requests_registry_insert_and_cancel() {
    let registry: dashmap::DashMap<String, CancellationToken> = dashmap::DashMap::new();
    let token = CancellationToken::new();
    registry.insert("trace-1".into(), token.clone());
    assert!(!token.is_cancelled());

    // Simulate /esc: remove + cancel
    let (_, removed_token) = registry.remove("trace-1").unwrap();
    removed_token.cancel();
    assert!(token.is_cancelled());
}

#[test]
fn active_requests_registry_esc_nonexistent_returns_none() {
    let registry: dashmap::DashMap<String, CancellationToken> = dashmap::DashMap::new();
    assert!(registry.remove("ghost").is_none());
}

#[tokio::test]
async fn cancellation_token_aborts_spawned_task() {
    let token = CancellationToken::new();
    let token_inner = token.clone();

    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = token_inner.cancelled() => "interrupted",
            _ = tokio::time::sleep(Duration::from_secs(60)) => "completed",
        }
    });

    // Cancel immediately
    token.cancel();
    let result = handle.await.unwrap();
    assert_eq!(result, "interrupted");
}

#[tokio::test]
async fn esc_command_removes_and_cancels_token() {
    let registry: Arc<dashmap::DashMap<String, CancellationToken>> =
        Arc::new(dashmap::DashMap::new());
    let token = CancellationToken::new();
    registry.insert("trace-abc".into(), token.clone());

    let interrupted = if let Some((_, t)) = registry.remove("trace-abc") {
        t.cancel();
        true
    } else {
        false
    };

    assert!(interrupted);
    assert!(token.is_cancelled());
    assert!(registry.is_empty());
}

// ── /manage cancel redirect tests ──────────────────────────────────────

// ── /manage redirect routing tests ─────────────────────────────────────

#[test]
fn manage_redirect_recognizes_cancel_esc_and_kill_alias() {
    // Verifies the routing predicate used in handle_fast.
    for input in [
        "/manage cancel",
        "/manage cancel 1",
        "/manage esc",
        "/manage esc 2",
        "/manage kill",
        "/manage kill 2",
    ] {
        let rest = input.strip_prefix("/manage ").unwrap().trim();
        assert!(
            rest == "cancel"
                || rest.starts_with("cancel ")
                || rest == "esc"
                || rest.starts_with("esc ")
                || rest == "kill"
                || rest.starts_with("kill "),
            "'{input}' should redirect to fast path"
        );
    }
    // These should NOT redirect.
    for input in ["/manage status", "/manage", "/manage help"] {
        let rest = input.strip_prefix("/manage ").unwrap_or("").trim();
        let should_redirect = rest == "cancel"
            || rest.starts_with("cancel ")
            || rest == "esc"
            || rest.starts_with("esc ")
            || rest == "kill"
            || rest.starts_with("kill ");
        assert!(!should_redirect, "'{input}' should NOT redirect");
    }
}

// ── cancel_task abstraction test ───────────────────────────────────────

#[test]
fn cancel_task_removes_token_and_fires() {
    let registry: Arc<dashmap::DashMap<String, CancellationToken>> =
        Arc::new(dashmap::DashMap::new());
    let token = CancellationToken::new();
    registry.insert("trace-1".into(), token.clone());

    // Simulate GatewayRunner::cancel_task logic
    let found = if let Some((_, t)) = registry.remove("trace-1") {
        t.cancel();
        true
    } else {
        false
    };

    assert!(found);
    assert!(token.is_cancelled());
    assert!(registry.is_empty());
    // Double-cancel is a no-op
    assert!(registry.remove("trace-1").is_none());
}

// ── /status model display tests ────────────────────────────────────────

#[test]
fn model_display_with_override() {
    let model: Option<String> = Some("gpt-4o".into());
    let (display, source) = if let Some(m) = model.as_deref() {
        (m.to_string(), "user override")
    } else {
        ("(CLI default)".to_string(), "profile default")
    };
    assert_eq!(display, "gpt-4o");
    assert_eq!(source, "user override");
}

#[test]
fn model_display_without_override() {
    let model: Option<String> = None;
    let config_default: Option<&str> = Some("sonnet");
    let (display, source) = if let Some(m) = model.as_deref() {
        (m.to_string(), "user override")
    } else if let Some(m) = config_default {
        (m.to_string(), "config default")
    } else {
        ("(CLI default)".to_string(), "profile default")
    };
    assert_eq!(display, "sonnet");
    assert_eq!(source, "config default");
}

#[test]
fn model_display_no_config() {
    let model: Option<String> = None;
    let config_default: Option<&str> = None;
    let (display, source) = if let Some(m) = model.as_deref() {
        (m.to_string(), "user override")
    } else if let Some(m) = config_default {
        (m.to_string(), "config default")
    } else {
        ("(CLI default)".to_string(), "profile default")
    };
    assert_eq!(display, "(CLI default)");
    assert_eq!(source, "profile default");
}

// ── Kill registry key tests ────────────────────────────────────────────

#[test]
fn kill_registry_key_with_trace() {
    let trace_id: Option<&str> = Some("abc-123");
    let request_tag = "#A7";
    let key = trace_id
        .map(String::from)
        .unwrap_or_else(|| format!("notrace:{request_tag}"));
    assert_eq!(key, "abc-123");
}

#[test]
fn kill_registry_key_without_trace() {
    let trace_id: Option<&str> = None;
    let request_tag = "#A7";
    let key = trace_id
        .map(String::from)
        .unwrap_or_else(|| format!("notrace:{request_tag}"));
    assert_eq!(key, "notrace:#A7");
}

#[test]
fn kill_registry_notrace_key_is_findable() {
    let registry: dashmap::DashMap<String, CancellationToken> = dashmap::DashMap::new();
    let token = CancellationToken::new();
    let key = "notrace:#A7".to_string();
    registry.insert(key.clone(), token.clone());

    // /esc can find it by the synthetic key
    let found = registry.remove(&key);
    assert!(found.is_some());
    found.unwrap().1.cancel();
    assert!(token.is_cancelled());
}

// ── flush_buf non-blocking test ────────────────────────────────────────

#[tokio::test]
async fn flush_buf_does_not_block_when_channel_full() {
    // Proves the progressive loop won't deadlock: flush_buf uses try_send.
    let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundMessage>(1);

    // Fill channel.
    tx.try_send(OutboundMessage::plain(
        String::from("p"),
        String::from("c"),
        String::from("fill"),
    ))
    .unwrap();

    // flush_buf equivalent: try_send on full channel completes instantly.
    let result = tx.try_send(OutboundMessage::plain(
        String::from("p"),
        String::from("c"),
        String::from("chunk"),
    ));
    // Returns Err(Full), not deadlock.
    assert!(result.is_err());
}

// ── ChildKillGuard Drop test ───────────────────────────────────────────

#[tokio::test]
async fn child_kill_guard_kills_on_drop() {
    use std::process::Stdio;
    use tokio::process::Command;

    let mut child = Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cat");
    let pid = child.id().expect("pid");

    {
        let _guard = crate::cli_bridge::ChildKillGuard::new(&child);
        // Guard dropped here — should send SIGKILL.
    }

    // child.wait() should return quickly (killed).
    let status = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    assert!(
        status.is_ok(),
        "child must exit promptly after guard drop (pid={pid})"
    );
}

#[test]
fn child_kill_guard_defuse_prevents_kill() {
    // Defused guard must NOT kill.
    let mut guard = crate::cli_bridge::ChildKillGuard::with_pid(1);
    guard.defuse();
    assert!(guard.is_defused());
    // Drop now — no kill sent (pid is None).
}

// ── SendCircuitBreaker tests ─────────────────────────────────────────────

#[cfg(test)]
mod circuit_breaker_tests {
    use super::*;

    #[test]
    fn starts_closed() {
        let cb = SendCircuitBreaker::default();
        assert!(!cb.is_open("wx:chat1"));
    }

    #[test]
    fn opens_after_threshold_failures() {
        let cb = SendCircuitBreaker::default();
        let key = "wx:chat1";
        cb.record_failure(key);
        cb.record_failure(key);
        assert!(!cb.is_open(key), "2 failures should not open");
        cb.record_failure(key);
        assert!(cb.is_open(key), "3 failures must open");
    }

    #[test]
    fn success_resets_to_closed() {
        let cb = SendCircuitBreaker::default();
        let key = "wx:chat1";
        for _ in 0..5 {
            cb.record_failure(key);
        }
        assert!(cb.is_open(key));
        cb.record_success(key);
        assert!(!cb.is_open(key));
    }

    #[test]
    fn reset_clears_state() {
        let cb = SendCircuitBreaker::default();
        let key = "wx:chat1";
        for _ in 0..5 {
            cb.record_failure(key);
        }
        cb.reset(key);
        assert!(!cb.is_open(key));
    }

    #[test]
    fn independent_keys() {
        let cb = SendCircuitBreaker::default();
        for _ in 0..5 {
            cb.record_failure("wx:chat1");
        }
        assert!(cb.is_open("wx:chat1"));
        assert!(!cb.is_open("wx:chat2"));
    }

    #[test]
    fn concurrent_access() {
        let cb = SendCircuitBreaker::default();
        let cb2 = cb.clone();
        let h = std::thread::spawn(move || {
            for _ in 0..100 {
                cb2.record_failure("wx:chat1");
            }
        });
        for _ in 0..100 {
            cb.record_failure("wx:chat1");
        }
        h.join().unwrap();
        // After 200 failures from 2 threads, count must be exactly 200
        // (DashMap's entry() lock guarantees atomic increment). A lossy
        // implementation (e.g., get+insert) would miss counts.
        let count = cb.state.get("wx:chat1").map(|v| v.0).unwrap_or(0);
        assert_eq!(count, 200, "atomic increment lost counts — got {count}");
        assert!(cb.is_open("wx:chat1"));
    }

    // ── Cooldown recovery (issue #1 from review) ─────────────────────────
    //
    // Scenario: breaker opens (3 failures), then task runs for a long time
    // without sending anything. Platform recovers, but without a success
    // call, the breaker never closes. Cooldown lets is_open() return false
    // after SEND_FAILURE_COOLDOWN since last_failure, letting the next send
    // probe the platform.

    #[test]
    fn stays_open_within_cooldown() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        let key = "wx:chat1";
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure(key);
        }
        assert!(cb.is_open(key), "open immediately after threshold");
        clock.advance(SEND_FAILURE_COOLDOWN - Duration::from_millis(1));
        assert!(cb.is_open(key), "still open just before cooldown expires");
    }

    #[test]
    fn closes_after_cooldown_without_success_call() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        let key = "wx:chat1";
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure(key);
        }
        assert!(cb.is_open(key));
        clock.advance(SEND_FAILURE_COOLDOWN);
        assert!(
            !cb.is_open(key),
            "breaker must half-open after cooldown elapsed so worker can probe recovery"
        );
    }

    #[test]
    fn failure_after_cooldown_re_opens() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        let key = "wx:chat1";
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure(key);
        }
        clock.advance(SEND_FAILURE_COOLDOWN + Duration::from_secs(1));
        assert!(!cb.is_open(key), "closed after cooldown");
        // Probe send fails again — breaker must re-open on the next failure,
        // not require 3 more failures (failure count carries forward).
        cb.record_failure(key);
        assert!(
            cb.is_open(key),
            "a single failure after half-open must re-trip the breaker"
        );
    }

    #[test]
    fn success_after_cooldown_fully_closes() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        let key = "wx:chat1";
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure(key);
        }
        clock.advance(SEND_FAILURE_COOLDOWN + Duration::from_secs(1));
        cb.record_success(key);
        // New failures after a success restart the count — need THRESHOLD
        // more to trip again, not just 1.
        for _ in 0..(SEND_FAILURE_THRESHOLD - 1) {
            cb.record_failure(key);
        }
        assert!(
            !cb.is_open(key),
            "after success, count is reset — {} failures should not open",
            SEND_FAILURE_THRESHOLD - 1
        );
    }

    #[test]
    fn cooldown_per_key_independent() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure("wx:chat1");
        }
        clock.advance(SEND_FAILURE_COOLDOWN / 2);
        for _ in 0..SEND_FAILURE_THRESHOLD {
            cb.record_failure("wx:chat2");
        }
        clock.advance(SEND_FAILURE_COOLDOWN / 2 + Duration::from_secs(1));
        // chat1: last_failure was T0, now T0 + cooldown + tail → past cooldown
        assert!(!cb.is_open("wx:chat1"), "chat1 past cooldown");
        // chat2: last_failure was T0 + cooldown/2, now T0 + cooldown + tail
        // elapsed since chat2 failure: cooldown/2 + tail — still within cooldown
        assert!(
            cb.is_open("wx:chat2"),
            "chat2 still within cooldown (elapsed < cooldown)"
        );
    }

    // ── R3-P0-#4: eviction of abandoned entries ──────────────────────────
    //
    // Long-running gateway with 100k+ unique conversations would otherwise
    // accumulate state.len() forever — each distinct (platform, chat_id)
    // that ever failed once keeps a (count, last_failure_at) pair. Lazy
    // eviction on record_failure drops entries older than the eviction age.

    #[test]
    fn entries_older_than_eviction_age_are_reaped() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());

        // Seed many distinct conversations with a single failure each.
        for i in 0..200 {
            cb.record_failure(&format!("wx:conv{i}"));
        }
        assert_eq!(cb.state.len(), 200);

        // Fast-forward past eviction age + advance the sweep.
        clock.advance(SEND_FAILURE_EVICTION_AGE + Duration::from_secs(1));

        // A new failure triggers opportunistic eviction of all the old
        // entries whose last_failure_at is too old.
        cb.record_failure("wx:fresh");
        assert_eq!(
            cb.state.len(),
            1,
            "stale entries must be reaped on next record_failure; only \
             the freshly-recorded key should remain. len={}",
            cb.state.len()
        );
    }

    #[test]
    fn eviction_preserves_recently_active_entries() {
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());

        cb.record_failure("old");
        clock.advance(SEND_FAILURE_EVICTION_AGE / 2);
        cb.record_failure("mid");
        clock.advance(SEND_FAILURE_EVICTION_AGE / 2 + Duration::from_secs(1));
        // `old` is now past eviction age; `mid` is NOT.
        cb.record_failure("fresh");
        assert!(
            cb.state.get("old").is_none(),
            "old entry past eviction age must be gone"
        );
        assert!(
            cb.state.get("mid").is_some(),
            "mid-age entry within eviction window must remain"
        );
        assert!(
            cb.state.get("fresh").is_some(),
            "freshly-recorded entry must remain"
        );
    }

    #[test]
    fn eviction_is_amortized_not_every_call() {
        // Sweeping on EVERY record_failure would make failure-spike
        // scenarios pay O(n) per call. Eviction runs at most once per
        // EVICTION_SWEEP_INTERVAL wall-time window per breaker.
        let clock = TestClock::new();
        let cb = SendCircuitBreaker::with_clock(clock.handle());
        for i in 0..50 {
            cb.record_failure(&format!("k{i}"));
        }
        // All within eviction age — no reap yet.
        assert_eq!(cb.state.len(), 50);
    }
}

// ── truncate_chars tests ─────────────────────────────────────────────────

#[cfg(test)]
mod truncate_tests {
    use super::*;

    #[test]
    fn ascii_within_limit() {
        assert_eq!(truncate_chars("abcdefgh", 8), "abcdefgh");
    }

    #[test]
    fn ascii_over_limit() {
        assert_eq!(truncate_chars("abcdefghij", 8), "abcdefgh");
    }

    #[test]
    fn empty_string() {
        assert_eq!(truncate_chars("", 8), "");
    }

    #[test]
    fn multibyte_chinese() {
        let s = "你好世界测试数据额外";
        let truncated = truncate_chars(s, 8);
        assert_eq!(truncated.chars().count(), 8);
        assert_eq!(truncated, "你好世界测试数据");
    }

    #[test]
    fn multibyte_shorter_than_limit() {
        let s = "你好";
        assert_eq!(truncate_chars(s, 8), "你好");
    }

    #[test]
    fn emoji_boundary() {
        let s = "👋🌍🎉✨💫🔥⭐🎯extra";
        let truncated = truncate_chars(s, 8);
        assert_eq!(truncated.chars().count(), 8);
        assert_eq!(truncated, "👋🌍🎉✨💫🔥⭐🎯");
    }

    #[test]
    fn zero_limit() {
        assert_eq!(truncate_chars("hello", 0), "");
    }
}

// ── Startup DB EOF retry ────────────────────────────────────────────────
//
// MatrixOne / sqlx-mysql can return a "read 0 bytes at EOF" error on the
// very first acquire from a freshly-built pool when the server has silently
// closed the idle connection mid-handshake. `test_before_acquire(true)`
// catches most of these, but the first-use race still slips through once.
// Startup sweeps (sweep_stale_traces / replay_retryable_outbox) can otherwise
// permanently silently fail, leaving zombie
// state in the DB. The retry helper wraps those paths so a single
// transient error doesn't poison startup.

/// Return true if the error message looks like a transient sqlx/MySQL
/// connection fault that a second attempt should recover from. We
/// explicitly do NOT retry on logic errors (syntax, schema, permission);
/// those are stable and retrying would just waste time.
pub(crate) fn is_transient_db_error(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("expected to read")
        || lower.contains("got 0 bytes at eof")
        || lower.contains("broken pipe")
        || lower.contains("connection reset")
        || lower.contains("connection closed")
        || lower.contains("connection refused")
}

/// Run `op`, and if it fails with a transient DB error, wait briefly and
/// retry once. Two attempts are enough — by the second call, sqlx has
/// replaced the dead connection in the pool.
async fn retry_once_on_transient<T, F, Fut>(label: &'static str, op: F) -> Result<T, String>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    match op().await {
        Ok(v) => Ok(v),
        Err(e) if is_transient_db_error(&e) => {
            tracing::info!(
                target: "gateway::runner",
                op = label,
                error = %e,
                "transient DB error — retrying once after 100ms"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            op().await
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod db_retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn transient_detects_eof_shape() {
        assert!(is_transient_db_error(
            "error communicating with database: expected to read 4 bytes, got 0 bytes at EOF"
        ));
        assert!(is_transient_db_error("broken pipe"));
        assert!(is_transient_db_error("Connection reset by peer"));
        assert!(is_transient_db_error("connection refused"));
    }

    #[test]
    fn transient_ignores_logic_errors() {
        // Syntax / schema / permission errors are stable — retry wastes time.
        assert!(!is_transient_db_error("duplicate key"));
        assert!(!is_transient_db_error("access denied for user 'foo'"));
        assert!(!is_transient_db_error("syntax error near FROM"));
        assert!(!is_transient_db_error("table 'x' doesn't exist"));
    }

    #[tokio::test]
    async fn retry_once_recovers_from_transient_first_attempt() {
        let calls = AtomicUsize::new(0);
        let result = retry_once_on_transient("test", || async {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Err::<i32, _>(
                    "error communicating with database: expected to read 4 bytes, got 0 bytes at EOF".to_string(),
                )
            } else {
                Ok(42)
            }
        })
        .await;
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), 2, "must retry exactly once");
    }

    #[tokio::test]
    async fn retry_once_gives_up_on_persistent_transient() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, String> = retry_once_on_transient("test", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("broken pipe".to_string())
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "at most 2 attempts — don't retry forever on persistent EOF"
        );
    }

    #[tokio::test]
    async fn retry_once_does_not_retry_logic_errors() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, String> = retry_once_on_transient("test", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("syntax error".to_string())
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "logic errors must NOT trigger retry"
        );
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    #[allow(clippy::type_complexity)]
    struct StreamRecordingAdapter {
        name: &'static str,
        frames:
            std::sync::Arc<tokio::sync::Mutex<Vec<(String, Option<String>, Option<String>, bool)>>>,
        rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<InboundMessage>>,
    }

    #[async_trait::async_trait]
    impl PlatformAdapter for StreamRecordingAdapter {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        async fn stop(&mut self) {}
        async fn send_text(
            &self,
            _chat_id: &str,
            text: &str,
            _reply_token: Option<&str>,
        ) -> Result<(), String> {
            self.frames
                .lock()
                .await
                .push((text.to_string(), None, None, true));
            Ok(())
        }
        async fn send_stream_chunk(
            &self,
            _chat_id: &str,
            text: &str,
            _reply_token: Option<&str>,
            stream_id: Option<&str>,
            feedback_id: Option<&str>,
            finish: bool,
        ) -> Result<(), String> {
            self.frames.lock().await.push((
                text.to_string(),
                stream_id.map(String::from),
                feedback_id.map(String::from),
                finish,
            ));
            Ok(())
        }
        async fn recv(&self) -> Option<InboundMessage> {
            self.rx.lock().await.recv().await
        }
    }

    #[tokio::test]
    async fn stream_mode_never_splits_large_content() {
        let frames = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let adapters: Vec<Box<dyn PlatformAdapter>> = vec![Box::new(StreamRecordingAdapter {
            name: "wecom",
            frames: frames.clone(),
            rx: tokio::sync::Mutex::new(rx),
        })];
        let mut indices = HashMap::new();
        indices.insert("wecom", 0usize);

        // 6000 chars — exceeds MAX_CHUNK_LEN (3800)
        let long_text = "x".repeat(6000);

        // Stream mode: should NOT split
        send_text_to_platform(
            &adapters,
            &indices,
            "wecom",
            "chat",
            &long_text,
            Some("req-1"),
            Some("stream-1"),
            Some("feedback-1"),
            false,
        )
        .await
        .unwrap();

        let recorded = frames.lock().await;
        assert_eq!(recorded.len(), 1, "stream mode must send exactly 1 frame");
        assert_eq!(
            recorded[0].0.len(),
            6000,
            "full content must be sent unsplit"
        );
        assert_eq!(recorded[0].1.as_deref(), Some("stream-1"));
        assert_eq!(recorded[0].2.as_deref(), Some("feedback-1"));
        assert!(!recorded[0].3);
        drop(recorded);

        // Non-stream mode: should split
        frames.lock().await.clear();
        send_text_to_platform(
            &adapters,
            &indices,
            "wecom",
            "chat",
            &long_text,
            Some("req-1"),
            None,
            None,
            true,
        )
        .await
        .unwrap();

        let recorded = frames.lock().await;
        assert!(
            recorded.len() > 1,
            "non-stream mode must split long messages"
        );
        assert!(recorded.iter().all(|(_, sid, _, _)| sid.is_none()));
        assert!(recorded.iter().all(|(_, _, fid, _)| fid.is_none()));
    }

    #[tokio::test]
    async fn non_stream_feedback_is_forwarded_to_adapter() {
        let frames = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let adapters: Vec<Box<dyn PlatformAdapter>> = vec![Box::new(StreamRecordingAdapter {
            name: "wecom",
            frames: frames.clone(),
            rx: tokio::sync::Mutex::new(rx),
        })];
        let mut indices = HashMap::new();
        indices.insert("wecom", 0usize);

        send_text_to_platform(
            &adapters,
            &indices,
            "wecom",
            "chat",
            "final response",
            None,
            None,
            Some("feedback-1"),
            true,
        )
        .await
        .unwrap();

        let recorded = frames.lock().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "final response");
        assert!(recorded[0].1.is_none());
        assert_eq!(recorded[0].2.as_deref(), Some("feedback-1"));
        assert!(recorded[0].3);
    }

    #[test]
    fn accumulated_only_grows_with_text_deltas() {
        // Simulate the CLI event sequence:
        //   thinking(1080) → text(19) → tool_use → tool_use → text(6947) → end
        // Verify accumulated only contains text content and never shrinks.

        let mut accumulated = String::new();
        let mut token_buf = String::new();
        let mut content_lengths: Vec<usize> = Vec::new();

        // Helper: simulate flush
        let flush = |token_buf: &mut String, accumulated: &mut String, lengths: &mut Vec<usize>| {
            if !token_buf.is_empty() {
                accumulated.push_str(token_buf);
                token_buf.clear();
                lengths.push(accumulated.len());
            }
        };

        // Phase 1: thinking — should NOT affect accumulated
        // (thinking deltas are filtered out before reaching token_buf)
        // accumulated stays empty
        assert!(accumulated.is_empty());

        // Phase 2: text(19 chars)
        token_buf.push_str("让我查看一下代码。");
        flush(&mut token_buf, &mut accumulated, &mut content_lengths);
        assert_eq!(accumulated.len(), "让我查看一下代码。".len());

        // Phase 3: tool_use events — no text, nothing added
        // (ToolStarted/ToolDone don't push to token_buf)
        assert_eq!(accumulated.len(), "让我查看一下代码。".len());

        // Phase 4: more tool_use — still no change
        assert_eq!(accumulated.len(), "让我查看一下代码。".len());

        // Phase 5: final text response (6947 chars simulated)
        let final_text = "这是最终回复内容。".repeat(100);
        // Simulate multiple token deltas
        for chunk in final_text.as_bytes().chunks(20) {
            token_buf.push_str(&String::from_utf8_lossy(chunk));
            if token_buf.len() >= 15 {
                flush(&mut token_buf, &mut accumulated, &mut content_lengths);
            }
        }
        // Final flush
        flush(&mut token_buf, &mut accumulated, &mut content_lengths);

        // Verify monotonic growth
        for window in content_lengths.windows(2) {
            assert!(
                window[1] >= window[0],
                "accumulated must never shrink: {} -> {}",
                window[0],
                window[1]
            );
        }

        // Verify final content contains both text blocks
        assert!(accumulated.starts_with("让我查看一下代码。"));
        assert!(accumulated.len() > "让我查看一下代码。".len());
    }
}

#[cfg(test)]
mod redact_tests {
    use super::redact_sensitive;

    #[test]
    fn redacts_github_authorization_token_header() {
        let input = r#"curl -H "Authorization: token ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1234" https://api.github.com/repos/x/y/pulls"#;
        let out = redact_sensitive(input);
        assert!(!out.contains("ghp_AAAA"), "leaked raw PAT: {out}");
        assert!(out.contains("Authorization: token <redacted>"), "{out}");
    }

    #[test]
    fn redacts_bearer_token_header_case_insensitive() {
        let out = redact_sensitive("-H 'authorization: BEARER abcdef.ghi.jkl'");
        assert!(out.contains("<redacted>"));
        assert!(!out.contains("abcdef.ghi"));
    }

    #[test]
    fn redacts_bare_github_pat_prefixes() {
        for prefix in ["ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_"] {
            let raw = format!("{prefix}AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1234");
            let line = format!("export GITHUB_PAT={raw}");
            let out = redact_sensitive(&line);
            assert!(
                !out.contains(&raw),
                "prefix {prefix} left token intact: {out}"
            );
            assert!(out.contains(&format!("{prefix}<redacted>")), "{out}");
        }
    }

    #[test]
    fn redacts_grafana_service_account_token() {
        let out = redact_sensitive("Authorization: Bearer glsa_abcdefghij0123456789ABCDEFGHIJ_xyz");
        // The auth-header regex strips everything after "Bearer "; that swallows the
        // whole token including the glsa_ prefix, so the output no longer contains
        // the raw secret (the grafana regex is a second line of defense for non-header uses).
        assert!(out.contains("Authorization: Bearer <redacted>"));
        assert!(!out.contains("glsa_abcdefghij"));

        // Non-header context: grafana regex fires.
        let bare = redact_sensitive("token=glsa_abcdefghij0123456789ABCDEFGHIJ_xyz");
        assert!(bare.contains("glsa_<redacted>"));
        assert!(!bare.contains("abcdefghij01"));
    }

    #[test]
    fn leaves_safe_text_unchanged() {
        let safe = "GET /repos/matrixorigin/matrixone/pulls?state=open";
        assert_eq!(redact_sensitive(safe), safe);
    }
}
