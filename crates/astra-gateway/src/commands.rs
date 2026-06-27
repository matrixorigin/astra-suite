//! Slash command handlers for the gateway.

use crate::access_control::{ActionCapability, ActionSource};
use crate::cli_bridge::{REASONING_PREF_KEY, ReasoningDisplay};
use crate::codex_app_pool::CodexAppPool;
use crate::config::GatewayConfig;
use crate::store::{self, GatewayStore};
use crate::trace_model::{
    ActiveRequestSummary, CancelRequestOutcome, ConversationKey, GatewayEvent, GatewayEventKind,
    OutboxStatus, RequestStatus, TraceId, TraceRepository,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

static MODEL_ENTRY_CACHE: LazyLock<Mutex<HashMap<String, ModelEntryCacheValue>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Clone)]
struct ModelEntryCacheValue {
    entries: Vec<ModelEntry>,
    created_at: Instant,
}

struct ModelEntriesResult {
    entries: Vec<ModelEntry>,
    cache_age: Option<Duration>,
}

pub struct CommandContext<'a> {
    pub astra: &'a astra::Client,
    pub config: &'a GatewayConfig,
    pub store: Option<&'a dyn GatewayStore>,
    pub platform: &'a str,
    pub chat_id: &'a str,
    pub user_id: &'a str,
    pub resolved_cli: &'a crate::cli_bridge::CliProfile,
    /// Provider environment resolved for the active CLI run. Model discovery
    /// uses this same environment so `/model` reflects how the CLI is actually
    /// launched by gateway.
    pub resolved_provider_config: Option<&'a crate::config::ProviderConfig>,
    pub trace_repo: Option<&'a dyn TraceRepository>,
    pub project_dirs: &'a [String],
    pub cli_availability: &'a [(String, crate::cli_bridge::CliAvailability)],
    pub auth_status: Option<String>,
    /// Active task registry — allows /esc to interrupt live CLI turns.
    pub active_requests: Option<&'a dashmap::DashMap<String, tokio_util::sync::CancellationToken>>,
    /// Long-lived Codex/Astra app-server pool, used by approval slash commands.
    pub(crate) codex_app_pool: Option<&'a Arc<Mutex<CodexAppPool>>>,
    /// Gateway process start time. Used by /running to flag zombie
    /// requests whose created_at predates this process (i.e. leftovers
    /// from the previous gateway lifecycle whose cancel tokens died).
    pub gateway_start: chrono::DateTime<chrono::Utc>,
}

/// Helper: get store or return error message for storage-dependent commands.
macro_rules! require_store {
    ($ctx:expr) => {
        match $ctx.store {
            Some(s) => s,
            None => return Some("⚠️ 此命令需要存储。当前以无持久化模式运行。".into()),
        }
    };
}

macro_rules! require_trace_repo {
    ($ctx:expr) => {
        match $ctx.trace_repo {
            Some(repo) => repo,
            None => return Some("⚠️ 此命令需要 MySQL 存储。当前没有启用 trace/durable 能力。".into()),
        }
    };
}

pub async fn handle_command(ctx: &CommandContext<'_>, text: &str) -> Option<String> {
    let text = text.trim();
    if !text.starts_with('/') {
        return None;
    }

    let (cmd, arg) = text.split_once(' ').unwrap_or((text, ""));
    let arg = arg.trim();

    match cmd {
        "/new" | "/reset" => {
            if let Some(denial) = slash_denial(ctx, ActionCapability::SessionMutation) {
                return Some(denial);
            }
            let store = require_store!(ctx);
            let cli_name = ctx.resolved_cli.name();
            match store
                .reset_session(ctx.platform, ctx.chat_id, cli_name)
                .await
            {
                Ok(()) => Some(format!(
                    "🔄 `{cli_name}` 会话已重置。发送新消息开始新对话。"
                )),
                Err(e) => Some(format!("⚠️ 会话重置失败: {e}")),
            }
        }

        "/status" => {
            let cli_name = ctx.resolved_cli.name();
            let session = if let Some(store) = ctx.store {
                match store
                    .get_current_session(ctx.platform, ctx.chat_id, cli_name)
                    .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(error = %e, "session lookup failed in /status");
                        None
                    }
                }
            } else {
                None
            };
            let resolved_model = ctx.resolved_cli.model_name();
            let yaml_default = ctx.config.cli.model_name();
            let model_line = match resolved_model {
                Some(id) => {
                    let source = if Some(id) == yaml_default {
                        "配置默认"
                    } else {
                        "用户切换"
                    };
                    format!("- 模型: `{id}` ({source})")
                }
                None => "- 模型: (CLI 默认,yaml 未配置 cli.model)".to_string(),
            };
            let mut lines = vec![
                "📊 **状态**".to_string(),
                format!("- CLI: `{cli_name}`"),
                model_line,
                format!("- 用户: `{}`", ctx.user_id),
                format!("- 会话: `{}`", session.as_deref().unwrap_or("(无)")),
                format!(
                    "- 存储: `{}`",
                    if ctx.store.is_some() { "on" } else { "off" }
                ),
            ];
            if let Some(auth_status) = ctx.auth_status.as_deref() {
                lines.push(format!("- 认证: {auth_status}"));
            }

            if let (Some(store), Some(sid)) = (ctx.store, session.as_deref()) {
                match store
                    .get_usage_session(ctx.platform, ctx.user_id, sid)
                    .await
                {
                    Ok(usage) if usage.messages > 0 => {
                        lines.push(String::new());
                        lines.push("**💳 Session Usage**".into());
                        lines.extend(format_session_usage_lines(&usage));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "session usage lookup failed in /status");
                    }
                }
            }

            if let Some(repo) = ctx.trace_repo {
                let conversation = ConversationKey::new(ctx.platform, ctx.chat_id, cli_name);
                if let Ok(status) = repo.gateway_status(&conversation).await {
                    lines.push(format!(
                        "- 队列: queued={} running={} outbox_pending={} retrying={}",
                        status.queued_count,
                        status.running_count,
                        status.pending_outbox_count,
                        status.retrying_outbox_count
                    ));
                    if let Some(last) = status.last_trace {
                        lines.push(format!(
                            "- 最近 trace: `{}` ({})",
                            short_id(last.trace_id.as_str()),
                            last.status.as_str()
                        ));
                    }
                }
            }

            if let Some(ref sid) = session
                && let Some(snap) =
                    fetch_harness_snapshot(ctx.astra, sid, &ctx.config.astra.api_key).await
            {
                lines.push(String::new());
                lines.push("**🔭 Harness**".into());
                lines.push(format!(
                    "- 轮次: {}/{}",
                    snap.turns_used,
                    snap.turns_limit_str()
                ));
                lines.push(format!(
                    "- Token: ↓{} ↑{}",
                    format_tokens(snap.tokens_prompt),
                    format_tokens(snap.tokens_completion)
                ));
                lines.push(format!(
                    "- 工具: {} ({})",
                    snap.tool_calls,
                    snap.tool_summary()
                ));
                lines.push(format!("- 成本: ~${:.4}", snap.cost_estimate_usd()));
                for w in snap.warnings() {
                    lines.push(format!("- {w}"));
                }
            }
            Some(lines.join("\n"))
        }

        "/inspect" => {
            let cli_name = ctx.resolved_cli.name();
            let sid = match require_store!(ctx)
                .get_current_session(ctx.platform, ctx.chat_id, cli_name)
                .await
            {
                Ok(Some(s)) => s,
                _ => return Some("❌ 当前无活跃会话。".into()),
            };
            match fetch_harness_snapshot(ctx.astra, &sid, &ctx.config.astra.api_key).await {
                Some(snap) => Some(snap.format_full()),
                None => Some("⚠️ 暂无 harness 数据。".into()),
            }
        }

        "/session" => {
            let cli_name = ctx.resolved_cli.name();
            if arg.is_empty() || arg == "current" {
                let store = require_store!(ctx);
                let sid = store
                    .get_current_session(ctx.platform, ctx.chat_id, cli_name)
                    .await
                    .ok()
                    .flatten();
                if sid.is_some() {
                    return Some(format!(
                        "📋 **当前会话** (CLI: `{cli_name}`)\n- ID: `{}`",
                        sid.as_deref().unwrap_or("(无)")
                    ));
                }
                // Current CLI has no session — check all CLIs (including default) and show which ones do.
                let mut found = Vec::new();
                let default_cli_name = ctx.config.cli.name();
                if default_cli_name != cli_name
                    && let Ok(Some(other_sid)) = store
                        .get_current_session(ctx.platform, ctx.chat_id, default_cli_name)
                        .await
                {
                    let short = crate::runner::truncate_chars(&other_sid, 8);
                    found.push(format!("  `{default_cli_name}`: `{short}…`"));
                }
                for (name, _) in ctx.config.cli_profiles.iter() {
                    if name == cli_name {
                        continue;
                    }
                    if let Ok(Some(other_sid)) = store
                        .get_current_session(ctx.platform, ctx.chat_id, name)
                        .await
                    {
                        let short = crate::runner::truncate_chars(&other_sid, 8);
                        found.push(format!("  `{name}`: `{short}…`"));
                    }
                }
                if found.is_empty() {
                    return Some(format!(
                        "📋 **当前会话** (CLI: `{cli_name}`)\n- ID: (无)\n\n所有 CLI 均无活跃会话。发送消息开始新对话。"
                    ));
                }
                let mut lines = vec![
                    format!("📋 **当前会话** (CLI: `{cli_name}`)"),
                    "- ID: (无)".into(),
                    String::new(),
                    "其他 CLI 有活跃会话:".into(),
                ];
                lines.extend(found);
                lines.push(format!(
                    "\n使用 `/cli <name>` 切换，或发送消息创建新 `{cli_name}` 会话。"
                ));
                return Some(lines.join("\n"));
            }

            if arg == "list" {
                let sessions = require_store!(ctx)
                    .list_sessions(ctx.platform, ctx.chat_id, cli_name)
                    .await
                    .unwrap_or_default();
                if sessions.is_empty() {
                    return Some(format!("📋 `{cli_name}` 没有历史会话。"));
                }
                let mut lines = vec![format!("📋 **`{cli_name}` 会话列表**")];
                for s in &sessions {
                    let marker = if s.is_current { "→ " } else { "  " };
                    let short = &s.session_id[..8.min(s.session_id.len())];
                    lines.push(format!("{marker}`{short}…` ({})", s.created_at));
                }
                lines.push("\n使用 `/session switch <id>` 切换".into());
                return Some(lines.join("\n"));
            }

            if let Some(target) = arg
                .strip_prefix("switch ")
                .or_else(|| arg.strip_prefix("sw "))
            {
                let target = target.trim();
                match require_store!(ctx)
                    .switch_session(ctx.platform, ctx.chat_id, target)
                    .await
                {
                    Ok(true) => Some(format!(
                        "✅ 已切换到会话 `{}`",
                        &target[..8.min(target.len())]
                    )),
                    Ok(false) => Some(format!("❌ 找不到会话 `{target}`")),
                    Err(e) => Some(format!("⚠️ 切换失败: {e}")),
                }
            } else {
                Some("用法: `/session [list|switch <id>|current]`".into())
            }
        }

        "/cron" => {
            if arg.is_empty() || arg == "list" {
                let jobs = require_store!(ctx)
                    .list_cron_jobs(ctx.platform, ctx.chat_id)
                    .await
                    .unwrap_or_default();
                if jobs.is_empty() {
                    return Some("⏰ 没有定时任务。用 `/cron add` 创建。".into());
                }
                let mut lines = vec!["⏰ **定时任务**".to_string()];
                for j in &jobs {
                    let status = if j.enabled { "✅" } else { "⏸" };
                    let short_id = &j.job_id[..8.min(j.job_id.len())];
                    lines.push(format!(
                        "{status} `{short_id}` | `{}` | {}",
                        j.cron_expr, j.description
                    ));
                }
                lines.push(
                    "\n`/cron add <cron_expr> <消息>` — 创建\n`/cron del <id>` — 删除".into(),
                );
                return Some(lines.join("\n"));
            }

            if let Some(rest) = arg.strip_prefix("add ") {
                if let Some(denial) = slash_denial(ctx, ActionCapability::CronMutation) {
                    return Some(denial);
                }
                // Parse: /cron add "0 9 * * 1-5" 每天早上9点汇报
                let (cron_expr, message) = match parse_cron_add(rest) {
                    Some(parsed) => parsed,
                    None => {
                        return Some(
                            "⚠️ 格式错误。用法:\n\
                             `/cron add \"0 9 * * 1-5\" 每天早上9点汇报`\n\
                             `/cron add 0 9 * * * 每天早上9点汇报`"
                                .into(),
                        );
                    }
                };
                let job_id = uuid::Uuid::new_v4().to_string();
                let store = require_store!(ctx);
                match store
                    .create_cron_job(&store::CronJobSpec {
                        job_id: job_id.clone(),
                        platform: ctx.platform.to_string(),
                        chat_id: ctx.chat_id.to_string(),
                        user_id: ctx.user_id.to_string(),
                        cron_expr: cron_expr.clone(),
                        message: message.clone(),
                        description: message.clone(),
                    })
                    .await
                {
                    Ok(()) => Some(format!(
                        "✅ 定时任务已创建\n- ID: `{}`\n- 表达式: `{cron_expr}`\n- 任务: {message}",
                        &job_id[..8]
                    )),
                    Err(e) => Some(format!("⚠️ 创建失败: {e}")),
                }
            } else if let Some(id) = arg.strip_prefix("del ").or_else(|| arg.strip_prefix("rm ")) {
                if let Some(denial) = slash_denial(ctx, ActionCapability::CronMutation) {
                    return Some(denial);
                }
                let id = id.trim();
                let store = require_store!(ctx);
                // Support prefix matching (list shows 8-char short IDs)
                let jobs = store
                    .list_cron_jobs(ctx.platform, ctx.chat_id)
                    .await
                    .unwrap_or_default();
                let matches: Vec<_> = jobs.iter().filter(|j| j.job_id.starts_with(id)).collect();
                if matches.len() > 1 {
                    return Some(format!(
                        "⚠️ 前缀 `{id}` 匹配到 {} 个任务，请提供更多字符",
                        matches.len()
                    ));
                }
                let target_id = matches.first().map(|j| j.job_id.as_str()).unwrap_or(id);
                match store.delete_cron_job(target_id).await {
                    Ok(true) => Some("✅ 任务已删除".into()),
                    Ok(false) => Some("❌ 找不到该任务".into()),
                    Err(e) => Some(format!("⚠️ 删除失败: {e}")),
                }
            } else {
                Some("用法: `/cron [list|add <expr> <msg>|del <id>]`".into())
            }
        }

        "/model" => {
            let current_model = ctx.resolved_cli.model_name();
            // Config default: the `cli.model` field in gateway.yaml. When the
            // user has no per-user override, resolve_cli_profile returns this
            // untouched, so current_model == config_default_model.
            let config_default_model = ctx.config.cli.model_name();
            let refresh = arg.eq_ignore_ascii_case("refresh");
            let model_list = match model_entries_for_context(ctx, arg, refresh).await {
                Ok(model_list) => model_list,
                Err(e) => return Some(format!("⚠️ 获取模型列表失败: {e}")),
            };
            let entries = model_list.entries;

            if arg.is_empty() || refresh {
                let current_display = current_model
                    .map(|m| display_model_name(m, &entries))
                    .unwrap_or_else(|| "默认".to_string());
                let mut lines = if refresh {
                    vec![
                        format!("🤖 当前: **{current_display}**"),
                        "缓存 已刷新 · `/model refresh`".to_string(),
                        String::new(),
                    ]
                } else {
                    vec![
                        format!("🤖 当前: **{current_display}**"),
                        format!(
                            "缓存 {} · `/model refresh`",
                            format_cache_age(model_list.cache_age)
                        ),
                        String::new(),
                    ]
                };
                for (i, entry) in entries.iter().enumerate() {
                    let mark = if entry.matches_current(current_model) {
                        " ✓"
                    } else {
                        ""
                    };
                    // "默认" row: show what yaml.cli.model points to today.
                    // Other rows: show the short per-model description.
                    let desc = if entry.full_id.is_none() {
                        match config_default_model {
                            Some(m) => {
                                let pretty = display_model_name(m, &entries);
                                if pretty == m {
                                    format!("yaml → {m}")
                                } else {
                                    format!("yaml → {pretty}")
                                }
                            }
                            None => "yaml 未配".to_string(),
                        }
                    } else {
                        entry.desc.to_string()
                    };
                    lines.push(format!(
                        "{idx}. **{label}**{mark} · {desc}",
                        idx = i + 1,
                        label = entry.label,
                    ));
                }
                lines.push(String::new());
                lines.push("切换: `/model <编号|名称|完整id>`".into());
                return Some(lines.join("\n"));
            }

            if let Some(denial) = slash_denial(ctx, ActionCapability::ModelMutation) {
                return Some(denial);
            }

            let resolved = resolve_model_input(arg, &entries);
            if matches!(resolved, ResolvedModel::Unrecognized) {
                return Some(format!(
                    "⚠️ 未识别模型: `{arg}`，当前模型未改变。\n\
                     发送 `/model` 查看可选列表后重新切换。"
                ));
            }

            let Some(store) = ctx.store else {
                let display = match &resolved {
                    ResolvedModel::Default => "默认".to_string(),
                    ResolvedModel::Id(id) => display_model_name(id, &entries),
                    ResolvedModel::Unrecognized => unreachable!(),
                };
                return Some(format!("🤖 模型已切换: `{display}`\n(下次消息生效)"));
            };

            let model_key = store::model_preference_key(ctx.resolved_cli.name(), Some(ctx.chat_id));
            let (stored_value, display) = match &resolved {
                ResolvedModel::Default => ("".to_string(), "默认".to_string()),
                ResolvedModel::Id(id) => (id.clone(), display_model_name(id, &entries)),
                ResolvedModel::Unrecognized => unreachable!(),
            };
            if let Err(e) = store
                .set_user_preference(ctx.platform, ctx.user_id, &model_key, &stored_value)
                .await
            {
                return Some(format!("⚠️ 模型设置失败: {e}"));
            }
            Some(format!("🤖 模型已切换: `{display}`\n(下次消息生效)"))
        }

        "/reasoning" => {
            let store = require_store!(ctx);
            if arg.is_empty() {
                let current = store
                    .get_user_preference(ctx.platform, ctx.user_id, REASONING_PREF_KEY)
                    .await
                    .ok()
                    .flatten();
                let current = ReasoningDisplay::from_pref(current.as_deref());
                return Some(format!(
                    "🧠 reasoning: `{}`\n\
                     用 `/reasoning on` 展示底层 CLI 明确输出的 reasoning/thinking block。\n\
                     用 `/reasoning off` 关闭。\n\
                     也可用 `/cli <name> thinking-chain` 切换 CLI 并打开。",
                    current.label()
                ));
            }

            let Some(mode) = ReasoningDisplay::from_command_arg(arg) else {
                return Some(
                    "⚠️ 用法: `/reasoning on`、`/reasoning off`、`/reasoning raw-if-available`"
                        .into(),
                );
            };
            if let Err(e) = store
                .set_user_preference(
                    ctx.platform,
                    ctx.user_id,
                    REASONING_PREF_KEY,
                    mode.as_pref(),
                )
                .await
            {
                return Some(format!("⚠️ reasoning 设置失败: {e}"));
            }
            Some(format!("🧠 reasoning 已设置为 `{}`", mode.label()))
        }

        "/cli" => {
            if arg.is_empty() {
                // Show current CLI + available profiles + workspace
                let current = ctx.resolved_cli.name();
                let caps = ctx.resolved_cli.capabilities();
                let workspace = if let Some(s) = ctx.store {
                    s.get_user_preference(ctx.platform, ctx.user_id, "workspace")
                        .await
                        .ok()
                        .flatten()
                } else {
                    None
                };
                let reasoning_display = if let Some(s) = ctx.store {
                    let pref = s
                        .get_user_preference(ctx.platform, ctx.user_id, REASONING_PREF_KEY)
                        .await
                        .ok()
                        .flatten();
                    ReasoningDisplay::from_pref(pref.as_deref())
                } else {
                    ReasoningDisplay::Off
                };
                let ws_display = workspace.as_deref().unwrap_or("(默认)");
                let mut lines = vec![
                    format!("🔧 **当前 CLI: `{current}`**"),
                    format!("📂 工作目录: `{ws_display}`"),
                    format!("🧠 reasoning: `{}`", reasoning_display.label()),
                    format!(
                        "  能力: {}session {}model {}harness {}tools",
                        if caps.supports_session { "✅" } else { "❌" },
                        if caps.supports_model_switch {
                            "✅"
                        } else {
                            "❌"
                        },
                        if caps.supports_harness { "✅" } else { "❌" },
                        if caps.supports_tools { "✅" } else { "❌" },
                    ),
                ];
                if !ctx.config.cli_profiles.is_empty() {
                    lines.push("\n**可用 CLI:**".into());
                    for (name, profile) in &ctx.config.cli_profiles {
                        let c = profile.capabilities();
                        // Look up availability from pre-probed list
                        let (status_icon, version_info) = ctx
                            .cli_availability
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, avail)| {
                                let icon = if avail.is_available() { "✅" } else { "❌" };
                                let ver = match avail {
                                    crate::cli_bridge::CliAvailability::Available { version } => {
                                        version
                                            .as_deref()
                                            .map(|v| format!(" {v}"))
                                            .unwrap_or_default()
                                    }
                                    crate::cli_bridge::CliAvailability::NotInstalled => {
                                        " — 未安装".into()
                                    }
                                    crate::cli_bridge::CliAvailability::NotExecutable(e) => {
                                        format!(" — {e}")
                                    }
                                };
                                (icon, ver)
                            })
                            .unwrap_or(("  ", String::new()));
                        lines.push(format!(
                            "  {status_icon} `{name}` ({}{}{}){version_info}",
                            profile.name(),
                            if c.supports_harness { " +harness" } else { "" },
                            if c.supports_session { " +session" } else { "" },
                        ));
                    }
                    lines.push("\n用 `/cli <name>` 切换".into());
                }
                return Some(lines.join("\n"));
            }

            // Switch to a named profile
            let mut parts = arg.split_whitespace();
            let profile_name = parts.next().unwrap_or_default();
            let reasoning_arg = parts.next();
            if parts.next().is_some() {
                return Some("⚠️ 用法: `/cli <name>` 或 `/cli <name> thinking-chain`".into());
            }

            let reasoning_mode = match reasoning_arg {
                Some(value) => match ReasoningDisplay::from_command_arg(value) {
                    Some(mode) => Some(mode),
                    None => {
                        return Some(
                            "⚠️ 用法: `/cli <name>` 或 `/cli <name> thinking-chain`".into(),
                        );
                    }
                },
                None => None,
            };

            if let Some(profile) = ctx.config.cli_profiles.get(profile_name) {
                if let Some(denial) = slash_denial(ctx, ActionCapability::CliMutation) {
                    return Some(denial);
                }
                let caps = profile.capabilities();
                let cap_str = format!(
                    "session={} model={} harness={} tools={}",
                    if caps.supports_session { "✅" } else { "❌" },
                    if caps.supports_model_switch {
                        "✅"
                    } else {
                        "❌"
                    },
                    if caps.supports_harness { "✅" } else { "❌" },
                    if caps.supports_tools { "✅" } else { "❌" },
                );
                if let Some(s) = ctx.store
                    && let Err(e) = s
                        .set_user_preference(ctx.platform, ctx.user_id, "cli_profile", profile_name)
                        .await
                {
                    return Some(format!("⚠️ CLI 切换失败: {e}"));
                }
                if let Some(mode) = reasoning_mode {
                    let store = require_store!(ctx);
                    if let Err(e) = store
                        .set_user_preference(
                            ctx.platform,
                            ctx.user_id,
                            REASONING_PREF_KEY,
                            mode.as_pref(),
                        )
                        .await
                    {
                        return Some(format!("⚠️ reasoning 设置失败: {e}"));
                    }
                    Some(format!(
                        "✅ 已切换到 `{profile_name}` ({name})\n{cap_str}\n🧠 reasoning: `{reasoning}`",
                        name = profile.name(),
                        reasoning = mode.label(),
                    ))
                } else {
                    Some(format!(
                        "✅ 已切换到 `{profile_name}` ({name})\n{cap_str}",
                        name = profile.name()
                    ))
                }
            } else {
                let available: Vec<&str> =
                    ctx.config.cli_profiles.keys().map(|s| s.as_str()).collect();
                Some(format!(
                    "❌ 未找到 CLI `{profile_name}`\n可用: {}",
                    if available.is_empty() {
                        "(无配置)".into()
                    } else {
                        available.join(", ")
                    }
                ))
            }
        }

        "/approve" => Some(handle_approval_response_command(ctx, arg, false).await),

        "/deny" => Some(handle_approval_response_command(ctx, arg, true).await),

        "/help" => Some(
            "💡 **命令列表**\n\n\
             **对话**\n\
             `/new` (`/reset`) — 新建会话\n\
             `/session list` — 历史会话\n\
             `/session switch <id>` — 切换会话\n\n\
             **模型**\n\
             `/model` — 当前模型 + 快捷列表\n\
             `/model <name>` — 切换模型 (默认/编号/名称/完整 id)\n\n\
             **CLI & 工作区**\n\
             `/cli` — 查看当前 CLI + 能力 + 工作目录\n\
             `/cli <name>` — 切换 CLI (astra/claude)\n\
             `/cli <name> thinking-chain` — 切换 CLI 并展示可获取的思考块\n\
             `/reasoning on|off` — 开关可获取的 reasoning/thinking block\n\
             `/ws` — 当前工作目录\n\
             `/ws ls` — 列出可用项目\n\
             `/ws <name|path>` — 切换工作目录\n\n\
             **任务管理**\n\
             `/running` — 查看正在执行的任务 (带编号)\n\
             `/cancel [N|text]` — 取消排队请求 (序号/文本/ID)\n\
             `/esc [N|text]` — 中断当前运行 turn (序号/文本/ID；`/kill` 兼容别名)\n\
             `/retry` — 查看发送失败的消息\n\
             `/retry dismiss` — 清除所有失败消息\n\
             `/manage [指令]` — AI 辅助任务管理\n\
             `/usage` — 用量统计\n\n\
             **监控**\n\
             `/status` — 状态 + harness\n\
             `/inspect` — harness 详情 (token/cost/tools/warnings)\n\
             `/audit` — 审计记录 (最近 N 轮决策链)\n\
             `/trace [id]` — 查看 trace 详情\n\n\
             **定时任务**\n\
             `/task list` — 查看定时任务/提醒\n\
             `/task cancel <id>` — 取消定时任务/提醒\n\
             `/cron list` — 查看任务\n\
             `/cron add <expr> <msg>` — 创建\n\
             `/cron del <id>` — 删除\n\n\
             **其他**\n\
             `/auth` — 认证状态 + 重置 + 自动重登录\n\
             `/gateway` — 完整能力概览"
                .into(),
        ),

        "/task" => {
            if arg.is_empty() || arg == "list" {
                let mut lines = Vec::new();

                // Scheduled tasks (cron jobs + reminders)
                if let Some(store) = ctx.store {
                    let jobs = store
                        .list_cron_jobs(ctx.platform, ctx.chat_id)
                        .await
                        .unwrap_or_default();
                    if !jobs.is_empty() {
                        for j in &jobs {
                            let short_id = &j.job_id[..8.min(j.job_id.len())];
                            let icon = if j.cron_expr == "once" { "⏰" } else { "🔁" };
                            let schedule = if j.cron_expr == "once" {
                                "一次性".to_string()
                            } else {
                                format!("`{}`", j.cron_expr)
                            };
                            lines.push(format!(
                                "{icon} `{short_id}` | {} | {schedule}",
                                j.description
                            ));
                        }
                    }
                }

                if lines.is_empty() {
                    return Some("📋 没有任务。".into());
                }
                lines.insert(0, format!("📋 **任务** ({} 个)", lines.len()));
                Some(lines.join("\n"))
            } else if let Some(id) = arg
                .strip_prefix("cancel ")
                .or_else(|| arg.strip_prefix("rm "))
                .or_else(|| arg.strip_prefix("del "))
            {
                let id = id.trim();

                if let Some(store) = ctx.store {
                    let jobs = store
                        .list_cron_jobs(ctx.platform, ctx.chat_id)
                        .await
                        .unwrap_or_default();
                    if let Some(job) = jobs.iter().find(|j| j.job_id.starts_with(id)) {
                        return match store.delete_cron_job(&job.job_id).await {
                            Ok(true) => Some("🚫 定时任务/提醒已取消".into()),
                            Ok(false) => Some("❌ 找不到该任务".into()),
                            Err(e) => Some(format!("⚠️ {e}")),
                        };
                    }
                }

                Some("❌ 找不到该任务".into())
            } else {
                Some("用法: `/task [list|cancel <id>]`".into())
            }
        }

        "/audit" => {
            let cli_name = ctx.resolved_cli.name();
            let store = require_store!(ctx);
            let sid = match store
                .get_current_session(ctx.platform, ctx.chat_id, cli_name)
                .await
            {
                Ok(Some(s)) => s,
                _ => return Some("❌ 当前无活跃会话。".into()),
            };
            let history =
                fetch_harness_history(ctx.astra, &sid, &ctx.config.astra.api_key, 50).await;
            if history.is_empty() {
                return Some("📋 暂无审计记录。".into());
            }
            Some(format_audit_history(history))
        }

        "/running" => {
            let repo = require_trace_repo!(ctx);
            let conversation =
                ConversationKey::new(ctx.platform, ctx.chat_id, ctx.resolved_cli.name());
            let rows = repo
                .list_active_requests(&conversation, 20)
                .await
                .unwrap_or_default();
            if rows.is_empty() {
                Some("✅ 当前没有正在执行的任务。".into())
            } else {
                let mut lines = vec![format!("🔄 **正在执行** ({} 个)", rows.len())];
                let mut stuck_outbox_count = 0usize;
                let mut zombie_count = 0usize;
                for (i, row) in rows.iter().enumerate() {
                    let icon = status_icon(row.display_status());
                    let tag = crate::runner::short_request_tag(row.trace_id.as_str());
                    let short_text = truncate_text(&row.text_preview, 40);
                    let ts = short_timestamp(&row.created_at);
                    let zombie_mark = if is_zombie_request(&row.created_at, ctx.gateway_start) {
                        zombie_count += 1;
                        " 🧟"
                    } else {
                        ""
                    };
                    lines.push(format!(
                        "[{}] {} {} | {} | {} | {}{}",
                        i + 1,
                        icon,
                        row.display_status(),
                        tag,
                        short_text,
                        ts,
                        zombie_mark,
                    ));
                    if row.status.is_terminal() && row.outbox_status == Some(OutboxStatus::Failed) {
                        stuck_outbox_count += 1;
                    }
                }
                if zombie_count > 0 {
                    lines.push(format!(
                        "\n🧟 发现 {zombie_count} 个僵尸请求 (创建时间早于本次 gateway 启动)。用 `/esc all` 一键清空。"
                    ));
                }
                if stuck_outbox_count > 0 {
                    lines.push(format!(
                        "\n📬 有 {stuck_outbox_count} 个消息发送失败。用 `/retry` 查看，`/retry dismiss` 清除。"
                    ));
                }
                lines.push("\n💡 `/esc 1` 中断 | `/esc all` 清空 | `/cancel 2` 取消 | `/manage` AI 辅助管理".into());
                Some(lines.join("\n"))
            }
        }

        "/trace" => {
            let repo = require_trace_repo!(ctx);
            let conversation =
                ConversationKey::new(ctx.platform, ctx.chat_id, ctx.resolved_cli.name());
            if arg.is_empty() || arg == "list" {
                let traces = repo
                    .list_recent_traces(&conversation, 10)
                    .await
                    .unwrap_or_default();
                if traces.is_empty() {
                    return Some("📋 暂无 trace。".into());
                }
                let mut lines = vec![format!("🧭 **最近 Trace** ({} 个)", traces.len())];
                for trace in traces {
                    lines.push(format!(
                        "- `{}` {} | {} | {} events | {}",
                        short_id(trace.trace_id.as_str()),
                        trace.status.as_str(),
                        trace.text_preview,
                        trace.event_count,
                        trace.created_at
                    ));
                }
                lines.push("\n用 `/trace <trace_id>` 查看详情".into());
                return Some(lines.join("\n"));
            }

            let trace_id = resolve_trace_selector(repo, &conversation, arg).await;
            let Some(trace_id) = trace_id else {
                return Some(format!("❌ 找不到 trace `{arg}`"));
            };
            let events = repo
                .list_events_for_trace(&trace_id, 80)
                .await
                .unwrap_or_default();
            if events.is_empty() {
                return Some(format!(
                    "📋 Trace `{}` 暂无事件。",
                    short_id(trace_id.as_str())
                ));
            }
            Some(format_trace_events(&trace_id, events))
        }

        "/cancel" => {
            let repo = require_trace_repo!(ctx);
            let conversation =
                ConversationKey::new(ctx.platform, ctx.chat_id, ctx.resolved_cli.name());
            if arg == "all" {
                return Some(
                    kill_or_cancel_all(
                        repo,
                        &conversation,
                        ctx.active_requests,
                        "cancelled by user via /cancel all",
                    )
                    .await,
                );
            }
            if arg.is_empty() {
                // Auto-pick first cancellable
                let row = repo
                    .list_active_requests(&conversation, 20)
                    .await
                    .ok()
                    .and_then(|rows| rows.into_iter().find(|r| r.is_cancellable()));
                let Some(row) = row else {
                    return Some("✅ 当前没有可取消的排队请求。运行中的请求请用 `/esc`。".into());
                };
                match repo
                    .cancel_accepted_request(
                        &conversation,
                        row.trace_id.as_str(),
                        "cancelled by user",
                    )
                    .await
                {
                    Ok(CancelRequestOutcome::Cancelled(r)) => {
                        Some(format!("🚫 已取消: {}", truncate_text(&r.text_preview, 40)))
                    }
                    Ok(CancelRequestOutcome::AlreadyRunning(_)) => {
                        Some("⚠️ 请求已在运行，用 `/esc` 中断。".into())
                    }
                    Ok(CancelRequestOutcome::NotFound) => Some("❌ 找不到可取消请求".into()),
                    Err(e) => Some(format!("⚠️ 取消失败: {e}")),
                }
            } else {
                // Resolve by number, text, or ID
                let Some(row) = resolve_active_request(repo, &conversation, arg).await else {
                    return Some(format!("❌ 找不到匹配 `{arg}` 的请求"));
                };
                if !row.is_cancellable() {
                    return Some(format!(
                        "⚠️ 请求 [{}] 已在运行，用 `/esc` 中断。",
                        truncate_text(&row.text_preview, 30)
                    ));
                }
                match repo
                    .cancel_accepted_request(
                        &conversation,
                        row.trace_id.as_str(),
                        "cancelled by user",
                    )
                    .await
                {
                    Ok(CancelRequestOutcome::Cancelled(r)) => {
                        Some(format!("🚫 已取消: {}", truncate_text(&r.text_preview, 40)))
                    }
                    Ok(CancelRequestOutcome::AlreadyRunning(_)) => {
                        Some("⚠️ 请求已在运行，用 `/esc` 中断。".into())
                    }
                    Ok(CancelRequestOutcome::NotFound) => {
                        Some(format!("❌ 找不到可取消请求 `{arg}`"))
                    }
                    Err(e) => Some(format!("⚠️ 取消失败: {e}")),
                }
            }
        }

        "/esc" | "/kill" => {
            let repo = require_trace_repo!(ctx);
            let conversation =
                ConversationKey::new(ctx.platform, ctx.chat_id, ctx.resolved_cli.name());
            let command_name = cmd;

            if arg == "all" {
                return Some(
                    kill_or_cancel_all(
                        repo,
                        &conversation,
                        ctx.active_requests,
                        if command_name == "/kill" {
                            "interrupted by user via /esc all (/kill alias)"
                        } else {
                            "interrupted by user via /esc all"
                        },
                    )
                    .await,
                );
            }

            let row = if arg.is_empty() {
                // Auto-pick the most recent running request
                repo.list_active_requests(&conversation, 20)
                    .await
                    .ok()
                    .and_then(|rows| {
                        rows.into_iter()
                            .find(|r| r.status == RequestStatus::Running)
                    })
            } else {
                // Resolve by number, text, or ID
                resolve_active_request(repo, &conversation, arg).await
            };

            let Some(row) = row else {
                if arg.is_empty() {
                    return Some("✅ 当前没有运行中的请求。".into());
                }
                return Some(format!("❌ 找不到匹配 `{arg}` 的请求"));
            };
            match repo
                .force_fail_request(
                    &row.trace_id,
                    if command_name == "/kill" {
                        "interrupted by user via /esc (/kill alias)"
                    } else {
                        "interrupted by user via /esc"
                    },
                )
                .await
            {
                Ok(true) => {
                    // Fire the cancellation token. Persistent CLIs translate
                    // this to their native Esc/interrupt protocol for the
                    // active turn; one-shot CLIs are terminated by the bridge.
                    let interrupted_live_turn = ctx
                        .active_requests
                        .map(|tasks| {
                            if let Some((_, token)) = tasks.remove(row.trace_id.as_str()) {
                                token.cancel();
                                true
                            } else {
                                false
                            }
                        })
                        .unwrap_or(false);
                    let suffix = if interrupted_live_turn {
                        ""
                    } else {
                        " (等待自然退出)"
                    };
                    Some(format!(
                        "⎋ 已中断: {}{}",
                        truncate_text(&row.text_preview, 40),
                        suffix
                    ))
                }
                Ok(false) => Some(format!(
                    "⚠️ 请求已是终态: {}",
                    truncate_text(&row.text_preview, 30)
                )),
                Err(e) => Some(format!("⚠️ 中断失败: {e}")),
            }
        }

        "/retry" => {
            let repo = require_trace_repo!(ctx);
            let conversation =
                ConversationKey::new(ctx.platform, ctx.chat_id, ctx.resolved_cli.name());

            // Find requests with failed outbox
            let rows = repo
                .list_active_requests(&conversation, 20)
                .await
                .unwrap_or_default();
            let stuck: Vec<_> = rows
                .iter()
                .filter(|r| r.status.is_terminal() && r.outbox_status == Some(OutboxStatus::Failed))
                .collect();

            if stuck.is_empty() {
                return Some("✅ 没有需要重试的消息。".into());
            }

            if arg == "dismiss" || arg == "clear" {
                let mut dismissed = 0usize;
                for row in &stuck {
                    if repo.dismiss_failed_outbox(&row.request_id).await.is_ok() {
                        dismissed += 1;
                    }
                }
                return Some(format!("🧹 已清除 {dismissed} 个失败消息。"));
            }

            // Support `/retry 1` to dismiss a specific item by index
            if let Ok(idx) = arg.parse::<usize>() {
                if idx >= 1 && idx <= stuck.len() {
                    let row = stuck[idx - 1];
                    match repo.dismiss_failed_outbox(&row.request_id).await {
                        Ok(()) => {
                            return Some(format!(
                                "🧹 已清除: {}",
                                truncate_text(&row.text_preview, 40)
                            ));
                        }
                        Err(e) => return Some(format!("⚠️ 清除失败: {e}")),
                    }
                } else {
                    return Some(format!("❌ 序号 {idx} 超出范围 (1-{})", stuck.len()));
                }
            }

            let mut lines = vec![format!("📬 **待重试消息** ({} 个)", stuck.len())];
            for (i, row) in stuck.iter().enumerate() {
                lines.push(format!(
                    "[{}] 📬 {} | {}",
                    i + 1,
                    truncate_text(&row.text_preview, 40),
                    row.outbox_error_message
                        .as_deref()
                        .unwrap_or("unknown error"),
                ));
            }
            lines.push("\n`/retry dismiss` 清除全部 | `/retry 1` 清除指定".into());
            Some(lines.join("\n"))
        }

        "/usage" => {
            let store = require_store!(ctx);
            let today = store
                .get_usage_today(ctx.platform, ctx.user_id)
                .await
                .unwrap_or_default();
            let total = store
                .get_usage_total(ctx.platform, ctx.user_id)
                .await
                .unwrap_or_default();
            let fmt_extra = |s: &crate::store::UsageSummary| {
                let mut parts = Vec::new();
                if s.cache_read_input_tokens > 0 || s.cached_input_tokens > 0 {
                    parts.push(format!(
                        "cache read {}",
                        format_tokens(s.cache_read_input_tokens.max(s.cached_input_tokens))
                    ));
                }
                if s.cache_creation_input_tokens > 0 {
                    parts.push(format!(
                        "cache create {}",
                        format_tokens(s.cache_creation_input_tokens)
                    ));
                }
                if s.reasoning_output_tokens > 0 {
                    parts.push(format!(
                        "reasoning {}",
                        format_tokens(s.reasoning_output_tokens)
                    ));
                }
                if s.total_tokens > 0 {
                    parts.push(format!("total {}", format_tokens(s.total_tokens)));
                }
                if let Some(ctx) = s.context_window
                    && ctx > 0
                {
                    parts.push(format!("ctx {}", format_tokens(ctx)));
                }
                if s.cost_usd > 0.0 {
                    parts.push(format!("${:.4}", s.cost_usd));
                }
                if parts.is_empty() {
                    "—".to_string()
                } else {
                    parts.join(" · ")
                }
            };
            Some(format!(
                "📊 **用量统计**\n\n\
                 **今日**\n\
                 - 消息: {}\n\
                 - Token: ↓{} ↑{}\n\
                 - 细分: {}\n\
                 - 工具: {}\n\n\
                 **累计**\n\
                 - 消息: {}\n\
                 - Token: ↓{} ↑{}\n\
                 - 细分: {}\n\
                 - 工具: {}",
                today.messages,
                format_tokens(today.tokens_prompt),
                format_tokens(today.tokens_completion),
                fmt_extra(&today),
                today.tool_calls,
                total.messages,
                format_tokens(total.tokens_prompt),
                format_tokens(total.tokens_completion),
                fmt_extra(&total),
                total.tool_calls,
            ))
        }

        "/workspace" | "/ws" => {
            // /ws ls | /ws list — list discovered projects (no store needed)
            if arg == "ls" || arg == "list" {
                let projects = crate::workspace::discover_all_projects(ctx.project_dirs);
                if projects.is_empty() {
                    return Some("📂 没有发现项目。配置 `project_dirs` 后重试。".into());
                }
                let mut lines = vec![format!("📂 **可用项目** ({} 个)", projects.len())];
                for p in &projects {
                    lines.push(format!("  {}", p.summary()));
                }
                lines.push("\n用 `/ws <项目名>` 切换".into());
                return Some(lines.join("\n"));
            }

            let store = require_store!(ctx);
            if arg.is_empty() {
                let ws = store
                    .get_user_preference(ctx.platform, ctx.user_id, "workspace")
                    .await
                    .ok()
                    .flatten();
                return Some(format!(
                    "📂 当前工作目录: `{}`",
                    ws.as_deref().unwrap_or("(默认)")
                ));
            }

            // Try name-based fuzzy match against discovered projects
            let projects = crate::workspace::discover_all_projects(ctx.project_dirs);
            let arg_lower = arg.to_lowercase();
            let matches: Vec<_> = projects
                .iter()
                .filter(|p| {
                    p.name.eq_ignore_ascii_case(arg) || p.name.to_lowercase().contains(&arg_lower)
                })
                .collect();
            let target = match matches.len() {
                1 => matches[0].path.clone(),
                0 => {
                    // No project match — fall through to path-based logic
                    if arg.starts_with('~') {
                        let home = std::env::var("HOME").unwrap_or_default();
                        arg.replacen('~', &home, 1)
                    } else {
                        arg.to_string()
                    }
                }
                _ => {
                    let names: Vec<_> = matches
                        .iter()
                        .map(|p| format!("  {}", p.summary()))
                        .collect();
                    return Some(format!(
                        "⚠️ 多个匹配:\n{}\n请更精确指定。",
                        names.join("\n")
                    ));
                }
            };

            let path = std::path::Path::new(&target);
            if !path.is_dir() {
                return Some(format!("❌ 目录不存在: `{target}`"));
            }
            if let Some(denial) = slash_denial(ctx, ActionCapability::WorkspaceMutation) {
                return Some(denial);
            }
            if let Err(denial) = ctx.config.action_policy.workspace_allowed(path) {
                return Some(denial);
            }
            let canonical = path
                .canonicalize()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or(target);
            match store
                .set_user_preference(ctx.platform, ctx.user_id, "workspace", &canonical)
                .await
            {
                Ok(()) => Some(format!("📂 工作目录已切换: `{canonical}`")),
                Err(e) => Some(format!("⚠️ 工作目录设置失败: {e}")),
            }
        }

        "/gateway" => {
            let cli_name = ctx.resolved_cli.name();
            let caps = ctx.resolved_cli.capabilities();
            let has_store = ctx.store.is_some();

            let model = ctx
                .resolved_cli
                .model_name()
                .or(ctx.config.astra.default_model.as_deref())
                .unwrap_or("default");

            let workspace = if let Some(store) = ctx.store {
                store
                    .get_user_preference(ctx.platform, ctx.user_id, "workspace")
                    .await
                    .ok()
                    .flatten()
            } else {
                None
            };

            let mut lines = vec![
                "🌐 **Gateway Context**".to_string(),
                String::new(),
                "**Identity**".to_string(),
                format!("- Platform: `{}`", ctx.platform),
                format!("- User: `{}`", ctx.user_id),
                format!("- CLI: `{cli_name}`"),
                format!("- Model: `{model}`"),
                format!(
                    "- Storage: {}",
                    if has_store { "✅ active" } else { "❌ none" }
                ),
                format!(
                    "- Workspace: `{}`",
                    workspace.as_deref().unwrap_or("(default)")
                ),
                String::new(),
                "**Capabilities**".to_string(),
                format!(
                    "- Session management: {}",
                    if caps.supports_session { "✅" } else { "❌" }
                ),
                format!(
                    "- Model switching: {}",
                    if caps.supports_model_switch {
                        "✅"
                    } else {
                        "❌"
                    }
                ),
                format!(
                    "- Harness monitoring: {}",
                    if caps.supports_harness { "✅" } else { "❌" }
                ),
                format!("- Cron/scheduling: {}", if has_store { "✅" } else { "❌" }),
                format!(
                    "- Tool execution: {}",
                    if caps.supports_tools { "✅" } else { "❌" }
                ),
            ];

            lines.push(String::new());
            lines.push("**Commands**".to_string());
            lines.push("| Command | Description |".to_string());
            lines.push("|---------|-------------|".to_string());
            lines.push("| `/new` | Reset conversation |".to_string());
            lines.push("| `/status` | Status + harness |".to_string());
            lines.push("| `/model <name>` | Switch model |".to_string());
            lines.push("| `/cli <name>` | Switch CLI backend |".to_string());
            lines.push("| `/reasoning on\\|off` | Toggle reasoning blocks |".to_string());
            lines.push("| `/ws ls` | List projects |".to_string());
            lines.push("| `/ws <name>` | Switch workspace |".to_string());
            lines.push("| `/session list` | Session history |".to_string());
            lines.push("| `/cron list` | Scheduled tasks |".to_string());
            lines.push("| `/task list` | Scheduled tasks/reminders |".to_string());
            lines.push("| `/running` | Active requests (numbered) |".to_string());
            lines.push("| `/cancel [N\\|text]` | Cancel queued request |".to_string());
            lines
                .push("| `/esc [N\\|text]` | Interrupt running turn (`/kill` alias) |".to_string());
            lines.push("| `/retry` | View/dismiss failed outbox |".to_string());
            lines.push("| `/manage [hint]` | AI-assisted task management |".to_string());
            lines.push("| `/trace [id]` | Request trace |".to_string());
            lines.push("| `/usage` | Token/cost stats |".to_string());
            lines.push("| `/inspect` | Harness details |".to_string());
            lines.push("| `/audit` | Decision chain |".to_string());
            lines.push("| `/auth` | Auth status + reset + auto-relogin |".to_string());
            lines.push("| `/gateway` | This context dump |".to_string());

            lines.push(String::new());
            lines.push("**MCP Tools**".to_string());
            lines.push("| Tool | Description |".to_string());
            lines.push("|------|-------------|".to_string());
            lines.push("| `gateway_cron_list` | List scheduled tasks/reminders |".to_string());
            lines.push("| `gateway_cron_add` | Create recurring scheduled task |".to_string());
            lines
                .push("| `gateway_cron_delete` | Delete scheduled task by ID prefix |".to_string());
            lines.push(
                "| `gateway_remind_after` | Create one-time reminder or scheduled exec |"
                    .to_string(),
            );
            lines.push("| `gateway_skills_list` | List saved reusable skills |".to_string());
            lines.push("| `gateway_skills_read` | Read saved skill content |".to_string());
            lines.push("| `gateway_skills_add` | Save reusable skill |".to_string());
            lines.push("| `gateway_skills_delete` | Delete saved skill |".to_string());
            lines.push("| `gateway_workspace_current` | Show current workspace |".to_string());
            lines.push("| `gateway_workspace_list` | List available workspaces |".to_string());
            lines.push("| `gateway_workspace_switch` | Switch workspace |".to_string());
            lines
                .push("| `gateway_send_attachment` | Send a local file to this chat |".to_string());

            if let Some(store) = ctx.store {
                let cron_jobs = store
                    .list_cron_jobs(ctx.platform, ctx.chat_id)
                    .await
                    .unwrap_or_default();
                if !cron_jobs.is_empty() {
                    lines.push(String::new());
                    lines.push(format!("**Scheduled Tasks** ({} 个)", cron_jobs.len()));
                    for j in &cron_jobs {
                        let short = &j.job_id[..8.min(j.job_id.len())];
                        lines.push(format!(
                            "- `{short}` | `{}` | {}",
                            j.cron_expr, j.description
                        ));
                    }
                }
            }

            Some(lines.join("\n"))
        }

        _ => None,
    }
}

async fn handle_approval_response_command(
    ctx: &CommandContext<'_>,
    arg: &str,
    is_deny: bool,
) -> String {
    let Some(pool) = ctx.codex_app_pool else {
        return "⚠️ 当前 CLI 不支持审批响应。".to_string();
    };

    if !arg.trim().is_empty() {
        return if is_deny {
            "用法: `/deny`".to_string()
        } else {
            "用法: `/approve`".to_string()
        };
    }

    let decision = if is_deny { "deny" } else { "allow_once" };
    match pool
        .lock()
        .await
        .respond_current_approval(ctx.chat_id, decision)
        .await
    {
        Ok(()) => {
            if is_deny {
                "🚫 已拒绝当前操作。".to_string()
            } else {
                "✅ 已批准当前操作。".to_string()
            }
        }
        Err(e) => format!("⚠️ 审批处理失败: {e}"),
    }
}

fn parse_cron_add(input: &str) -> Option<(String, String)> {
    let input = input.trim();
    // Try quoted: /cron add "0 9 * * *" message
    if let Some(after_quote) = input.strip_prefix('"')
        && let Some(end) = after_quote.find('"')
    {
        let expr = after_quote[..end].to_string();
        let msg = after_quote[end + 1..].trim().to_string();
        if !expr.is_empty() && !msg.is_empty() && store::is_valid_cron_expr(&expr) {
            return Some((expr, msg));
        }
    }
    // Try unquoted: first 5 space-separated tokens are cron, rest is message
    let parts: Vec<&str> = input.splitn(6, ' ').collect();
    if parts.len() >= 6 {
        let expr = parts[..5].join(" ");
        let msg = parts[5].to_string();
        if !msg.is_empty() && store::is_valid_cron_expr(&expr) {
            return Some((expr, msg));
        }
    }
    None
}

fn slash_denial(ctx: &CommandContext<'_>, capability: ActionCapability) -> Option<String> {
    ctx.config
        .action_policy
        .check(ActionSource::SlashCommand, capability)
        .err()
}

async fn resolve_trace_selector(
    repo: &dyn TraceRepository,
    conversation: &ConversationKey,
    selector: &str,
) -> Option<TraceId> {
    let traces = repo.list_recent_traces(conversation, 50).await.ok()?;
    traces
        .into_iter()
        .find(|trace| {
            trace.trace_id.as_str() == selector
                || trace.request_id.as_str() == selector
                || trace.trace_id.as_str().starts_with(selector)
                || trace.request_id.as_str().starts_with(selector)
        })
        .map(|trace| trace.trace_id)
        .or_else(|| Some(TraceId::from_string(selector.to_string())))
}

/// Virtual CLI profile name used to route `/manage` requests into their
/// own conversation worker / queue. Physically the same real CLI profile
/// runs (`resolve_cli_profile` still picks the user's actual CLI at
/// execution time), but the `ConversationKey` differs so `/manage` does
/// NOT queue behind the user's normal tasks — the whole point of
/// `/manage` is to inspect/fix a stuck queue, so it must not join it.
pub const MANAGE_CLI_PROFILE: &str = "_manage";

/// Return true if `created_at` (as reported by the DB — various formats)
/// is before `gateway_start`. Such requests can't progress on this
/// process — their cancellation tokens and subprocess state died with
/// the previous gateway lifecycle.
///
/// Unparseable timestamps return false (conservative — better to
/// under-flag zombies than to scare operators about timestamp format
/// drift).
pub fn is_zombie_request(created_at: &str, gateway_start: chrono::DateTime<chrono::Utc>) -> bool {
    let parsed = chrono::NaiveDateTime::parse_from_str(created_at, "%Y-%m-%d %H:%M:%S%.f")
        .map(|dt| dt.and_utc())
        .or_else(|_| {
            chrono::DateTime::parse_from_rfc3339(created_at)
                .map(|dt| dt.with_timezone(&chrono::Utc))
        });
    match parsed {
        Ok(ts) => ts < gateway_start,
        Err(_) => false,
    }
}

/// Sweep every active request in `conversation`: force-fail in the DB,
/// cancel the in-memory cancellation token (persistent CLIs translate that
/// to native Esc/interrupt), and remove it from `active_requests`. Returns a
/// user-facing summary with the count interrupted + the count of stale DB
/// rows whose process was already gone (no token in the map — typical
/// zombie case).
///
/// Shared by `/esc all`, `/kill all`, and `/cancel all` since all clear
/// thing for already-running requests.
async fn kill_or_cancel_all(
    repo: &dyn TraceRepository,
    conversation: &ConversationKey,
    active_requests: Option<&dashmap::DashMap<String, tokio_util::sync::CancellationToken>>,
    reason: &str,
) -> String {
    let rows = match repo.list_active_requests(conversation, 200).await {
        Ok(r) => r,
        Err(e) => return format!("⚠️ 清理失败: {e}"),
    };
    if rows.is_empty() {
        return "✅ 当前会话没有活跃请求 (0 个清理)。".into();
    }
    let mut db_cleared = 0usize;
    let mut live_interrupted = 0usize;
    let mut zombie_cleared = 0usize;
    for row in &rows {
        match repo.force_fail_request(&row.trace_id, reason).await {
            Ok(true) => db_cleared += 1,
            Ok(false) => {
                // Already terminal — likely an outbox-failed retry; count
                // it as zombie so the user sees we didn't ignore it.
                zombie_cleared += 1;
            }
            Err(e) => {
                tracing::warn!(
                    target: "gateway::commands",
                    trace_id = %row.trace_id.as_str(),
                    error = %e,
                    "force_fail_request failed during /esc all"
                );
            }
        }
        if let Some(tasks) = active_requests
            && let Some((_, token)) = tasks.remove(row.trace_id.as_str())
        {
            token.cancel();
            live_interrupted += 1;
        }
    }
    let mut out = format!("⎋ 已中断 {db_cleared} 个请求");
    if live_interrupted > 0 {
        out.push_str(&format!(" (实时中断 {live_interrupted})"));
    }
    if zombie_cleared > 0 {
        out.push_str(&format!("，清理 🧟 僵尸 {zombie_cleared} 个"));
    }
    out
}

/// - Numeric index "1", "2" (from `/running` output order)
/// - Trace ID or prefix match
/// - Text substring match against message content
async fn resolve_active_request(
    repo: &dyn TraceRepository,
    conversation: &ConversationKey,
    selector: &str,
) -> Option<ActiveRequestSummary> {
    let rows = repo.list_active_requests(conversation, 50).await.ok()?;

    let is_numeric = selector.chars().all(|c| c.is_ascii_digit()) && !selector.is_empty();

    // Try numeric index first (1-based)
    if is_numeric {
        if let Ok(idx) = selector.parse::<usize>()
            && idx >= 1
            && idx <= rows.len()
        {
            return Some(rows[idx - 1].clone());
        }
        // Short pure numbers are task indices only; longer numeric strings may
        // be valid UUID prefixes.
        if selector.len() < 4 {
            return None;
        }
    }

    // Try trace ID or request ID prefix match (only for non-numeric selectors)
    if let Some(row) = rows.iter().find(|r| {
        r.trace_id.as_str() == selector
            || r.trace_id.as_str().starts_with(selector)
            || r.request_id.as_str() == selector
            || r.request_id.as_str().starts_with(selector)
    }) {
        return Some(row.clone());
    }

    // Try text fuzzy match
    let lower = selector.to_lowercase();
    rows.into_iter()
        .find(|r| r.text_preview.to_lowercase().contains(&lower))
}

fn status_icon(status: &str) -> &'static str {
    match status {
        "running" => "\u{1f504}",
        "queued" => "\u{231b}",
        "completed" => "\u{2705}",
        "failed" => "\u{274c}",
        "reply_retrying" => "\u{1f4ec}",
        "reply_pending" => "\u{1f4e4}",
        _ => "\u{2753}",
    }
}

/// Format timestamp: strip date prefix if it's today.
fn short_timestamp(ts: &str) -> &str {
    // created_at is typically "YYYY-MM-DD HH:MM:SS.ffffff" or similar.
    // If it starts with today's date, strip to just time.
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    if let Some(rest) = ts.strip_prefix(&today) {
        rest.trim_start_matches([' ', 'T'])
    } else {
        ts
    }
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() > max_chars {
        format!("{}…", text.chars().take(max_chars).collect::<String>())
    } else {
        text.to_string()
    }
}

fn format_trace_events(trace_id: &TraceId, events: Vec<GatewayEvent>) -> String {
    let mut lines = vec![format!("🧭 **Trace `{}`**", short_id(trace_id.as_str()))];
    for event in events {
        let payload = compact_event_payload(event.kind, &event.payload);
        lines.push(format!(
            "- #{} `{}` {} {}",
            event.sequence,
            event.kind.as_str(),
            event.created_at,
            payload
        ));
    }
    lines.join("\n")
}

fn compact_event_payload(kind: GatewayEventKind, payload: &serde_json::Value) -> String {
    match kind {
        GatewayEventKind::RequestQueued => payload["queue_depth"]
            .as_u64()
            .map(|depth| format!("depth={depth}"))
            .unwrap_or_default(),
        GatewayEventKind::RunStarted => payload["session_id"]
            .as_str()
            .map(|sid| format!("session={}", short_id(sid)))
            .unwrap_or_default(),
        GatewayEventKind::RunFailed
        | GatewayEventKind::RequestFailed
        | GatewayEventKind::OutboxFailed => payload["error"]
            .as_str()
            .unwrap_or("")
            .chars()
            .take(80)
            .collect(),
        GatewayEventKind::OutboxQueued => payload["outbox_id"]
            .as_str()
            .map(|id| format!("outbox={}", short_id(id)))
            .unwrap_or_default(),
        GatewayEventKind::OutboxSent => "sent".into(),
        _ => String::new(),
    }
}

fn short_id(id: &str) -> String {
    if id.len() <= 8 {
        id.to_string()
    } else {
        format!("{}…", &id[..8])
    }
}

// ─── Harness snapshot ───────────────────────────────────────────────────────

struct HarnessSnapshot {
    // Identity
    session_id: String,
    turn_number: u32,
    model: Option<String>,
    // Context
    context_utilization: Option<f32>,
    context_message_count: u32,
    context_total_tokens: Option<u32>,
    // Budget
    turns_used: u32,
    turns_limit: Option<u32>,
    tokens_used: u64,
    tokens_prompt: u64,
    tokens_completion: u64,
    tokens_cache_read: u64,
    elapsed_ms: u64,
    // Tools
    tool_calls: u32,
    unique_tools: Vec<String>,
    last_tool: Option<String>,
    consecutive_same_tool: u32,
    // Delegation
    #[allow(dead_code)]
    delegations: u32,
    // Errors
    consecutive_errors: u32,
}

impl HarnessSnapshot {
    fn turns_limit_str(&self) -> String {
        self.turns_limit
            .map(|l| l.to_string())
            .unwrap_or_else(|| "∞".into())
    }
    fn utilization_pct(&self) -> String {
        self.context_utilization
            .map(|u| format!("{:.0}%", u * 100.0))
            .unwrap_or_else(|| "—".into())
    }
    fn cost_estimate_usd(&self) -> f64 {
        // Rough estimate: $3/M input, $15/M output (Sonnet-class pricing)
        (self.tokens_prompt as f64 * 3.0 + self.tokens_completion as f64 * 15.0) / 1_000_000.0
    }
    fn tool_summary(&self) -> String {
        if self.unique_tools.is_empty() {
            return if self.tool_calls > 0 {
                "详情已脱敏".into()
            } else {
                "—".into()
            };
        }
        self.unique_tools
            .iter()
            .take(5)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    }
    fn warnings(&self) -> Vec<String> {
        let mut w = Vec::new();
        if self.consecutive_same_tool > 2 {
            w.push(format!(
                "⚠️ 重复工具 {}次: {}",
                self.consecutive_same_tool,
                self.last_tool.as_deref().unwrap_or("详情已脱敏")
            ));
        }
        if let Some(u) = self.context_utilization
            && u > 0.85
        {
            w.push(format!("⚠️ Context 使用率 {:.0}%，接近上限", u * 100.0));
        }
        if self.consecutive_errors > 1 {
            w.push(format!("⚠️ 连续 {} 次错误", self.consecutive_errors));
        }
        w
    }

    fn format_full(&self) -> String {
        let mut lines = vec![
            format!(
                "🔭 **Harness — Session `{}`**",
                &self.session_id[..8.min(self.session_id.len())]
            ),
            String::new(),
            format!(
                "**状态** Turn {}/{} | {} | 🔧 {}",
                self.turns_used,
                self.turns_limit_str(),
                format_duration(self.elapsed_ms),
                self.tool_calls
            ),
            String::new(),
            format!(
                "**Token** ↓{} ↑{} 缓存↩{} | 总{}",
                format_tokens(self.tokens_prompt),
                format_tokens(self.tokens_completion),
                format_tokens(self.tokens_cache_read),
                format_tokens(self.tokens_used)
            ),
            format!(
                "**Context** {} msgs | {} | {}",
                self.context_message_count,
                self.utilization_pct(),
                self.context_total_tokens
                    .map(|t| format_tokens(t as u64))
                    .unwrap_or_else(|| "—".into())
            ),
            format!("**成本** ~${:.4}", self.cost_estimate_usd()),
        ];
        if let Some(ref model) = self.model {
            lines.push(format!("**模型** `{model}`"));
        }
        if self.tool_calls > 0 {
            lines.push(format!(
                "**工具** {} ({})",
                self.tool_calls,
                self.tool_summary()
            ));
        }
        let warnings = self.warnings();
        if !warnings.is_empty() {
            lines.push(String::new());
            for w in &warnings {
                lines.push(w.clone());
            }
        }
        lines.join("\n")
    }
}

fn format_audit_history(history: Vec<HarnessSnapshot>) -> String {
    let mut latest_by_turn = std::collections::BTreeMap::new();
    for snap in history {
        // The harness history endpoint returns newest-first snapshots.
        latest_by_turn.entry(snap.turn_number).or_insert(snap);
    }

    let turns: Vec<_> = latest_by_turn.into_values().rev().take(10).collect();
    let mut turns: Vec<_> = turns.into_iter().rev().collect();
    let mut lines = vec![format!("📋 **审计记录** (最近 {} 轮)", turns.len())];
    for snap in turns.drain(..) {
        lines.push(format!(
            "**Turn {}** ↓{} ↑{} | 🔧 {} ({}) | ctx:{} | ${:.4}",
            snap.turn_number,
            format_tokens(snap.tokens_prompt),
            format_tokens(snap.tokens_completion),
            snap.tool_calls,
            snap.tool_summary(),
            snap.utilization_pct(),
            snap.cost_estimate_usd(),
        ));
        for w in snap.warnings() {
            lines.push(format!("  {w}"));
        }
    }
    lines.join("\n")
}

async fn fetch_harness_snapshot(
    astra: &astra::Client,
    session_id: &str,
    api_key: &str,
) -> Option<HarnessSnapshot> {
    let path = format!("/sessions/{session_id}/harness/snapshot");
    let text = astra
        .get_bearer_path_query_text(api_key, &path, &[])
        .await
        .ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    Some(parse_harness_snapshot(&v, session_id))
}

async fn fetch_harness_history(
    astra: &astra::Client,
    session_id: &str,
    api_key: &str,
    n: usize,
) -> Vec<HarnessSnapshot> {
    let path = format!("/sessions/{session_id}/harness/history?n={n}");
    let text = match astra.get_bearer_path_query_text(api_key, &path, &[]).await {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    let v: serde_json::Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    v.as_array()
        .map(|arr| {
            arr.iter()
                .map(|s| parse_harness_snapshot(s, session_id))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_harness_snapshot(v: &serde_json::Value, session_id: &str) -> HarnessSnapshot {
    HarnessSnapshot {
        session_id: v["session_id"].as_str().unwrap_or(session_id).to_string(),
        turn_number: v["turn_number"].as_u64().unwrap_or(0) as u32,
        model: v["model"].as_str().map(String::from),
        context_utilization: v["context_utilization"].as_f64().map(|u| u as f32),
        context_message_count: v["context_message_count"].as_u64().unwrap_or(0) as u32,
        context_total_tokens: v["context_total_tokens"].as_u64().map(|t| t as u32),
        turns_used: v["turns_used"].as_u64().unwrap_or(0) as u32,
        turns_limit: v["turns_limit"].as_u64().map(|l| l as u32),
        tokens_used: v["tokens_used_session"].as_u64().unwrap_or(0),
        tokens_prompt: v["tokens_prompt"].as_u64().unwrap_or(0),
        tokens_completion: v["tokens_completion"].as_u64().unwrap_or(0),
        tokens_cache_read: v["tokens_cache_read"].as_u64().unwrap_or(0),
        elapsed_ms: v["elapsed_millis"].as_u64().unwrap_or(0),
        tool_calls: v["tool_calls_this_session"].as_u64().unwrap_or(0) as u32,
        unique_tools: v["unique_tools_used"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|t| t.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        last_tool: v["last_tool_called"].as_str().map(String::from),
        consecutive_same_tool: v["consecutive_same_tool"].as_u64().unwrap_or(0) as u32,
        delegations: v["delegations_this_turn"].as_u64().unwrap_or(0) as u32,
        consecutive_errors: v["consecutive_errors"].as_u64().unwrap_or(0) as u32,
    }
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

fn format_session_usage_lines(usage: &store::UsageSummary) -> Vec<String> {
    let cache_read = usage.cache_read_input_tokens.max(usage.cached_input_tokens);
    let cache_denom = usage
        .tokens_prompt
        .saturating_add(usage.cache_creation_input_tokens)
        .saturating_add(cache_read);
    let cache_hit = if cache_denom > 0 {
        format!("{:.1}%", cache_read as f64 * 100.0 / cache_denom as f64)
    } else {
        "—".to_string()
    };

    let mut lines = vec![
        format!("- 轮次: {}", usage.messages),
        format!(
            "- Token: ↓{} ↑{} total {}",
            format_tokens(usage.tokens_prompt),
            format_tokens(usage.tokens_completion),
            format_tokens(usage.total_tokens)
        ),
        format!(
            "- Cache: read {} create {} hit {}",
            format_tokens(cache_read),
            format_tokens(usage.cache_creation_input_tokens),
            cache_hit
        ),
    ];
    if usage.reasoning_output_tokens > 0 {
        lines.push(format!(
            "- Reasoning: {}",
            format_tokens(usage.reasoning_output_tokens)
        ));
    }
    if let Some(window) = usage.context_window
        && window > 0
    {
        lines.push(format!("- Context window: {}", format_tokens(window)));
    }
    if usage.cost_usd > 0.0 {
        lines.push(format!("- Cost: ${:.4}", usage.cost_usd));
    }
    if usage.tool_calls > 0 {
        lines.push(format!("- 工具: {}", usage.tool_calls));
    }
    lines
}

fn format_duration(ms: u64) -> String {
    if ms >= 60_000 {
        format!("{}m {}s", ms / 60_000, (ms % 60_000) / 1000)
    } else if ms >= 1_000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

/// An entry in the `/model` selection list.
#[derive(Clone)]
pub(crate) struct ModelEntry {
    label: String,
    desc: String,
    /// Full model identifier, or `None` to mean "follow default".
    full_id: Option<String>,
    /// Backend model id resolved by probing the CLI. This is populated for
    /// default rows where `full_id` intentionally remains `None`.
    resolved_id: Option<String>,
    /// Extra shorthand aliases for resolution (e.g. ["deepseek", "ds"]).
    aliases: Vec<String>,
}

impl ModelEntry {
    fn matches_current(&self, current: Option<&str>) -> bool {
        match (self.full_id.as_deref(), current) {
            (None, None) => true,
            (Some(id), Some(cur)) => id == cur,
            _ => false,
        }
    }
}

async fn model_entries_for_context(
    ctx: &CommandContext<'_>,
    arg: &str,
    force_refresh: bool,
) -> Result<ModelEntriesResult, String> {
    let trimmed = arg.trim();
    if trimmed == "默认" || trimmed.eq_ignore_ascii_case("default") {
        return Ok(ModelEntriesResult {
            entries: vec![default_model_entry()],
            cache_age: None,
        });
    }

    let Some(cache_key) = model_entries_cache_key(ctx) else {
        return Ok(ModelEntriesResult {
            entries: model_entries_uncached(ctx).await?,
            cache_age: None,
        });
    };
    if force_refresh {
        MODEL_ENTRY_CACHE.lock().await.remove(&cache_key);
    } else if let Some(cached) = MODEL_ENTRY_CACHE.lock().await.get(&cache_key).cloned() {
        let age = cached.created_at.elapsed();
        tracing::debug!(
            cli = ctx.resolved_cli.name(),
            age_ms = age.as_millis(),
            "using cached model entries"
        );
        return Ok(ModelEntriesResult {
            entries: cached.entries,
            cache_age: Some(age),
        });
    }

    let entries = model_entries_uncached(ctx).await?;
    MODEL_ENTRY_CACHE.lock().await.insert(
        cache_key,
        ModelEntryCacheValue {
            entries: entries.clone(),
            created_at: Instant::now(),
        },
    );
    Ok(ModelEntriesResult {
        entries,
        cache_age: None,
    })
}

pub(crate) async fn current_resolved_model_id_for_context(
    ctx: &CommandContext<'_>,
) -> Result<Option<String>, String> {
    let model_list = model_entries_for_context(ctx, "", false).await?;
    let resolved =
        resolved_model_id_from_entries(ctx.resolved_cli.model_name(), &model_list.entries)
            .or_else(|| ctx.config.astra.default_model.clone())
            .or_else(|| ctx.config.cli.model_name().map(str::to_string));
    Ok(resolved)
}

fn resolved_model_id_from_entries(
    current_model: Option<&str>,
    entries: &[ModelEntry],
) -> Option<String> {
    if current_model.is_none()
        && let Some(default) = entries.iter().find(|entry| entry.full_id.is_none())
    {
        return default.resolved_id.clone();
    }

    let current = current_model?;
    match resolve_model_input(current, entries) {
        ResolvedModel::Id(id) => Some(id),
        ResolvedModel::Default => entries
            .iter()
            .find(|entry| entry.full_id.is_none())
            .and_then(|entry| entry.resolved_id.clone()),
        ResolvedModel::Unrecognized if looks_like_configured_model_id(current) => {
            Some(current.into())
        }
        ResolvedModel::Unrecognized => None,
    }
}

async fn model_entries_uncached(ctx: &CommandContext<'_>) -> Result<Vec<ModelEntry>, String> {
    match ctx.resolved_cli {
        crate::cli_bridge::CliProfile::Astra { .. } => astra_model_entries(ctx.resolved_cli).await,
        crate::cli_bridge::CliProfile::Claude { .. } => claude_model_entries(ctx).await,
        crate::cli_bridge::CliProfile::Codex { .. } => codex_model_entries(ctx.resolved_cli).await,
        _ => Ok(vec![default_model_entry()]),
    }
}

fn model_entries_cache_key(ctx: &CommandContext<'_>) -> Option<String> {
    match ctx.resolved_cli {
        crate::cli_bridge::CliProfile::Claude { .. } => {
            let base = base_profile_for_model_probe(ctx);
            Some(format!(
                "claude:{}:{}",
                cli_profile_model_cache_key(&base),
                provider_model_cache_key(ctx.resolved_provider_config)
            ))
        }
        crate::cli_bridge::CliProfile::Codex { .. } => Some(format!(
            "codex:{}",
            cli_profile_model_cache_key(ctx.resolved_cli)
        )),
        crate::cli_bridge::CliProfile::Astra { .. } => Some(format!(
            "astra:{}",
            cli_profile_model_cache_key(ctx.resolved_cli)
        )),
        _ => None,
    }
}

fn cli_profile_model_cache_key(profile: &crate::cli_bridge::CliProfile) -> String {
    match profile {
        crate::cli_bridge::CliProfile::Astra {
            bin,
            model,
            permission_mode,
            app_server_url,
        } => format!(
            "bin={bin};model={model:?};permission={permission_mode};app_server={app_server_url:?}"
        ),
        crate::cli_bridge::CliProfile::Claude {
            bin,
            model,
            stream_json,
            extra_args,
            env,
            env_file,
        } => format!(
            "bin={bin};model={model:?};stream={stream_json};extra={extra_args:?};env_file={env_file:?};env={}",
            env_cache_key(env)
        ),
        crate::cli_bridge::CliProfile::Codex {
            bin,
            model,
            sandbox,
            stream_json,
            extra_args,
            skip_git_repo_check,
            ephemeral,
        } => format!(
            "bin={bin};model={model:?};sandbox={sandbox};stream={stream_json};extra={extra_args:?};skip_git={skip_git_repo_check};ephemeral={ephemeral}"
        ),
        crate::cli_bridge::CliProfile::Copilot {
            bin,
            model,
            env,
            env_file,
            launcher,
            stream_json,
            allow_all_tools,
            extra_args,
        } => format!(
            "bin={bin};model={model:?};stream={stream_json};allow_all={allow_all_tools};extra={extra_args:?};env_file={env_file:?};launcher={launcher:?};env={}",
            env_cache_key(env)
        ),
        crate::cli_bridge::CliProfile::Custom {
            bin,
            args_template,
            json_output,
            session_id_field,
            text_field,
        } => format!(
            "bin={bin};args={args_template:?};json={json_output};session={session_id_field:?};text={text_field:?}"
        ),
    }
}

fn provider_model_cache_key(pc: Option<&crate::config::ProviderConfig>) -> String {
    match pc {
        Some(pc) => format!(
            "enabled={};env_file={:?};env={}",
            pc.enabled,
            pc.env_file,
            env_cache_key(&pc.env)
        ),
        None => "none".into(),
    }
}

fn env_cache_key(env: &std::collections::BTreeMap<String, String>) -> String {
    env.iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(";")
}

fn default_model_entry() -> ModelEntry {
    ModelEntry {
        label: "默认".into(),
        desc: "跟随配置".into(),
        full_id: None,
        resolved_id: None,
        aliases: vec![],
    }
}

#[derive(serde::Deserialize)]
struct AstraModelListItem {
    name: String,
    provider: Option<String>,
    description: Option<String>,
    #[serde(default)]
    is_active: bool,
    context_window: Option<u64>,
}

async fn astra_model_entries(
    profile: &crate::cli_bridge::CliProfile,
) -> Result<Vec<ModelEntry>, String> {
    let crate::cli_bridge::CliProfile::Astra { bin, .. } = profile else {
        return Ok(Vec::new());
    };

    let mut command = tokio::process::Command::new(bin);
    command.arg("model").arg("list");
    if let Some(url) = profile
        .app_server_url()
        .filter(|url| !url.trim().is_empty())
    {
        command.env("ASTRA_API_URL", url);
    }
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .map_err(|_| format!("`{bin} model list` 超时"))?
        .map_err(|e| format!("运行 `{bin} model list`: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "`{bin} model list` 退出码 {}: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }

    let items: Vec<AstraModelListItem> = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("解析 `{bin} model list` 输出: {e}"))?;

    let mut entries = vec![default_model_entry()];
    for item in items.into_iter().filter(|m| m.is_active) {
        let provider = item.provider.unwrap_or_else(|| "unknown".into());
        let mut desc = provider;
        if let Some(window) = item.context_window {
            desc.push_str(&format!(" · ctx {}", format_context_window(window)));
        }
        if let Some(description) = item.description.filter(|d| !d.trim().is_empty()) {
            desc.push_str(" · ");
            desc.push_str(description.trim());
        }
        let aliases = astra_model_aliases(&item.name);
        entries.push(ModelEntry {
            label: item.name.clone(),
            desc,
            full_id: Some(item.name),
            resolved_id: None,
            aliases,
        });
    }

    if entries.len() == 1 {
        return Err("Astra 返回的 active 模型列表为空".into());
    }
    Ok(entries)
}

async fn claude_model_entries(ctx: &CommandContext<'_>) -> Result<Vec<ModelEntry>, String> {
    let base = base_profile_for_model_probe(ctx);
    let mut entries = vec![default_model_entry()];
    if let Some(model) = probe_claude_model(&base, None, ctx.resolved_provider_config).await? {
        entries[0].desc = format!("当前默认 → {model}");
        entries[0].resolved_id = Some(model);
    }

    let candidates = [
        ("opus", "Claude alias: opus", vec!["opus"]),
        (
            "opus[1m]",
            "Claude alias: opus[1m]",
            vec!["opus1m", "opus 1m"],
        ),
        ("sonnet", "Claude alias: sonnet", vec!["sonnet"]),
        (
            "sonnet[1m]",
            "Claude alias: sonnet[1m]",
            vec!["sonnet1m", "sonnet 1m"],
        ),
        ("haiku", "Claude alias: haiku", vec!["haiku"]),
    ];

    for (candidate, desc, aliases) in candidates {
        if let Some(model) =
            probe_claude_model(&base, Some(candidate), ctx.resolved_provider_config).await?
        {
            push_unique_model_entry(
                &mut entries,
                ModelEntry {
                    label: model.clone(),
                    desc: desc.into(),
                    full_id: Some(model),
                    resolved_id: None,
                    aliases: aliases.into_iter().map(str::to_string).collect(),
                },
            );
        }
    }

    for model in claude_custom_env_models(&base, ctx.resolved_provider_config) {
        push_unique_model_entry(
            &mut entries,
            ModelEntry {
                label: model.clone(),
                desc: "Claude custom model".into(),
                full_id: Some(model),
                resolved_id: None,
                aliases: vec!["custom".into()],
            },
        );
    }

    if entries.len() == 1 {
        return Err("Claude 未返回可用模型".into());
    }
    Ok(entries)
}

fn base_profile_for_model_probe(ctx: &CommandContext<'_>) -> crate::cli_bridge::CliProfile {
    let active_name = ctx.resolved_cli.name();
    if ctx.config.cli.name() == active_name {
        return ctx.config.cli.clone();
    }
    ctx.config
        .cli_profiles
        .values()
        .find(|profile| profile.name() == active_name)
        .cloned()
        .unwrap_or_else(|| ctx.resolved_cli.clone())
}

async fn probe_claude_model(
    base: &crate::cli_bridge::CliProfile,
    model_arg: Option<&str>,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Result<Option<String>, String> {
    let mut profile = base.clone();
    if let Some(model) = model_arg {
        profile.set_model_override(model.to_string());
    }
    let mut command = profile.build_command_with_context("/model", None, None, None);
    profile
        .apply_runtime_environment(&mut command)
        .map_err(|e| {
            format!(
                "failed to prepare `{}` CLI environment for model probe: {e}",
                profile.name()
            )
        })?;
    if let Some(pc) = provider_config {
        crate::cli_bridge::apply_provider_environment(&mut command, pc).map_err(|e| {
            format!(
                "failed to prepare provider environment for `{}` model probe: {e}",
                profile.name()
            )
        })?;
    }

    let output = tokio::time::timeout(Duration::from_secs(8), command.output())
        .await
        .map_err(|_| "Claude model probe 超时".to_string())?
        .map_err(|e| format!("运行 Claude model probe: {e}"))?;
    if let Some(model) = parse_claude_init_model(&output.stdout) {
        return Ok(Some(model));
    }
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "Claude model probe 退出码 {}: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }
    Ok(None)
}

fn parse_claude_init_model(stdout: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(stdout).ok()?;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value["type"] == "system"
            && value["subtype"] == "init"
            && let Some(model) = value["model"].as_str()
            && !model.trim().is_empty()
        {
            return Some(model.to_string());
        }
    }
    None
}

fn claude_custom_env_models(
    profile: &crate::cli_bridge::CliProfile,
    provider_config: Option<&crate::config::ProviderConfig>,
) -> Vec<String> {
    let mut models = Vec::new();
    if let Some(model) = cli_profile_env_value(profile, "ANTHROPIC_MODEL") {
        models.push(model);
    }
    if let Some(pc) = provider_config
        && let Some(model) = pc.env.get("ANTHROPIC_MODEL")
        && !model.trim().is_empty()
    {
        models.push(model.trim().to_string());
    }
    models.sort();
    models.dedup();
    models
}

fn cli_profile_env_value(profile: &crate::cli_bridge::CliProfile, key: &str) -> Option<String> {
    match profile {
        crate::cli_bridge::CliProfile::Claude { env, .. }
        | crate::cli_bridge::CliProfile::Copilot { env, .. } => env
            .get(key)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty()),
        _ => None,
    }
}

#[derive(serde::Deserialize)]
struct CodexModelCatalog {
    models: Vec<CodexModelItem>,
}

#[derive(serde::Deserialize)]
struct CodexModelItem {
    slug: String,
    display_name: Option<String>,
    description: Option<String>,
    visibility: Option<String>,
}

async fn codex_model_entries(
    profile: &crate::cli_bridge::CliProfile,
) -> Result<Vec<ModelEntry>, String> {
    let crate::cli_bridge::CliProfile::Codex { bin, .. } = profile else {
        return Ok(Vec::new());
    };

    let output = tokio::time::timeout(
        Duration::from_secs(8),
        tokio::process::Command::new(bin)
            .arg("debug")
            .arg("models")
            .output(),
    )
    .await
    .map_err(|_| format!("`{bin} debug models` 超时"))?
    .map_err(|e| format!("运行 `{bin} debug models`: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "`{bin} debug models` 退出码 {}: {}",
            output.status.code().unwrap_or(-1),
            stderr.trim()
        ));
    }

    let catalog: CodexModelCatalog = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("解析 `{bin} debug models` 输出: {e}"))?;
    let mut entries = vec![default_model_entry()];
    for item in catalog
        .models
        .into_iter()
        .filter(|item| item.visibility.as_deref() == Some("list"))
    {
        let aliases = codex_model_aliases(&item.slug);
        push_unique_model_entry(
            &mut entries,
            ModelEntry {
                label: item.display_name.unwrap_or_else(|| item.slug.clone()),
                desc: item.description.unwrap_or_else(|| "Codex model".into()),
                full_id: Some(item.slug),
                resolved_id: None,
                aliases,
            },
        );
    }
    if entries.len() == 1 {
        return Err("Codex 返回的模型列表为空".into());
    }
    Ok(entries)
}

fn codex_model_aliases(slug: &str) -> Vec<String> {
    let mut aliases = vec![slug.replace(['-', '.'], "")];
    if slug == "gpt-5.5" {
        aliases.push("gpt55".into());
    } else if slug == "gpt-5.4" {
        aliases.push("gpt54".into());
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn push_unique_model_entry(entries: &mut Vec<ModelEntry>, entry: ModelEntry) {
    if let Some(id) = entry.full_id.as_deref()
        && entries
            .iter()
            .any(|existing| existing.full_id.as_deref() == Some(id))
    {
        return;
    }
    entries.push(entry);
}

fn astra_model_aliases(name: &str) -> Vec<String> {
    let mut aliases = Vec::new();
    if let Some(prefix) = name.split('-').next()
        && prefix != name
        && !prefix.is_empty()
    {
        aliases.push(prefix.to_string());
    }
    if name.starts_with("deepseek") {
        aliases.push("ds".into());
    }
    if name.starts_with("qwen") {
        aliases.push("qwen".into());
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn format_context_window(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{}k", n / 1_000)
    } else {
        n.to_string()
    }
}

fn format_cache_age(age: Option<Duration>) -> String {
    let Some(age) = age else {
        return "刚刚".into();
    };
    let secs = age.as_secs();
    if secs < 60 {
        "刚刚".into()
    } else if secs < 3600 {
        format!("{}分钟前", secs / 60)
    } else if secs < 86_400 {
        format!("{}小时前", secs / 3600)
    } else {
        format!("{}天前", secs / 86_400)
    }
}

/// Result of resolving a user-provided `/model` argument.
pub(crate) enum ResolvedModel {
    /// Clear the override — runner falls back to yaml default.
    Default,
    /// A concrete model identifier.
    Id(String),
    /// Input didn't match any known label or full id.
    Unrecognized,
}

/// Match against: numeric index · "默认"/"default" · label
/// (case-insensitive, whitespace ignored) · alias · exact id.
/// Anything else → `Unrecognized`.
pub(crate) fn resolve_model_input(input: &str, entries: &[ModelEntry]) -> ResolvedModel {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return ResolvedModel::Unrecognized;
    }

    // Numeric index (1-based)
    if let Ok(idx) = trimmed.parse::<usize>()
        && idx >= 1
        && idx <= entries.len()
    {
        return match &entries[idx - 1].full_id {
            None => ResolvedModel::Default,
            Some(id) => ResolvedModel::Id(id.clone()),
        };
    }

    // "默认" / "default"
    if trimmed == "默认" || trimmed.eq_ignore_ascii_case("default") {
        return ResolvedModel::Default;
    }

    // Label match with whitespace ignored, case-insensitive.
    let stripped = strip_whitespace(trimmed);
    for entry in entries {
        if stripped.eq_ignore_ascii_case(&strip_whitespace(&entry.label)) {
            return match &entry.full_id {
                None => ResolvedModel::Default,
                Some(id) => ResolvedModel::Id(id.clone()),
            };
        }
    }

    // Alias match (case-insensitive).
    for entry in entries {
        for alias in &entry.aliases {
            if trimmed.eq_ignore_ascii_case(alias) {
                return match &entry.full_id {
                    None => ResolvedModel::Default,
                    Some(id) => ResolvedModel::Id(id.clone()),
                };
            }
        }
    }

    // Exact id match against entries (case-insensitive).
    for entry in entries {
        if let Some(id) = &entry.full_id
            && trimmed.eq_ignore_ascii_case(id)
        {
            return ResolvedModel::Id(id.clone());
        }
    }

    if is_explicit_model_id(trimmed) {
        return ResolvedModel::Id(trimmed.to_string());
    }

    ResolvedModel::Unrecognized
}

fn is_explicit_model_id(input: &str) -> bool {
    !input.chars().any(char::is_whitespace) && input.contains('.')
}

fn looks_like_configured_model_id(input: &str) -> bool {
    !input.chars().any(char::is_whitespace)
        && (input.contains('.') || input.contains('-') || input.chars().any(|c| c.is_ascii_digit()))
}

fn strip_whitespace(s: &str) -> String {
    s.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Render a model id using the friendly label from the entries list,
/// falling back to the raw id if unknown.
fn display_model_name(id: &str, entries: &[ModelEntry]) -> String {
    for entry in entries {
        if entry.full_id.as_deref() == Some(id) {
            return entry.label.clone();
        }
    }
    id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cron_quoted() {
        let (expr, msg) = parse_cron_add("\"0 9 * * *\" 每天早上汇报").unwrap();
        assert_eq!(expr, "0 9 * * *");
        assert_eq!(msg, "每天早上汇报");
    }

    #[test]
    fn parse_cron_unquoted() {
        let (expr, msg) = parse_cron_add("0 9 * * 1-5 每个工作日早上汇报").unwrap();
        assert_eq!(expr, "0 9 * * 1-5");
        assert_eq!(msg, "每个工作日早上汇报");
    }

    #[test]
    fn format_tokens_values() {
        assert_eq!(format_tokens(500), "500");
        assert_eq!(format_tokens(1500), "1.5k");
        assert_eq!(format_tokens(1_500_000), "1.5M");
    }

    #[test]
    fn format_duration_values() {
        assert_eq!(format_duration(500), "500ms");
        assert_eq!(format_duration(3500), "3.5s");
        assert_eq!(format_duration(125_000), "2m 5s");
    }

    #[test]
    fn format_cache_age_values() {
        assert_eq!(format_cache_age(None), "刚刚");
        assert_eq!(format_cache_age(Some(Duration::from_secs(59))), "刚刚");
        assert_eq!(format_cache_age(Some(Duration::from_secs(120))), "2分钟前");
        assert_eq!(format_cache_age(Some(Duration::from_secs(7200))), "2小时前");
        assert_eq!(format_cache_age(Some(Duration::from_secs(172800))), "2天前");
    }

    // ── Harness snapshot tests ──────────────────────────────────

    fn test_snapshot() -> HarnessSnapshot {
        HarnessSnapshot {
            session_id: "abc12345-def6-7890".into(),
            turn_number: 5,
            model: Some("claude-opus-4-6".into()),
            context_utilization: Some(0.42),
            context_message_count: 12,
            context_total_tokens: Some(50000),
            turns_used: 5,
            turns_limit: Some(20),
            tokens_used: 25000,
            tokens_prompt: 20000,
            tokens_completion: 5000,
            tokens_cache_read: 10000,
            elapsed_ms: 35000,
            tool_calls: 8,
            unique_tools: vec!["bash".into(), "read_file".into(), "edit_file".into()],
            last_tool: Some("edit_file".into()),
            consecutive_same_tool: 1,
            delegations: 0,
            consecutive_errors: 0,
        }
    }

    #[test]
    fn snapshot_format_full_contains_key_fields() {
        let s = test_snapshot();
        let full = s.format_full();
        assert!(full.contains("abc12345"), "session id");
        assert!(full.contains("5/20"), "turns");
        assert!(full.contains("🔧 8"), "tool calls");
        assert!(full.contains("42%"), "utilization");
        assert!(full.contains("claude-opus-4-6"), "model");
        assert!(full.contains("bash"), "tool name");
        assert!(full.contains("$"), "cost");
    }

    #[test]
    fn snapshot_warnings_consecutive_tool() {
        let mut s = test_snapshot();
        s.consecutive_same_tool = 5;
        s.last_tool = Some("bash".into());
        let w = s.warnings();
        assert!(!w.is_empty());
        assert!(w[0].contains("重复工具"));
        assert!(w[0].contains("bash"));
    }

    #[test]
    fn snapshot_warnings_high_utilization() {
        let mut s = test_snapshot();
        s.context_utilization = Some(0.92);
        let w = s.warnings();
        assert!(!w.is_empty());
        assert!(w[0].contains("接近上限"));
    }

    #[test]
    fn snapshot_warnings_consecutive_errors() {
        let mut s = test_snapshot();
        s.consecutive_errors = 3;
        let w = s.warnings();
        assert!(!w.is_empty());
        assert!(w[0].contains("连续"));
    }

    #[test]
    fn snapshot_no_warnings_when_healthy() {
        let s = test_snapshot();
        assert!(s.warnings().is_empty());
    }

    #[test]
    fn snapshot_cost_estimate() {
        let s = test_snapshot();
        let cost = s.cost_estimate_usd();
        // 20k * $3/M + 5k * $15/M = $0.06 + $0.075 = $0.135
        assert!(cost > 0.1 && cost < 0.2, "cost={cost}");
    }

    #[test]
    fn parse_snapshot_from_json() {
        let v = serde_json::json!({
            "session_id": "test-123",
            "turn_number": 3,
            "model": "opus",
            "turns_used": 3,
            "turns_limit": 10,
            "tokens_used_session": 15000,
            "tokens_prompt": 12000,
            "tokens_completion": 3000,
            "tokens_cache_read": 5000,
            "elapsed_millis": 8000,
            "tool_calls_this_session": 4,
            "unique_tools_used": ["bash", "read_file"],
            "last_tool_called": "read_file",
            "consecutive_same_tool": 0,
            "context_utilization": 0.35,
            "context_message_count": 8,
        });
        let snap = parse_harness_snapshot(&v, "fallback");
        assert_eq!(snap.session_id, "test-123");
        assert_eq!(snap.turn_number, 3);
        assert_eq!(snap.tokens_prompt, 12000);
        assert_eq!(snap.unique_tools.len(), 2);
    }

    #[test]
    fn sanitized_snapshot_tool_summary_does_not_look_empty() {
        let mut s = test_snapshot();
        s.tool_calls = 4;
        s.unique_tools.clear();
        s.last_tool = None;

        assert_eq!(s.tool_summary(), "详情已脱敏");
        let full = s.format_full();
        assert!(
            full.contains("🔧 4"),
            "tool call count should remain visible"
        );
        assert!(
            full.contains("详情已脱敏"),
            "sanitized details should be explicit"
        );
    }

    #[test]
    fn audit_history_dedupes_same_turn_and_sorts_chronologically() {
        let mut newest_turn_2 = test_snapshot();
        newest_turn_2.turn_number = 2;
        newest_turn_2.tokens_prompt = 2_000;
        newest_turn_2.tool_calls = 3;

        let mut older_turn_2 = test_snapshot();
        older_turn_2.turn_number = 2;
        older_turn_2.tokens_prompt = 1_000;
        older_turn_2.tool_calls = 1;

        let mut turn_1 = test_snapshot();
        turn_1.turn_number = 1;
        turn_1.tokens_prompt = 500;

        let audit = format_audit_history(vec![newest_turn_2, older_turn_2, turn_1]);

        assert_eq!(audit.matches("**Turn 2**").count(), 1);
        assert!(audit.find("**Turn 1**").unwrap() < audit.find("**Turn 2**").unwrap());
        assert!(
            audit.contains("↓2.0k"),
            "keeps latest snapshot for duplicate turn"
        );
        assert!(!audit.contains("↓1.0k"), "drops older duplicate snapshot");
    }

    // ── handle_command dispatch tests ──────────────────────────────

    fn test_config() -> GatewayConfig {
        GatewayConfig {
            astra: crate::config::AstraServerConfig {
                base_url: "http://localhost:8080".into(),
                api_key: String::new(),
                default_model: None,
                username: None,
                password: None,
            },
            storage: Default::default(),
            database: None,
            cli: Default::default(),
            cli_profiles: Default::default(),
            providers: Default::default(),
            cli_timeout_secs: 3600,
            response_footer: false,
            platforms: Default::default(),
            skills_dir: None,
            session_reset: Default::default(),
            access: Default::default(),
            action_policy: Default::default(),
            max_concurrent_runs: 4,
            group_sessions_per_user: true,
            group_require_mention: false,
            bot_name: String::new(),
            timezone: None,
            project_dirs: vec![],
            working_dir: None,
            system_prompt_extra: None,
            vision_models: Default::default(),
            github_tokens: Default::default(),
            api_port: None,
        }
    }

    macro_rules! cmd_test {
        ($name:ident, $input:expr, $check:expr) => {
            #[tokio::test]
            async fn $name() {
                let config = test_config();
                let cli = crate::cli_bridge::CliProfile::default();
                let astra = astra::Client::new("http://localhost:8080", None).unwrap();
                let ctx = CommandContext {
                    astra: &astra,
                    config: &config,
                    store: None,
                    platform: "test",
                    chat_id: "chat_1",
                    user_id: "user_1",
                    resolved_cli: &cli,
                    resolved_provider_config: None,
                    trace_repo: None,
                    project_dirs: &config.project_dirs,
                    cli_availability: &[],
                    auth_status: None,
                    active_requests: None,
                    codex_app_pool: None,
                    gateway_start: chrono::Utc::now(),
                };
                let result = handle_command(&ctx, $input).await;
                let check: fn(Option<String>) = $check;
                check(result);
            }
        };
    }

    cmd_test!(cmd_non_slash_returns_none, "hello world", |r| assert!(
        r.is_none()
    ));
    cmd_test!(cmd_unknown_returns_none, "/nonexistent", |r| assert!(
        r.is_none()
    ));
    cmd_test!(cmd_help_returns_command_list, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("命令列表"));
        assert!(s.contains("/new"), "missing /new");
        assert!(s.contains("/reset"), "missing /reset alias");
        assert!(s.contains("/model"), "missing /model");
        assert!(s.contains("/reasoning"), "missing /reasoning");
        assert!(s.contains("/session"), "missing /session");
        assert!(s.contains("/task"), "missing /task");
        assert!(s.contains("/trace"), "missing /trace");
        assert!(s.contains("/cancel"), "missing /cancel");
        assert!(s.contains("/ws"), "missing /ws alias");
    });
    #[tokio::test]
    async fn cmd_model_no_arg_shows_current() {
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::Custom {
            bin: "echo".into(),
            args_template: vec![],
            json_output: false,
            session_id_field: None,
            text_field: None,
        };
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();
        let ctx = CommandContext {
            astra: &astra,
            config: &config,
            store: None,
            platform: "test",
            chat_id: "chat_1",
            user_id: "user_1",
            resolved_cli: &cli,
            resolved_provider_config: None,
            trace_repo: None,
            project_dirs: &config.project_dirs,
            cli_availability: &[],
            auth_status: None,
            active_requests: None,
            codex_app_pool: None,
            gateway_start: chrono::Utc::now(),
        };
        let r = handle_command(&ctx, "/model").await;
        let s = r.unwrap();
        assert!(s.contains("当前"));
        assert!(s.contains("默认"));
        assert!(s.contains("切换"));
    }

    #[test]
    fn resolve_model_input_variants() {
        let e = vec![
            default_model_entry(),
            ModelEntry {
                label: "qwen3.7-max".into(),
                desc: "Claude alias: haiku".into(),
                full_id: Some("qwen3.7-max".into()),
                resolved_id: None,
                aliases: vec!["haiku".into()],
            },
            ModelEntry {
                label: "deepseek-v4-pro".into(),
                desc: "Claude custom model".into(),
                full_id: Some("deepseek-v4-pro".into()),
                resolved_id: None,
                aliases: vec!["deepseek".into(), "ds".into()],
            },
        ];
        fn id(r: ResolvedModel) -> String {
            match r {
                ResolvedModel::Id(s) => s,
                ResolvedModel::Default => "__DEFAULT__".to_string(),
                ResolvedModel::Unrecognized => "__UNRECOGNIZED__".to_string(),
            }
        }

        assert_eq!(id(resolve_model_input("haiku", &e)), "qwen3.7-max");
        assert_eq!(id(resolve_model_input("QWEN3.7-MAX", &e)), "qwen3.7-max");
        assert_eq!(id(resolve_model_input("deepseek", &e)), "deepseek-v4-pro");
        assert_eq!(id(resolve_model_input("ds", &e)), "deepseek-v4-pro");
        assert!(matches!(
            resolve_model_input("1", &e),
            ResolvedModel::Default
        ));
        assert_eq!(id(resolve_model_input("2", &e)), "qwen3.7-max");
        assert!(matches!(
            resolve_model_input("默认", &e),
            ResolvedModel::Default
        ));
        assert!(matches!(
            resolve_model_input("default", &e),
            ResolvedModel::Default
        ));
        assert_eq!(
            id(resolve_model_input("deepseek-v4-pro", &e)),
            "deepseek-v4-pro"
        );
        assert_eq!(
            id(resolve_model_input("vendor.model-v1", &e)),
            "vendor.model-v1"
        );
        assert!(matches!(
            resolve_model_input("xyz-model", &e),
            ResolvedModel::Unrecognized
        ));
        assert!(matches!(
            resolve_model_input("opus 12345", &e),
            ResolvedModel::Unrecognized
        ));
        assert!(matches!(
            resolve_model_input("opus 5", &e),
            ResolvedModel::Unrecognized
        ));
        assert!(matches!(
            resolve_model_input("random text", &e),
            ResolvedModel::Unrecognized
        ));
        assert!(matches!(
            resolve_model_input("claude-opus-9-9", &e),
            ResolvedModel::Unrecognized
        ));
    }

    #[test]
    fn display_model_name_uses_discovered_entries() {
        let entries = vec![
            default_model_entry(),
            ModelEntry {
                label: "qwen3.7-max".into(),
                desc: "Claude alias: haiku".into(),
                full_id: Some("qwen3.7-max".into()),
                resolved_id: None,
                aliases: vec!["haiku".into()],
            },
        ];
        assert_eq!(display_model_name("qwen3.7-max", &entries), "qwen3.7-max");
        assert_eq!(display_model_name("unknown-id", &entries), "unknown-id");
    }

    #[test]
    fn dynamic_claude_entries_display_resolved_model_id() {
        let entries = vec![
            default_model_entry(),
            ModelEntry {
                label: "qwen3.7-max".into(),
                desc: "Claude alias: haiku".into(),
                full_id: Some("qwen3.7-max".into()),
                resolved_id: None,
                aliases: vec!["haiku".into()],
            },
        ];

        assert_eq!(display_model_name("qwen3.7-max", &entries), "qwen3.7-max");
        assert!(matches!(
            resolve_model_input("haiku", &entries),
            ResolvedModel::Id(id) if id == "qwen3.7-max"
        ));
    }

    #[test]
    fn current_model_resolution_uses_structured_default_probe_result() {
        let mut default = default_model_entry();
        default.resolved_id = Some("claude-sonnet-4-20250514".into());
        let entries = vec![default];

        assert_eq!(
            resolved_model_id_from_entries(None, &entries),
            Some("claude-sonnet-4-20250514".into())
        );
    }

    #[test]
    fn current_model_resolution_maps_alias_to_probe_result() {
        let entries = vec![
            default_model_entry(),
            ModelEntry {
                label: "qwen3.7-max".into(),
                desc: "Claude alias: haiku".into(),
                full_id: Some("qwen3.7-max".into()),
                resolved_id: None,
                aliases: vec!["haiku".into()],
            },
        ];

        assert_eq!(
            resolved_model_id_from_entries(Some("haiku"), &entries),
            Some("qwen3.7-max".into())
        );
    }

    #[test]
    fn current_model_resolution_unknown_alias_is_unknown() {
        let entries = vec![default_model_entry()];

        assert_eq!(
            resolved_model_id_from_entries(Some("haiku"), &entries),
            None
        );
    }

    #[test]
    fn current_model_resolution_preserves_explicit_dash_model_id() {
        let entries = vec![default_model_entry()];

        assert_eq!(
            resolved_model_id_from_entries(Some("claude-sonnet-4"), &entries),
            Some("claude-sonnet-4".into())
        );
    }

    cmd_test!(
        cmd_model_set_without_db_still_succeeds,
        "/model default",
        |r| {
            assert!(r.unwrap().contains("模型已切换"));
        }
    );
    cmd_test!(cmd_cli_no_arg_shows_current, "/cli", |r| {
        assert!(r.unwrap().contains("astra"));
    });
    cmd_test!(cmd_reasoning_requires_db, "/reasoning on", |r| {
        let msg = r.unwrap();
        assert!(msg.contains("存储"), "expected storage error, got: {msg}");
    });
    cmd_test!(cmd_new_requires_db, "/new", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(cmd_session_requires_db, "/session list", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(cmd_cron_requires_db, "/cron list", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(cmd_usage_requires_db, "/usage", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(cmd_workspace_requires_db, "/workspace", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(cmd_running_requires_trace_repo, "/running", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_task_list_returns_response, "/task list", |r| {
        assert!(r.is_some());
    });
    cmd_test!(cmd_status_works_without_db, "/status", |r| {
        assert!(r.unwrap().contains("astra"));
    });

    #[tokio::test]
    async fn cmd_status_includes_auth_circuit_status_when_available() {
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::default();
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();
        let ctx = CommandContext {
            astra: &astra,
            config: &config,
            store: None,
            platform: "test",
            chat_id: "chat_1",
            user_id: "user_1",
            resolved_cli: &cli,
            resolved_provider_config: None,
            trace_repo: None,
            project_dirs: &config.project_dirs,
            cli_availability: &[],
            auth_status: Some("⚠️ 暂停 (剩余 3m 42s)".to_string()),
            active_requests: None,
            codex_app_pool: None,
            gateway_start: chrono::Utc::now(),
        };

        let result = handle_command(&ctx, "/status").await.unwrap();
        assert!(result.contains("- 认证: ⚠️ 暂停 (剩余 3m 42s)"), "{result}");
    }
    cmd_test!(
        cmd_cron_add_malformed_gives_error,
        "/cron add badformat",
        |r| {
            let s = r.unwrap();
            assert!(s.contains("格式错误"), "should show format error, got: {s}");
        }
    );
    cmd_test!(cmd_inspect_requires_db, "/inspect", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_audit_requires_db, "/audit", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_trace_requires_trace_repo, "/trace", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_cancel_requires_trace_repo, "/cancel", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_esc_requires_trace_repo, "/esc abc123", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("MySQL"),
                "expected storage error, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_esc_no_arg_auto_pick_or_no_db, "/esc", |r| {
        {
            let msg = r.unwrap();
            assert!(
                msg.contains("存储") || msg.contains("没有运行中"),
                "expected db error or no running message, got: {msg}"
            );
        }
    });
    cmd_test!(cmd_help_includes_esc, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("/esc"), "help should include /esc");
    });
    cmd_test!(cmd_help_includes_gateway, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("/gateway"), "help should include /gateway");
    });
    cmd_test!(cmd_help_includes_ws_ls, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("/ws ls"), "help should include /ws ls");
    });
    cmd_test!(cmd_gateway_shows_context, "/gateway", |r| {
        let s = r.unwrap();
        assert!(s.contains("Gateway Context"), "missing header");
        assert!(s.contains("Identity"), "missing identity section");
        assert!(s.contains("Capabilities"), "missing capabilities section");
        assert!(s.contains("Commands"), "missing commands section");
        assert!(s.contains("astra"), "missing CLI name");
        assert!(s.contains("test"), "missing platform");
    });
    cmd_test!(cmd_ws_ls_empty_projects, "/ws ls", |r| {
        let s = r.unwrap();
        assert!(s.contains("没有发现项目"), "expected empty project message");
    });

    cmd_test!(cmd_retry_requires_trace_repo, "/retry", |r| {
        {
            let msg = r.unwrap();
            assert!(msg.contains("存储"), "expected storage error, got: {msg}");
        }
    });
    cmd_test!(
        cmd_retry_dismiss_requires_trace_repo,
        "/retry dismiss",
        |r| {
            {
                let msg = r.unwrap();
                assert!(msg.contains("存储"), "expected storage error, got: {msg}");
            }
        }
    );
    cmd_test!(cmd_help_includes_retry, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("/retry"), "help should include /retry");
    });
    cmd_test!(cmd_gateway_includes_retry, "/gateway", |r| {
        let s = r.unwrap();
        assert!(s.contains("/retry"), "gateway should include /retry");
    });

    cmd_test!(cmd_ws_no_arg_requires_store, "/ws", |r| {
        let s = r.unwrap();
        assert!(s.contains("存储"), "expected storage error, got: {s}");
    });

    cmd_test!(
        cmd_ws_nonexistent_path,
        "/ws /nonexistent/path/12345",
        |r| {
            let s = r.unwrap();
            // Without store, /ws <arg> requires store first
            assert!(
                s.contains("存储") || s.contains("不存在"),
                "expected storage error or not-found, got: {s}"
            );
        }
    );

    // ── /manage is NOT a fast-path command (goes to slow path) ──

    cmd_test!(cmd_manage_not_handled_as_command, "/manage", |r| {
        assert!(r.is_none(), "/manage should not be a fast-path command");
    });
    cmd_test!(
        cmd_manage_with_arg_not_handled,
        "/manage 清理所有卡住的任务",
        |r| {
            assert!(
                r.is_none(),
                "/manage with arg should not be a fast-path command"
            );
        }
    );

    // ── /help and /gateway include /manage ──

    cmd_test!(cmd_help_includes_manage, "/help", |r| {
        let s = r.unwrap();
        assert!(s.contains("/manage"), "help should include /manage");
    });
    cmd_test!(cmd_gateway_includes_manage, "/gateway", |r| {
        let s = r.unwrap();
        assert!(s.contains("/manage"), "gateway should include /manage");
    });
    // ── GAP 4: /gateway content completeness ────────────────────

    cmd_test!(cmd_gateway_content_completeness, "/gateway", |r| {
        let s = r.unwrap();
        assert!(s.contains("Gateway Context"), "missing header");
        assert!(s.contains("Identity"), "missing identity section");
        assert!(s.contains("Capabilities"), "missing capabilities section");
        assert!(s.contains("Commands"), "missing commands section");
        assert!(s.contains("astra"), "missing CLI name");
        assert!(s.contains("test"), "missing platform");
        // Verify capabilities matrix
        assert!(
            s.contains("Session management"),
            "missing session capability"
        );
        assert!(s.contains("Model switching"), "missing model capability");
        assert!(s.contains("Tool execution"), "missing tool capability");
        // Verify commands table
        assert!(s.contains("/new"), "missing /new in commands table");
        assert!(s.contains("/model"), "missing /model in commands table");
        assert!(s.contains("/esc"), "missing /esc in commands table");
        assert!(s.contains("/manage"), "missing /manage in commands table");
        assert!(s.contains("/retry"), "missing /retry in commands table");
        assert!(s.contains("/trace"), "missing /trace in commands table");
        assert!(s.contains("/usage"), "missing /usage in commands table");
        assert!(s.contains("/inspect"), "missing /inspect in commands table");
        assert!(s.contains("/audit"), "missing /audit in commands table");
        // Verify storage status (no store in test)
        assert!(
            s.contains("none"),
            "should show no storage when store is None"
        );
    });

    // ── GAP 8: resolve_active_request edge cases ────────────────

    #[tokio::test]
    async fn resolve_active_request_returns_none_for_empty_list() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("wx", "c1", "astra");
        let result = resolve_active_request(&repo, &conv, "1").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_active_request_numeric_zero_returns_none() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("wx", "c1", "astra");
        let req = crate::trace_model::GatewayRequest::new(conv.clone(), "m1", "u1", "test");
        crate::trace_model::TraceWriter::begin(&repo, req)
            .await
            .unwrap();
        let result = resolve_active_request(&repo, &conv, "0").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_active_request_out_of_range_returns_none() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("wx", "c1", "astra");
        let req = crate::trace_model::GatewayRequest::new(conv.clone(), "m1", "u1", "test");
        crate::trace_model::TraceWriter::begin(&repo, req)
            .await
            .unwrap();
        // Only 1 request, asking for index 5
        let result = resolve_active_request(&repo, &conv, "5").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn resolve_active_request_by_trace_id_prefix() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("wx", "c1", "astra");
        let req = crate::trace_model::GatewayRequest::new(conv.clone(), "m1", "u1", "test");
        let trace_id = req.trace_id.clone();
        crate::trace_model::TraceWriter::begin(&repo, req)
            .await
            .unwrap();

        // Full trace ID
        let found = resolve_active_request(&repo, &conv, trace_id.as_str())
            .await
            .unwrap();
        assert_eq!(found.trace_id, trace_id);

        // Prefix match (first 8 chars)
        let prefix = &trace_id.as_str()[..8];
        let found = resolve_active_request(&repo, &conv, prefix).await.unwrap();
        assert_eq!(found.trace_id, trace_id);
    }

    #[tokio::test]
    async fn resolve_active_request_no_match_returns_none() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("wx", "c1", "astra");
        let req =
            crate::trace_model::GatewayRequest::new(conv.clone(), "m1", "u1", "something specific");
        crate::trace_model::TraceWriter::begin(&repo, req)
            .await
            .unwrap();

        let result = resolve_active_request(&repo, &conv, "nonexistent_text").await;
        assert!(result.is_none());
    }

    // ── status_icon ──

    #[test]
    fn status_icon_maps_known_statuses() {
        assert_eq!(status_icon("running"), "\u{1f504}");
        assert_eq!(status_icon("queued"), "\u{231b}");
        assert_eq!(status_icon("completed"), "\u{2705}");
        assert_eq!(status_icon("failed"), "\u{274c}");
        assert_eq!(status_icon("reply_retrying"), "\u{1f4ec}");
        assert_eq!(status_icon("reply_pending"), "\u{1f4e4}");
        assert_eq!(status_icon("unknown"), "\u{2753}");
    }

    // ── short_timestamp ──

    #[test]
    fn short_timestamp_strips_today() {
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let ts = format!("{today} 12:34:56.789");
        let short = short_timestamp(&ts);
        assert_eq!(short, "12:34:56.789");
    }

    #[test]
    fn short_timestamp_keeps_other_dates() {
        let ts = "2020-01-01 12:34:56";
        assert_eq!(short_timestamp(ts), ts);
    }

    // ── resolve_active_request ──

    #[tokio::test]
    async fn resolve_active_request_by_index() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("test", "chat", "astra");

        // Create two requests
        let req1 = crate::trace_model::GatewayRequest::new(
            conv.clone(),
            "msg-1",
            "user-1",
            "first request",
        );
        let trace1 = req1.trace_id.clone();
        crate::trace_model::TraceWriter::begin(&repo, req1)
            .await
            .unwrap();

        let req2 = crate::trace_model::GatewayRequest::new(
            conv.clone(),
            "msg-2",
            "user-1",
            "second request",
        );
        let trace2 = req2.trace_id.clone();
        crate::trace_model::TraceWriter::begin(&repo, req2)
            .await
            .unwrap();

        // Index 1 → first (both are Accepted, sorted by status then order)
        let r1 = resolve_active_request(&repo, &conv, "1").await.unwrap();
        assert_eq!(r1.trace_id, trace1);

        // Index 2 → second
        let r2 = resolve_active_request(&repo, &conv, "2").await.unwrap();
        assert_eq!(r2.trace_id, trace2);

        // Index 0 → None (1-based)
        assert!(resolve_active_request(&repo, &conv, "0").await.is_none());

        // Index 3 → None (out of range; pure numbers don't fall through to ID match)
        assert!(resolve_active_request(&repo, &conv, "3").await.is_none());
    }

    #[tokio::test]
    async fn resolve_active_request_by_text() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let conv = ConversationKey::new("test", "chat", "astra");

        let req = crate::trace_model::GatewayRequest::new(
            conv.clone(),
            "msg-1",
            "user-1",
            "帮我写个周报",
        );
        let trace = req.trace_id.clone();
        crate::trace_model::TraceWriter::begin(&repo, req)
            .await
            .unwrap();

        let found = resolve_active_request(&repo, &conv, "周报").await.unwrap();
        assert_eq!(found.trace_id, trace);

        // No match
        assert!(
            resolve_active_request(&repo, &conv, "不存在的内容")
                .await
                .is_none()
        );
    }

    #[test]
    fn parse_cron_add_rejects_invalid_cron_expression() {
        assert!(parse_cron_add("99 99 99 99 99 impossible").is_none());
        assert!(parse_cron_add("\"bad expr\" impossible").is_none());
    }

    // ── R5-#3: /manage routes to a SEPARATE conversation worker ───────────
    //
    // The problem: if the user's main CLI is stuck (worker blocked on an
    // in-flight subprocess), posting `/manage 清一下` would enqueue to
    // the SAME worker and wait behind the stuck tasks — the user's
    // request to fix the queue joins the queue.
    //
    // The solution: route `/manage` slow-path requests through a virtual
    // cli_profile (MANAGE_CLI_PROFILE = "_manage") so enqueue_cli_request
    // picks a DIFFERENT ConversationKey → DIFFERENT worker → independent
    // queue. The worker still resolves the user's real CLI at execute
    // time (see handle_message_inner), so the actual subprocess that
    // runs is the same CLI the user expected.

    #[test]
    fn manage_cli_profile_constant_is_namespaced() {
        // Must not collide with any real user-configurable CLI profile
        // name. Convention: leading underscore = gateway-internal.
        assert!(MANAGE_CLI_PROFILE.starts_with('_'));
        assert_eq!(MANAGE_CLI_PROFILE, "_manage");
    }

    #[test]
    fn manage_and_regular_messages_produce_distinct_conversation_keys() {
        // The whole point of R5-#3: distinct keys ⇒ distinct workers ⇒
        // /manage does NOT block behind the user's stuck tasks.
        let user_key = ConversationKey::new("weixin", "chat-1", "astra");
        let manage_key = ConversationKey::new("weixin", "chat-1", MANAGE_CLI_PROFILE);
        assert_ne!(user_key, manage_key);
        // Same (platform, chat_id) but different cli_profile field.
        assert_eq!(user_key.platform(), manage_key.platform());
        assert_eq!(user_key.chat_id(), manage_key.chat_id());
        assert_ne!(user_key.cli_profile(), manage_key.cli_profile());
    }

    // ── R5-#2: zombie detection on /running output ────────────────────────
    //
    // A request whose `created_at` predates the current gateway process
    // start cannot make progress (its cancel_token is gone, its CLI
    // subprocess is gone, its outbox scheduler is gone). Tag these with
    // 🧟 in /running so the operator knows they need /esc all.

    #[test]
    fn is_zombie_flags_request_created_before_gateway_start() {
        use chrono::{Duration as ChronoDuration, Utc};
        let gateway_start = Utc::now() - ChronoDuration::hours(1);
        let created_at = (gateway_start - ChronoDuration::minutes(30))
            .format("%Y-%m-%d %H:%M:%S.%6f")
            .to_string();
        assert!(
            is_zombie_request(&created_at, gateway_start),
            "request created 30 min before gateway start must be flagged zombie"
        );
    }

    #[test]
    fn is_zombie_skips_request_created_after_gateway_start() {
        use chrono::{Duration as ChronoDuration, Utc};
        let gateway_start = Utc::now() - ChronoDuration::hours(1);
        let created_at = (gateway_start + ChronoDuration::minutes(5))
            .format("%Y-%m-%d %H:%M:%S.%6f")
            .to_string();
        assert!(
            !is_zombie_request(&created_at, gateway_start),
            "request created after gateway start is NOT a zombie"
        );
    }

    #[test]
    fn is_zombie_tolerates_unparseable_timestamp() {
        // DB timestamp formats drift. An unparseable string must NOT be
        // flagged as zombie — conservatively treat it as recent.
        let gateway_start = chrono::Utc::now();
        assert!(!is_zombie_request("not a date", gateway_start));
        assert!(!is_zombie_request("", gateway_start));
    }

    #[test]
    fn is_zombie_handles_iso8601_with_tz() {
        // Some drivers return "2026-05-04T09:55:12Z" — also parseable.
        use chrono::{Duration as ChronoDuration, Utc};
        let gateway_start = Utc::now();
        let iso = (gateway_start - ChronoDuration::minutes(10)).to_rfc3339();
        assert!(
            is_zombie_request(&iso, gateway_start),
            "RFC3339 timestamps before gateway start must be recognized"
        );
    }

    // ── R5-#1: /esc all — sweep every active request in the conversation ──
    //
    // Scenario: 8 running + queued requests pile up (user repeatedly
    // retries because gateway is stuck). Operator needs a single command
    // to clear them all without guessing trace_ids. Previous /esc only
    // accepted ONE selector — "all" returned "not found".

    async fn build_ctx_with_repo<'a>(
        config: &'a GatewayConfig,
        cli: &'a crate::cli_bridge::CliProfile,
        astra: &'a astra::Client,
        repo: &'a dyn crate::trace_model::TraceRepository,
        active_requests: Option<&'a dashmap::DashMap<String, tokio_util::sync::CancellationToken>>,
    ) -> CommandContext<'a> {
        CommandContext {
            astra,
            config,
            store: None,
            platform: "test",
            chat_id: "chat_kill_all",
            user_id: "user_1",
            resolved_cli: cli,
            resolved_provider_config: None,
            trace_repo: Some(repo),
            project_dirs: &config.project_dirs,
            cli_availability: &[],
            auth_status: None,
            active_requests,
            codex_app_pool: None,
            gateway_start: chrono::Utc::now(),
        }
    }

    async fn seed_running_request(
        repo: &crate::trace_model::InMemoryTraceRepository,
        cli_name: &str,
        chat_id: &str,
        text: &str,
    ) -> crate::trace_model::TraceId {
        let conv = ConversationKey::new("test", chat_id, cli_name);
        let req = crate::trace_model::GatewayRequest::new(conv, "msg", "user", text);
        let trace_id = req.trace_id.clone();
        let writer = crate::trace_model::TraceWriter::begin(repo, req)
            .await
            .unwrap();
        writer.mark_queued(0).await.unwrap();
        writer.mark_running().await.unwrap();
        trace_id
    }

    #[tokio::test]
    async fn esc_all_fails_every_active_request_in_conversation() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::default();
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();

        // Seed 3 running requests in the same conversation.
        let t1 = seed_running_request(&repo, cli.name(), "chat_kill_all", "msg1").await;
        let t2 = seed_running_request(&repo, cli.name(), "chat_kill_all", "msg2").await;
        let t3 = seed_running_request(&repo, cli.name(), "chat_kill_all", "msg3").await;
        // And a request in a DIFFERENT conversation — must NOT be touched.
        let other = seed_running_request(&repo, cli.name(), "other_chat", "msg4").await;

        let ctx = build_ctx_with_repo(&config, &cli, &astra, &repo, None).await;
        let result = handle_command(&ctx, "/esc all").await.unwrap();

        assert!(
            result.contains("3") && (result.contains("中断") || result.contains("interrupted")),
            "response should report 3 interrupted: {result}"
        );

        // Confirm the target conversation is empty afterward.
        let conv = ConversationKey::new("test", "chat_kill_all", cli.name());
        let remaining = repo.list_active_requests(&conv, 20).await.unwrap();
        assert!(
            remaining.is_empty(),
            "target conversation should have no active requests left, got {}",
            remaining.len()
        );
        // And trace_ids match what we seeded.
        let _ = (t1, t2, t3);

        // The other conversation's request stays running — /esc all is
        // scoped to the invoker's conversation, not global.
        let other_conv = ConversationKey::new("test", "other_chat", cli.name());
        let still_there = repo.list_active_requests(&other_conv, 20).await.unwrap();
        assert_eq!(
            still_there.len(),
            1,
            "requests in other conversations must NOT be swept by /esc all"
        );
        assert_eq!(still_there[0].trace_id, other);
    }

    #[tokio::test]
    async fn esc_all_on_empty_conversation_reports_zero() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::default();
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();

        let ctx = build_ctx_with_repo(&config, &cli, &astra, &repo, None).await;
        let result = handle_command(&ctx, "/esc all").await.unwrap();
        assert!(
            result.contains("0") || result.contains("没有"),
            "empty-conversation /esc all should report zero: {result}"
        );
    }

    #[tokio::test]
    async fn esc_all_also_cancels_in_memory_tokens() {
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::default();
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();
        let active_requests: dashmap::DashMap<String, tokio_util::sync::CancellationToken> =
            dashmap::DashMap::new();

        let t1 = seed_running_request(&repo, cli.name(), "chat_kill_all", "live").await;
        let token = tokio_util::sync::CancellationToken::new();
        active_requests.insert(t1.as_str().to_string(), token.clone());

        let ctx = build_ctx_with_repo(&config, &cli, &astra, &repo, Some(&active_requests)).await;
        handle_command(&ctx, "/esc all").await.unwrap();

        assert!(
            token.is_cancelled(),
            "/esc all must cancel the in-memory cancellation token too, so the \
             live turn is interrupted — not just mark DB as failed"
        );
        assert!(
            active_requests.get(t1.as_str()).is_none(),
            "cancelled token entry should be removed from active_requests"
        );
    }

    #[tokio::test]
    async fn cancel_all_sweeps_like_kill_all() {
        // Symmetry: /cancel all behaves identically to /esc all for
        // already-running requests (cancel is just a gentler noun).
        use crate::trace_model::InMemoryTraceRepository;
        let repo = InMemoryTraceRepository::default();
        let config = test_config();
        let cli = crate::cli_bridge::CliProfile::default();
        let astra = astra::Client::new("http://localhost:8080", None).unwrap();

        seed_running_request(&repo, cli.name(), "chat_kill_all", "a").await;
        seed_running_request(&repo, cli.name(), "chat_kill_all", "b").await;

        let ctx = build_ctx_with_repo(&config, &cli, &astra, &repo, None).await;
        let result = handle_command(&ctx, "/cancel all").await.unwrap();
        assert!(
            result.contains("2"),
            "/cancel all should report 2 cleared: {result}"
        );
    }
}
