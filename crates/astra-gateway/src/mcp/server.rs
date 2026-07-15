use std::sync::Arc;

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerInfo};
use rmcp::schemars;
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};

use crate::store::{self, GatewayStore};

use super::tools_cron;
use super::tools_skills;
use super::tools_workspace;

pub struct GatewayMcpServer {
    pub store: Arc<dyn GatewayStore>,
    pub platform: String,
    pub chat_id: String,
    pub user_id: String,
    pub project_dirs: Vec<String>,
    pub runtime_api_url: Option<String>,
    pub runtime_api_token: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct CronAddParams {
    pub cron_expr: String,
    pub message: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct CronDeleteParams {
    pub job_id: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct RemindAfterParams {
    pub minutes: u64,
    pub message: String,
    #[serde(default)]
    pub exec: bool,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct SkillReadParams {
    pub name: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct SkillAddParams {
    pub name: String,
    pub content: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct SkillDeleteParams {
    pub name: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct WorkspaceSwitchParams {
    pub path: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct SendAttachmentParams {
    /// Absolute or workspace-relative path to the file to send.
    pub path: String,
    /// Optional display filename.
    #[serde(default)]
    pub filename: Option<String>,
    /// Optional explanatory message sent before the file.
    #[serde(default)]
    pub caption: Option<String>,
    /// Optional MIME type, for example application/pdf or image/png.
    #[serde(default)]
    pub mime: Option<String>,
    /// Optional attachment kind: image, file, video, or audio.
    #[serde(default)]
    pub kind: Option<String>,
}

#[derive(Serialize, schemars::JsonSchema)]
pub struct TextResult {
    pub text: String,
}

impl TextResult {
    pub fn new(s: impl Into<String>) -> Self {
        Self { text: s.into() }
    }
}

#[tool_router(server_handler)]
impl GatewayMcpServer {
    #[allow(dead_code)]
    fn get_info(&self) -> ServerInfo {
        ServerInfo::default().with_server_info(Implementation::new(
            "astra-gateway",
            env!("CARGO_PKG_VERSION"),
        ))
    }

    #[tool(
        name = "gateway_cron_list",
        description = "List scheduled tasks and one-time reminders for the current conversation. Use this when the user asks what reminders, schedules, cron jobs, or timed tasks are active."
    )]
    async fn cron_list(&self) -> Json<TextResult> {
        let result = tools_cron::cron_list(Some(&*self.store), &self.platform, &self.chat_id).await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_cron_add",
        description = "Create a recurring scheduled task. Use this when the user asks for repeated reminders or repeated work, such as daily, weekly, every weekday, every hour, or at a regular time. cron_expr is a standard 5-field cron expression (minute hour day month weekday). message is the prompt to execute on each trigger. For a task that should keep checking until a result is ready and then stop, message MUST begin with [[ASTRA_POLL_UNTIL_RESULT]] on its own first line. Then instruct each scheduled turn to return only [[ASTRA_SILENT]] while pending, or the normal user-facing result when ready. Gateway automatically removes the job after the first visible result; do not instruct the scheduled agent to delete it. [[ASTRA_POLL_UNTIL_RESULT]] is input metadata and must not be included in the scheduled turn's output. Do not use the polling marker for recurring reports that should continue indefinitely."
    )]
    async fn cron_add(&self, Parameters(params): Parameters<CronAddParams>) -> Json<TextResult> {
        let result = tools_cron::cron_add(
            Some(&*self.store),
            &self.platform,
            &self.chat_id,
            &self.user_id,
            &params.cron_expr,
            &params.message,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_cron_delete",
        description = "Delete a scheduled task or reminder by job ID. Prefix match is supported. Use gateway_cron_list first if the user identifies the task by description rather than ID."
    )]
    async fn cron_delete(
        &self,
        Parameters(params): Parameters<CronDeleteParams>,
    ) -> Json<TextResult> {
        let result = tools_cron::cron_delete(
            Some(&*self.store),
            &self.platform,
            &self.chat_id,
            &params.job_id,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_remind_after",
        description = "Set a one-time reminder or one-time delayed task. Use this when the user asks for a relative-time reminder, such as in 10 minutes, after half an hour, tomorrow, or later today. Do NOT use this for repeated checks or poll-until-ready tasks; those must use gateway_cron_add. minutes: delay in minutes (1-10080). message: content. exec=false sends plain reminder text; exec=true runs the message as an agent prompt at trigger time."
    )]
    async fn remind_after(
        &self,
        Parameters(params): Parameters<RemindAfterParams>,
    ) -> Json<TextResult> {
        let result = tools_cron::remind_after(
            Some(&*self.store),
            &self.platform,
            &self.chat_id,
            &self.user_id,
            params.minutes,
            &params.message,
            params.exec,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_skills_list",
        description = "List all saved skills (reusable procedures) with their names and descriptions"
    )]
    async fn skills_list(&self) -> Json<TextResult> {
        let result = tools_skills::skills_list(&*self.store, &self.platform, &self.chat_id).await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_skills_read",
        description = "Read the full content of a saved skill by name"
    )]
    async fn skills_read(
        &self,
        Parameters(params): Parameters<SkillReadParams>,
    ) -> Json<TextResult> {
        let result =
            tools_skills::skills_read(&*self.store, &self.platform, &self.chat_id, &params.name)
                .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_skills_add",
        description = "Save a reusable skill (procedure/workflow). Only for non-trivial procedures worth reusing."
    )]
    async fn skills_add(&self, Parameters(params): Parameters<SkillAddParams>) -> Json<TextResult> {
        let result = tools_skills::skills_add(
            &*self.store,
            &self.platform,
            &self.chat_id,
            &params.name,
            &params.content,
            &params.description,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_skills_delete",
        description = "Delete a saved skill by name"
    )]
    async fn skills_delete(
        &self,
        Parameters(params): Parameters<SkillDeleteParams>,
    ) -> Json<TextResult> {
        let result =
            tools_skills::skills_delete(&*self.store, &self.platform, &self.chat_id, &params.name)
                .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_workspace_current",
        description = "Get the current workspace (working directory) path"
    )]
    async fn workspace_current(&self) -> Json<TextResult> {
        let result =
            tools_workspace::workspace_current(&*self.store, &self.platform, &self.user_id).await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_workspace_list",
        description = "List available projects/workspaces"
    )]
    fn workspace_list(&self) -> Json<TextResult> {
        let result = tools_workspace::workspace_list(&self.project_dirs);
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_workspace_switch",
        description = "Switch working directory to a different project path"
    )]
    async fn workspace_switch(
        &self,
        Parameters(params): Parameters<WorkspaceSwitchParams>,
    ) -> Json<TextResult> {
        let result = tools_workspace::workspace_switch(
            &*self.store,
            &self.platform,
            &self.user_id,
            &params.path,
            &self.project_dirs,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_send_attachment",
        description = "Send a local file to the current chat through the gateway. Use this when the user asks you to send, deliver, or return a document/image/file. path may be absolute or relative to the current workspace; caption is optional text sent before the file."
    )]
    async fn send_attachment(
        &self,
        Parameters(params): Parameters<SendAttachmentParams>,
    ) -> Json<TextResult> {
        Json(TextResult::new(
            self.send_attachment_inner(params)
                .await
                .unwrap_or_else(|e| format!("failed to send attachment: {e}")),
        ))
    }
}

impl GatewayMcpServer {
    async fn send_attachment_inner(&self, params: SendAttachmentParams) -> Result<String, String> {
        let Some(runtime_api_url) = self.runtime_api_url.as_deref() else {
            return Err("gateway runtime API is not configured".into());
        };
        let path = resolve_allowed_attachment_path(&params.path, &self.project_dirs)?;
        let payload = serde_json::json!({
            "platform": self.platform,
            "chat_id": self.chat_id,
            "path": path.to_string_lossy(),
            "filename": params.filename,
            "caption": params.caption,
            "mime": params.mime,
            "kind": params.kind,
        });
        let url = format!(
            "{}/outbound/attachment",
            runtime_api_url.trim_end_matches('/')
        );
        let client = reqwest::Client::new();
        let mut request = client.post(url).json(&payload);
        if let Some(token) = self.runtime_api_token.as_deref().filter(|s| !s.is_empty()) {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("runtime API request failed: {e}"))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("runtime API returned {status}: {text}"));
        }
        Ok(format!("attachment sent: {}", path.display()))
    }
}

fn resolve_allowed_attachment_path(
    path: &str,
    project_dirs: &[String],
) -> Result<std::path::PathBuf, String> {
    let input = std::path::PathBuf::from(path);
    let candidate = if input.is_absolute() {
        input
    } else {
        std::env::current_dir()
            .map_err(|e| format!("cannot read current directory: {e}"))?
            .join(input)
    };
    let canonical = candidate
        .canonicalize()
        .map_err(|e| format!("cannot access `{}`: {e}", candidate.display()))?;
    if !canonical.is_file() {
        return Err(format!("`{}` is not a file", canonical.display()));
    }

    let mut roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir().and_then(|p| p.canonicalize()) {
        roots.push(cwd);
    }
    for root in project_dirs {
        if let Ok(root) = std::path::PathBuf::from(root).canonicalize() {
            roots.push(root);
        }
    }
    if roots.iter().any(|root| canonical.starts_with(root)) {
        Ok(canonical)
    } else {
        Err(format!(
            "`{}` is outside the allowed workspace directories",
            canonical.display()
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_stdio_server(
    database_url: Option<String>,
    sqlite_path: Option<String>,
    platform: Option<String>,
    chat_id: Option<String>,
    user_id: Option<String>,
    project_dirs: Vec<String>,
    runtime_api_url: Option<String>,
    runtime_api_token: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let platform = platform.unwrap_or_else(|| "mcp".into());
    let chat_id = chat_id.unwrap_or_else(|| "default".into());
    let user_id = user_id.unwrap_or_else(|| "default".into());

    let storage_config = if let Some(url) = database_url {
        store::StorageConfig::Mysql { url }
    } else if let Some(path) = sqlite_path {
        store::StorageConfig::Sqlite { path }
    } else {
        store::StorageConfig::default()
    };

    let bundle = store::open_store_bundle(&storage_config)
        .await
        .map_err(|e| format!("failed to open store: {e}"))?
        .ok_or("storage not available")?;

    let server = GatewayMcpServer {
        store: bundle.store,
        platform,
        chat_id,
        user_id,
        project_dirs,
        runtime_api_url,
        runtime_api_token,
    };

    let transport = rmcp::transport::io::stdio();
    let service = rmcp::serve_server(server, transport).await?;
    service.waiting().await?;
    Ok(())
}
