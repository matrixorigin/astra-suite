use std::sync::Arc;

use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerInfo};
use rmcp::schemars;
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};

use crate::store::{self, GatewayStore};

use super::tools_cron;
use super::tools_skills;
use super::tools_tasks;
use super::tools_workspace;

pub struct GatewayMcpServer {
    pub store: Arc<dyn GatewayStore>,
    pub durable_store: Option<Arc<dyn crate::durable_task_store::DurableTaskStoreExt>>,
    pub platform: String,
    pub chat_id: String,
    pub user_id: String,
    pub project_dirs: Vec<String>,
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
pub struct TaskCreateParams {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct TaskIdParams {
    pub task_id: String,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct TaskFailParams {
    pub task_id: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema, Default)]
pub struct WorkspaceSwitchParams {
    pub path: String,
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
        description = "List all scheduled tasks (cron jobs and one-time reminders) for the current conversation"
    )]
    async fn cron_list(&self) -> Json<TextResult> {
        let result = tools_cron::cron_list(&*self.store, &self.platform, &self.chat_id).await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_cron_add",
        description = "Create a recurring scheduled task. cron_expr is a standard 5-field cron expression (minute hour day month weekday). message is the prompt to execute on each trigger."
    )]
    async fn cron_add(&self, Parameters(params): Parameters<CronAddParams>) -> Json<TextResult> {
        let result = tools_cron::cron_add(
            &*self.store,
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
        description = "Delete a scheduled task by job ID (prefix match supported)"
    )]
    async fn cron_delete(
        &self,
        Parameters(params): Parameters<CronDeleteParams>,
    ) -> Json<TextResult> {
        let result =
            tools_cron::cron_delete(&*self.store, &self.platform, &self.chat_id, &params.job_id)
                .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_remind_after",
        description = "Set a one-time reminder or task. minutes: delay in minutes (1-10080). message: content. exec: if true, the message is executed as a prompt by an agent at trigger time; if false, it's sent as plain text reminder."
    )]
    async fn remind_after(
        &self,
        Parameters(params): Parameters<RemindAfterParams>,
    ) -> Json<TextResult> {
        let result = tools_cron::remind_after(
            &*self.store,
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
        name = "gateway_tasks_list",
        description = "List active durable tasks (long-running, checkpointable tasks)"
    )]
    async fn tasks_list(&self) -> Json<TextResult> {
        let result =
            tools_tasks::tasks_list(self.durable_store.as_deref(), &self.platform, &self.chat_id)
                .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_tasks_create",
        description = "Create a new durable task for interruptible multi-step work"
    )]
    async fn tasks_create(
        &self,
        Parameters(params): Parameters<TaskCreateParams>,
    ) -> Json<TextResult> {
        let result = tools_tasks::tasks_create(
            self.durable_store.as_deref(),
            &self.platform,
            &self.chat_id,
            &params.name,
            params.description.as_deref(),
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_tasks_status",
        description = "Get status of a durable task by ID (prefix match supported)"
    )]
    async fn tasks_status(&self, Parameters(params): Parameters<TaskIdParams>) -> Json<TextResult> {
        let result = tools_tasks::tasks_status(
            self.durable_store.as_deref(),
            &self.platform,
            &self.chat_id,
            &params.task_id,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_tasks_complete",
        description = "Mark a durable task as completed"
    )]
    async fn tasks_complete(
        &self,
        Parameters(params): Parameters<TaskIdParams>,
    ) -> Json<TextResult> {
        let result = tools_tasks::tasks_complete(
            self.durable_store.as_deref(),
            &self.platform,
            &self.chat_id,
            &params.task_id,
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(
        name = "gateway_tasks_fail",
        description = "Mark a durable task as failed with optional error message"
    )]
    async fn tasks_fail(&self, Parameters(params): Parameters<TaskFailParams>) -> Json<TextResult> {
        let result = tools_tasks::tasks_fail(
            self.durable_store.as_deref(),
            &self.platform,
            &self.chat_id,
            &params.task_id,
            params.error.as_deref(),
        )
        .await;
        Json(TextResult::new(result))
    }

    #[tool(name = "gateway_tasks_cancel", description = "Cancel a durable task")]
    async fn tasks_cancel(&self, Parameters(params): Parameters<TaskIdParams>) -> Json<TextResult> {
        let result = tools_tasks::tasks_cancel(
            self.durable_store.as_deref(),
            &self.platform,
            &self.chat_id,
            &params.task_id,
        )
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
        )
        .await;
        Json(TextResult::new(result))
    }
}

pub async fn run_stdio_server(
    database_url: Option<String>,
    sqlite_path: Option<String>,
    platform: Option<String>,
    chat_id: Option<String>,
    user_id: Option<String>,
    project_dirs: Vec<String>,
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

    let durable_store = bundle.durable_store;

    let server = GatewayMcpServer {
        store: bundle.store,
        durable_store,
        platform,
        chat_id,
        user_id,
        project_dirs,
    };

    let transport = rmcp::transport::io::stdio();
    let service = rmcp::serve_server(server, transport).await?;
    service.waiting().await?;
    Ok(())
}
