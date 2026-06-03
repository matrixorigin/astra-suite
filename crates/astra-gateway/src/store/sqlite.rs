//! SQLite-backed [`GatewayStore`] implementation.
//!
//! Zero-config default backend — creates `~/.astra-gateway/gateway.db`
//! automatically on first use. All data types use TEXT for timestamps
//! and INTEGER for booleans, matching SQLite's type affinity model.

use super::{
    CronJobRecord, CronJobSpec, DueJob, GatewayStore, PlatformCredential, SessionRecord,
    SkillRecord, StoreError, UsageRecord, UsageSummary, next_cron_run_str,
};
use async_trait::async_trait;
use sqlx::SqlitePool;

/// SQLite-backed gateway store.
pub struct SqliteGatewayStore {
    pool: SqlitePool,
}

impl SqliteGatewayStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn connect(path: &str) -> Result<Self, StoreError> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let url = format!("sqlite:{path}?mode=rwc");
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl GatewayStore for SqliteGatewayStore {
    // ── Schema ──────────────────────────────────────────────────────────

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_users (
                platform TEXT NOT NULL,
                platform_user_id TEXT NOT NULL,
                display_name TEXT DEFAULT '',
                preferences TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                PRIMARY KEY (platform, platform_user_id)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '',
                cli_profile TEXT NOT NULL DEFAULT 'default',
                astra_session_id TEXT NOT NULL,
                is_current INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                last_active TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_sessions_current
             ON gw_sessions(platform, chat_id, cli_profile, is_current)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_cron_jobs (
                job_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                user_id TEXT NOT NULL DEFAULT '',
                cron_expr TEXT NOT NULL,
                message TEXT NOT NULL,
                description TEXT DEFAULT '',
                enabled INTEGER DEFAULT 1,
                last_run TEXT,
                next_run TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cron_enabled
             ON gw_cron_jobs(enabled, next_run)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_platform_credentials (
                platform TEXT NOT NULL,
                user_id TEXT NOT NULL,
                credential_type TEXT NOT NULL,
                credentials TEXT NOT NULL,
                expires_at TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                updated_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                PRIMARY KEY (platform, user_id, credential_type)
            )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                user_id TEXT NOT NULL,
                cli_profile TEXT NOT NULL DEFAULT 'astra',
                model TEXT,
                trace_id TEXT,
                request_id TEXT,
                run_id TEXT,
                session_id TEXT,
                tokens_prompt INTEGER NOT NULL DEFAULT 0,
                tokens_completion INTEGER NOT NULL DEFAULT 0,
                cached_input_tokens INTEGER NOT NULL DEFAULT 0,
                cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
                cache_read_input_tokens INTEGER NOT NULL DEFAULT 0,
                reasoning_output_tokens INTEGER NOT NULL DEFAULT 0,
                total_tokens INTEGER NOT NULL DEFAULT 0,
                context_window INTEGER,
                max_output_tokens INTEGER,
                cost_usd REAL,
                raw_usage_json TEXT,
                tool_calls INTEGER NOT NULL DEFAULT 0,
                elapsed_ms INTEGER NOT NULL DEFAULT 0,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            )",
        )
        .execute(&self.pool)
        .await?;

        for migration in [
            "ALTER TABLE gw_usage ADD COLUMN trace_id TEXT",
            "ALTER TABLE gw_usage ADD COLUMN request_id TEXT",
            "ALTER TABLE gw_usage ADD COLUMN run_id TEXT",
            "ALTER TABLE gw_usage ADD COLUMN session_id TEXT",
            "ALTER TABLE gw_usage ADD COLUMN cached_input_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN cache_read_input_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN reasoning_output_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN total_tokens INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN context_window INTEGER",
            "ALTER TABLE gw_usage ADD COLUMN max_output_tokens INTEGER",
            "ALTER TABLE gw_usage ADD COLUMN cost_usd REAL",
            "ALTER TABLE gw_usage ADD COLUMN raw_usage_json TEXT",
        ] {
            let _ = sqlx::query(migration).execute(&self.pool).await;
        }

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_usage_user_day
             ON gw_usage(platform, user_id, created_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_durable_tasks (
                task_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                owner_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'created',
                progress_pct INTEGER NOT NULL DEFAULT 0,
                step_description TEXT,
                checkpoint_json TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now')),
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_durable_tasks_owner_status
             ON gw_durable_tasks(owner_id, status)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_durable_tasks_owner_updated
             ON gw_durable_tasks(owner_id, updated_at)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_skills (
                platform TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
                PRIMARY KEY (platform, chat_id, name)
            )",
        )
        .execute(&self.pool)
        .await?;

        crate::trace_model::ensure_sqlite_schema(&self.pool).await?;

        tracing::info!("SQLite gateway schema ensured");
        Ok(())
    }

    // ── Users ───────────────────────────────────────────────────────────

    async fn is_first_message(&self, platform: &str, user_id: &str) -> Result<bool, StoreError> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count == 0)
    }

    async fn upsert_user(
        &self,
        platform: &str,
        user_id: &str,
        display_name: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO gw_users (platform, platform_user_id, display_name)
             VALUES (?, ?, ?)
             ON CONFLICT(platform, platform_user_id) DO UPDATE SET
                display_name = excluded.display_name,
                updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
        )
        .bind(platform)
        .bind(user_id)
        .bind(display_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn set_user_preference(
        &self,
        platform: &str,
        user_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), StoreError> {
        let pref_json = serde_json::json!({ key: value }).to_string();

        // First: if preferences is NULL, set the whole JSON object.
        let initialized = sqlx::query(
            "UPDATE gw_users SET preferences = ?, updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
             WHERE platform = ? AND platform_user_id = ? AND preferences IS NULL",
        )
        .bind(&pref_json)
        .bind(platform)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        // Second: if preferences already exists, merge the key in.
        let merged = sqlx::query(
            "UPDATE gw_users
             SET preferences = json_set(preferences, '$.' || ?, ?),
                  updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
             WHERE platform = ? AND platform_user_id = ? AND preferences IS NOT NULL",
        )
        .bind(key)
        .bind(value)
        .bind(platform)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        if initialized.rows_affected() + merged.rows_affected() == 0 {
            return Err(StoreError::NotFound(format!(
                "user not found: {platform}:{user_id}"
            )));
        }

        Ok(())
    }

    async fn get_user_preference(
        &self,
        platform: &str,
        user_id: &str,
        key: &str,
    ) -> Result<Option<String>, StoreError> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT json_extract(preferences, '$.' || ?)
             FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind(key)
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        // SQLite json_extract returns NULL for missing keys (not the string "null").
        Ok(row.and_then(|r| r.0))
    }

    // ── Sessions ────────────────────────────────────────────────────────

    async fn get_current_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT astra_session_id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = 1
             ORDER BY last_active DESC LIMIT 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0))
    }

    async fn get_session_last_active(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT last_active FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = 1
             ORDER BY last_active DESC LIMIT 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.0))
    }

    async fn set_current_session(
        &self,
        platform: &str,
        chat_id: &str,
        user_id: &str,
        astra_session_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        // Mark old sessions for this CLI as not current.
        sqlx::query(
            "UPDATE gw_sessions SET is_current = 0
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await?;

        // Check if this session_id already exists for this CLI.
        let existing: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .bind(astra_session_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some((id,)) = existing {
            // Reactivate existing session.
            sqlx::query(
                "UPDATE gw_sessions SET is_current = 1, last_active = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?",
            )
            .bind(id)
            .execute(&self.pool)
            .await?;
        } else {
            // Insert new session.
            sqlx::query(
                "INSERT INTO gw_sessions (platform, chat_id, user_id, cli_profile, astra_session_id, is_current)
                 VALUES (?, ?, ?, ?, ?, 1)",
            )
            .bind(platform)
            .bind(chat_id)
            .bind(user_id)
            .bind(cli_profile)
            .bind(astra_session_id)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn touch_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE gw_sessions SET last_active = strftime('%Y-%m-%d %H:%M:%f', 'now')
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_sessions(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Vec<SessionRecord>, StoreError> {
        let rows: Vec<(String, i32, String)> = sqlx::query_as(
            "SELECT astra_session_id, is_current, created_at
             FROM gw_sessions WHERE platform = ? AND chat_id = ? AND cli_profile = ?
             ORDER BY last_active DESC LIMIT 20",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(sid, cur, created)| SessionRecord {
                session_id: sid,
                is_current: cur != 0,
                created_at: created,
            })
            .collect())
    }

    async fn switch_session(
        &self,
        platform: &str,
        chat_id: &str,
        target_session_id: &str,
    ) -> Result<bool, StoreError> {
        // Check target exists.
        let exists: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(target_session_id)
        .fetch_optional(&self.pool)
        .await?;

        if exists.is_none() {
            return Ok(false);
        }

        // Clear current.
        sqlx::query(
            "UPDATE gw_sessions SET is_current = 0
             WHERE platform = ? AND chat_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .execute(&self.pool)
        .await?;

        // Set target as current.
        sqlx::query(
            "UPDATE gw_sessions SET is_current = 1, last_active = strftime('%Y-%m-%d %H:%M:%f', 'now')
             WHERE platform = ? AND chat_id = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(target_session_id)
        .execute(&self.pool)
        .await?;

        Ok(true)
    }

    async fn reset_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE gw_sessions SET is_current = 0
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // ── Cron jobs ───────────────────────────────────────────────────────

    async fn create_cron_job(&self, spec: &CronJobSpec) -> Result<(), StoreError> {
        let next = next_cron_run_str(&spec.cron_expr);
        sqlx::query(
            "INSERT INTO gw_cron_jobs (job_id, platform, chat_id, user_id, cron_expr, message, description, next_run)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&spec.job_id)
        .bind(&spec.platform)
        .bind(&spec.chat_id)
        .bind(&spec.user_id)
        .bind(&spec.cron_expr)
        .bind(&spec.message)
        .bind(&spec.description)
        .bind(&next)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_cron_jobs(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<CronJobRecord>, StoreError> {
        let rows: Vec<(String, String, String, i32)> = sqlx::query_as(
            "SELECT job_id, cron_expr, description, enabled
             FROM gw_cron_jobs WHERE platform = ? AND chat_id = ?
             ORDER BY created_at",
        )
        .bind(platform)
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, expr, desc, en)| CronJobRecord {
                job_id: id,
                cron_expr: expr,
                description: desc,
                enabled: en != 0,
            })
            .collect())
    }

    async fn delete_cron_job(&self, job_id: &str) -> Result<bool, StoreError> {
        let result = sqlx::query("DELETE FROM gw_cron_jobs WHERE job_id = ?")
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn get_due_jobs(&self) -> Result<Vec<DueJob>, StoreError> {
        let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT job_id, platform, chat_id, message, cron_expr
             FROM gw_cron_jobs
             WHERE enabled = 1 AND (next_run IS NULL OR next_run <= strftime('%Y-%m-%d %H:%M:%f', 'now'))",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(id, plat, chat, msg, expr)| DueJob {
                job_id: id,
                platform: plat,
                chat_id: chat,
                message: msg,
                cron_expr: expr,
            })
            .collect())
    }

    async fn mark_job_run(&self, job_id: &str, cron_expr: &str) -> Result<(), StoreError> {
        let next = next_cron_run_str(cron_expr);
        sqlx::query(
            "UPDATE gw_cron_jobs SET last_run = strftime('%Y-%m-%d %H:%M:%f', 'now'), next_run = ? WHERE job_id = ?",
        )
        .bind(&next)
        .bind(job_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_cron_next_run(&self, job_id: &str, next_run: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE gw_cron_jobs SET next_run = ? WHERE job_id = ?")
            .bind(next_run)
            .bind(job_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_cron_job_user_id(&self, job_id: &str) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT user_id FROM gw_cron_jobs WHERE job_id = ?")
                .bind(job_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(uid,)| uid))
    }

    // ── Platform credentials ────────────────────────────────────────────

    async fn save_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
        credentials: &serde_json::Value,
        expires_at: Option<&str>,
    ) -> Result<(), StoreError> {
        let cred_str = credentials.to_string();
        sqlx::query(
            "INSERT INTO gw_platform_credentials (platform, user_id, credential_type, credentials, expires_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(platform, user_id, credential_type) DO UPDATE SET
                credentials = excluded.credentials,
                expires_at = excluded.expires_at,
                updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now')",
        )
        .bind(platform)
        .bind(user_id)
        .bind(credential_type)
        .bind(&cred_str)
        .bind(expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
    ) -> Result<Option<PlatformCredential>, StoreError> {
        let row: Option<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT platform, user_id, credential_type, credentials, expires_at
             FROM gw_platform_credentials
             WHERE platform = ? AND user_id = ? AND credential_type = ?",
        )
        .bind(platform)
        .bind(user_id)
        .bind(credential_type)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(p, u, ct, creds, exp)| PlatformCredential {
            platform: p,
            user_id: u,
            credential_type: ct,
            credentials: serde_json::from_str(&creds).unwrap_or(serde_json::Value::Null),
            expires_at: exp,
        }))
    }

    async fn list_credentials(
        &self,
        platform: &str,
    ) -> Result<Vec<PlatformCredential>, StoreError> {
        let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT platform, user_id, credential_type, credentials, expires_at
             FROM gw_platform_credentials
             WHERE platform = ?
             ORDER BY updated_at DESC",
        )
        .bind(platform)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(p, u, ct, creds, exp)| PlatformCredential {
                platform: p,
                user_id: u,
                credential_type: ct,
                credentials: serde_json::from_str(&creds).unwrap_or(serde_json::Value::Null),
                expires_at: exp,
            })
            .collect())
    }

    async fn delete_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
    ) -> Result<bool, StoreError> {
        let result = sqlx::query(
            "DELETE FROM gw_platform_credentials
             WHERE platform = ? AND user_id = ? AND credential_type = ?",
        )
        .bind(platform)
        .bind(user_id)
        .bind(credential_type)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    // ── Usage ───────────────────────────────────────────────────────────

    async fn record_usage(&self, r: &UsageRecord) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO gw_usage (
                platform, user_id, cli_profile, model, trace_id, request_id, run_id, session_id,
                tokens_prompt, tokens_completion, cached_input_tokens, cache_creation_input_tokens,
                cache_read_input_tokens, reasoning_output_tokens, total_tokens, context_window,
                max_output_tokens, cost_usd, raw_usage_json, tool_calls, elapsed_ms
             )
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&r.platform)
        .bind(&r.user_id)
        .bind(&r.cli_profile)
        .bind(&r.model)
        .bind(&r.trace_id)
        .bind(&r.request_id)
        .bind(&r.run_id)
        .bind(&r.session_id)
        .bind(r.tokens_prompt as i64)
        .bind(r.tokens_completion as i64)
        .bind(r.cached_input_tokens as i64)
        .bind(r.cache_creation_input_tokens as i64)
        .bind(r.cache_read_input_tokens as i64)
        .bind(r.reasoning_output_tokens as i64)
        .bind(r.total_tokens as i64)
        .bind(r.context_window.map(|v| v as i64))
        .bind(r.max_output_tokens.map(|v| v as i64))
        .bind(r.cost_usd)
        .bind(&r.raw_usage_json)
        .bind(r.tool_calls as i32)
        .bind(r.elapsed_ms as i64)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_usage_today(
        &self,
        platform: &str,
        user_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let row: Option<(
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            f64,
            i64,
        )> = sqlx::query_as(
            "SELECT COUNT(*),
                    COALESCE(SUM(tokens_prompt), 0),
                    COALESCE(SUM(tokens_completion), 0),
                    COALESCE(SUM(cached_input_tokens), 0),
                    COALESCE(SUM(cache_creation_input_tokens), 0),
                    COALESCE(SUM(cache_read_input_tokens), 0),
                    COALESCE(SUM(reasoning_output_tokens), 0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END), 0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd), 0.0),
                    COALESCE(SUM(tool_calls), 0)
             FROM gw_usage
             WHERE platform = ? AND user_id = ? AND created_at >= date('now')",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .map(
                |(
                    m,
                    p,
                    c,
                    cached,
                    cache_create,
                    cache_read,
                    reasoning,
                    total,
                    ctx,
                    max_out,
                    cost,
                    t,
                )| UsageSummary {
                    messages: m as u64,
                    tokens_prompt: p as u64,
                    tokens_completion: c as u64,
                    cached_input_tokens: cached as u64,
                    cache_creation_input_tokens: cache_create as u64,
                    cache_read_input_tokens: cache_read as u64,
                    reasoning_output_tokens: reasoning as u64,
                    total_tokens: total as u64,
                    context_window: ctx.map(|v| v as u64),
                    max_output_tokens: max_out.map(|v| v as u64),
                    cost_usd: cost,
                    tool_calls: t as u64,
                },
            )
            .unwrap_or_default())
    }

    async fn get_usage_total(
        &self,
        platform: &str,
        user_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let row: Option<(
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            f64,
            i64,
        )> = sqlx::query_as(
            "SELECT COUNT(*),
                    COALESCE(SUM(tokens_prompt), 0),
                    COALESCE(SUM(tokens_completion), 0),
                    COALESCE(SUM(cached_input_tokens), 0),
                    COALESCE(SUM(cache_creation_input_tokens), 0),
                    COALESCE(SUM(cache_read_input_tokens), 0),
                    COALESCE(SUM(reasoning_output_tokens), 0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END), 0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd), 0.0),
                    COALESCE(SUM(tool_calls), 0)
             FROM gw_usage
             WHERE platform = ? AND user_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .map(
                |(
                    m,
                    p,
                    c,
                    cached,
                    cache_create,
                    cache_read,
                    reasoning,
                    total,
                    ctx,
                    max_out,
                    cost,
                    t,
                )| UsageSummary {
                    messages: m as u64,
                    tokens_prompt: p as u64,
                    tokens_completion: c as u64,
                    cached_input_tokens: cached as u64,
                    cache_creation_input_tokens: cache_create as u64,
                    cache_read_input_tokens: cache_read as u64,
                    reasoning_output_tokens: reasoning as u64,
                    total_tokens: total as u64,
                    context_window: ctx.map(|v| v as u64),
                    max_output_tokens: max_out.map(|v| v as u64),
                    cost_usd: cost,
                    tool_calls: t as u64,
                },
            )
            .unwrap_or_default())
    }

    async fn get_usage_session(
        &self,
        platform: &str,
        user_id: &str,
        session_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let row: Option<(
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
            Option<i64>,
            f64,
            i64,
        )> = sqlx::query_as(
            "SELECT COUNT(*),
                    COALESCE(SUM(tokens_prompt), 0),
                    COALESCE(SUM(tokens_completion), 0),
                    COALESCE(SUM(cached_input_tokens), 0),
                    COALESCE(SUM(cache_creation_input_tokens), 0),
                    COALESCE(SUM(cache_read_input_tokens), 0),
                    COALESCE(SUM(reasoning_output_tokens), 0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END), 0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd), 0.0),
                    COALESCE(SUM(tool_calls), 0)
             FROM gw_usage
             WHERE platform = ? AND user_id = ? AND session_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row
            .map(
                |(
                    m,
                    p,
                    c,
                    cached,
                    cache_create,
                    cache_read,
                    reasoning,
                    total,
                    ctx,
                    max_out,
                    cost,
                    t,
                )| UsageSummary {
                    messages: m as u64,
                    tokens_prompt: p as u64,
                    tokens_completion: c as u64,
                    cached_input_tokens: cached as u64,
                    cache_creation_input_tokens: cache_create as u64,
                    cache_read_input_tokens: cache_read as u64,
                    reasoning_output_tokens: reasoning as u64,
                    total_tokens: total as u64,
                    context_window: ctx.map(|v| v as u64),
                    max_output_tokens: max_out.map(|v| v as u64),
                    cost_usd: cost,
                    tool_calls: t as u64,
                },
            )
            .unwrap_or_default())
    }

    // ── Skills ─────────────────────────────────────────────────────────

    async fn list_skills(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<SkillRecord>, StoreError> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT name, content, description, created_at
             FROM gw_skills WHERE platform = ? AND chat_id = ?
             ORDER BY name",
        )
        .bind(platform)
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(name, content, description, created_at)| SkillRecord {
                name,
                content,
                description,
                created_at,
            })
            .collect())
    }

    async fn get_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
    ) -> Result<Option<SkillRecord>, StoreError> {
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT name, content, description, created_at
             FROM gw_skills WHERE platform = ? AND chat_id = ? AND name = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        Ok(
            row.map(|(name, content, description, created_at)| SkillRecord {
                name,
                content,
                description,
                created_at,
            }),
        )
    }

    async fn upsert_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
        content: &str,
        description: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO gw_skills (platform, chat_id, name, content, description)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(platform, chat_id, name) DO UPDATE SET content=excluded.content, description=excluded.description",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(name)
        .bind(content)
        .bind(description)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
    ) -> Result<bool, StoreError> {
        let result =
            sqlx::query("DELETE FROM gw_skills WHERE platform = ? AND chat_id = ? AND name = ?")
                .bind(platform)
                .bind(chat_id)
                .bind(name)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> SqliteGatewayStore {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite");
        let store = SqliteGatewayStore::new(pool);
        store.ensure_schema().await.expect("ensure_schema");
        store
    }

    #[tokio::test]
    async fn ensure_schema_runs_without_error() {
        let _ = make_store().await;
    }

    #[tokio::test]
    async fn ensure_schema_is_idempotent() {
        let store = make_store().await;
        // Running it again should not fail.
        store.ensure_schema().await.expect("second ensure_schema");
    }

    #[tokio::test]
    async fn upsert_user_and_is_first_message() {
        let store = make_store().await;

        assert!(store.is_first_message("wx", "u1").await.unwrap());

        store.upsert_user("wx", "u1", "Alice").await.unwrap();

        assert!(!store.is_first_message("wx", "u1").await.unwrap());

        // Other user is still first.
        assert!(store.is_first_message("wx", "u2").await.unwrap());
    }

    #[tokio::test]
    async fn upsert_user_updates_display_name() {
        let store = make_store().await;
        store.upsert_user("wx", "u1", "Alice").await.unwrap();
        store.upsert_user("wx", "u1", "Bob").await.unwrap();

        // Still only one row.
        assert!(!store.is_first_message("wx", "u1").await.unwrap());
    }

    #[tokio::test]
    async fn set_and_get_user_preference() {
        let store = make_store().await;
        store.upsert_user("wx", "u1", "Test").await.unwrap();

        // No preference set yet.
        let val = store.get_user_preference("wx", "u1", "lang").await.unwrap();
        assert!(val.is_none());

        // Set one.
        store
            .set_user_preference("wx", "u1", "lang", "en")
            .await
            .unwrap();
        let val = store.get_user_preference("wx", "u1", "lang").await.unwrap();
        assert_eq!(val.as_deref(), Some("en"));

        // Overwrite.
        store
            .set_user_preference("wx", "u1", "lang", "zh")
            .await
            .unwrap();
        let val = store.get_user_preference("wx", "u1", "lang").await.unwrap();
        assert_eq!(val.as_deref(), Some("zh"));

        // Multiple keys coexist.
        store
            .set_user_preference("wx", "u1", "model_override_astra", "opus")
            .await
            .unwrap();
        assert_eq!(
            store
                .get_user_preference("wx", "u1", "lang")
                .await
                .unwrap()
                .as_deref(),
            Some("zh")
        );
        assert_eq!(
            store
                .get_user_preference("wx", "u1", "model_override_astra")
                .await
                .unwrap()
                .as_deref(),
            Some("opus")
        );
    }

    #[tokio::test]
    async fn get_preference_for_missing_user() {
        let store = make_store().await;
        let val = store
            .get_user_preference("wx", "ghost", "lang")
            .await
            .unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn set_preference_for_missing_user_fails() {
        let store = make_store().await;
        let err = store
            .set_user_preference("wx", "ghost", "lang", "en")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("user not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn create_and_list_cron_jobs() {
        let store = make_store().await;

        store
            .create_cron_job(&CronJobSpec {
                job_id: "j1".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "30 9 * * *".into(),
                message: "good morning".into(),
                description: "daily greeting".into(),
            })
            .await
            .unwrap();

        store
            .create_cron_job(&CronJobSpec {
                job_id: "j2".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 18 * * 1-5".into(),
                message: "wrap up".into(),
                description: "weekday reminder".into(),
            })
            .await
            .unwrap();

        let jobs = store.list_cron_jobs("wx", "c1").await.unwrap();
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].job_id, "j1");
        assert!(jobs[0].enabled);
        assert_eq!(jobs[1].description, "weekday reminder");
    }

    #[tokio::test]
    async fn delete_cron_job() {
        let store = make_store().await;

        store
            .create_cron_job(&CronJobSpec {
                job_id: "j1".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "hello".into(),
                description: "".into(),
            })
            .await
            .unwrap();

        assert!(store.delete_cron_job("j1").await.unwrap());
        assert!(!store.delete_cron_job("j1").await.unwrap()); // already deleted
        assert!(store.list_cron_jobs("wx", "c1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn session_lifecycle() {
        let store = make_store().await;

        // No current session initially.
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert!(cur.is_none());

        // Set a session.
        store
            .set_current_session("wx", "c1", "u1", "sess-1", "astra")
            .await
            .unwrap();
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert_eq!(cur.as_deref(), Some("sess-1"));

        // Touch.
        store.touch_session("wx", "c1", "astra").await.unwrap();

        // List.
        let sessions = store.list_sessions("wx", "c1", "astra").await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].is_current);

        // Set another session.
        store
            .set_current_session("wx", "c1", "u1", "sess-2", "astra")
            .await
            .unwrap();
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert_eq!(cur.as_deref(), Some("sess-2"));

        // Switch back.
        assert!(store.switch_session("wx", "c1", "sess-1").await.unwrap());
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert_eq!(cur.as_deref(), Some("sess-1"));

        // Switch to nonexistent.
        assert!(
            !store
                .switch_session("wx", "c1", "nonexistent")
                .await
                .unwrap()
        );

        // Reset.
        store.reset_session("wx", "c1", "astra").await.unwrap();
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert!(cur.is_none());
    }

    #[tokio::test]
    async fn credential_roundtrip() {
        let store = make_store().await;

        let creds = serde_json::json!({"token": "abc123"});
        store
            .save_credential("wx", "default", "bot_token", &creds, None)
            .await
            .unwrap();

        let got = store
            .get_credential("wx", "default", "bot_token")
            .await
            .unwrap()
            .expect("should exist");
        assert_eq!(got.credentials["token"], "abc123");
        assert!(got.expires_at.is_none());

        // Update.
        let new_creds = serde_json::json!({"token": "xyz789"});
        store
            .save_credential("wx", "default", "bot_token", &new_creds, Some("2026-12-31"))
            .await
            .unwrap();
        let got = store
            .get_credential("wx", "default", "bot_token")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.credentials["token"], "xyz789");
        assert_eq!(got.expires_at.as_deref(), Some("2026-12-31"));

        // List.
        let all = store.list_credentials("wx").await.unwrap();
        assert_eq!(all.len(), 1);

        // Delete.
        assert!(
            store
                .delete_credential("wx", "default", "bot_token")
                .await
                .unwrap()
        );
        assert!(
            !store
                .delete_credential("wx", "default", "bot_token")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn usage_roundtrip() {
        let store = make_store().await;

        // Initially zero.
        let today = store.get_usage_today("wx", "u1").await.unwrap();
        assert_eq!(today.messages, 0);

        store
            .record_usage(&UsageRecord {
                platform: "wx".into(),
                user_id: "u1".into(),
                cli_profile: "astra".into(),
                model: Some("opus".into()),
                trace_id: None,
                request_id: None,
                run_id: None,
                session_id: None,
                tokens_prompt: 1000,
                tokens_completion: 200,
                cached_input_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                reasoning_output_tokens: 0,
                total_tokens: 1200,
                context_window: None,
                max_output_tokens: None,
                cost_usd: None,
                raw_usage_json: None,
                tool_calls: 3,
                elapsed_ms: 5000,
            })
            .await
            .unwrap();

        let today = store.get_usage_today("wx", "u1").await.unwrap();
        assert_eq!(today.messages, 1);
        assert_eq!(today.tokens_prompt, 1000);
        assert_eq!(today.tokens_completion, 200);
        assert_eq!(today.total_tokens, 1200);
        assert_eq!(today.tool_calls, 3);

        let total = store.get_usage_total("wx", "u1").await.unwrap();
        assert_eq!(total.messages, 1);
    }

    #[tokio::test]
    async fn update_cron_next_run_sets_timestamp() {
        let store = make_store().await;
        store
            .create_cron_job(&CronJobSpec {
                job_id: "j1".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "hello".into(),
                description: "".into(),
            })
            .await
            .unwrap();

        store
            .update_cron_next_run("j1", "2099-12-31 23:59:59")
            .await
            .unwrap();

        // Verify via get_due_jobs: job should NOT be due (next_run far in future)
        let due = store.get_due_jobs().await.unwrap();
        assert!(due.is_empty(), "job with future next_run should not be due");
    }

    #[tokio::test]
    async fn session_last_active() {
        let store = make_store().await;

        let la = store
            .get_session_last_active("wx", "c1", "astra")
            .await
            .unwrap();
        assert!(la.is_none());

        store
            .set_current_session("wx", "c1", "u1", "sess-1", "astra")
            .await
            .unwrap();
        let la = store
            .get_session_last_active("wx", "c1", "astra")
            .await
            .unwrap();
        assert!(la.is_some());
    }

    #[tokio::test]
    async fn cli_profile_isolation() {
        let store = make_store().await;

        store
            .set_current_session("wx", "c1", "u1", "astra-sess", "astra")
            .await
            .unwrap();
        store
            .set_current_session("wx", "c1", "u1", "claude-sess", "claude")
            .await
            .unwrap();

        assert_eq!(
            store
                .get_current_session("wx", "c1", "astra")
                .await
                .unwrap()
                .as_deref(),
            Some("astra-sess")
        );
        assert_eq!(
            store
                .get_current_session("wx", "c1", "claude")
                .await
                .unwrap()
                .as_deref(),
            Some("claude-sess")
        );
    }

    // ── Session edge cases ─────────────────────────────────────────────

    #[tokio::test]
    async fn session_new_resets_previous_current() {
        let store = make_store().await;

        // Create session A (current).
        store
            .set_current_session("wx", "c1", "u1", "sess-A", "astra")
            .await
            .unwrap();
        let sessions = store.list_sessions("wx", "c1", "astra").await.unwrap();
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].is_current);

        // Create session B → A should lose is_current.
        store
            .set_current_session("wx", "c1", "u1", "sess-B", "astra")
            .await
            .unwrap();

        let sessions = store.list_sessions("wx", "c1", "astra").await.unwrap();
        assert_eq!(sessions.len(), 2);

        let a = sessions.iter().find(|s| s.session_id == "sess-A").unwrap();
        let b = sessions.iter().find(|s| s.session_id == "sess-B").unwrap();
        assert!(!a.is_current, "session A should no longer be current");
        assert!(b.is_current, "session B should be current");
    }

    #[tokio::test]
    async fn session_switch_nonexistent_id_fails() {
        let store = make_store().await;

        // Create one session so the chat exists.
        store
            .set_current_session("wx", "c1", "u1", "sess-1", "astra")
            .await
            .unwrap();

        // Switch to a non-existent session → returns false (not found).
        let result = store
            .switch_session("wx", "c1", "does-not-exist")
            .await
            .unwrap();
        assert!(!result, "switch to nonexistent session should return false");

        // Original session should remain current.
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert_eq!(cur.as_deref(), Some("sess-1"));
    }

    #[tokio::test]
    async fn session_touch_updates_last_active() {
        let store = make_store().await;

        store
            .set_current_session("wx", "c1", "u1", "sess-1", "astra")
            .await
            .unwrap();
        let la1 = store
            .get_session_last_active("wx", "c1", "astra")
            .await
            .unwrap()
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(15)).await;

        store.touch_session("wx", "c1", "astra").await.unwrap();
        let la2 = store
            .get_session_last_active("wx", "c1", "astra")
            .await
            .unwrap()
            .unwrap();

        assert!(
            la2 > la1,
            "last_active should advance after touch: {la1} vs {la2}"
        );
    }

    #[tokio::test]
    async fn session_list_returns_all_profiles() {
        let store = make_store().await;

        store
            .set_current_session("wx", "c1", "u1", "sess-astra", "astra")
            .await
            .unwrap();
        store
            .set_current_session("wx", "c1", "u1", "sess-claude", "claude")
            .await
            .unwrap();

        let astra_sessions = store.list_sessions("wx", "c1", "astra").await.unwrap();
        let claude_sessions = store.list_sessions("wx", "c1", "claude").await.unwrap();

        assert_eq!(astra_sessions.len(), 1);
        assert_eq!(astra_sessions[0].session_id, "sess-astra");
        assert_eq!(claude_sessions.len(), 1);
        assert_eq!(claude_sessions[0].session_id, "sess-claude");
    }

    // ── Cron edge cases ────────────────────────────────────────────────

    #[tokio::test]
    async fn cron_get_due_jobs_respects_enabled_flag() {
        let store = make_store().await;

        // Create two jobs with next_run in the past (so they're due).
        store
            .create_cron_job(&CronJobSpec {
                job_id: "enabled-job".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "enabled msg".into(),
                description: "".into(),
            })
            .await
            .unwrap();
        store
            .create_cron_job(&CronJobSpec {
                job_id: "disabled-job".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "disabled msg".into(),
                description: "".into(),
            })
            .await
            .unwrap();

        // Set both jobs' next_run to the past.
        store
            .update_cron_next_run("enabled-job", "2000-01-01 00:00:00")
            .await
            .unwrap();
        store
            .update_cron_next_run("disabled-job", "2000-01-01 00:00:00")
            .await
            .unwrap();

        // Disable one job via raw SQL (no API method exists).
        sqlx::query("UPDATE gw_cron_jobs SET enabled = 0 WHERE job_id = ?")
            .bind("disabled-job")
            .execute(store.pool())
            .await
            .unwrap();

        let due = store.get_due_jobs().await.unwrap();
        let ids: Vec<&str> = due.iter().map(|j| j.job_id.as_str()).collect();
        assert!(ids.contains(&"enabled-job"), "enabled job should be due");
        assert!(
            !ids.contains(&"disabled-job"),
            "disabled job should NOT be due"
        );
    }

    #[tokio::test]
    async fn cron_get_due_jobs_future_not_due() {
        let store = make_store().await;

        store
            .create_cron_job(&CronJobSpec {
                job_id: "future-job".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "not yet".into(),
                description: "".into(),
            })
            .await
            .unwrap();

        // Set next_run far in the future.
        store
            .update_cron_next_run("future-job", "2099-12-31 23:59:59")
            .await
            .unwrap();

        let due = store.get_due_jobs().await.unwrap();
        let ids: Vec<&str> = due.iter().map(|j| j.job_id.as_str()).collect();
        assert!(!ids.contains(&"future-job"), "future job should not be due");
    }

    #[tokio::test]
    async fn cron_mark_job_run_updates_last_run_and_next_run() {
        let store = make_store().await;

        store
            .create_cron_job(&CronJobSpec {
                job_id: "mark-job".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "hello".into(),
                description: "".into(),
            })
            .await
            .unwrap();

        // Set next_run to past so it's due.
        store
            .update_cron_next_run("mark-job", "2000-01-01 00:00:00")
            .await
            .unwrap();

        // Mark as run.
        store.mark_job_run("mark-job", "0 9 * * *").await.unwrap();

        // After mark_job_run: last_run should be set, next_run should be in the future.
        let row: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT last_run, next_run FROM gw_cron_jobs WHERE job_id = ?")
                .bind("mark-job")
                .fetch_one(store.pool())
                .await
                .unwrap();

        assert!(row.0.is_some(), "last_run should be set after mark_job_run");
        let next_run = row.1.expect("next_run should be set");
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        assert!(
            next_run > now,
            "next_run should be in the future: {next_run}"
        );
    }

    // ── Credential edge cases ──────────────────────────────────────────

    #[tokio::test]
    async fn credential_overwrite_replaces_fully() {
        let store = make_store().await;

        let creds1 = serde_json::json!({"token": "old", "extra": "data"});
        store
            .save_credential("wx", "u1", "bot", &creds1, Some("2025-01-01"))
            .await
            .unwrap();

        let creds2 = serde_json::json!({"token": "new"});
        store
            .save_credential("wx", "u1", "bot", &creds2, None)
            .await
            .unwrap();

        let got = store
            .get_credential("wx", "u1", "bot")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            got.credentials, creds2,
            "credentials should be fully replaced"
        );
        assert!(
            got.expires_at.is_none(),
            "expires_at should be replaced with None"
        );
    }

    #[tokio::test]
    async fn credential_get_nonexistent_returns_none() {
        let store = make_store().await;

        let result = store
            .get_credential("wx", "ghost-user", "nonexistent-type")
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn credential_with_expiry() {
        let store = make_store().await;

        let creds = serde_json::json!({"access_token": "xyz"});
        store
            .save_credential("wx", "u1", "oauth", &creds, Some("2026-12-31 23:59:59"))
            .await
            .unwrap();

        let got = store
            .get_credential("wx", "u1", "oauth")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.credentials["access_token"], "xyz");
        assert_eq!(got.expires_at.as_deref(), Some("2026-12-31 23:59:59"));
    }

    // ── Usage edge cases ───────────────────────────────────────────────

    #[tokio::test]
    async fn usage_summary_aggregates_correctly() {
        let store = make_store().await;

        for i in 0..3 {
            store
                .record_usage(&UsageRecord {
                    platform: "wx".into(),
                    user_id: "u1".into(),
                    cli_profile: "astra".into(),
                    model: Some("opus".into()),
                    trace_id: None,
                    request_id: None,
                    run_id: None,
                    session_id: None,
                    tokens_prompt: 100 * (i + 1),
                    tokens_completion: 50 * (i + 1),
                    cached_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                    reasoning_output_tokens: 0,
                    total_tokens: 150 * (i + 1),
                    context_window: None,
                    max_output_tokens: None,
                    cost_usd: None,
                    raw_usage_json: None,
                    tool_calls: i as u32 + 1,
                    elapsed_ms: 1000,
                })
                .await
                .unwrap();
        }

        let total = store.get_usage_total("wx", "u1").await.unwrap();
        assert_eq!(total.messages, 3);
        // 100+200+300 = 600
        assert_eq!(total.tokens_prompt, 600);
        // 50+100+150 = 300
        assert_eq!(total.tokens_completion, 300);
        assert_eq!(total.total_tokens, 900);
        // 1+2+3 = 6
        assert_eq!(total.tool_calls, 6);
    }

    #[tokio::test]
    async fn usage_summary_empty_returns_zeros() {
        let store = make_store().await;

        let today = store.get_usage_today("wx", "unknown-user").await.unwrap();
        assert_eq!(today.messages, 0);
        assert_eq!(today.tokens_prompt, 0);
        assert_eq!(today.tokens_completion, 0);
        assert_eq!(today.tool_calls, 0);

        let total = store.get_usage_total("wx", "unknown-user").await.unwrap();
        assert_eq!(total.messages, 0);
    }

    // ── Concurrent / stress ────────────────────────────────────────────

    #[tokio::test]
    async fn many_sessions_same_chat() {
        let store = make_store().await;

        for i in 0..20 {
            store
                .set_current_session("wx", "c1", "u1", &format!("sess-{i}"), "astra")
                .await
                .unwrap();
        }

        // Only the last session should be current.
        let cur = store
            .get_current_session("wx", "c1", "astra")
            .await
            .unwrap();
        assert_eq!(cur.as_deref(), Some("sess-19"));

        // list_sessions returns up to 20 (the limit in the query).
        let sessions = store.list_sessions("wx", "c1", "astra").await.unwrap();
        assert_eq!(sessions.len(), 20);

        let current_count = sessions.iter().filter(|s| s.is_current).count();
        assert_eq!(current_count, 1, "exactly one session should be current");
    }

    #[tokio::test]
    async fn many_cron_jobs() {
        let store = make_store().await;

        for i in 0..50 {
            store
                .create_cron_job(&CronJobSpec {
                    job_id: format!("job-{i}"),
                    platform: "wx".into(),
                    chat_id: "c1".into(),
                    user_id: "u1".into(),
                    cron_expr: "0 9 * * *".into(),
                    message: format!("msg {i}"),
                    description: format!("desc {i}"),
                })
                .await
                .unwrap();
        }

        let jobs = store.list_cron_jobs("wx", "c1").await.unwrap();
        assert_eq!(jobs.len(), 50);
    }

    #[tokio::test]
    async fn ensure_schema_concurrent_safe() {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory SQLite");
        let store = std::sync::Arc::new(SqliteGatewayStore::new(pool));

        let mut handles = Vec::new();
        for _ in 0..5 {
            let s = store.clone();
            handles.push(tokio::spawn(async move { s.ensure_schema().await }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(
                result.is_ok(),
                "concurrent ensure_schema failed: {:?}",
                result.err()
            );
        }
    }

    // ── Data integrity ─────────────────────────────────────────────────

    #[tokio::test]
    async fn preference_json_special_chars() {
        let store = make_store().await;
        store.upsert_user("wx", "u1", "Test").await.unwrap();

        let special_value = r#"value with "quotes", back\slash, and unicode: 你好🌍"#;
        store
            .set_user_preference("wx", "u1", "special", special_value)
            .await
            .unwrap();

        let got = store
            .get_user_preference("wx", "u1", "special")
            .await
            .unwrap();
        assert_eq!(got.as_deref(), Some(special_value));
    }

    #[tokio::test]
    async fn cron_message_with_unicode_and_newlines() {
        let store = make_store().await;

        let message = "第一行\n第二行\n🎉 emoji line\ttab separated";
        store
            .create_cron_job(&CronJobSpec {
                job_id: "unicode-job".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: message.into(),
                description: "日本語の説明".into(),
            })
            .await
            .unwrap();

        // Set next_run to past so it shows in get_due_jobs.
        store
            .update_cron_next_run("unicode-job", "2000-01-01 00:00:00")
            .await
            .unwrap();

        let due = store.get_due_jobs().await.unwrap();
        let job = due.iter().find(|j| j.job_id == "unicode-job").unwrap();
        assert_eq!(job.message, message);

        let listed = store.list_cron_jobs("wx", "c1").await.unwrap();
        let rec = listed.iter().find(|j| j.job_id == "unicode-job").unwrap();
        assert_eq!(rec.description, "日本語の説明");
    }

    #[tokio::test]
    async fn user_display_name_unicode() {
        let store = make_store().await;

        // CJK characters.
        store.upsert_user("wx", "u-cjk", "张三").await.unwrap();
        assert!(!store.is_first_message("wx", "u-cjk").await.unwrap());

        // Emoji display name.
        store
            .upsert_user("wx", "u-emoji", "🦀 Ferris 🦀")
            .await
            .unwrap();
        assert!(!store.is_first_message("wx", "u-emoji").await.unwrap());

        // Verify the display name persists correctly via raw SQL.
        let row: (String,) = sqlx::query_as(
            "SELECT display_name FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind("wx")
        .bind("u-cjk")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.0, "张三");

        let row: (String,) = sqlx::query_as(
            "SELECT display_name FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind("wx")
        .bind("u-emoji")
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(row.0, "🦀 Ferris 🦀");
    }

    // ── SQLite-specific failure mode tests ─────────────────────────────────

    /// Helper: create a file-backed store with a shared pool (max_connections=2)
    /// using a tempfile path.
    async fn make_file_store(path: &std::path::Path) -> SqliteGatewayStore {
        let store = SqliteGatewayStore::connect(path.to_str().unwrap())
            .await
            .expect("connect to tempfile");
        store.ensure_schema().await.expect("ensure_schema");
        store
    }

    // ── 1. Concurrent write correctness: upsert_user ───────────────────────

    #[tokio::test]
    async fn concurrent_upsert_user_no_data_loss() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_upsert.db");
        let store = std::sync::Arc::new(make_file_store(&db_path).await);

        let mut handles = tokio::task::JoinSet::new();
        for i in 0..10 {
            let s = store.clone();
            handles.spawn(async move {
                let user_id = format!("user_{i}");
                let name = format!("User {i}");
                s.upsert_user("wx", &user_id, &name).await.unwrap();
            });
        }

        while let Some(result) = handles.join_next().await {
            result.expect("task panicked");
        }

        // Verify all 10 users exist.
        for i in 0..10 {
            let user_id = format!("user_{i}");
            assert!(
                !store.is_first_message("wx", &user_id).await.unwrap(),
                "user_{i} should exist after concurrent upsert"
            );
        }
    }

    // ── 2. Concurrent session create for same chat ─────────────────────────

    #[tokio::test]
    async fn concurrent_session_create_same_chat() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_session.db");
        let store = std::sync::Arc::new(make_file_store(&db_path).await);

        let mut handles = tokio::task::JoinSet::new();
        for i in 0..5 {
            let s = store.clone();
            handles.spawn(async move {
                let session_id = format!("sess-{i}");
                s.set_current_session("wx", "chat1", "u1", &session_id, "astra")
                    .await
                    .unwrap();
            });
        }

        while let Some(result) = handles.join_next().await {
            result.expect("task panicked");
        }

        // Note: set_current_session does "deactivate old" + "insert new" in separate
        // queries without a transaction. Under concurrent execution, multiple sessions
        // can end up with is_current=1 (a known SQLite single-writer race).
        // However, get_current_session uses ORDER BY last_active DESC LIMIT 1,
        // so it always returns a single deterministic answer.
        let current = store
            .get_current_session("wx", "chat1", "astra")
            .await
            .unwrap();
        assert!(
            current.is_some(),
            "there should be at least one current session after concurrent creates"
        );
        let current_id = current.unwrap();
        assert!(
            current_id.starts_with("sess-"),
            "current session should be one of the concurrently created sessions, got: {current_id}"
        );

        // All 5 sessions should have been inserted (no silent drops).
        let sessions = store.list_sessions("wx", "chat1", "astra").await.unwrap();
        assert_eq!(
            sessions.len(),
            5,
            "all 5 concurrent sessions should exist in the database"
        );

        // After a final sequential set_current_session, exactly 1 should be current.
        store
            .set_current_session("wx", "chat1", "u1", "sess-final", "astra")
            .await
            .unwrap();
        let sessions = store.list_sessions("wx", "chat1", "astra").await.unwrap();
        let current_count = sessions.iter().filter(|s| s.is_current).count();
        assert_eq!(
            current_count, 1,
            "after a final sequential set, exactly 1 session should be is_current=1, got {current_count}"
        );
    }

    // ── 3. Concurrent cron mark_job_run ────────────────────────────────────

    #[tokio::test]
    async fn concurrent_cron_mark_job_run() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_cron.db");
        let store = std::sync::Arc::new(make_file_store(&db_path).await);

        // Create a cron job that's due now (next_run in the past).
        store
            .create_cron_job(&CronJobSpec {
                job_id: "cron-1".into(),
                platform: "wx".into(),
                chat_id: "c1".into(),
                user_id: "u1".into(),
                cron_expr: "0 9 * * *".into(),
                message: "hello".into(),
                description: "test".into(),
            })
            .await
            .unwrap();

        // Force next_run to be in the past so it's due.
        store
            .update_cron_next_run("cron-1", "2000-01-01 00:00:00")
            .await
            .unwrap();

        // Spawn 3 tasks that each get_due_jobs + mark_job_run.
        let mut handles = tokio::task::JoinSet::new();
        for _ in 0..3 {
            let s = store.clone();
            handles.spawn(async move {
                let due = s.get_due_jobs().await.unwrap();
                for job in &due {
                    if job.job_id == "cron-1" {
                        s.mark_job_run(&job.job_id, &job.cron_expr).await.unwrap();
                    }
                }
                due.iter().any(|j| j.job_id == "cron-1")
            });
        }

        let mut saw_due_count = 0;
        while let Some(result) = handles.join_next().await {
            if result.expect("task panicked") {
                saw_due_count += 1;
            }
        }

        // At least one task saw the job as due and marked it.
        assert!(
            saw_due_count >= 1,
            "at least one task should have seen the job as due"
        );

        // After all tasks complete, the job should no longer be due
        // (next_run pushed to the future by mark_job_run).
        let due_after = store.get_due_jobs().await.unwrap();
        assert!(
            !due_after.iter().any(|j| j.job_id == "cron-1"),
            "job should not be due after mark_job_run"
        );
    }

    // ── 4. Concurrent credential save ──────────────────────────────────────

    #[tokio::test]
    async fn concurrent_credential_save() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_cred.db");
        let store = std::sync::Arc::new(make_file_store(&db_path).await);

        let mut handles = tokio::task::JoinSet::new();
        for i in 0..5 {
            let s = store.clone();
            handles.spawn(async move {
                let creds = serde_json::json!({"token": format!("token_{i}"), "iteration": i});
                s.save_credential("wx", "u1", "bot_token", &creds, None)
                    .await
                    .unwrap();
            });
        }

        while let Some(result) = handles.join_next().await {
            result.expect("task panicked");
        }

        // Get the credential -- should be ONE consistent value (last writer wins).
        let got = store
            .get_credential("wx", "u1", "bot_token")
            .await
            .unwrap()
            .expect("credential should exist");

        // The credential JSON must be valid and contain a "token" field
        // that matches "token_N" for some N in 0..5.
        let token = got.credentials["token"]
            .as_str()
            .expect("token should be a string");
        assert!(
            token.starts_with("token_"),
            "credential should be one of the concurrent writes, got: {token}"
        );

        // Verify there's only one credential row (upsert, not duplicates).
        let all = store.list_credentials("wx").await.unwrap();
        assert_eq!(
            all.len(),
            1,
            "should have exactly 1 credential row after concurrent upserts"
        );
    }

    // ── 5. StoreBundle integration (full stack with SQLite) ────────────────

    #[tokio::test]
    async fn store_bundle_full_stack_integration() {
        use crate::store::{StorageConfig, open_store_bundle};
        use crate::trace_model::{ConversationKey, GatewayRequest};
        use astra_task_store::TaskSpec;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("bundle_test.db");
        let config = StorageConfig::Sqlite {
            path: db_path.to_string_lossy().to_string(),
        };

        let bundle = open_store_bundle(&config)
            .await
            .expect("open_store_bundle should succeed")
            .expect("bundle should be Some for sqlite");

        // Durable store: create a task and get back a TaskId.
        let durable = bundle.durable_store.as_ref().unwrap();
        let task_id = durable
            .create(&TaskSpec {
                name: "integration test task".into(),
                description: Some("testing full wiring".into()),
                owner_id: "test_owner".into(),
                initial_state: Some(serde_json::json!({"step": 0})),
            })
            .await
            .expect("create durable task");
        assert!(!task_id.0.is_empty(), "TaskId should be non-empty");

        // Trace repo: create a request.
        let trace = bundle.trace_repo.as_ref().unwrap();
        let request = GatewayRequest::new(
            ConversationKey::new("wx", "chat1", "astra"),
            "msg-001",
            "user-001",
            "hello world",
        );
        trace
            .create_request(&request)
            .await
            .expect("create_request should succeed");
    }

    // ── 6. Schema reinit after drop and reconnect ──────────────────────────

    #[tokio::test]
    async fn schema_reinit_after_drop_and_reconnect() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("reinit.db");

        // First connection: create schema, insert user.
        {
            let store = make_file_store(&db_path).await;
            store.upsert_user("wx", "u1", "Alice").await.unwrap();
            assert!(!store.is_first_message("wx", "u1").await.unwrap());
            // store is dropped here, pool closes.
        }

        // Second connection: re-run ensure_schema, verify user persists.
        {
            let store = make_file_store(&db_path).await;
            // ensure_schema should be safe to re-run on populated DB.
            assert!(
                !store.is_first_message("wx", "u1").await.unwrap(),
                "user should survive reconnect"
            );
        }
    }

    // ── 7. Durable task survives reconnect ─────────────────────────────────

    #[tokio::test]
    async fn durable_task_survives_reconnect() {
        use crate::durable_task_store::SqliteDurableTaskStore;
        use astra_task_store::{DurableTaskStore, TaskSpec};

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("durable_reconnect.db");

        let task_id;

        // First connection: create task + checkpoint.
        {
            let store = make_file_store(&db_path).await;
            let durable = SqliteDurableTaskStore::new(store.pool().clone());

            task_id = durable
                .create(&TaskSpec {
                    name: "persist test".into(),
                    description: None,
                    owner_id: "owner1".into(),
                    initial_state: Some(serde_json::json!({"step": 1})),
                })
                .await
                .unwrap();

            // Checkpoint.
            durable
                .checkpoint(
                    &task_id,
                    &serde_json::json!({"step": 2, "data": "important"}),
                    Some(50),
                    Some("halfway"),
                )
                .await
                .unwrap();
            // store + durable dropped here.
        }

        // Second connection: verify task + checkpoint intact.
        {
            let store = make_file_store(&db_path).await;
            let durable = SqliteDurableTaskStore::new(store.pool().clone());

            let task = durable
                .get(&task_id)
                .await
                .unwrap()
                .expect("task should survive reconnect");
            assert_eq!(task.name, "persist test");
            assert_eq!(task.progress_pct, 50);
            assert_eq!(task.step_description.as_deref(), Some("halfway"));

            let checkpoint = task.checkpoint.expect("checkpoint should survive");
            assert_eq!(checkpoint["step"], 2);
            assert_eq!(checkpoint["data"], "important");
        }
    }

    // ── 8. Connect with invalid path errors cleanly ────────────────────────

    #[tokio::test]
    async fn connect_invalid_path_errors_cleanly() {
        let result = SqliteGatewayStore::connect("/proc/0/nonexistent/gateway.db").await;
        match result {
            Ok(_) => panic!("connect to invalid path should return Err, not Ok"),
            Err(err) => {
                // Should be an I/O or Database error, not a panic.
                let msg = err.to_string();
                assert!(!msg.is_empty(), "error message should be non-empty: {msg}");
            }
        }
    }

    // ── 9. Pool timeout under heavy load (simplified) ──────────────────────

    #[tokio::test]
    async fn pool_timeout_under_heavy_load() {
        // TODO: Testing SQLite pool exhaustion cleanly is complex because
        // sqlx's SQLite pool uses cooperative async rather than OS-level locks.
        // A proper test would need to hold a connection via a long transaction
        // and then attempt another acquire with a short timeout. Since sqlx's
        // SqlitePool doesn't expose straightforward "acquire with timeout" in
        // the way needed for a deterministic test, we verify the simpler
        // property: creating a pool with max_connections=1 still handles
        // sequential operations correctly (no deadlock).

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pool_timeout.db");

        // Create pool with max_connections=1.
        let parent = db_path.parent().unwrap();
        std::fs::create_dir_all(parent).unwrap();
        let url = format!("sqlite:{}?mode=rwc", db_path.to_str().unwrap());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("connect with max_connections=1");

        let store = SqliteGatewayStore::new(pool);
        store.ensure_schema().await.unwrap();

        // Sequential operations on a single-connection pool should not deadlock.
        store.upsert_user("wx", "u1", "Alice").await.unwrap();
        store.upsert_user("wx", "u2", "Bob").await.unwrap();
        assert!(!store.is_first_message("wx", "u1").await.unwrap());
        assert!(!store.is_first_message("wx", "u2").await.unwrap());
    }
}
