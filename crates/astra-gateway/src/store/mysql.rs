//! MySQL / MatrixOne implementation of [`GatewayStore`].

use super::{
    CronJobRecord, CronJobSpec, DueJob, GatewayStore, PlatformCredential, SessionRecord,
    SkillRecord, StoreError, UsageRecord, UsageSummary, next_cron_run_str,
};

/// MySQL-backed gateway store.
///
/// Wraps a [`sqlx::MySqlPool`] and implements every [`GatewayStore`] method
/// using the same SQL statements that were previously in `storage.rs` and
/// `usage.rs`.
pub struct MysqlGatewayStore {
    pool: sqlx::MySqlPool,
}

impl MysqlGatewayStore {
    pub fn new(pool: sqlx::MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(5)
            .idle_timeout(std::time::Duration::from_secs(60))
            .max_lifetime(std::time::Duration::from_secs(300))
            .acquire_timeout(std::time::Duration::from_secs(5))
            .test_before_acquire(true)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &sqlx::MySqlPool {
        &self.pool
    }

    /// Create the usage-tracking table if it does not already exist.
    ///
    /// Separated from [`GatewayStore::ensure_schema`] because usage tracking
    /// is optional and may be initialized independently.
    pub async fn ensure_usage_table(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_usage (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                platform VARCHAR(32) NOT NULL,
                user_id VARCHAR(128) NOT NULL,
                cli_profile VARCHAR(32) NOT NULL DEFAULT 'astra',
                model VARCHAR(128),
                trace_id VARCHAR(64),
                request_id VARCHAR(64),
                run_id VARCHAR(64),
                session_id VARCHAR(128),
                tokens_prompt BIGINT NOT NULL DEFAULT 0,
                tokens_completion BIGINT NOT NULL DEFAULT 0,
                cached_input_tokens BIGINT NOT NULL DEFAULT 0,
                cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0,
                cache_read_input_tokens BIGINT NOT NULL DEFAULT 0,
                reasoning_output_tokens BIGINT NOT NULL DEFAULT 0,
                total_tokens BIGINT NOT NULL DEFAULT 0,
                context_window BIGINT,
                max_output_tokens BIGINT,
                cost_usd DOUBLE,
                raw_usage_json JSON,
                tool_calls INT NOT NULL DEFAULT 0,
                elapsed_ms BIGINT NOT NULL DEFAULT 0,
                created_at DATETIME(6) DEFAULT NOW(6),
                INDEX idx_user_day (platform, user_id, created_at)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        for migration in [
            "ALTER TABLE gw_usage ADD COLUMN trace_id VARCHAR(64)",
            "ALTER TABLE gw_usage ADD COLUMN request_id VARCHAR(64)",
            "ALTER TABLE gw_usage ADD COLUMN run_id VARCHAR(64)",
            "ALTER TABLE gw_usage ADD COLUMN session_id VARCHAR(128)",
            "ALTER TABLE gw_usage ADD COLUMN cached_input_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN cache_read_input_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN reasoning_output_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN total_tokens BIGINT NOT NULL DEFAULT 0",
            "ALTER TABLE gw_usage ADD COLUMN context_window BIGINT",
            "ALTER TABLE gw_usage ADD COLUMN max_output_tokens BIGINT",
            "ALTER TABLE gw_usage ADD COLUMN cost_usd DOUBLE",
            "ALTER TABLE gw_usage ADD COLUMN raw_usage_json JSON",
        ] {
            let _ = sqlx::query(migration).execute(&self.pool).await;
        }
        Ok(())
    }
}

// ─── GatewayStore trait implementation ──────────────────────────────────────

#[async_trait::async_trait]
impl GatewayStore for MysqlGatewayStore {
    // ── Schema ──────────────────────────────────────────────────────────

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_users (
                platform VARCHAR(32) NOT NULL,
                platform_user_id VARCHAR(128) NOT NULL,
                display_name VARCHAR(256) DEFAULT '',
                preferences JSON,
                created_at DATETIME(6) DEFAULT NOW(6),
                updated_at DATETIME(6) DEFAULT NOW(6),
                PRIMARY KEY (platform, platform_user_id)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_sessions (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                platform VARCHAR(32) NOT NULL,
                chat_id VARCHAR(128) NOT NULL,
                user_id VARCHAR(128) NOT NULL DEFAULT '',
                cli_profile VARCHAR(32) NOT NULL DEFAULT 'default',
                astra_session_id VARCHAR(64) NOT NULL,
                is_current BOOLEAN DEFAULT TRUE,
                created_at DATETIME(6) DEFAULT NOW(6),
                last_active DATETIME(6) DEFAULT NOW(6),
                INDEX idx_current (platform, chat_id, cli_profile, is_current),
                INDEX idx_user (platform, user_id)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        // Migration: add cli_profile column if missing (existing deployments)
        let _ = sqlx::query(
            "ALTER TABLE gw_sessions ADD COLUMN cli_profile VARCHAR(32) NOT NULL DEFAULT 'default'",
        )
        .execute(&self.pool)
        .await;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_cron_jobs (
                job_id VARCHAR(64) PRIMARY KEY,
                platform VARCHAR(32) NOT NULL,
                chat_id VARCHAR(128) NOT NULL,
                user_id VARCHAR(128) NOT NULL DEFAULT '',
                cron_expr VARCHAR(128) NOT NULL,
                message TEXT NOT NULL,
                description VARCHAR(512) DEFAULT '',
                enabled BOOLEAN DEFAULT TRUE,
                last_run DATETIME(6),
                next_run DATETIME(6),
                created_at DATETIME(6) DEFAULT NOW(6),
                INDEX idx_enabled (enabled, next_run),
                INDEX idx_user_jobs (platform, user_id)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_platform_credentials (
                platform VARCHAR(32) NOT NULL,
                user_id VARCHAR(128) NOT NULL,
                credential_type VARCHAR(64) NOT NULL,
                credentials TEXT NOT NULL,
                expires_at DATETIME(6),
                created_at DATETIME(6) DEFAULT NOW(6),
                updated_at DATETIME(6) DEFAULT NOW(6),
                PRIMARY KEY (platform, user_id, credential_type)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS gw_skills (
                platform VARCHAR(64) NOT NULL,
                chat_id VARCHAR(128) NOT NULL,
                name VARCHAR(128) NOT NULL,
                content LONGTEXT NOT NULL,
                description TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (platform, chat_id, name)
            )",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        self.ensure_usage_table().await?;

        tracing::info!("gateway schema ensured");
        Ok(())
    }

    // ── Users ───────────────────────────────────────────────────────────

    async fn upsert_user(
        &self,
        platform: &str,
        user_id: &str,
        display_name: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO gw_users (platform, platform_user_id, display_name)
             VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE updated_at = NOW(6)",
        )
        .bind(platform)
        .bind(user_id)
        .bind(display_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn set_user_preference(
        &self,
        platform: &str,
        user_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), StoreError> {
        let pref_json = serde_json::json!({key: value}).to_string();

        // First ensure preferences is not NULL, then JSON_SET
        let initialized = sqlx::query(
            "UPDATE gw_users SET preferences = ?, updated_at = NOW(6)
             WHERE platform = ? AND platform_user_id = ? AND preferences IS NULL",
        )
        .bind(&pref_json)
        .bind(platform)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        let merged = sqlx::query(
            "UPDATE gw_users SET preferences = JSON_SET(preferences, CONCAT('$.', ?), ?), updated_at = NOW(6)
             WHERE platform = ? AND platform_user_id = ? AND preferences IS NOT NULL",
        )
        .bind(key)
        .bind(value)
        .bind(platform)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(preferences, CONCAT('$.', ?)))
             FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind(key)
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(row.and_then(|r| r.0).filter(|v| v != "null"))
    }

    async fn is_first_message(&self, platform: &str, user_id: &str) -> Result<bool, StoreError> {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM gw_users WHERE platform = ? AND platform_user_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(row.0 == 0)
    }

    // ── Session operations ──────────────────────────────────────────────

    async fn get_current_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT astra_session_id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = TRUE
             ORDER BY last_active DESC LIMIT 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(row.map(|r| r.0))
    }

    async fn get_session_last_active(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT CAST(last_active AS CHAR) FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = TRUE
             ORDER BY last_active DESC LIMIT 1",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
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
        // Mark old sessions for this CLI as not current
        sqlx::query(
            "UPDATE gw_sessions SET is_current = FALSE
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = TRUE",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        // Check if this session already exists for this CLI
        let existing: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .bind(astra_session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        if let Some((id,)) = existing {
            // Reactivate existing session
            sqlx::query(
                "UPDATE gw_sessions SET is_current = TRUE, last_active = NOW(6) WHERE id = ?",
            )
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        } else {
            // Insert new
            sqlx::query(
                "INSERT INTO gw_sessions (platform, chat_id, user_id, cli_profile, astra_session_id, is_current)
                 VALUES (?, ?, ?, ?, ?, TRUE)",
            )
            .bind(platform)
            .bind(chat_id)
            .bind(user_id)
            .bind(cli_profile)
            .bind(astra_session_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
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
            "UPDATE gw_sessions SET last_active = NOW(6)
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = TRUE",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn list_sessions(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Vec<SessionRecord>, StoreError> {
        let rows: Vec<(String, i32, String)> = sqlx::query_as(
            "SELECT astra_session_id, CAST(is_current AS SIGNED), CAST(created_at AS CHAR) as created
             FROM gw_sessions WHERE platform = ? AND chat_id = ? AND cli_profile = ?
             ORDER BY last_active DESC LIMIT 20",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
        cli_profile: &str,
        target_session_id: &str,
    ) -> Result<bool, StoreError> {
        // Check target exists
        let exists: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .bind(target_session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        if exists.is_none() {
            return Ok(false);
        }

        // Clear current
        sqlx::query(
            "UPDATE gw_sessions SET is_current = FALSE
             WHERE platform = ? AND chat_id = ? AND cli_profile = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        // Set target as current
        sqlx::query(
            "UPDATE gw_sessions SET is_current = TRUE, last_active = NOW(6)
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND astra_session_id = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .bind(target_session_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        Ok(true)
    }

    async fn find_sessions_by_prefix(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
        prefix: &str,
    ) -> Result<Vec<String>, StoreError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT astra_session_id FROM gw_sessions
             WHERE platform = ? AND chat_id = ? AND cli_profile = ?
               AND LEFT(astra_session_id, CHAR_LENGTH(?)) = ?
             ORDER BY last_active DESC LIMIT 2",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .bind(prefix)
        .bind(prefix)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(rows.into_iter().map(|(session_id,)| session_id).collect())
    }

    async fn reset_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE gw_sessions SET is_current = FALSE
             WHERE platform = ? AND chat_id = ? AND cli_profile = ? AND is_current = TRUE",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(cli_profile)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    // ── Cron job operations ─────────────────────────────────────────────

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
        .bind(next)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn list_cron_jobs(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<CronJobRecord>, StoreError> {
        // MatrixOne returns BOOL as string, so CAST to SIGNED for SQLx compat
        let rows: Vec<(String, String, String, i32)> = sqlx::query_as(
            "SELECT job_id, cron_expr, description, CAST(enabled AS SIGNED)
             FROM gw_cron_jobs WHERE platform = ? AND chat_id = ?
             ORDER BY created_at",
        )
        .bind(platform)
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn get_due_jobs(&self) -> Result<Vec<DueJob>, StoreError> {
        let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT job_id, platform, chat_id, message, cron_expr
             FROM gw_cron_jobs
             WHERE enabled = TRUE AND (next_run IS NULL OR next_run <= NOW(6))",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(jid, plat, cid, msg, expr)| DueJob {
                job_id: jid,
                platform: plat,
                chat_id: cid,
                message: msg,
                cron_expr: expr,
            })
            .collect())
    }

    async fn mark_job_run(&self, job_id: &str, cron_expr: &str) -> Result<(), StoreError> {
        let next = next_cron_run_str(cron_expr);
        sqlx::query("UPDATE gw_cron_jobs SET last_run = NOW(6), next_run = ? WHERE job_id = ?")
            .bind(&next)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn update_cron_next_run(&self, job_id: &str, next_run: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE gw_cron_jobs SET next_run = ? WHERE job_id = ?")
            .bind(next_run)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn get_cron_job_user_id(&self, job_id: &str) -> Result<Option<String>, StoreError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT user_id FROM gw_cron_jobs WHERE job_id = ?")
                .bind(job_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(row.map(|(uid,)| uid))
    }

    // ── Credential operations ───────────────────────────────────────────

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
             ON DUPLICATE KEY UPDATE credentials = VALUES(credentials),
                                     expires_at = VALUES(expires_at),
                                     updated_at = NOW(6)",
        )
        .bind(platform)
        .bind(user_id)
        .bind(credential_type)
        .bind(&cred_str)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }

    async fn get_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
    ) -> Result<Option<PlatformCredential>, StoreError> {
        let row: Option<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT platform, user_id, credential_type, credentials, CAST(expires_at AS CHAR)
             FROM gw_platform_credentials
             WHERE platform = ? AND user_id = ? AND credential_type = ?",
        )
        .bind(platform)
        .bind(user_id)
        .bind(credential_type)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
            "SELECT platform, user_id, credential_type, credentials, CAST(expires_at AS CHAR)
             FROM gw_platform_credentials
             WHERE platform = ?
             ORDER BY updated_at DESC",
        )
        .bind(platform)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    // ── Usage tracking ──────────────────────────────────────────────────

    async fn record_usage(&self, record: &UsageRecord) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO gw_usage (
                platform, user_id, cli_profile, model, trace_id, request_id, run_id, session_id,
                tokens_prompt, tokens_completion, cached_input_tokens, cache_creation_input_tokens,
                cache_read_input_tokens, reasoning_output_tokens, total_tokens, context_window,
                max_output_tokens, cost_usd, raw_usage_json, tool_calls, elapsed_ms
             )
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&record.platform)
        .bind(&record.user_id)
        .bind(&record.cli_profile)
        .bind(&record.model)
        .bind(&record.trace_id)
        .bind(&record.request_id)
        .bind(&record.run_id)
        .bind(&record.session_id)
        .bind(record.tokens_prompt as i64)
        .bind(record.tokens_completion as i64)
        .bind(record.cached_input_tokens as i64)
        .bind(record.cache_creation_input_tokens as i64)
        .bind(record.cache_read_input_tokens as i64)
        .bind(record.reasoning_output_tokens as i64)
        .bind(record.total_tokens as i64)
        .bind(record.context_window.map(|v| v as i64))
        .bind(record.max_output_tokens.map(|v| v as i64))
        .bind(record.cost_usd)
        .bind(&record.raw_usage_json)
        .bind(record.tool_calls as i32)
        .bind(record.elapsed_ms as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
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
                    COALESCE(SUM(tokens_prompt),0),
                    COALESCE(SUM(tokens_completion),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(cache_creation_input_tokens),0),
                    COALESCE(SUM(cache_read_input_tokens),0),
                    COALESCE(SUM(reasoning_output_tokens),0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END),0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd),0.0),
                    COALESCE(SUM(tool_calls),0)
             FROM gw_usage WHERE platform = ? AND user_id = ? AND created_at >= CURDATE()",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
                    COALESCE(SUM(tokens_prompt),0),
                    COALESCE(SUM(tokens_completion),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(cache_creation_input_tokens),0),
                    COALESCE(SUM(cache_read_input_tokens),0),
                    COALESCE(SUM(reasoning_output_tokens),0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END),0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd),0.0),
                    COALESCE(SUM(tool_calls),0)
             FROM gw_usage WHERE platform = ? AND user_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
        cli_profile: &str,
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
                    COALESCE(SUM(tokens_prompt),0),
                    COALESCE(SUM(tokens_completion),0),
                    COALESCE(SUM(cached_input_tokens),0),
                    COALESCE(SUM(cache_creation_input_tokens),0),
                    COALESCE(SUM(cache_read_input_tokens),0),
                    COALESCE(SUM(reasoning_output_tokens),0),
                    COALESCE(SUM(CASE
                        WHEN total_tokens > 0 THEN total_tokens
                        ELSE tokens_prompt + tokens_completion + cache_creation_input_tokens
                             + cache_read_input_tokens + reasoning_output_tokens
                    END),0),
                    MAX(context_window),
                    MAX(max_output_tokens),
                    COALESCE(SUM(cost_usd),0.0),
                    COALESCE(SUM(tool_calls),0)
             FROM gw_usage
             WHERE platform = ? AND user_id = ? AND cli_profile = ? AND session_id = ?",
        )
        .bind(platform)
        .bind(user_id)
        .bind(cli_profile)
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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

    // ── Skill operations ───────────────────────────────────────────────

    async fn list_skills(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<SkillRecord>, StoreError> {
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT name, content, description, CAST(created_at AS CHAR)
             FROM gw_skills WHERE platform = ? AND chat_id = ?
             ORDER BY name",
        )
        .bind(platform)
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
            "SELECT name, content, description, CAST(created_at AS CHAR)
             FROM gw_skills WHERE platform = ? AND chat_id = ? AND name = ?",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;

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
             ON DUPLICATE KEY UPDATE content = VALUES(content), description = VALUES(description)",
        )
        .bind(platform)
        .bind(chat_id)
        .bind(name)
        .bind(content)
        .bind(description)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Database(e.to_string()))?;
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
                .await
                .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}
