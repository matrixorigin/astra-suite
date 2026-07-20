//! File-based GatewayStore — JSON files in a local directory.
//!
//! Zero-dependency operation: no database required. Suitable for single-user
//! setups or environments where installing MySQL/SQLite is impractical.
//!
//! Directory layout under `base_dir`:
//!   users.json        — keyed by "platform:user_id"
//!   sessions.json     — keyed by "platform:chat_id:cli_profile"
//!   cron_jobs.json    — keyed by job_id
//!   credentials.json  — keyed by "platform:user_id:type"
//!   usage/            — one YYYY-MM-DD.jsonl per day

use super::{
    CronJobRecord, CronJobSpec, DueJob, GatewayStore, PlatformCredential, SessionRecord,
    SkillRecord, StoreError, UsageRecord, UsageStatus, UsageSummary, next_cron_run_str,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock};

pub struct FileGatewayStore {
    base_dir: PathBuf,
    users: RwLock<HashMap<String, UserEntry>>,
    sessions: RwLock<HashMap<String, Vec<SessionEntry>>>,
    cron_jobs: RwLock<HashMap<String, CronJobEntry>>,
    credentials: RwLock<HashMap<String, CredentialEntry>>,
    disk_write_lock: Mutex<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEntry {
    platform: String,
    user_id: String,
    display_name: String,
    preferences: HashMap<String, String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEntry {
    session_id: String,
    platform: String,
    chat_id: String,
    user_id: String,
    cli_profile: String,
    is_current: bool,
    created_at: String,
    last_active: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronJobEntry {
    job_id: String,
    platform: String,
    chat_id: String,
    user_id: String,
    cron_expr: String,
    message: String,
    description: String,
    enabled: bool,
    last_run: Option<String>,
    next_run: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialEntry {
    platform: String,
    user_id: String,
    credential_type: String,
    credentials: serde_json::Value,
    expires_at: Option<String>,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UsageEntry {
    platform: String,
    user_id: String,
    cli_profile: String,
    model: Option<String>,
    trace_id: Option<String>,
    request_id: Option<String>,
    run_id: Option<String>,
    session_id: Option<String>,
    tokens_prompt: u64,
    tokens_completion: u64,
    #[serde(default)]
    cached_input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    #[serde(default)]
    reasoning_output_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    context_window: Option<u64>,
    max_output_tokens: Option<u64>,
    cost_usd: Option<f64>,
    raw_usage_json: Option<String>,
    #[serde(default)]
    status: UsageStatus,
    #[serde(default)]
    failure_kind: Option<String>,
    tool_calls: u32,
    elapsed_ms: u64,
    created_at: String,
}

fn now_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn today_str() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

fn user_key(platform: &str, user_id: &str) -> String {
    format!("{platform}:{user_id}")
}

fn session_key(platform: &str, chat_id: &str, cli_profile: &str) -> String {
    format!("{platform}:{chat_id}:{cli_profile}")
}

fn cred_key(platform: &str, user_id: &str, credential_type: &str) -> String {
    format!("{platform}:{user_id}:{credential_type}")
}

impl FileGatewayStore {
    pub async fn open(base_dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&base_dir).await?;
        tokio::fs::create_dir_all(base_dir.join("usage")).await?;

        let users = load_json_map(&base_dir.join("users.json")).await?;
        let sessions = load_json_map(&base_dir.join("sessions.json")).await?;
        let cron_jobs = load_json_map(&base_dir.join("cron_jobs.json")).await?;
        let credentials = load_json_map(&base_dir.join("credentials.json")).await?;

        Ok(Self {
            base_dir,
            users: RwLock::new(users),
            sessions: RwLock::new(sessions),
            cron_jobs: RwLock::new(cron_jobs),
            credentials: RwLock::new(credentials),
            disk_write_lock: Mutex::new(()),
        })
    }

    async fn flush_users(&self) -> Result<(), StoreError> {
        let _guard = self.disk_write_lock.lock().await;
        let data = self.users.read().await;
        save_json_map(&self.base_dir.join("users.json"), &*data).await
    }

    async fn flush_sessions(&self) -> Result<(), StoreError> {
        let _guard = self.disk_write_lock.lock().await;
        let data = self.sessions.read().await;
        save_json_map(&self.base_dir.join("sessions.json"), &*data).await
    }

    async fn flush_cron_jobs(&self) -> Result<(), StoreError> {
        let _guard = self.disk_write_lock.lock().await;
        let data = self.cron_jobs.read().await;
        save_json_map(&self.base_dir.join("cron_jobs.json"), &*data).await
    }

    async fn flush_credentials(&self) -> Result<(), StoreError> {
        let _guard = self.disk_write_lock.lock().await;
        let data = self.credentials.read().await;
        save_json_map(&self.base_dir.join("credentials.json"), &*data).await
    }
}

#[async_trait::async_trait]
impl GatewayStore for FileGatewayStore {
    async fn ensure_schema(&self) -> Result<(), StoreError> {
        Ok(())
    }

    // ─── Users ────────────────────────────────────────────────────────
    async fn upsert_user(
        &self,
        platform: &str,
        user_id: &str,
        display_name: &str,
    ) -> Result<(), StoreError> {
        let key = user_key(platform, user_id);
        let mut users = self.users.write().await;
        users.entry(key).or_insert_with(|| UserEntry {
            platform: platform.to_string(),
            user_id: user_id.to_string(),
            display_name: display_name.to_string(),
            preferences: HashMap::new(),
            created_at: now_str(),
        });
        drop(users);
        self.flush_users().await
    }

    async fn set_user_preference(
        &self,
        platform: &str,
        user_id: &str,
        key: &str,
        value: &str,
    ) -> Result<(), StoreError> {
        let ukey = user_key(platform, user_id);
        let mut users = self.users.write().await;
        let Some(entry) = users.get_mut(&ukey) else {
            return Err(StoreError::NotFound(format!(
                "user not found: {platform}:{user_id}"
            )));
        };
        entry.preferences.insert(key.to_string(), value.to_string());
        drop(users);
        self.flush_users().await
    }

    async fn get_user_preference(
        &self,
        platform: &str,
        user_id: &str,
        key: &str,
    ) -> Result<Option<String>, StoreError> {
        let ukey = user_key(platform, user_id);
        let users = self.users.read().await;
        Ok(users
            .get(&ukey)
            .and_then(|e| e.preferences.get(key).cloned()))
    }

    async fn is_first_message(&self, platform: &str, user_id: &str) -> Result<bool, StoreError> {
        let key = user_key(platform, user_id);
        let users = self.users.read().await;
        Ok(!users.contains_key(&key))
    }

    // ─── Sessions ─────────────────────────────────────────────────────
    async fn get_current_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let sessions = self.sessions.read().await;
        Ok(sessions.get(&key).and_then(|entries| {
            entries
                .iter()
                .filter(|e| e.is_current)
                .max_by(|a, b| a.last_active.cmp(&b.last_active))
                .map(|e| e.session_id.clone())
        }))
    }

    async fn get_session_last_active(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Option<String>, StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let sessions = self.sessions.read().await;
        Ok(sessions.get(&key).and_then(|entries| {
            entries
                .iter()
                .filter(|e| e.is_current)
                .max_by(|a, b| a.last_active.cmp(&b.last_active))
                .map(|e| e.last_active.clone())
        }))
    }

    async fn set_current_session(
        &self,
        platform: &str,
        chat_id: &str,
        user_id: &str,
        session_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let mut sessions = self.sessions.write().await;
        let entries = sessions.entry(key).or_insert_with(Vec::new);
        for e in entries.iter_mut() {
            e.is_current = false;
        }
        if let Some(existing) = entries.iter_mut().find(|e| e.session_id == session_id) {
            existing.is_current = true;
            existing.last_active = now_str();
        } else {
            entries.push(SessionEntry {
                session_id: session_id.to_string(),
                platform: platform.to_string(),
                chat_id: chat_id.to_string(),
                user_id: user_id.to_string(),
                cli_profile: cli_profile.to_string(),
                is_current: true,
                created_at: now_str(),
                last_active: now_str(),
            });
        }
        drop(sessions);
        self.flush_sessions().await
    }

    async fn touch_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let mut sessions = self.sessions.write().await;
        if let Some(entries) = sessions.get_mut(&key) {
            for e in entries.iter_mut().filter(|e| e.is_current) {
                e.last_active = now_str();
            }
        }
        drop(sessions);
        self.flush_sessions().await
    }

    async fn list_sessions(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<Vec<SessionRecord>, StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let sessions = self.sessions.read().await;
        Ok(sessions
            .get(&key)
            .map(|entries| {
                let mut sorted: Vec<_> = entries.iter().collect();
                sorted.sort_by(|a, b| b.last_active.cmp(&a.last_active));
                sorted
                    .into_iter()
                    .take(20)
                    .map(|e| SessionRecord {
                        session_id: e.session_id.clone(),
                        is_current: e.is_current,
                        created_at: e.created_at.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn switch_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
        target_session_id: &str,
    ) -> Result<bool, StoreError> {
        let mut sessions = self.sessions.write().await;
        let key = session_key(platform, chat_id, cli_profile);
        if let Some(entries) = sessions.get_mut(&key) {
            if !entries.iter().any(|e| e.session_id == target_session_id) {
                return Ok(false);
            }
            for e in entries.iter_mut() {
                if e.session_id == target_session_id {
                    e.is_current = true;
                    e.last_active = now_str();
                } else {
                    e.is_current = false;
                }
            }
        } else {
            return Ok(false);
        }
        drop(sessions);
        self.flush_sessions().await?;
        Ok(true)
    }

    async fn find_sessions_by_prefix(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
        prefix: &str,
    ) -> Result<Vec<String>, StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let sessions = self.sessions.read().await;
        Ok(sessions
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|entry| entry.session_id.starts_with(prefix))
            .take(2)
            .map(|entry| entry.session_id.clone())
            .collect())
    }

    async fn reset_session(
        &self,
        platform: &str,
        chat_id: &str,
        cli_profile: &str,
    ) -> Result<(), StoreError> {
        let key = session_key(platform, chat_id, cli_profile);
        let mut sessions = self.sessions.write().await;
        if let Some(entries) = sessions.get_mut(&key) {
            for e in entries.iter_mut().filter(|e| e.is_current) {
                e.is_current = false;
            }
        }
        drop(sessions);
        self.flush_sessions().await
    }

    // ─── Cron Jobs ────────────────────────────────────────────────────
    async fn create_cron_job(&self, spec: &CronJobSpec) -> Result<(), StoreError> {
        let next = next_cron_run_str(&spec.cron_expr);
        let entry = CronJobEntry {
            job_id: spec.job_id.to_string(),
            platform: spec.platform.to_string(),
            chat_id: spec.chat_id.to_string(),
            user_id: spec.user_id.to_string(),
            cron_expr: spec.cron_expr.to_string(),
            message: spec.message.to_string(),
            description: spec.description.to_string(),
            enabled: true,
            last_run: None,
            next_run: Some(next),
            created_at: now_str(),
        };
        let mut jobs = self.cron_jobs.write().await;
        jobs.insert(spec.job_id.to_string(), entry);
        drop(jobs);
        self.flush_cron_jobs().await
    }

    async fn list_cron_jobs(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<CronJobRecord>, StoreError> {
        let jobs = self.cron_jobs.read().await;
        Ok(jobs
            .values()
            .filter(|j| j.platform == platform && j.chat_id == chat_id)
            .map(|j| CronJobRecord {
                job_id: j.job_id.clone(),
                cron_expr: j.cron_expr.clone(),
                description: j.description.clone(),
                enabled: j.enabled,
            })
            .collect())
    }

    async fn delete_cron_job(&self, job_id: &str) -> Result<bool, StoreError> {
        let mut jobs = self.cron_jobs.write().await;
        let removed = jobs.remove(job_id).is_some();
        drop(jobs);
        if removed {
            self.flush_cron_jobs().await?;
        }
        Ok(removed)
    }

    async fn get_due_jobs(&self) -> Result<Vec<DueJob>, StoreError> {
        let now = now_str();
        let jobs = self.cron_jobs.read().await;
        Ok(jobs
            .values()
            .filter(|j| {
                j.enabled
                    && j.next_run
                        .as_ref()
                        .map(|nr| nr.as_str() <= now.as_str())
                        .unwrap_or(true)
            })
            .map(|j| DueJob {
                job_id: j.job_id.clone(),
                platform: j.platform.clone(),
                chat_id: j.chat_id.clone(),
                message: j.message.clone(),
                cron_expr: j.cron_expr.clone(),
            })
            .collect())
    }

    async fn mark_job_run(&self, job_id: &str, cron_expr: &str) -> Result<(), StoreError> {
        let next = next_cron_run_str(cron_expr);
        let mut jobs = self.cron_jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            job.last_run = Some(now_str());
            job.next_run = Some(next);
        }
        drop(jobs);
        self.flush_cron_jobs().await
    }

    async fn update_cron_next_run(&self, job_id: &str, next_run: &str) -> Result<(), StoreError> {
        let mut jobs = self.cron_jobs.write().await;
        if let Some(job) = jobs.get_mut(job_id) {
            job.next_run = Some(next_run.to_string());
        }
        drop(jobs);
        self.flush_cron_jobs().await
    }

    async fn get_cron_job_user_id(&self, job_id: &str) -> Result<Option<String>, StoreError> {
        let jobs = self.cron_jobs.read().await;
        Ok(jobs.get(job_id).map(|j| j.user_id.clone()))
    }

    // ─── Platform Credentials ─────────────────────────────────────────
    async fn save_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
        credentials: &serde_json::Value,
        expires_at: Option<&str>,
    ) -> Result<(), StoreError> {
        let key = cred_key(platform, user_id, credential_type);
        let entry = CredentialEntry {
            platform: platform.to_string(),
            user_id: user_id.to_string(),
            credential_type: credential_type.to_string(),
            credentials: credentials.clone(),
            expires_at: expires_at.map(String::from),
            updated_at: now_str(),
        };
        let mut creds = self.credentials.write().await;
        creds.insert(key, entry);
        drop(creds);
        self.flush_credentials().await
    }

    async fn get_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
    ) -> Result<Option<PlatformCredential>, StoreError> {
        let key = cred_key(platform, user_id, credential_type);
        let creds = self.credentials.read().await;
        Ok(creds.get(&key).map(|e| PlatformCredential {
            platform: e.platform.clone(),
            user_id: e.user_id.clone(),
            credential_type: e.credential_type.clone(),
            credentials: e.credentials.clone(),
            expires_at: e.expires_at.clone(),
        }))
    }

    async fn list_credentials(
        &self,
        platform: &str,
    ) -> Result<Vec<PlatformCredential>, StoreError> {
        let creds = self.credentials.read().await;
        Ok(creds
            .values()
            .filter(|e| e.platform == platform)
            .map(|e| PlatformCredential {
                platform: e.platform.clone(),
                user_id: e.user_id.clone(),
                credential_type: e.credential_type.clone(),
                credentials: e.credentials.clone(),
                expires_at: e.expires_at.clone(),
            })
            .collect())
    }

    async fn delete_credential(
        &self,
        platform: &str,
        user_id: &str,
        credential_type: &str,
    ) -> Result<bool, StoreError> {
        let key = cred_key(platform, user_id, credential_type);
        let mut creds = self.credentials.write().await;
        let removed = creds.remove(&key).is_some();
        drop(creds);
        if removed {
            self.flush_credentials().await?;
        }
        Ok(removed)
    }

    // ─── Usage ────────────────────────────────────────────────────────
    async fn record_usage(&self, record: &UsageRecord) -> Result<(), StoreError> {
        let day = today_str();
        let path = self.base_dir.join("usage").join(format!("{day}.jsonl"));
        let entry = UsageEntry {
            platform: record.platform.clone(),
            user_id: record.user_id.clone(),
            cli_profile: record.cli_profile.clone(),
            model: record.model.clone(),
            trace_id: record.trace_id.clone(),
            request_id: record.request_id.clone(),
            run_id: record.run_id.clone(),
            session_id: record.session_id.clone(),
            tokens_prompt: record.tokens_prompt,
            tokens_completion: record.tokens_completion,
            cached_input_tokens: record.cached_input_tokens,
            cache_creation_input_tokens: record.cache_creation_input_tokens,
            cache_read_input_tokens: record.cache_read_input_tokens,
            reasoning_output_tokens: record.reasoning_output_tokens,
            total_tokens: record.total_tokens,
            context_window: record.context_window,
            max_output_tokens: record.max_output_tokens,
            cost_usd: record.cost_usd,
            raw_usage_json: record.raw_usage_json.clone(),
            status: record.status,
            failure_kind: record.failure_kind.clone(),
            tool_calls: record.tool_calls,
            elapsed_ms: record.elapsed_ms,
            created_at: now_str(),
        };
        let mut line =
            serde_json::to_string(&entry).map_err(|e| StoreError::Serialization(e.to_string()))?;
        line.push('\n');
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }

    async fn get_usage_today(
        &self,
        platform: &str,
        user_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let day = today_str();
        let path = self.base_dir.join("usage").join(format!("{day}.jsonl"));
        load_usage_summary(&path, platform, user_id).await
    }

    async fn get_usage_total(
        &self,
        platform: &str,
        user_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let dir = self.base_dir.join("usage");
        let mut total = UsageSummary::default();
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return Ok(total),
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                let day = load_usage_summary(&path, platform, user_id).await?;
                total.messages += day.messages;
                total.tokens_prompt += day.tokens_prompt;
                total.tokens_completion += day.tokens_completion;
                total.cached_input_tokens += day.cached_input_tokens;
                total.cache_creation_input_tokens += day.cache_creation_input_tokens;
                total.cache_read_input_tokens += day.cache_read_input_tokens;
                total.reasoning_output_tokens += day.reasoning_output_tokens;
                total.total_tokens += day.total_tokens;
                total.context_window = total.context_window.max(day.context_window);
                total.max_output_tokens = total.max_output_tokens.max(day.max_output_tokens);
                total.cost_usd += day.cost_usd;
                total.tool_calls += day.tool_calls;
            }
        }
        Ok(total)
    }

    async fn get_usage_session(
        &self,
        platform: &str,
        user_id: &str,
        cli_profile: &str,
        session_id: &str,
    ) -> Result<UsageSummary, StoreError> {
        let dir = self.base_dir.join("usage");
        let mut total = UsageSummary::default();
        let mut entries = match tokio::fs::read_dir(&dir).await {
            Ok(e) => e,
            Err(_) => return Ok(total),
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
                let day = load_usage_summary_for_session(
                    &path,
                    platform,
                    user_id,
                    cli_profile,
                    session_id,
                )
                .await?;
                total.messages += day.messages;
                total.tokens_prompt += day.tokens_prompt;
                total.tokens_completion += day.tokens_completion;
                total.cached_input_tokens += day.cached_input_tokens;
                total.cache_creation_input_tokens += day.cache_creation_input_tokens;
                total.cache_read_input_tokens += day.cache_read_input_tokens;
                total.reasoning_output_tokens += day.reasoning_output_tokens;
                total.total_tokens += day.total_tokens;
                total.context_window = total.context_window.max(day.context_window);
                total.max_output_tokens = total.max_output_tokens.max(day.max_output_tokens);
                total.cost_usd += day.cost_usd;
                total.tool_calls += day.tool_calls;
            }
        }
        Ok(total)
    }

    // ─── Skills ───────────────────────────────────────────────────────
    async fn list_skills(
        &self,
        platform: &str,
        chat_id: &str,
    ) -> Result<Vec<SkillRecord>, StoreError> {
        let path = self
            .base_dir
            .join(format!("skills_{platform}_{chat_id}.json"));
        let skills: Vec<SkillRecord> = load_json_vec(&path).await?;
        Ok(skills)
    }

    async fn get_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
    ) -> Result<Option<SkillRecord>, StoreError> {
        let path = self
            .base_dir
            .join(format!("skills_{platform}_{chat_id}.json"));
        let skills: Vec<SkillRecord> = load_json_vec(&path).await?;
        Ok(skills.into_iter().find(|s| s.name == name))
    }

    async fn upsert_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
        content: &str,
        description: &str,
    ) -> Result<(), StoreError> {
        let path = self
            .base_dir
            .join(format!("skills_{platform}_{chat_id}.json"));
        let _guard = self.disk_write_lock.lock().await;
        let mut skills: Vec<SkillRecord> = load_json_vec(&path).await?;
        if let Some(existing) = skills.iter_mut().find(|s| s.name == name) {
            existing.content = content.to_string();
            existing.description = description.to_string();
        } else {
            skills.push(SkillRecord {
                name: name.to_string(),
                content: content.to_string(),
                description: description.to_string(),
                created_at: now_str(),
            });
        }
        let json = serde_json::to_string_pretty(&skills)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        atomic_write(&path, json.as_bytes()).await?;
        Ok(())
    }

    async fn delete_skill(
        &self,
        platform: &str,
        chat_id: &str,
        name: &str,
    ) -> Result<bool, StoreError> {
        let path = self
            .base_dir
            .join(format!("skills_{platform}_{chat_id}.json"));
        let _guard = self.disk_write_lock.lock().await;
        let mut skills: Vec<SkillRecord> = load_json_vec(&path).await?;
        let original_len = skills.len();
        skills.retain(|s| s.name != name);
        if skills.len() == original_len {
            return Ok(false);
        }
        let json = serde_json::to_string_pretty(&skills)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        atomic_write(&path, json.as_bytes()).await?;
        Ok(true)
    }
}

// ─── File I/O helpers ─────────────────────────────────────────────────────

async fn load_json_map<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> Result<HashMap<String, T>, StoreError> {
    match tokio::fs::read_to_string(path).await {
        Ok(data) => {
            if data.trim().is_empty() {
                return Ok(HashMap::new());
            }
            serde_json::from_str(&data).map_err(|e| StoreError::Serialization(e.to_string()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e.into()),
    }
}

async fn load_json_vec<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Vec<T>, StoreError> {
    match tokio::fs::read_to_string(path).await {
        Ok(data) => {
            if data.trim().is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str(&data).map_err(|e| StoreError::Serialization(e.to_string()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e.into()),
    }
}

async fn save_json_map<T: serde::Serialize>(
    path: &Path,
    data: &HashMap<String, T>,
) -> Result<(), StoreError> {
    let json =
        serde_json::to_string_pretty(data).map_err(|e| StoreError::Serialization(e.to_string()))?;
    atomic_write(path, json.as_bytes()).await?;
    Ok(())
}

async fn atomic_write(path: &Path, data: &[u8]) -> Result<(), StoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| StoreError::Io(std::io::Error::other("path has no parent directory")))?;
    tokio::fs::create_dir_all(parent).await?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| StoreError::Io(std::io::Error::other("path has no valid file name")))?;
    let tmp_path = parent.join(format!(
        ".{file_name}.{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));

    let mut file = tokio::fs::File::create(&tmp_path).await?;
    file.write_all(data).await?;
    file.sync_all().await?;
    drop(file);

    tokio::fs::rename(&tmp_path, path).await?;
    if let Ok(dir) = tokio::fs::File::open(parent).await {
        let _ = dir.sync_all().await;
    }
    Ok(())
}

async fn load_usage_summary(
    path: &Path,
    platform: &str,
    user_id: &str,
) -> Result<UsageSummary, StoreError> {
    load_usage_summary_filtered(path, platform, user_id, None, None).await
}

async fn load_usage_summary_for_session(
    path: &Path,
    platform: &str,
    user_id: &str,
    cli_profile: &str,
    session_id: &str,
) -> Result<UsageSummary, StoreError> {
    load_usage_summary_filtered(path, platform, user_id, Some(cli_profile), Some(session_id)).await
}

async fn load_usage_summary_filtered(
    path: &Path,
    platform: &str,
    user_id: &str,
    cli_profile: Option<&str>,
    session_id: Option<&str>,
) -> Result<UsageSummary, StoreError> {
    let mut summary = UsageSummary::default();
    let data = match tokio::fs::read_to_string(path).await {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(summary),
        Err(e) => return Err(e.into()),
    };
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<UsageEntry>(line)
            && entry.platform == platform
            && entry.user_id == user_id
            && cli_profile.is_none_or(|profile| entry.cli_profile == profile)
            && session_id.is_none_or(|sid| entry.session_id.as_deref() == Some(sid))
        {
            if entry.status.counts_as_message() {
                summary.messages += 1;
            }
            summary.tokens_prompt += entry.tokens_prompt;
            summary.tokens_completion += entry.tokens_completion;
            summary.cached_input_tokens += entry.cached_input_tokens;
            summary.cache_creation_input_tokens += entry.cache_creation_input_tokens;
            summary.cache_read_input_tokens += entry.cache_read_input_tokens;
            summary.reasoning_output_tokens += entry.reasoning_output_tokens;
            summary.total_tokens += if entry.total_tokens > 0 {
                entry.total_tokens
            } else {
                entry.tokens_prompt
                    + entry.tokens_completion
                    + entry.cache_creation_input_tokens
                    + entry.cache_read_input_tokens
                    + entry.reasoning_output_tokens
            };
            summary.context_window = summary.context_window.max(entry.context_window);
            summary.max_output_tokens = summary.max_output_tokens.max(entry.max_output_tokens);
            summary.cost_usd += entry.cost_usd.unwrap_or(0.0);
            summary.tool_calls += entry.tool_calls as u64;
        }
    }
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_store() -> (tempfile::TempDir, FileGatewayStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FileGatewayStore::open(dir.path()).await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn user_roundtrip() {
        let (_dir, store) = test_store().await;
        assert!(store.is_first_message("wx", "u1").await.unwrap());
        store.upsert_user("wx", "u1", "Test").await.unwrap();
        assert!(!store.is_first_message("wx", "u1").await.unwrap());
    }

    #[tokio::test]
    async fn preference_roundtrip() {
        let (_dir, store) = test_store().await;
        store.upsert_user("wx", "u1", "Test").await.unwrap();
        store
            .set_user_preference("wx", "u1", "theme", "dark")
            .await
            .unwrap();
        let val = store
            .get_user_preference("wx", "u1", "theme")
            .await
            .unwrap();
        assert_eq!(val.as_deref(), Some("dark"));
    }

    #[tokio::test]
    async fn setting_preference_for_missing_user_fails() {
        let (_dir, store) = test_store().await;
        let err = store
            .set_user_preference("wx", "missing", "theme", "dark")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("user not found"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn session_lifecycle() {
        let (_dir, store) = test_store().await;
        assert!(
            store
                .get_current_session("wx", "c1", "astra")
                .await
                .unwrap()
                .is_none()
        );
        store
            .set_current_session("wx", "c1", "u1", "s1", "astra")
            .await
            .unwrap();
        assert_eq!(
            store
                .get_current_session("wx", "c1", "astra")
                .await
                .unwrap()
                .as_deref(),
            Some("s1")
        );
        store.reset_session("wx", "c1", "astra").await.unwrap();
        assert!(
            store
                .get_current_session("wx", "c1", "astra")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn cron_job_lifecycle() {
        let (_dir, store) = test_store().await;
        let spec = CronJobSpec {
            job_id: "j1".into(),
            platform: "wx".into(),
            chat_id: "c1".into(),
            user_id: "u1".into(),
            cron_expr: "0 9 * * *".into(),
            message: "hello".into(),
            description: "greeting".into(),
        };
        store.create_cron_job(&spec).await.unwrap();
        let jobs = store.list_cron_jobs("wx", "c1").await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].job_id, "j1");
        assert!(store.delete_cron_job("j1").await.unwrap());
        assert!(store.list_cron_jobs("wx", "c1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn credential_roundtrip() {
        let (_dir, store) = test_store().await;
        let creds = serde_json::json!({"token": "abc"});
        store
            .save_credential("wx", "u1", "bot_token", &creds, None)
            .await
            .unwrap();
        let got = store
            .get_credential("wx", "u1", "bot_token")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.credentials["token"], "abc");
        assert!(
            store
                .delete_credential("wx", "u1", "bot_token")
                .await
                .unwrap()
        );
    }

    // ── GAP 3: persistence roundtrip ──────────────────────────────

    #[tokio::test]
    async fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();

        // Write
        {
            let store = FileGatewayStore::open(dir.path()).await.unwrap();
            store.upsert_user("wx", "u1", "Alice").await.unwrap();
            store
                .set_user_preference("wx", "u1", "theme", "dark")
                .await
                .unwrap();
            store
                .set_current_session("wx", "c1", "u1", "s1", "astra")
                .await
                .unwrap();
            store
                .create_cron_job(&CronJobSpec {
                    job_id: "j1".into(),
                    platform: "wx".into(),
                    chat_id: "c1".into(),
                    user_id: "u1".into(),
                    cron_expr: "0 9 * * *".into(),
                    message: "hello".into(),
                    description: "daily".into(),
                })
                .await
                .unwrap();
        }
        // Store dropped -- all in-memory state gone

        // Reopen
        {
            let store = FileGatewayStore::open(dir.path()).await.unwrap();
            assert!(
                !store.is_first_message("wx", "u1").await.unwrap(),
                "user should persist"
            );
            assert_eq!(
                store
                    .get_user_preference("wx", "u1", "theme")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("dark"),
                "preference should persist"
            );
            assert_eq!(
                store
                    .get_current_session("wx", "c1", "astra")
                    .await
                    .unwrap()
                    .as_deref(),
                Some("s1"),
                "session should persist"
            );
            let jobs = store.list_cron_jobs("wx", "c1").await.unwrap();
            assert_eq!(jobs.len(), 1, "cron job should persist");
            assert_eq!(jobs[0].job_id, "j1");
        }
    }

    #[tokio::test]
    async fn credentials_persist_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = FileGatewayStore::open(dir.path()).await.unwrap();
            store
                .save_credential("wx", "u1", "token", &serde_json::json!({"k": "v"}), None)
                .await
                .unwrap();
        }
        {
            let store = FileGatewayStore::open(dir.path()).await.unwrap();
            let cred = store
                .get_credential("wx", "u1", "token")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(cred.credentials["k"], "v");
        }
    }

    #[tokio::test]
    async fn usage_recording() {
        let (_dir, store) = test_store().await;
        let record = UsageRecord {
            platform: "wx".into(),
            user_id: "u1".into(),
            cli_profile: "astra".into(),
            model: None,
            trace_id: None,
            request_id: None,
            run_id: None,
            session_id: None,
            tokens_prompt: 100,
            tokens_completion: 50,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            reasoning_output_tokens: 0,
            total_tokens: 150,
            context_window: None,
            max_output_tokens: None,
            cost_usd: None,
            raw_usage_json: None,
            status: UsageStatus::Success,
            failure_kind: None,
            tool_calls: 2,
            elapsed_ms: 3000,
        };
        store.record_usage(&record).await.unwrap();
        let today = store.get_usage_today("wx", "u1").await.unwrap();
        assert_eq!(today.messages, 1);
        assert_eq!(today.tokens_prompt, 100);
        assert_eq!(today.total_tokens, 150);
        let total = store.get_usage_total("wx", "u1").await.unwrap();
        assert_eq!(total.messages, 1);
        assert_eq!(total.total_tokens, 150);
    }
}
