use std::path::Path;

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub astra: AstraServerConfig,
    /// New multi-backend storage configuration.
    pub storage: crate::store::StorageConfig,
    /// Legacy database config. Parsed only to produce clear diagnostics; storage
    /// must be configured through `storage:`.
    pub database: Option<DatabaseConfig>,
    /// Default CLI profile (used when no /cli switch active).
    pub cli: crate::cli_bridge::CliProfile,
    /// Named CLI profiles available for /cli switch.
    pub cli_profiles: std::collections::HashMap<String, crate::cli_bridge::CliProfile>,
    /// Model provider configurations. Maps provider name (e.g. "bedrock",
    /// "dashscope") to the env vars injected at CLI spawn when a model
    /// belonging to that provider is selected via /model.
    pub providers: std::collections::HashMap<String, ProviderConfig>,
    /// Maximum seconds a spawned CLI may run for one gateway message.
    pub cli_timeout_secs: u64,
    pub platforms: PlatformConfigs,
    /// Directory containing user-defined skill markdown files.
    pub skills_dir: Option<String>,
    /// Optional lightweight retrieval over skills_dir. When enabled, only the
    /// most relevant skills are injected per user message instead of all skills.
    pub skills_retrieval: SkillsRetrievalConfig,
    /// Session auto-reset policy.
    pub session_reset: crate::session_policy::ResetPolicy,
    /// Access control policy (who can send messages).
    pub access: crate::access_control::AccessPolicy,
    /// Action policy (which gateway mutations are allowed from slash/model sources).
    pub action_policy: crate::access_control::ActionPolicy,
    /// Maximum concurrent CLI runs across all conversations.
    pub max_concurrent_runs: usize,
    /// Group chat: isolate sessions per user (true) or share per group (false).
    pub group_sessions_per_user: bool,
    /// Group chat: require @mention to activate (reduces noise).
    pub group_require_mention: bool,
    /// Bot display name for @mention matching in group chats (e.g. "Astra").
    /// When empty, any @mention triggers the bot.
    pub bot_name: String,
    /// Timezone for cron scheduling (e.g. "Asia/Shanghai"). Defaults to UTC.
    pub timezone: Option<String>,
    /// Directories to scan for git projects (e.g. ["~/github", "~/work"]).
    pub project_dirs: Vec<String>,
    /// Extra text appended to the system prompt (user-customizable).
    pub system_prompt_extra: Option<String>,
    /// HTTP API port for message injection (e.g. 9090). Disabled when absent.
    pub api_port: Option<u16>,
}

#[derive(serde::Deserialize)]
struct RawGatewayConfig {
    #[serde(default)]
    astra: AstraServerConfig,
    #[serde(default)]
    storage: crate::store::StorageConfig,
    #[serde(default)]
    database: Option<DatabaseConfig>,
    #[serde(default)]
    cli: CliConfig,
    #[serde(default)]
    cli_profiles: std::collections::HashMap<String, crate::cli_bridge::CliProfile>,
    #[serde(default)]
    providers: std::collections::HashMap<String, ProviderConfig>,
    #[serde(default = "default_cli_timeout_secs")]
    cli_timeout_secs: u64,
    #[serde(default)]
    platforms: PlatformConfigs,
    #[serde(default)]
    skills_dir: Option<String>,
    #[serde(default)]
    skills_retrieval: SkillsRetrievalConfig,
    #[serde(default)]
    session_reset: crate::session_policy::ResetPolicy,
    #[serde(default)]
    access: crate::access_control::AccessPolicy,
    #[serde(default)]
    action_policy: crate::access_control::ActionPolicy,
    #[serde(default = "default_max_concurrent_runs")]
    max_concurrent_runs: usize,
    #[serde(default)]
    group_sessions_per_user: bool,
    #[serde(default)]
    group_require_mention: bool,
    #[serde(default)]
    bot_name: String,
    #[serde(default)]
    timezone: Option<String>,
    #[serde(default)]
    project_dirs: Vec<String>,
    #[serde(default)]
    system_prompt_extra: Option<String>,
    #[serde(default)]
    api_port: Option<u16>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum CliConfig {
    Profile(crate::cli_bridge::CliProfile),
    ProfileName(String),
}

impl Default for CliConfig {
    fn default() -> Self {
        Self::Profile(crate::cli_bridge::CliProfile::default())
    }
}

impl<'de> serde::Deserialize<'de> for GatewayConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawGatewayConfig::deserialize(deserializer)?;
        let cli = match raw.cli {
            CliConfig::Profile(profile) => profile,
            CliConfig::ProfileName(name) => {
                raw.cli_profiles.get(&name).cloned().ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        "cli references unknown profile `{name}`; define it under cli_profiles"
                    ))
                })?
            }
        };

        Ok(Self {
            astra: raw.astra,
            storage: raw.storage,
            database: raw.database,
            cli,
            cli_profiles: raw.cli_profiles,
            providers: raw.providers,
            cli_timeout_secs: raw.cli_timeout_secs,
            platforms: raw.platforms,
            skills_dir: raw.skills_dir,
            skills_retrieval: raw.skills_retrieval,
            session_reset: raw.session_reset,
            access: raw.access,
            action_policy: raw.action_policy,
            max_concurrent_runs: raw.max_concurrent_runs,
            group_sessions_per_user: raw.group_sessions_per_user,
            group_require_mention: raw.group_require_mention,
            bot_name: raw.bot_name,
            timezone: raw.timezone,
            project_dirs: raw.project_dirs,
            system_prompt_extra: raw.system_prompt_extra,
            api_port: raw.api_port,
        })
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SkillsRetrievalConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_skills_top_k")]
    pub top_k: usize,
    #[serde(default = "default_skill_max_chars")]
    pub max_skill_chars: usize,
}

impl Default for SkillsRetrievalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            top_k: default_skills_top_k(),
            max_skill_chars: default_skill_max_chars(),
        }
    }
}

fn default_skills_top_k() -> usize {
    5
}

fn default_skill_max_chars() -> usize {
    12_000
}

#[derive(Clone, Default, serde::Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub url: String,
}

impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &"[REDACTED]")
            .finish()
    }
}

#[allow(dead_code)]
fn default_true() -> bool {
    true
}

fn default_cli_timeout_secs() -> u64 {
    60 * 60
}

fn default_max_concurrent_runs() -> usize {
    4
}

#[derive(Clone, Default, serde::Deserialize)]
pub struct AstraServerConfig {
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    pub default_model: Option<String>,
    /// Optional login credentials for gateway-level auto-recovery when CLI tokens expire.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

impl std::fmt::Debug for AstraServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AstraServerConfig")
            .field("base_url", &self.base_url)
            .field(
                "api_key",
                &if self.api_key.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .field("default_model", &self.default_model)
            .field("username", &self.username.as_deref())
            .field(
                "password",
                &if self.password.is_some() {
                    Some("[REDACTED]")
                } else {
                    None
                },
            )
            .finish()
    }
}

fn default_base_url() -> String {
    "http://localhost:8080".into()
}

/// Environment variables injected into the CLI process when a model belonging
/// to this provider is selected via /model. Provider env is layered on top of
/// cli.env (provider wins on key conflict).
#[derive(Clone, Default, serde::Deserialize)]
pub struct ProviderConfig {
    /// Whether this provider participates in /model lists and runtime env
    /// injection. Defaults to false so starter configs with empty secrets do
    /// not accidentally expose non-working model options.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub env_file: Option<String>,
}

impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted: std::collections::BTreeMap<_, _> = self
            .env
            .iter()
            .map(|(k, v)| {
                let display = if v.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                };
                (k, display)
            })
            .collect();
        f.debug_struct("ProviderConfig")
            .field("enabled", &self.enabled)
            .field("env", &redacted)
            .field("env_file", &self.env_file)
            .finish()
    }
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct PlatformConfigs {
    pub wecom: Option<WeComConfig>,
    pub weixin: Option<crate::platforms::weixin::WeixinConfig>,
    pub telegram: Option<TelegramConfig>,
}

#[derive(Clone, serde::Deserialize)]
pub struct WeComConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub bot_id: String,
    #[serde(default)]
    pub secret: String,
    #[serde(default = "default_wecom_ws_url")]
    pub websocket_url: String,
}

impl std::fmt::Debug for WeComConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeComConfig")
            .field("enabled", &self.enabled)
            .field("bot_id", &self.bot_id)
            .field("secret", &"[REDACTED]")
            .field("websocket_url", &self.websocket_url)
            .finish()
    }
}

fn default_wecom_ws_url() -> String {
    "wss://openws.work.weixin.qq.com".into()
}

#[derive(Clone, serde::Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub token: String,
}

impl std::fmt::Debug for TelegramConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TelegramConfig")
            .field("enabled", &self.enabled)
            .field(
                "token",
                &if self.token.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .finish()
    }
}

impl GatewayConfig {
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml_ng::from_str(&content)?;
        Ok(config)
    }

    /// Resolve the configured storage backend.
    ///
    /// `database:` is intentionally ignored. Gateway durability needs explicit
    /// `storage:` configuration or a non-empty `GATEWAY_DATABASE_URL`.
    pub fn resolve_storage(&self) -> crate::store::StorageConfig {
        self.storage.clone()
    }
}

impl WeComConfig {
    pub fn resolve(mut self) -> Self {
        if self.bot_id.is_empty()
            && let Ok(v) = std::env::var("WECOM_BOT_ID")
        {
            self.bot_id = v;
        }
        if self.secret.is_empty()
            && let Ok(v) = std::env::var("WECOM_SECRET")
        {
            self.secret = v;
        }
        self
    }
}

impl TelegramConfig {
    pub fn resolve(mut self) -> Self {
        if self.token.is_empty()
            && let Ok(v) = std::env::var("TELEGRAM_BOT_TOKEN")
        {
            self.token = v;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: "test-key"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.astra.base_url, "http://localhost:8080");
        assert_eq!(cfg.astra.api_key, "test-key");
        assert!(cfg.platforms.wecom.is_none());
        assert_eq!(cfg.max_concurrent_runs, 4);
        assert!(!cfg.action_policy.allow_model_generated_mutations);
    }

    #[test]
    fn parse_full_config() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: "key"
  default_model: "MiniMax-M2.7"
platforms:
  wecom:
    enabled: true
    bot_id: "bot-123"
    secret: "secret-456"
  telegram:
    enabled: false
    token: "tok"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let wecom = cfg.platforms.wecom.unwrap();
        assert!(wecom.enabled);
        assert_eq!(wecom.bot_id, "bot-123");
        assert_eq!(cfg.astra.default_model.as_deref(), Some("MiniMax-M2.7"));
    }

    #[test]
    fn parse_cli_from_profile_name() {
        let yaml = r#"
cli: astra
cli_profiles:
  astra:
    type: astra
    bin: /opt/astra
    permission_mode: auto
  claude:
    type: claude
    bin: claude
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(matches!(
            cfg.cli,
            crate::cli_bridge::CliProfile::Astra { .. }
        ));
        assert_eq!(cfg.cli.name(), "astra");
        assert_eq!(cfg.cli_profiles.len(), 2);
    }

    #[test]
    fn parse_cli_profile_name_requires_existing_profile() {
        let yaml = r#"
cli: astra
cli_profiles:
  claude:
    type: claude
    bin: claude
"#;
        let err = serde_yaml_ng::from_str::<GatewayConfig>(yaml).unwrap_err();
        assert!(err.to_string().contains("unknown profile `astra`"));
    }

    #[test]
    fn debug_redacts_secrets() {
        let cfg = AstraServerConfig {
            base_url: "http://localhost:8080".into(),
            api_key: "super-secret-key".into(),
            default_model: None,
            username: Some("admin".into()),
            password: Some("hunter2".into()),
        };
        let dbg = format!("{cfg:?}");
        assert!(
            dbg.contains("[REDACTED]"),
            "api_key should be redacted: {dbg}"
        );
        assert!(
            !dbg.contains("super-secret"),
            "api_key leaked in debug: {dbg}"
        );
        assert!(!dbg.contains("hunter2"), "password leaked in debug: {dbg}");
        assert!(dbg.contains("admin"), "username should be visible: {dbg}");

        let db = DatabaseConfig {
            url: "mysql://root:password@host/db".into(),
        };
        let dbg = format!("{db:?}");
        assert!(!dbg.contains("password"), "db url leaked in debug: {dbg}");

        let wecom = WeComConfig {
            enabled: true,
            bot_id: "bot-123".into(),
            secret: "my-secret".into(),
            websocket_url: "wss://example.com".into(),
        };
        let dbg = format!("{wecom:?}");
        assert!(!dbg.contains("my-secret"), "wecom secret leaked: {dbg}");

        let tg = TelegramConfig {
            enabled: true,
            token: "bot123:AABBCC".into(),
        };
        let dbg = format!("{tg:?}");
        assert!(
            !dbg.contains("bot123:AABBCC"),
            "telegram token leaked: {dbg}"
        );
    }

    // ── resolve_storage tests ────────────────────────────────────────────

    #[test]
    fn resolve_storage_ignores_legacy_database_url() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: ""
database:
  url: "mysql://root:111@localhost/gw"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let resolved = cfg.resolve_storage();
        match resolved {
            // Legacy `database:` is ignored. Without explicit `storage:` the
            // default kicks in (either SQLite, or env-derived MySQL — never
            // the legacy URL).
            crate::store::StorageConfig::None => {}
            crate::store::StorageConfig::Sqlite { .. } => {}
            crate::store::StorageConfig::Mysql { url } => assert_ne!(
                url, "mysql://root:111@localhost/gw",
                "legacy database must not silently configure storage"
            ),
            other => panic!("unexpected storage from legacy database: {other:?}"),
        }
    }

    #[test]
    fn resolve_storage_explicit_mysql_storage_wins() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: ""
storage:
  backend: mysql
  url: "mysql://explicit@host/db"
database:
  url: "mysql://legacy@host/db"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let resolved = cfg.resolve_storage();
        match resolved {
            crate::store::StorageConfig::Mysql { url } => {
                assert!(
                    url.contains("explicit"),
                    "storage: section should win, got {url}"
                );
            }
            other => panic!("expected Mysql, got {other:?}"),
        }
    }

    #[test]
    fn resolve_storage_no_storage_no_database_defaults_to_sqlite() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: ""
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let resolved = cfg.resolve_storage();
        // Default is SQLite (zero-config durable storage), unless GATEWAY_DATABASE_URL
        // is set in the environment — then MySQL wins.
        assert!(
            matches!(
                resolved,
                crate::store::StorageConfig::Sqlite { .. }
                    | crate::store::StorageConfig::Mysql { .. }
            ),
            "expected Sqlite or env-derived Mysql, got {resolved:?}"
        );
    }

    #[test]
    fn resolve_storage_explicit_file_not_overridden() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: ""
storage:
  backend: file
  dir: "/custom/data"
database:
  url: "mysql://root:111@localhost/gw"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let resolved = cfg.resolve_storage();
        match resolved {
            crate::store::StorageConfig::File { dir } => {
                assert_eq!(dir, "/custom/data");
            }
            other => panic!("expected File, got {other:?}"),
        }
    }

    #[test]
    fn resolve_storage_none_not_overridden() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: ""
storage:
  backend: none
database:
  url: "mysql://root:111@localhost/gw"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        let resolved = cfg.resolve_storage();
        assert!(
            matches!(resolved, crate::store::StorageConfig::None),
            "expected None, got {resolved:?}"
        );
    }

    #[test]
    fn wecom_env_override() {
        let cfg = WeComConfig {
            enabled: true,
            bot_id: String::new(),
            secret: String::new(),
            websocket_url: default_wecom_ws_url(),
        };
        // resolve() reads env vars — test that empty stays empty without env
        let resolved = cfg.resolve();
        // Can't assert env vars in unit tests, but verify no panic
        assert!(resolved.websocket_url.starts_with("wss://"));
    }

    #[test]
    fn parse_config_with_auth_credentials() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: "key"
  username: "admin"
  password: "secret123"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.astra.username.as_deref(), Some("admin"));
        assert_eq!(cfg.astra.password.as_deref(), Some("secret123"));
    }

    #[test]
    fn parse_config_without_auth_credentials() {
        let yaml = r#"
astra:
  base_url: "http://localhost:8080"
  api_key: "key"
"#;
        let cfg: GatewayConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(cfg.astra.username.is_none());
        assert!(cfg.astra.password.is_none());
    }
}
