use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Storage info passed to the MCP server subprocess via environment variables.
/// The generated Claude MCP config is 0600 because env values can include credentials.
#[derive(Clone)]
pub struct McpStorageEnv {
    pub database_url: Option<String>,
    pub sqlite_path: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CodexMcpConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

impl CodexMcpConfig {
    pub fn thread_config(&self) -> serde_json::Value {
        serde_json::json!({
            "mcp_servers": {
                "gateway": {
                    "command": self.command,
                    "args": self.args,
                    "env": self.env,
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct GeneratedMcpConfig {
    pub claude_config_path: PathBuf,
    pub codex: CodexMcpConfig,
}

struct GatewayMcpServer {
    command: String,
    args: Vec<String>,
    env: BTreeMap<String, String>,
}

pub fn generate_gateway_mcp_config(
    storage_env: &McpStorageEnv,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    project_dirs: &[String],
    runtime_api_url: Option<&str>,
    runtime_api_token: Option<&str>,
) -> Result<GeneratedMcpConfig, std::io::Error> {
    let server = gateway_mcp_server(
        storage_env,
        platform,
        chat_id,
        user_id,
        project_dirs,
        runtime_api_url,
        runtime_api_token,
    );

    let config = serde_json::json!({
        "mcpServers": {
            "gateway": {
                "command": server.command.clone(),
                "args": server.args.clone(),
                "env": server.env.clone()
            }
        }
    });

    let hash = simple_hash(chat_id);
    let path = std::env::temp_dir().join(format!("gw-mcp-{hash}.json"));
    let content = serde_json::to_string_pretty(&config).map_err(std::io::Error::other)?;
    std::fs::write(&path, &content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(GeneratedMcpConfig {
        claude_config_path: path,
        codex: CodexMcpConfig {
            command: server.command,
            args: server.args,
            env: server.env,
        },
    })
}

fn gateway_mcp_server(
    storage_env: &McpStorageEnv,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    project_dirs: &[String],
    runtime_api_url: Option<&str>,
    runtime_api_token: Option<&str>,
) -> GatewayMcpServer {
    let gateway_bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("astra-gateway"));

    // Claude reads these from the MCP JSON file. Codex receives the same values through
    // thread/start config because app-server does not pass its own env to MCP children.
    // The JSON file is created with 0600 permissions because it can contain storage credentials.
    let mut env = BTreeMap::new();
    if let Some(ref url) = storage_env.database_url {
        env.insert("GATEWAY_DATABASE_URL".into(), url.clone());
    }
    if let Some(ref path) = storage_env.sqlite_path {
        env.insert(
            "GW_MCP_SQLITE_PATH".into(),
            absolutize(path).to_string_lossy().into_owned(),
        );
    }
    env.insert("GW_MCP_PLATFORM".into(), platform.to_string());
    env.insert("GW_MCP_CHAT_ID".into(), chat_id.to_string());
    env.insert("GW_MCP_USER_ID".into(), user_id.to_string());
    if !project_dirs.is_empty() {
        env.insert("GW_MCP_PROJECT_DIRS".into(), project_dirs.join(":"));
    }
    if let Some(url) = runtime_api_url.filter(|s| !s.trim().is_empty()) {
        env.insert("GW_MCP_RUNTIME_API_URL".into(), url.to_string());
    }
    if let Some(token) = runtime_api_token.filter(|s| !s.trim().is_empty()) {
        env.insert("GW_MCP_RUNTIME_API_TOKEN".into(), token.to_string());
    }

    GatewayMcpServer {
        command: gateway_bin.to_string_lossy().into_owned(),
        args: vec!["mcp-serve".into()],
        env,
    }
}

fn absolutize(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

pub fn cleanup_mcp_config(chat_id: &str) {
    let hash = simple_hash(chat_id);
    let path = std::env::temp_dir().join(format!("gw-mcp-{hash}.json"));
    let _ = std::fs::remove_file(path);
}

fn simple_hash(s: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_mcp_config_generates_claude_file_and_codex_config() {
        let chat_id = format!("mcp-config-test-{}", std::process::id());
        let storage_env = McpStorageEnv {
            database_url: None,
            sqlite_path: Some("/tmp/gateway.sqlite".into()),
        };

        let generated = generate_gateway_mcp_config(
            &storage_env,
            "wecom",
            &chat_id,
            "user-1",
            &["/tmp/project".into()],
            Some("http://127.0.0.1:18080"),
            Some("runtime-token"),
        )
        .unwrap();

        let content = std::fs::read_to_string(&generated.claude_config_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&content).unwrap();
        let gateway = &json["mcpServers"]["gateway"];
        assert_eq!(gateway["args"], serde_json::json!(["mcp-serve"]));
        assert_eq!(gateway["env"]["GW_MCP_PLATFORM"], "wecom");
        assert_eq!(gateway["env"]["GW_MCP_CHAT_ID"], chat_id);
        assert_eq!(gateway["env"]["GW_MCP_USER_ID"], "user-1");
        assert_eq!(gateway["env"]["GW_MCP_SQLITE_PATH"], "/tmp/gateway.sqlite");
        assert_eq!(
            gateway["env"]["GW_MCP_RUNTIME_API_URL"],
            "http://127.0.0.1:18080"
        );
        assert_eq!(gateway["env"]["GW_MCP_RUNTIME_API_TOKEN"], "runtime-token");

        assert_eq!(generated.codex.env.get("GW_MCP_CHAT_ID"), Some(&chat_id));
        assert_eq!(generated.codex.args, ["mcp-serve"]);
        let codex_config = generated.codex.thread_config();
        assert_eq!(
            codex_config["mcp_servers"]["gateway"]["args"],
            serde_json::json!(["mcp-serve"])
        );
        assert_eq!(
            codex_config["mcp_servers"]["gateway"]["env"]["GW_MCP_CHAT_ID"],
            chat_id
        );
        assert_eq!(
            generated.codex.env.get("GW_MCP_SQLITE_PATH"),
            Some(&"/tmp/gateway.sqlite".to_string())
        );

        cleanup_mcp_config(&chat_id);
    }
}
