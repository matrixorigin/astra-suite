use std::path::PathBuf;

/// Storage info passed to the MCP server subprocess via environment variables.
/// No credentials are written to the temp config file.
pub struct McpStorageEnv {
    pub database_url: Option<String>,
    pub sqlite_path: Option<String>,
}

pub fn generate_mcp_config(
    storage_env: &McpStorageEnv,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    project_dirs: &[String],
    runtime_api_url: Option<&str>,
    runtime_api_token: Option<&str>,
) -> Result<PathBuf, std::io::Error> {
    let gateway_bin = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("astra-gateway"));

    // Env vars are set on the child process by Claude CLI when spawning the MCP server.
    // DB credentials go here (not in the JSON file) so they never touch disk.
    let mut env = serde_json::Map::new();
    if let Some(ref url) = storage_env.database_url {
        env.insert(
            "GATEWAY_DATABASE_URL".into(),
            serde_json::Value::String(url.clone()),
        );
    }
    if let Some(ref path) = storage_env.sqlite_path {
        env.insert(
            "GW_MCP_SQLITE_PATH".into(),
            serde_json::Value::String(path.clone()),
        );
    }
    env.insert(
        "GW_MCP_PLATFORM".into(),
        serde_json::Value::String(platform.to_string()),
    );
    env.insert(
        "GW_MCP_CHAT_ID".into(),
        serde_json::Value::String(chat_id.to_string()),
    );
    env.insert(
        "GW_MCP_USER_ID".into(),
        serde_json::Value::String(user_id.to_string()),
    );
    if !project_dirs.is_empty() {
        env.insert(
            "GW_MCP_PROJECT_DIRS".into(),
            serde_json::Value::String(project_dirs.join(":")),
        );
    }
    if let Some(url) = runtime_api_url.filter(|s| !s.trim().is_empty()) {
        env.insert(
            "GW_MCP_RUNTIME_API_URL".into(),
            serde_json::Value::String(url.to_string()),
        );
    }
    if let Some(token) = runtime_api_token.filter(|s| !s.trim().is_empty()) {
        env.insert(
            "GW_MCP_RUNTIME_API_TOKEN".into(),
            serde_json::Value::String(token.to_string()),
        );
    }

    let config = serde_json::json!({
        "mcpServers": {
            "gateway": {
                "command": gateway_bin.to_string_lossy(),
                "args": ["mcp-serve"],
                "env": env
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
    Ok(path)
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
