use crate::store::GatewayStore;
use crate::workspace;

pub async fn workspace_current(store: &dyn GatewayStore, platform: &str, user_id: &str) -> String {
    match store
        .get_user_preference(platform, user_id, "workspace")
        .await
    {
        Ok(Some(ws)) => format!("Current workspace: `{ws}`"),
        Ok(None) => "No workspace set (using default)".into(),
        Err(e) => format!("Error: {e}"),
    }
}

pub fn workspace_list(project_dirs: &[String]) -> String {
    let projects = workspace::discover_all_projects(project_dirs);
    if projects.is_empty() {
        return "No projects discovered. Configure `project_dirs` in gateway.yaml.".into();
    }
    let mut lines = vec![format!("Available projects ({}):", projects.len())];
    for p in &projects {
        lines.push(format!("- {}", p.summary()));
    }
    lines.join("\n")
}

pub async fn workspace_switch(
    store: &dyn GatewayStore,
    platform: &str,
    user_id: &str,
    path: &str,
    project_dirs: &[String],
) -> String {
    let expanded = if path.starts_with('~') {
        let home = std::env::var("HOME").unwrap_or_default();
        path.replacen('~', &home, 1)
    } else {
        path.to_string()
    };
    let p = std::path::Path::new(&expanded);
    if !p.is_dir() {
        return format!("Error: directory does not exist: `{expanded}`");
    }
    let canonical = p
        .canonicalize()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or(expanded);

    if !is_allowed_path(&canonical, project_dirs) {
        return "Error: path is not within any configured project directory".into();
    }

    match store
        .set_user_preference(platform, user_id, "workspace", &canonical)
        .await
    {
        Ok(()) => format!("Workspace switched to: `{canonical}`"),
        Err(e) => format!("Error: {e}"),
    }
}

fn is_allowed_path(canonical: &str, project_dirs: &[String]) -> bool {
    let home = std::env::var("HOME").unwrap_or_default();
    if !home.is_empty() && canonical.starts_with(&home) {
        return true;
    }
    project_dirs
        .iter()
        .any(|dir| canonical.starts_with(dir) || dir.starts_with(canonical))
}
