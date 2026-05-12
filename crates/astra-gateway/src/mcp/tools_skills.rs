use crate::store::GatewayStore;

pub async fn skills_list(store: &dyn GatewayStore, platform: &str, chat_id: &str) -> String {
    match store.list_skills(platform, chat_id).await {
        Ok(skills) if skills.is_empty() => "No saved skills.".into(),
        Ok(skills) => {
            let mut lines = vec![format!("Skills ({}):", skills.len())];
            for s in &skills {
                let desc = if s.description.is_empty() {
                    "(no description)".to_string()
                } else {
                    s.description.clone()
                };
                lines.push(format!("- **{}** — {}", s.name, desc));
            }
            lines.join("\n")
        }
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn skills_read(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    name: &str,
) -> String {
    if name.is_empty() {
        return "Error: name cannot be empty".into();
    }
    match store.get_skill(platform, chat_id, name).await {
        Ok(Some(skill)) => skill.content,
        Ok(None) => format!("Skill `{name}` not found"),
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn skills_add(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    name: &str,
    content: &str,
    description: &str,
) -> String {
    if name.is_empty() {
        return "Error: name cannot be empty".into();
    }
    if content.is_empty() {
        return "Error: content cannot be empty".into();
    }
    if !is_safe_skill_name(name) {
        return "Error: name can only contain letters, digits, spaces, underscores, or hyphens"
            .into();
    }
    match store
        .upsert_skill(platform, chat_id, name, content, description)
        .await
    {
        Ok(()) => format!("Skill `{name}` saved"),
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn skills_delete(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    name: &str,
) -> String {
    if name.is_empty() {
        return "Error: name cannot be empty".into();
    }
    match store.delete_skill(platform, chat_id, name).await {
        Ok(true) => format!("Skill `{name}` deleted"),
        Ok(false) => format!("Skill `{name}` not found"),
        Err(e) => format!("Error: {e}"),
    }
}

fn is_safe_skill_name(name: &str) -> bool {
    let trimmed = name.trim();
    !trimmed.is_empty()
        && trimmed
            .chars()
            .all(|ch| ch.is_alphanumeric() || matches!(ch, ' ' | '_' | '-'))
}
