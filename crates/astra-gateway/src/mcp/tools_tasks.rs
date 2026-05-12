use astra_task_store::{DurableTaskStatus, TaskFilter, TaskSpec};
use crate::durable_task_store::DurableTaskStoreExt;

pub async fn tasks_list(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    let owner_id = format!("{platform}:{chat_id}");
    let filter = TaskFilter {
        owner_id: Some(owner_id),
        ..Default::default()
    };
    match store.list(filter).await {
        Ok(tasks) if tasks.is_empty() => "No durable tasks.".into(),
        Ok(tasks) => {
            let mut lines = vec![format!("Durable tasks ({}):", tasks.len())];
            for t in &tasks {
                let short_id = &t.id.0[..8.min(t.id.0.len())];
                let icon = match t.status {
                    DurableTaskStatus::Running => "🔄",
                    DurableTaskStatus::Suspended => "⏸",
                    DurableTaskStatus::Completed => "✅",
                    DurableTaskStatus::Failed => "❌",
                    _ => "📋",
                };
                lines.push(format!("{icon} `{short_id}` | {} | {} | {}%", t.name, t.status.as_str(), t.progress_pct));
            }
            lines.join("\n")
        }
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn tasks_create(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
    name: &str,
    description: Option<&str>,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    if name.is_empty() {
        return "Error: name cannot be empty".into();
    }
    let spec = TaskSpec {
        name: name.to_string(),
        description: description.map(String::from),
        owner_id: format!("{platform}:{chat_id}"),
        initial_state: None,
    };
    match store.create(&spec).await {
        Ok(id) => format!("Task created\n- ID: `{}`\n- Name: {name}", &id.0[..8.min(id.0.len())]),
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn tasks_status(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
    task_id: &str,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    let owner_id = format!("{platform}:{chat_id}");
    match resolve_task(store, &owner_id, task_id).await {
        Ok(t) => {
            let mut lines = vec![
                format!("Task: {}", t.name),
                format!("- Status: {}", t.status.as_str()),
                format!("- Progress: {}%", t.progress_pct),
            ];
            if let Some(ref step) = t.step_description {
                lines.push(format!("- Current step: {step}"));
            }
            if let Some(ref err) = t.error_message {
                lines.push(format!("- Error: {err}"));
            }
            lines.join("\n")
        }
        Err(e) => e,
    }
}

pub async fn tasks_complete(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
    task_id: &str,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    let owner_id = format!("{platform}:{chat_id}");
    match resolve_task(store, &owner_id, task_id).await {
        Ok(task) => match store
            .update_status(&task.id, DurableTaskStatus::Completed, None)
            .await
        {
            Ok(()) => "Task marked as completed".into(),
            Err(e) => format!("Error: {e}"),
        },
        Err(e) => e,
    }
}

pub async fn tasks_fail(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
    task_id: &str,
    error: Option<&str>,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    let owner_id = format!("{platform}:{chat_id}");
    match resolve_task(store, &owner_id, task_id).await {
        Ok(task) => match store
            .update_status(&task.id, DurableTaskStatus::Failed, error)
            .await
        {
            Ok(()) => "Task marked as failed".into(),
            Err(e) => format!("Error: {e}"),
        },
        Err(e) => e,
    }
}

pub async fn tasks_cancel(
    store: Option<&dyn DurableTaskStoreExt>,
    platform: &str,
    chat_id: &str,
    task_id: &str,
) -> String {
    let Some(store) = store else {
        return "Durable tasks not available (no storage configured)".into();
    };
    let owner_id = format!("{platform}:{chat_id}");
    match resolve_task(store, &owner_id, task_id).await {
        Ok(task) => match store
            .update_status(&task.id, DurableTaskStatus::Cancelled, None)
            .await
        {
            Ok(()) => "Task cancelled".into(),
            Err(e) => format!("Error: {e}"),
        },
        Err(e) => e,
    }
}

async fn resolve_task(
    store: &dyn DurableTaskStoreExt,
    owner_id: &str,
    task_id: &str,
) -> Result<astra_task_store::DurableTask, String> {
    let filter = TaskFilter {
        owner_id: Some(owner_id.to_string()),
        ..Default::default()
    };
    let tasks = store
        .list(filter)
        .await
        .map_err(|e| format!("Error: {e}"))?;
    tasks
        .into_iter()
        .find(|t| t.id.0 == task_id || t.id.0.starts_with(task_id) || t.name == task_id)
        .ok_or_else(|| format!("Task `{task_id}` not found"))
}
