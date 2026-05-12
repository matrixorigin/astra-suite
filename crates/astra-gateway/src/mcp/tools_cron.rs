use crate::store::{self, GatewayStore};

pub async fn cron_list(store: &dyn GatewayStore, platform: &str, chat_id: &str) -> String {
    match store.list_cron_jobs(platform, chat_id).await {
        Ok(jobs) if jobs.is_empty() => "No scheduled tasks.".into(),
        Ok(jobs) => {
            let mut lines = vec![format!("Scheduled tasks ({}):", jobs.len())];
            for j in &jobs {
                let status = if j.enabled { "✅" } else { "⏸" };
                let short_id = &j.job_id[..8.min(j.job_id.len())];
                lines.push(format!(
                    "{status} `{short_id}` | `{}` | {}",
                    j.cron_expr, j.description
                ));
            }
            lines.join("\n")
        }
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn cron_add(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    cron_expr: &str,
    message: &str,
) -> String {
    if message.is_empty() {
        return "Error: message cannot be empty".into();
    }
    if !store::is_valid_cron_expr(cron_expr) {
        return format!(
            "Error: invalid cron expression `{cron_expr}` (need 5 fields: min hour day month weekday)"
        );
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    match store
        .create_cron_job(&store::CronJobSpec {
            job_id: job_id.clone(),
            platform: platform.to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
            cron_expr: cron_expr.to_string(),
            message: message.to_string(),
            description: message.to_string(),
        })
        .await
    {
        Ok(()) => format!(
            "Cron job created\n- ID: `{}`\n- Schedule: `{cron_expr}`\n- Message: {message}",
            &job_id[..8]
        ),
        Err(e) => format!("Error creating cron job: {e}"),
    }
}

pub async fn cron_delete(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    job_id: &str,
) -> String {
    if job_id.is_empty() {
        return "Error: job_id cannot be empty".into();
    }
    match store.list_cron_jobs(platform, chat_id).await {
        Ok(jobs) => {
            let matched = jobs
                .iter()
                .find(|j| j.job_id == job_id || j.job_id.starts_with(job_id));
            if let Some(j) = matched {
                let desc = j.description.clone();
                match store.delete_cron_job(&j.job_id).await {
                    Ok(_) => format!("Deleted: {desc}"),
                    Err(e) => format!("Error deleting: {e}"),
                }
            } else {
                format!("Not found: `{job_id}`")
            }
        }
        Err(e) => format!("Error: {e}"),
    }
}

pub async fn remind_after(
    store: &dyn GatewayStore,
    platform: &str,
    chat_id: &str,
    user_id: &str,
    minutes: u64,
    message: &str,
    exec: bool,
) -> String {
    if message.is_empty() {
        return "Error: message cannot be empty".into();
    }
    if minutes == 0 {
        return "Error: minutes must be > 0".into();
    }
    if minutes > 1440 * 7 {
        return "Error: maximum 7 days (10080 minutes)".into();
    }

    let (cron_type, stored_message) = if exec {
        ("once_exec", message.to_string())
    } else {
        ("once", message.to_string())
    };

    let job_id = uuid::Uuid::new_v4().to_string();
    let next_run = chrono::Utc::now() + chrono::Duration::minutes(minutes as i64);
    let next_run_str = next_run.format("%Y-%m-%d %H:%M:%S").to_string();
    let desc = if exec {
        format!("🤖 {stored_message} (scheduled exec)")
    } else {
        format!("⏰ {stored_message} (one-time)")
    };

    match store
        .create_cron_job(&store::CronJobSpec {
            job_id: job_id.clone(),
            platform: platform.to_string(),
            chat_id: chat_id.to_string(),
            user_id: user_id.to_string(),
            cron_expr: cron_type.to_string(),
            message: stored_message,
            description: desc,
        })
        .await
    {
        Ok(()) => {
            let _ = store.update_cron_next_run(&job_id, &next_run_str).await;
            let time_str = if minutes >= 60 {
                let h = minutes / 60;
                let m = minutes % 60;
                if m == 0 {
                    format!("{h}h")
                } else {
                    format!("{h}h{m}m")
                }
            } else {
                format!("{minutes}m")
            };
            if exec {
                format!(
                    "Scheduled exec in {time_str}: {message}\n(ID: `{}`)",
                    &job_id[..8]
                )
            } else {
                format!(
                    "Reminder in {time_str}: {message}\n(ID: `{}`)",
                    &job_id[..8]
                )
            }
        }
        Err(e) => format!("Error: {e}"),
    }
}
