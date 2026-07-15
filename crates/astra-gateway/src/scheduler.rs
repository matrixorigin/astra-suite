//! Cron scheduler — polls gw_cron_jobs and executes due tasks.

use crate::platforms::{ChatType, InboundMessage};
use crate::runner::{OutboundMessage, OutboxDelivery, ScheduledAgentTurn};
use crate::store::GatewayStore;
use crate::trace_model::{ConversationKey, GatewayRequest, TraceRepository, TraceWriter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const POLL_INTERVAL: Duration = Duration::from_secs(60);
#[cfg(test)]
const CLI_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_UNTIL_RESULT_MARKER: &str = "[[ASTRA_POLL_UNTIL_RESULT]]";
const SILENT_MARKER: &str = "[[ASTRA_SILENT]]";

fn parse_poll_result(response: &str) -> (String, bool) {
    let silent = response.contains(SILENT_MARKER);
    (
        response
            .replace(SILENT_MARKER, "")
            .replace(POLL_UNTIL_RESULT_MARKER, "")
            .trim()
            .to_string(),
        silent,
    )
}

fn platform_name(name: &str) -> Option<&'static str> {
    match name {
        "wecom" => Some("wecom"),
        "weixin" => Some("weixin"),
        "whatsapp" => Some("whatsapp"),
        "whatsapp_web" => Some("whatsapp_web"),
        _ => None,
    }
}

pub struct CronScheduler {
    store: Arc<dyn GatewayStore>,
    trace_repo: Arc<dyn TraceRepository>,
    outbound_tx: mpsc::Sender<OutboundMessage>,
    agent_turn_tx: mpsc::Sender<ScheduledAgentTurn>,
}

impl CronScheduler {
    pub fn new(
        store: Arc<dyn GatewayStore>,
        trace_repo: Arc<dyn TraceRepository>,
        outbound_tx: mpsc::Sender<OutboundMessage>,
        agent_turn_tx: mpsc::Sender<ScheduledAgentTurn>,
    ) -> Self {
        Self {
            store,
            trace_repo,
            outbound_tx,
            agent_turn_tx,
        }
    }

    pub fn spawn(
        self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tracing::info!("cron scheduler started");
            let mut interval = tokio::time::interval(POLL_INTERVAL);

            loop {
                tokio::select! {
                    _ = interval.tick() => self.tick().await,
                    _ = shutdown.recv() => break,
                }
            }
            tracing::info!("cron scheduler stopped");
        })
    }

    async fn tick(&self) {
        let jobs = match self.store.get_due_jobs().await {
            Ok(j) => j,
            Err(e) => {
                tracing::error!(error = %e, "cron: query failed");
                return;
            }
        };

        for job in jobs {
            let job_id = &job.job_id;
            let platform = &job.platform;
            let chat_id = &job.chat_id;
            let message = &job.message;
            let cron_expr = &job.cron_expr;
            tracing::info!(job_id = %job_id, expr = %cron_expr, "cron: executing");

            let user_id = self.resolve_job_user_id(job_id).await.unwrap_or_default();

            let is_one_shot = cron_expr == "once" || cron_expr == "once_exec";

            // Pure text reminder — no agent invocation needed
            if cron_expr == "once" {
                let text = format!("⏰ 提醒: {message}");
                let outbound = self
                    .enqueue_scheduler_outbox(
                        job_id, platform, chat_id, &user_id, "reminder", &text, None,
                    )
                    .await
                    .unwrap_or_else(|_| {
                        OutboundMessage::plain(platform.clone(), chat_id.clone(), text)
                    });
                if let Err(e) = self.outbound_tx.send(outbound).await {
                    tracing::warn!(job_id = %job_id, error = %e, "failed to send one-shot reminder");
                }
                if let Err(e) = self.store.delete_cron_job(job_id).await {
                    tracing::error!(job_id = %job_id, error = %e, "failed to delete one-shot cron job — will re-fire");
                }
                continue;
            }
            // once_exec and recurring cron both invoke the agent below
            let Some(platform_name) = platform_name(platform) else {
                tracing::warn!(job_id = %job_id, platform = %platform, "unsupported cron platform");
                continue;
            };
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let turn = ScheduledAgentTurn {
                message: InboundMessage {
                    platform: platform_name,
                    chat_id: chat_id.clone(),
                    user_id: user_id.clone(),
                    text: message.clone(),
                    attachments: Vec::new(),
                    msg_id: format!("cron-{job_id}-{}", uuid::Uuid::new_v4()),
                    chat_type: ChatType::DirectMessage,
                    reply_token: None,
                    route_override: None,
                    feedback: None,
                },
                response_tx,
            };

            if let Err(e) = self.agent_turn_tx.send(turn).await {
                tracing::warn!(job_id = %job_id, error = %e, "failed to enqueue scheduled agent turn");
                continue;
            }
            let response = match response_rx.await {
                Ok(Some(response)) => response.text,
                Ok(None) => {
                    tracing::warn!(job_id = %job_id, "scheduled agent turn produced no response");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(job_id = %job_id, error = %e, "scheduled agent turn response dropped");
                    continue;
                }
            };

            let polling = message.contains(POLL_UNTIL_RESULT_MARKER);
            let (response, silent) = parse_poll_result(&response);
            let completed = is_one_shot || (polling && !silent);
            let deliver = !silent;

            if deliver {
                let prefix = format!("⏰ **定时任务 `{}`**\n\n", &job_id[..8.min(job_id.len())]);
                let body = format!("{prefix}{response}");
                let outbound = self
                    .enqueue_scheduler_outbox(
                        job_id,
                        platform,
                        chat_id,
                        &user_id,
                        "scheduled",
                        &body,
                        None,
                    )
                    .await
                    .unwrap_or_else(|_| {
                        OutboundMessage::plain(platform.clone(), chat_id.clone(), body)
                    });
                if let Err(e) = self.outbound_tx.send(outbound).await {
                    tracing::warn!(job_id = %job_id, error = %e, "scheduler: outbound send failed");
                }
            }

            if completed {
                if let Err(e) = self.store.delete_cron_job(job_id).await {
                    tracing::error!(job_id = %job_id, error = %e, "failed to delete completed scheduled job — will re-fire");
                }
            } else if let Err(e) = self.store.mark_job_run(job_id, cron_expr).await {
                tracing::warn!(job_id = %job_id, error = %e, "scheduler: failed to mark job run");
            }
        }
    }

    /// Resolve the user_id who created the cron job.
    async fn resolve_job_user_id(&self, job_id: &str) -> Option<String> {
        self.store.get_cron_job_user_id(job_id).await.ok().flatten()
    }

    async fn begin_scheduler_trace(
        &self,
        job_id: &str,
        platform: &str,
        chat_id: &str,
        cli_name: &str,
        message: &str,
    ) -> Option<TraceWriter<'_>> {
        let request = GatewayRequest::new(
            ConversationKey::new(platform, chat_id, cli_name),
            format!("cron-{job_id}"),
            "",
            message,
        );
        match TraceWriter::begin(self.trace_repo.as_ref(), request).await {
            Ok(writer) => Some(writer),
            Err(e) => {
                tracing::warn!(error = %e, "scheduler trace begin failed");
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn enqueue_scheduler_outbox(
        &self,
        job_id: &str,
        platform: &str,
        chat_id: &str,
        _user_id: &str,
        cli_name: &str,
        body: &str,
        reply_token: Option<String>,
    ) -> Result<OutboundMessage, String> {
        let writer = self
            .begin_scheduler_trace(job_id, platform, chat_id, cli_name, body)
            .await
            .ok_or_else(|| "scheduler trace unavailable".to_string())?;
        let outbox_id = writer
            .enqueue_outbox(platform, chat_id, reply_token.clone(), body)
            .await?;
        Ok(OutboundMessage::with_outbox(
            platform.to_string(),
            chat_id.to_string(),
            body.to_string(),
            reply_token,
            Some(writer.request_id().to_string()),
            OutboxDelivery {
                outbox_id,
                trace_id: writer.trace_id().clone(),
                request_id: writer.request_id().clone(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constants() {
        assert_eq!(POLL_INTERVAL.as_secs(), 60);
        assert_eq!(CLI_TIMEOUT.as_secs(), 300);
    }

    #[test]
    fn poll_result_silent() {
        let (text, silent) = parse_poll_result("still running\n[[ASTRA_SILENT]]");
        assert_eq!(text, "still running");
        assert!(silent);
    }

    #[test]
    fn poll_result_visible() {
        let (text, silent) = parse_poll_result("final report");
        assert_eq!(text, "final report");
        assert!(!silent);
    }

    #[test]
    fn poll_result_does_not_deliver_input_marker_if_model_echoes_it() {
        let (text, silent) = parse_poll_result("External task ready\n[[ASTRA_POLL_UNTIL_RESULT]]");
        assert_eq!(text, "External task ready");
        assert!(!silent);
    }

    #[test]
    fn poll_result_only_marker_becomes_empty() {
        let (text, silent) = parse_poll_result("[[ASTRA_SILENT]]");
        assert_eq!(text, "");
        assert!(silent);
    }

    #[test]
    fn platform_names_are_restricted_to_configured_adapters() {
        assert_eq!(platform_name("wecom"), Some("wecom"));
        assert_eq!(platform_name("weixin"), Some("weixin"));
        assert_eq!(platform_name("unknown"), None);
    }

    #[test]
    fn polling_mode_is_opt_in() {
        assert!(
            format!("{POLL_UNTIL_RESULT_MARKER}\ncheck status").contains(POLL_UNTIL_RESULT_MARKER)
        );
        assert!(!"ordinary recurring task".contains(POLL_UNTIL_RESULT_MARKER));
    }
}
