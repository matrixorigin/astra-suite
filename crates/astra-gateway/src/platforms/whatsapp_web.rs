//! WhatsApp Web sidecar adapter.
//!
//! This adapter pairs with `bridges/whatsapp-baileys`: inbound messages are
//! injected into gateway via `/inject`, while outbound replies are delivered to
//! the sidecar over local HTTP.

use super::{
    AdapterCapability, AdapterHealthEvent, AdapterHealthEventType, InboundMessage, PlatformAdapter,
    emit_adapter_health,
};
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::{Mutex, mpsc};

const WHATSAPP_WEB_CAPABILITIES: &[AdapterCapability] =
    &[AdapterCapability::ReceiveText, AdapterCapability::SendText];

#[derive(Clone, serde::Deserialize)]
pub struct WhatsAppWebConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bridge_url")]
    pub bridge_url: String,
    #[serde(default)]
    pub auth_token: String,
}

impl std::fmt::Debug for WhatsAppWebConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WhatsAppWebConfig")
            .field("enabled", &self.enabled)
            .field("bridge_url", &self.bridge_url)
            .field(
                "auth_token",
                &if self.auth_token.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .finish()
    }
}

impl WhatsAppWebConfig {
    pub fn resolve(mut self) -> Self {
        set_from_env_if_empty(&mut self.bridge_url, "WHATSAPP_WEB_BRIDGE_URL");
        set_from_env_if_empty(&mut self.auth_token, "WHATSAPP_WEB_AUTH_TOKEN");
        self
    }
}

fn set_from_env_if_empty(target: &mut String, key: &str) {
    if target.is_empty()
        && let Ok(v) = std::env::var(key)
    {
        *target = v;
    }
}

fn default_bridge_url() -> String {
    "http://127.0.0.1:8787".into()
}

pub struct WhatsAppWebAdapter {
    config: WhatsAppWebConfig,
    client: reqwest::Client,
    _msg_tx: mpsc::Sender<InboundMessage>,
    msg_rx: Mutex<mpsc::Receiver<InboundMessage>>,
}

impl WhatsAppWebAdapter {
    pub fn new(config: WhatsAppWebConfig) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            config: config.resolve(),
            client: http_client_with_env_proxy(),
            _msg_tx: tx,
            msg_rx: Mutex::new(rx),
        }
    }
}

#[async_trait]
impl PlatformAdapter for WhatsAppWebAdapter {
    fn name(&self) -> &'static str {
        "whatsapp_web"
    }

    fn capabilities(&self) -> &'static [AdapterCapability] {
        WHATSAPP_WEB_CAPABILITIES
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for capability in self.capabilities() {
            emit_adapter_health(AdapterHealthEvent::capability("whatsapp_web", *capability));
        }
        emit_adapter_health(AdapterHealthEvent::new(
            "whatsapp_web",
            AdapterHealthEventType::Connected,
            Some(self.config.bridge_url.clone()),
        ));
        Ok(())
    }

    async fn stop(&mut self) {
        emit_adapter_health(AdapterHealthEvent::new(
            "whatsapp_web",
            AdapterHealthEventType::Shutdown,
            None,
        ));
    }

    async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
        _reply_token: Option<&str>,
    ) -> Result<(), String> {
        let url = format!("{}/send", self.config.bridge_url.trim_end_matches('/'));
        let mut req = self.client.post(url).json(&json!({
            "to": chat_id,
            "text": text,
        }));
        if !self.config.auth_token.is_empty() {
            req = req.bearer_auth(&self.config.auth_token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("whatsapp_web send failed: {e}"))?;
        let status = resp.status();
        if status.is_success() {
            emit_adapter_health(AdapterHealthEvent::new(
                "whatsapp_web",
                AdapterHealthEventType::SendAck,
                Some(chat_id.to_string()),
            ));
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        emit_adapter_health(AdapterHealthEvent::new(
            "whatsapp_web",
            AdapterHealthEventType::SendError,
            Some(format!("status={status} body={body}")),
        ));
        Err(format!(
            "whatsapp_web send failed: status={status} body={body}"
        ))
    }

    async fn recv(&self) -> Option<InboundMessage> {
        self.msg_rx.lock().await.recv().await
    }
}

fn http_client_with_env_proxy() -> reqwest::Client {
    let mut builder = reqwest::Client::builder();
    let proxy = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .or_else(|_| std::env::var("HTTP_PROXY"))
        .or_else(|_| std::env::var("http_proxy"))
        .ok();
    if let Some(proxy) = proxy {
        match reqwest::Proxy::all(&proxy) {
            Ok(p) => builder = builder.proxy(p),
            Err(e) => tracing::warn!(proxy = %proxy, error = %e, "invalid proxy ignored"),
        }
    }
    builder.build().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "failed to build reqwest client; using default");
        reqwest::Client::new()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_auth_token() {
        let cfg = WhatsAppWebConfig {
            enabled: true,
            bridge_url: "http://127.0.0.1:8787".into(),
            auth_token: "secret-token".into(),
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("secret-token"), "auth token leaked: {dbg}");
    }
}
