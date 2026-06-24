//! WhatsApp Business Cloud API adapter.
//!
//! Inbound: Meta webhooks over HTTP. Outbound: Graph API `/messages`.

use super::{
    AdapterCapability, AdapterHealthEvent, AdapterHealthEventType, ChatType, InboundMessage,
    PlatformAdapter, emit_adapter_health,
};
use crate::dedup::MessageDeduplicator;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

type HmacSha256 = Hmac<Sha256>;

const WHATSAPP_CAPABILITIES: &[AdapterCapability] =
    &[AdapterCapability::ReceiveText, AdapterCapability::SendText];

#[derive(Clone, serde::Deserialize)]
pub struct WhatsAppConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_webhook_path")]
    pub webhook_path: String,
    #[serde(default)]
    pub verify_token: String,
    #[serde(default)]
    pub app_secret: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub phone_number_id: String,
    #[serde(default = "default_graph_base_url")]
    pub graph_base_url: String,
}

impl std::fmt::Debug for WhatsAppConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WhatsAppConfig")
            .field("enabled", &self.enabled)
            .field("bind", &self.bind)
            .field("webhook_path", &self.webhook_path)
            .field(
                "verify_token",
                &if self.verify_token.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .field(
                "app_secret",
                &if self.app_secret.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .field(
                "access_token",
                &if self.access_token.is_empty() {
                    "(empty)"
                } else {
                    "[REDACTED]"
                },
            )
            .field("phone_number_id", &self.phone_number_id)
            .field("graph_base_url", &self.graph_base_url)
            .finish()
    }
}

impl WhatsAppConfig {
    pub fn resolve(mut self) -> Self {
        set_from_env_if_empty(&mut self.bind, "WHATSAPP_BIND");
        set_from_env_if_empty(&mut self.webhook_path, "WHATSAPP_WEBHOOK_PATH");
        set_from_env_if_empty(&mut self.verify_token, "WHATSAPP_VERIFY_TOKEN");
        set_from_env_if_empty(&mut self.app_secret, "WHATSAPP_APP_SECRET");
        set_from_env_if_empty(&mut self.access_token, "WHATSAPP_ACCESS_TOKEN");
        set_from_env_if_empty(&mut self.phone_number_id, "WHATSAPP_PHONE_NUMBER_ID");
        set_from_env_if_empty(&mut self.graph_base_url, "WHATSAPP_GRAPH_BASE_URL");
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

fn default_bind() -> String {
    "0.0.0.0:8080".into()
}

fn default_webhook_path() -> String {
    "/webhook/whatsapp".into()
}

fn default_graph_base_url() -> String {
    "https://graph.facebook.com/v23.0".into()
}

pub struct WhatsAppAdapter {
    config: WhatsAppConfig,
    msg_tx: mpsc::Sender<InboundMessage>,
    msg_rx: Mutex<mpsc::Receiver<InboundMessage>>,
    client: reqwest::Client,
    shutdown: Option<tokio::sync::broadcast::Sender<()>>,
}

impl WhatsAppAdapter {
    pub fn new(config: WhatsAppConfig) -> Self {
        let (msg_tx, msg_rx) = mpsc::channel(256);
        Self {
            config: config.resolve(),
            msg_tx,
            msg_rx: Mutex::new(msg_rx),
            client: http_client_with_env_proxy(),
            shutdown: None,
        }
    }
}

#[async_trait]
impl PlatformAdapter for WhatsAppAdapter {
    fn name(&self) -> &'static str {
        "whatsapp"
    }

    fn capabilities(&self) -> &'static [AdapterCapability] {
        WHATSAPP_CAPABILITIES
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.config.verify_token.is_empty() {
            return Err("whatsapp: verify_token required".into());
        }
        if self.config.access_token.is_empty() || self.config.phone_number_id.is_empty() {
            return Err("whatsapp: access_token and phone_number_id required".into());
        }
        if self.config.app_secret.is_empty() {
            tracing::warn!("whatsapp app_secret is empty; webhook POST signature checks disabled");
        }
        for capability in self.capabilities() {
            emit_adapter_health(AdapterHealthEvent::capability("whatsapp", *capability));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        self.shutdown = Some(shutdown_tx);
        let config = self.config.clone();
        let msg_tx = self.msg_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = run_webhook_server(config, msg_tx, shutdown_rx).await {
                emit_adapter_health(AdapterHealthEvent::new(
                    "whatsapp",
                    AdapterHealthEventType::Disconnected,
                    Some(e.to_string()),
                ));
            }
        });

        tracing::info!(
            bind = %self.config.bind,
            path = %self.config.webhook_path,
            phone_number_id = %self.config.phone_number_id,
            "whatsapp adapter started"
        );
        Ok(())
    }

    async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            emit_adapter_health(AdapterHealthEvent::new(
                "whatsapp",
                AdapterHealthEventType::Shutdown,
                None,
            ));
            let _ = tx.send(());
        }
    }

    async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
        _reply_token: Option<&str>,
    ) -> Result<(), String> {
        let url = format!(
            "{}/{}/messages",
            self.config.graph_base_url.trim_end_matches('/'),
            self.config.phone_number_id
        );
        let body = json!({
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": chat_id,
            "type": "text",
            "text": {
                "preview_url": false,
                "body": text,
            }
        });
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.config.access_token)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("whatsapp send failed: {e}"))?;
        let status = resp.status();
        if status.is_success() {
            emit_adapter_health(AdapterHealthEvent::new(
                "whatsapp",
                AdapterHealthEventType::SendAck,
                Some(chat_id.to_string()),
            ));
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        emit_adapter_health(AdapterHealthEvent::new(
            "whatsapp",
            AdapterHealthEventType::SendError,
            Some(format!("status={status} body={body}")),
        ));
        Err(format!("whatsapp send failed: status={status} body={body}"))
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

async fn run_webhook_server(
    config: WhatsAppConfig,
    msg_tx: mpsc::Sender<InboundMessage>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(&config.bind).await?;
    emit_adapter_health(AdapterHealthEvent::new(
        "whatsapp",
        AdapterHealthEventType::Connected,
        Some(format!("{}{}", config.bind, config.webhook_path)),
    ));
    let dedup = Arc::new(Mutex::new(MessageDeduplicator::new()));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let config = config.clone();
                let msg_tx = msg_tx.clone();
                let dedup = dedup.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_webhook_connection(stream, config, msg_tx, dedup).await {
                        tracing::debug!(error = %e, "whatsapp webhook connection error");
                    }
                });
            }
            _ = shutdown.recv() => break,
        }
    }
    Ok(())
}

async fn handle_webhook_connection(
    mut stream: TcpStream,
    config: WhatsAppConfig,
    msg_tx: mpsc::Sender<InboundMessage>,
    dedup: Arc<Mutex<MessageDeduplicator>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request = read_http_request(&mut stream).await?;
    let response = if request.method == "GET" && request.path == config.webhook_path {
        handle_verify(&config, &request.query)
    } else if request.method == "POST" && request.path == config.webhook_path {
        handle_webhook_post(&config, request.headers, &request.body, msg_tx, dedup).await
    } else {
        HttpResponse::plain(404, "not found")
    };
    write_http_response(&mut stream, response).await?;
    Ok(())
}

async fn handle_webhook_post(
    config: &WhatsAppConfig,
    headers: HashMap<String, String>,
    body: &[u8],
    msg_tx: mpsc::Sender<InboundMessage>,
    dedup: Arc<Mutex<MessageDeduplicator>>,
) -> HttpResponse {
    if !config.app_secret.is_empty() {
        let Some(signature) = headers.get("x-hub-signature-256") else {
            return HttpResponse::plain(401, "missing signature");
        };
        if !verify_signature(&config.app_secret, body, signature) {
            return HttpResponse::plain(401, "bad signature");
        }
    }

    let payload: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return HttpResponse::plain(400, &format!("bad json: {e}")),
    };
    let messages = extract_text_messages(&payload);
    let mut accepted = 0usize;
    for msg in messages {
        if !dedup.lock().await.check(&msg.msg_id) {
            continue;
        }
        if msg_tx.send(msg).await.is_ok() {
            accepted += 1;
        }
    }
    if accepted == 0 {
        HttpResponse::json(200, json!({"ok": true, "accepted": 0}))
    } else {
        emit_adapter_health(AdapterHealthEvent::new(
            "whatsapp",
            AdapterHealthEventType::SubscribeAck,
            Some(format!("accepted={accepted}")),
        ));
        HttpResponse::json(200, json!({"ok": true, "accepted": accepted}))
    }
}

fn handle_verify(config: &WhatsAppConfig, query: &str) -> HttpResponse {
    let params = parse_query(query);
    let mode = params.get("hub.mode").map(String::as_str);
    let token = params.get("hub.verify_token").map(String::as_str);
    let challenge = params.get("hub.challenge").cloned().unwrap_or_default();
    if mode == Some("subscribe") && token == Some(config.verify_token.as_str()) {
        return HttpResponse::plain(200, &challenge);
    }
    HttpResponse::plain(403, "forbidden")
}

fn extract_text_messages(payload: &Value) -> Vec<InboundMessage> {
    let mut out = Vec::new();
    let Some(entries) = payload.get("entry").and_then(Value::as_array) else {
        return out;
    };
    for entry in entries {
        let Some(changes) = entry.get("changes").and_then(Value::as_array) else {
            continue;
        };
        for change in changes {
            let Some(value) = change.get("value") else {
                continue;
            };
            let Some(messages) = value.get("messages").and_then(Value::as_array) else {
                continue;
            };
            for msg in messages {
                if msg.get("type").and_then(Value::as_str) != Some("text") {
                    continue;
                }
                let from = msg.get("from").and_then(Value::as_str).unwrap_or_default();
                let msg_id = msg.get("id").and_then(Value::as_str).unwrap_or_default();
                let text = msg
                    .get("text")
                    .and_then(|v| v.get("body"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if from.is_empty() || msg_id.is_empty() || text.is_empty() {
                    continue;
                }
                out.push(InboundMessage {
                    platform: "whatsapp",
                    chat_id: from.to_string(),
                    user_id: from.to_string(),
                    text: text.to_string(),
                    attachments: Vec::new(),
                    msg_id: msg_id.to_string(),
                    chat_type: ChatType::DirectMessage,
                    reply_token: None,
                    route_override: None,
                    feedback: None,
                });
            }
        }
    }
    out
}

fn verify_signature(app_secret: &str, body: &[u8], signature: &str) -> bool {
    let Some(given) = signature.strip_prefix("sha256=") else {
        return false;
    };
    let mut mac = match HmacSha256::new_from_slice(app_secret.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());
    expected.as_bytes().ct_eq(given.as_bytes()).into()
}

struct HttpRequest {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn read_http_request(
    stream: &mut TcpStream,
) -> Result<HttpRequest, Box<dyn std::error::Error + Send + Sync>> {
    let (reader, _) = stream.split();
    let mut buf_reader = BufReader::new(reader);
    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default();
    let (path, query) = target
        .split_once('?')
        .map(|(p, q)| (p.to_string(), q.to_string()))
        .unwrap_or_else(|| (target.to_string(), String::new()));

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    let mut line = String::new();
    loop {
        line.clear();
        buf_reader.read_line(&mut line).await?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if key == "content-length" {
                content_length = value.parse().unwrap_or(0);
            }
            headers.insert(key, value);
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        tokio::io::AsyncReadExt::read_exact(&mut buf_reader, &mut body).await?;
    }
    Ok(HttpRequest {
        method,
        path,
        query,
        headers,
        body,
    })
}

struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: Vec<u8>,
}

impl HttpResponse {
    fn plain(status: u16, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.as_bytes().to_vec(),
        }
    }

    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(&body).unwrap_or_else(|_| b"{\"ok\":false}".to_vec()),
        }
    }
}

async fn write_http_response(
    stream: &mut TcpStream,
    response: HttpResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "OK",
    };
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        reason,
        response.content_type,
        response.body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(&response.body).await?;
    Ok(())
}

fn parse_query(query: &str) -> HashMap<String, String> {
    query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter_map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            Some((
                urlencoding::decode(key).ok()?.into_owned(),
                urlencoding::decode(value).ok()?.into_owned(),
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_signature_accepts_matching_hmac() {
        let body = br#"{"hello":"world"}"#;
        let secret = "secret";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert!(verify_signature(secret, body, &sig));
        assert!(!verify_signature(secret, body, "sha256=bad"));
    }

    #[test]
    fn extract_text_webhook_messages() {
        let payload = json!({
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "from": "8618729326612",
                            "id": "wamid.123",
                            "type": "text",
                            "text": {"body": "hello"}
                        }]
                    }
                }]
            }]
        });
        let messages = extract_text_messages(&payload);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].platform, "whatsapp");
        assert_eq!(messages[0].chat_id, "8618729326612");
        assert_eq!(messages[0].text, "hello");
    }

    #[test]
    fn verify_challenge_requires_token() {
        let cfg = WhatsAppConfig {
            enabled: true,
            bind: default_bind(),
            webhook_path: default_webhook_path(),
            verify_token: "tok".into(),
            app_secret: String::new(),
            access_token: "access".into(),
            phone_number_id: "phone".into(),
            graph_base_url: default_graph_base_url(),
        };
        let resp = handle_verify(
            &cfg,
            "hub.mode=subscribe&hub.verify_token=tok&hub.challenge=abc123",
        );
        assert_eq!(resp.status, 200);
        assert_eq!(String::from_utf8(resp.body).unwrap(), "abc123");
    }
}
