//! Lightweight HTTP server for injecting messages into the gateway.
//!
//! Listens on `127.0.0.1:{port}` and accepts `POST /inject` with JSON body.
//! The injected message is processed exactly like a real platform inbound message.

use crate::platforms::{ChatType, InboundMessage};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

pub async fn run(port: u16, tx: mpsc::Sender<InboundMessage>) {
    let addr = format!("127.0.0.1:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!(port, "inject API listening");
            l
        }
        Err(e) => {
            tracing::error!(port, error = %e, "inject API bind failed");
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx).await {
                tracing::debug!(error = %e, "inject connection error");
            }
        });
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    tx: mpsc::Sender<InboundMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);

    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    let mut content_length: usize = 0;
    let mut line = String::new();
    loop {
        line.clear();
        buf_reader.read_line(&mut line).await?;
        if line.trim().is_empty() {
            break;
        }
        if let Some(val) = line.strip_prefix("Content-Length: ").or_else(|| line.strip_prefix("content-length: ")) {
            content_length = val.trim().parse().unwrap_or(0);
        }
    }

    if !request_line.starts_with("POST /inject") {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        writer.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let mut body = vec![0u8; content_length];
    tokio::io::AsyncReadExt::read_exact(&mut buf_reader, &mut body).await?;

    let msg: Result<InjectRequest, _> = serde_json::from_slice(&body);
    match msg {
        Ok(req) => {
            let platform: &'static str = match req.platform.as_str() {
                "wecom" => "wecom",
                "weixin" => "weixin",
                "telegram" => "telegram",
                _ => "wecom",
            };
            let inbound = InboundMessage {
                platform,
                chat_id: req.chat_id,
                user_id: req.user_id,
                text: req.text,
                msg_id: format!("inject-{}", uuid::Uuid::new_v4()),
                chat_type: if req.group.unwrap_or(false) {
                    ChatType::Group
                } else {
                    ChatType::DirectMessage
                },
                reply_token: None,
                route_override: None,
            };
            let _ = tx.send(inbound).await;
            let resp = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"ok\":true}";
            writer.write_all(resp.as_bytes()).await?;
        }
        Err(e) => {
            let body = format!("{{\"error\":\"{e}\"}}");
            let resp = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            writer.write_all(resp.as_bytes()).await?;
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct InjectRequest {
    /// Platform to route response through (default: "wecom")
    #[serde(default = "default_platform")]
    platform: String,
    /// Chat/conversation ID (group chatid or user id for DM)
    chat_id: String,
    /// Sender user ID
    user_id: String,
    /// Message text
    text: String,
    /// Whether this is a group message (default: false)
    #[serde(default)]
    group: Option<bool>,
}

fn default_platform() -> String {
    "wecom".to_string()
}
