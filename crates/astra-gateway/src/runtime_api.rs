//! Lightweight local HTTP API for talking to the running gateway.
//!
//! Listens on `127.0.0.1:{port}`. `POST /inject` injects inbound messages;
//! `POST /outbound/attachment` asks the running gateway to send a file.

use crate::platforms::{AttachmentKind, ChatType, InboundMessage, OutboundAttachment};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum RuntimeCommand {
    SendAttachment {
        platform: String,
        chat_id: String,
        attachment: OutboundAttachment,
        caption: Option<String>,
    },
}

pub async fn run(
    port: u16,
    tx: mpsc::Sender<InboundMessage>,
    command_tx: mpsc::Sender<RuntimeCommand>,
    token: Option<String>,
) {
    let addr = format!("127.0.0.1:{port}");
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => {
            tracing::info!(port, "runtime API listening");
            l
        }
        Err(e) => {
            tracing::error!(port, error = %e, "runtime API bind failed");
            return;
        }
    };

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let tx = tx.clone();
        let command_tx = command_tx.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, tx, command_tx, token.as_deref()).await {
                tracing::debug!(error = %e, "runtime API connection error");
            }
        });
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    tx: mpsc::Sender<InboundMessage>,
    command_tx: mpsc::Sender<RuntimeCommand>,
    token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);

    let mut request_line = String::new();
    buf_reader.read_line(&mut request_line).await?;

    let mut content_length: usize = 0;
    let mut auth_header: Option<String> = None;
    let mut line = String::new();
    loop {
        line.clear();
        buf_reader.read_line(&mut line).await?;
        if line.trim().is_empty() {
            break;
        }
        if let Some(val) = line
            .strip_prefix("Content-Length: ")
            .or_else(|| line.strip_prefix("content-length: "))
        {
            content_length = val.trim().parse().unwrap_or(0);
        }
        if let Some(val) = line
            .strip_prefix("Authorization: ")
            .or_else(|| line.strip_prefix("authorization: "))
        {
            auth_header = Some(val.trim().to_string());
        }
    }

    let path = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    if !request_line.starts_with("POST ") {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        writer.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let mut body = vec![0u8; content_length];
    tokio::io::AsyncReadExt::read_exact(&mut buf_reader, &mut body).await?;

    if let Some(expected) = token
        && !expected.is_empty()
    {
        let got = auth_header
            .as_deref()
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or_default();
        if got != expected {
            write_json_response(&mut writer, 401, r#"{"error":"unauthorized"}"#).await?;
            return Ok(());
        }
    }

    if path == "/outbound/attachment" {
        return handle_outbound_attachment(&mut writer, command_tx, &body).await;
    }
    if path != "/inject" {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        writer.write_all(resp.as_bytes()).await?;
        return Ok(());
    }

    let msg: Result<InjectRequest, _> = serde_json::from_slice(&body);
    match msg {
        Ok(req) => {
            let platform: &'static str = match req.platform.as_str() {
                "wecom" => "wecom",
                "weixin" => "weixin",
                "whatsapp" => "whatsapp",
                "whatsapp_web" => "whatsapp_web",
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
                feedback: None,
            };
            let _ = tx.send(inbound).await;
            write_json_response(&mut writer, 200, r#"{"ok":true}"#).await?;
        }
        Err(e) => {
            let body = format!("{{\"error\":\"{e}\"}}");
            write_json_response(&mut writer, 400, &body).await?;
        }
    }
    Ok(())
}

async fn handle_outbound_attachment(
    writer: &mut tokio::net::tcp::WriteHalf<'_>,
    command_tx: mpsc::Sender<RuntimeCommand>,
    body: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let req: Result<OutboundAttachmentRequest, _> = serde_json::from_slice(body);
    match req {
        Ok(req) => {
            let path = std::path::PathBuf::from(&req.path);
            if !path.is_absolute() {
                write_json_response(writer, 400, r#"{"error":"path must be absolute"}"#).await?;
                return Ok(());
            }
            let attachment = OutboundAttachment {
                kind: AttachmentKind::from_hint(
                    req.kind.as_deref(),
                    req.mime.as_deref(),
                    Some(&path),
                ),
                name: req.filename,
                media_id: None,
                local_path: Some(path.to_string_lossy().to_string()),
                mime_type: req.mime,
            };
            let command = RuntimeCommand::SendAttachment {
                platform: req.platform.unwrap_or_else(default_platform),
                chat_id: req.chat_id,
                attachment,
                caption: req.caption,
            };
            if let Err(e) = command_tx.send(command).await {
                let body = format!("{{\"error\":\"runtime command channel closed: {e}\"}}");
                write_json_response(writer, 503, &body).await?;
                return Ok(());
            }
            write_json_response(writer, 200, r#"{"ok":true}"#).await?;
        }
        Err(e) => {
            let body = format!("{{\"error\":\"{e}\"}}");
            write_json_response(writer, 400, &body).await?;
        }
    }
    Ok(())
}

async fn write_json_response(
    writer: &mut tokio::net::tcp::WriteHalf<'_>,
    status: u16,
    body: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        503 => "Service Unavailable",
        _ => "OK",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    writer.write_all(resp.as_bytes()).await?;
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

#[derive(serde::Deserialize)]
struct OutboundAttachmentRequest {
    #[serde(default)]
    platform: Option<String>,
    chat_id: String,
    path: String,
    #[serde(default)]
    filename: Option<String>,
    #[serde(default)]
    caption: Option<String>,
    #[serde(default)]
    mime: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

fn default_platform() -> String {
    "wecom".to_string()
}
