//! WeCom (企业微信) AI Bot WebSocket adapter.
//!
//! Protocol: connect → aibot_subscribe → heartbeat loop + message receive + outbound send.
//! Inbound: aibot_msg_callback. Outbound: aibot_send_msg / aibot_respond_msg.

use super::{
    AdapterCapability, AdapterHealthEvent, AdapterHealthEventType, AttachmentKind, ChatType,
    FeedbackEvent, InboundAttachment, InboundMessage, OutboundAttachment, PlatformAdapter,
    emit_adapter_health,
};
use crate::config::WeComConfig;
use crate::dedup::MessageDeduplicator;
use aes::Aes256;
use aes::cipher::{BlockDecrypt, KeyInit};
use async_trait::async_trait;
use base64::Engine;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use md5::{Digest, Md5};
use reqwest::header::CONTENT_TYPE;
use serde_json::{Value, json};
#[cfg(not(test))]
use std::net::IpAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const HEARTBEAT_INTERVAL_SECS: u64 = 30;
const PONG_TIMEOUT_SECS: u64 = 60;
const MAX_TEXT_LENGTH: usize = 4000;
const MEDIA_UPLOAD_CHUNK_BYTES: usize = 512 * 1024;
const MEDIA_UPLOAD_MAX_CHUNKS: usize = 100;
const ATTACHMENT_NAME_KEYS: &[&str] = &[
    "filename",
    "file_name",
    "name",
    "title",
    "display_name",
    "origin_name",
];
const ATTACHMENT_MEDIA_ID_KEYS: &[&str] = &["media_id", "mediaid", "file_id", "fileid"];
const ATTACHMENT_MIME_KEYS: &[&str] = &["mime_type", "mimetype", "content_type"];
const ATTACHMENT_SIZE_KEYS: &[&str] = &["size", "file_size", "filesize", "size_bytes"];
const RECONNECT_DELAYS: &[u64] = &[2, 5, 10, 30, 60];
const WECOM_CAPABILITIES: &[AdapterCapability] = &[
    AdapterCapability::ReceiveText,
    AdapterCapability::SendText,
    AdapterCapability::SendAttachment,
    AdapterCapability::GroupReply,
    AdapterCapability::WebSocket,
];

/// Outbound message to send via WebSocket.
struct OutboundMessage {
    chat_id: String,
    text: String,
    attachment: Option<OutboundAttachment>,
    reply_token: Option<String>,
    stream_id: Option<String>,
    feedback_id: Option<String>,
    stream_finish: bool,
}

type WeComWs = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WeComWsWrite = SplitSink<WeComWs, Message>;
type WeComWsRead = SplitStream<WeComWs>;

pub struct WeComAdapter {
    config: WeComConfig,
    msg_tx: mpsc::Sender<InboundMessage>,
    msg_rx: Mutex<mpsc::Receiver<InboundMessage>>,
    out_tx: mpsc::Sender<OutboundMessage>,
    shutdown: Option<tokio::sync::broadcast::Sender<()>>,
}

impl WeComAdapter {
    pub fn new(config: WeComConfig) -> Self {
        let (msg_tx, msg_rx) = mpsc::channel(256);
        let (out_tx, _out_rx) = mpsc::channel(256);
        Self {
            config: config.resolve(),
            msg_tx,
            msg_rx: Mutex::new(msg_rx),
            out_tx,
            shutdown: None,
        }
    }
}

#[async_trait]
impl PlatformAdapter for WeComAdapter {
    fn name(&self) -> &'static str {
        "wecom"
    }

    fn capabilities(&self) -> &'static [AdapterCapability] {
        WECOM_CAPABILITIES
    }

    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.config.bot_id.is_empty() || self.config.secret.is_empty() {
            return Err("wecom: bot_id and secret required".into());
        }
        for capability in self.capabilities() {
            emit_adapter_health(AdapterHealthEvent::capability("wecom", *capability));
        }

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        self.shutdown = Some(shutdown_tx.clone());

        let config = self.config.clone();
        let msg_tx = self.msg_tx.clone();

        // Create the real outbound channel and replace the placeholder
        let (out_tx, out_rx) = mpsc::channel(256);
        self.out_tx = out_tx;

        tokio::spawn(async move {
            let mut attempt = 0usize;
            let out_rx = std::sync::Arc::new(tokio::sync::Mutex::new(out_rx));
            loop {
                let mut shutdown_rx = shutdown_tx.subscribe();
                let out_rx_clone = out_rx.clone();
                match run_wecom_connection(&config, &msg_tx, out_rx_clone, &mut shutdown_rx).await {
                    Ok(()) => break,
                    Err(e) => {
                        let delay = RECONNECT_DELAYS[attempt.min(RECONNECT_DELAYS.len() - 1)];
                        emit_adapter_health(AdapterHealthEvent::new(
                            "wecom",
                            AdapterHealthEventType::Reconnecting,
                            Some(format!("{e}; retrying in {delay}s")),
                        ));
                        tracing::warn!(
                            error = %e,
                            delay_s = delay,
                            attempt = attempt + 1,
                            "wecom connection failed, reconnecting"
                        );
                        attempt += 1;
                        if wait_reconnect_delay(
                            std::time::Duration::from_secs(delay),
                            &mut shutdown_rx,
                        )
                        .await
                        {
                            break;
                        }
                    }
                }
            }
        });

        tracing::info!(bot_id = %self.config.bot_id, "wecom adapter started");
        Ok(())
    }

    async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            emit_adapter_health(AdapterHealthEvent::new(
                "wecom",
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
        reply_token: Option<&str>,
    ) -> Result<(), String> {
        self.send_stream_chunk(chat_id, text, reply_token, None, None, true)
            .await
    }

    async fn send_stream_chunk(
        &self,
        chat_id: &str,
        text: &str,
        reply_token: Option<&str>,
        stream_id: Option<&str>,
        feedback_id: Option<&str>,
        finish: bool,
    ) -> Result<(), String> {
        // Stream mode: no truncation here — runner handles 20480 byte limit by splitting streams.
        // Non-stream (aibot_send_msg): truncate to MAX_TEXT_LENGTH.
        let text = if stream_id.is_none() && text.len() > MAX_TEXT_LENGTH {
            crate::text::truncate_with_suffix(text, MAX_TEXT_LENGTH, "…\n\n(truncated)")
        } else {
            text.to_string()
        };

        self.out_tx
            .send(OutboundMessage {
                chat_id: chat_id.to_string(),
                text,
                attachment: None,
                reply_token: reply_token.map(String::from),
                stream_id: stream_id.map(String::from),
                feedback_id: feedback_id.map(String::from),
                stream_finish: finish,
            })
            .await
            .map_err(|e| format!("outbound channel send failed: {e}"))
    }

    async fn send_attachment(
        &self,
        chat_id: &str,
        attachment: &OutboundAttachment,
        reply_token: Option<&str>,
    ) -> Result<(), String> {
        self.out_tx
            .send(OutboundMessage {
                chat_id: chat_id.to_string(),
                text: String::new(),
                attachment: Some(attachment.clone()),
                reply_token: reply_token.map(String::from),
                stream_id: None,
                feedback_id: None,
                stream_finish: true,
            })
            .await
            .map_err(|e| format!("outbound channel send failed: {e}"))
    }

    async fn recv(&self) -> Option<InboundMessage> {
        self.msg_rx.lock().await.recv().await
    }
}

#[cfg(test)]
mod truncation_tests {
    use super::*;

    #[test]
    fn outbound_truncation_preserves_utf8() {
        let long = "企业微信".repeat(1200);
        let truncated =
            crate::text::truncate_with_suffix(&long, MAX_TEXT_LENGTH, "…\n\n(truncated)");
        assert!(truncated.len() <= MAX_TEXT_LENGTH);
        assert!(truncated.ends_with("(truncated)"));
    }
}

async fn run_wecom_connection(
    config: &WeComConfig,
    msg_tx: &mpsc::Sender<InboundMessage>,
    out_rx: std::sync::Arc<tokio::sync::Mutex<mpsc::Receiver<OutboundMessage>>>,
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(&config.websocket_url).await?;
    let (mut ws_write, mut ws_read) = ws_stream.split();
    emit_adapter_health(AdapterHealthEvent::new(
        "wecom",
        AdapterHealthEventType::Connected,
        None,
    ));

    // Subscribe (WeCom AI Bot uses bot_id + secret in body, no signature)
    let subscribe_msg = json!({
        "cmd": "aibot_subscribe",
        "headers": {"req_id": format!("subscribe-{}", uuid::Uuid::new_v4())},
        "body": {
            "bot_id": &config.bot_id,
            "secret": &config.secret,
            "device_id": uuid::Uuid::new_v4().to_string().replace("-", ""),
        }
    });
    ws_write
        .send(Message::Text(subscribe_msg.to_string().into()))
        .await?;
    tracing::info!("wecom subscribe sent");

    let mut dedup = MessageDeduplicator::new();
    let mut heartbeat =
        tokio::time::interval(std::time::Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    heartbeat.tick().await;
    let bot_id = config.bot_id.clone();
    let mut last_recv = tokio::time::Instant::now();

    loop {
        if last_recv.elapsed().as_secs() > PONG_TIMEOUT_SECS {
            return Err("wecom connection timed out (no message received)".into());
        }

        let mut out_guard = out_rx.lock().await;

        tokio::select! {
            out = out_guard.recv() => {
                let Some(out) = out else {
                    emit_adapter_health(AdapterHealthEvent::new(
                        "wecom",
                        AdapterHealthEventType::Disconnected,
                        Some("outbound channel closed".to_string()),
                    ));
                    return Err("wecom outbound channel closed".into());
                };
                match send_wecom_outbound(&bot_id, out, &mut ws_write, &mut ws_read, msg_tx, &mut dedup, &mut last_recv).await {
                    Ok(detail) => {
                        emit_adapter_health(AdapterHealthEvent::new(
                            "wecom",
                            AdapterHealthEventType::SendAck,
                            Some(detail),
                        ));
                    }
                    Err(e) => {
                        emit_adapter_health(AdapterHealthEvent::new(
                            "wecom",
                            AdapterHealthEventType::SendError,
                            Some(e.to_string()),
                        ));
                        tracing::error!(error = %e, "wecom outbound send failed");
                        return Err(format!("wecom outbound send failed: {e}").into());
                    }
                }
            }
            _ = heartbeat.tick() => {
                let ping = json!({
                    "cmd": "ping",
                    "headers": {"req_id": format!("ping-{}", uuid::Uuid::new_v4())},
                    "body": {}
                });
                ws_write.send(Message::Text(ping.to_string().into())).await?;
            }
            msg = ws_read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_recv = tokio::time::Instant::now();
                        if let Ok(data) = serde_json::from_str::<Value>(&text) {
                            handle_wecom_message(&data, msg_tx, &mut dedup).await;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        emit_adapter_health(AdapterHealthEvent::new(
                            "wecom",
                            AdapterHealthEventType::Disconnected,
                            Some("websocket closed".to_string()),
                        ));
                        return Err("wecom websocket closed".into());
                    }
                    Some(Err(e)) => {
                        emit_adapter_health(AdapterHealthEvent::new(
                            "wecom",
                            AdapterHealthEventType::Disconnected,
                            Some(e.to_string()),
                        ));
                        return Err(format!("wecom ws error: {e}").into());
                    }
                    _ => {}
                }
            }
            _ = shutdown.recv() => {
                emit_adapter_health(AdapterHealthEvent::new(
                    "wecom",
                    AdapterHealthEventType::Shutdown,
                    None,
                ));
                let _ = ws_write.close().await;
                return Ok(());
            }
        }
    }
}

async fn send_wecom_outbound(
    bot_id: &str,
    mut out: OutboundMessage,
    ws_write: &mut WeComWsWrite,
    ws_read: &mut WeComWsRead,
    msg_tx: &mpsc::Sender<InboundMessage>,
    dedup: &mut MessageDeduplicator,
    last_recv: &mut tokio::time::Instant,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(attachment) = out.attachment.as_mut()
        && attachment.media_id.is_none()
    {
        let media_id =
            upload_wecom_media(attachment, ws_write, ws_read, msg_tx, dedup, last_recv).await?;
        attachment.media_id = Some(media_id);
    }

    let frame = build_send_frame(bot_id, &out);
    tracing::debug!(
        cmd = frame["cmd"].as_str().unwrap_or("?"),
        finish = %frame["body"]["stream"]["finish"],
        feedback = frame["body"]["stream"]["feedback"]["id"].as_str().is_some()
            || frame["body"]["feedback"]["id"].as_str().is_some(),
        content_len = out.text.len(),
        attachment = out.attachment.is_some(),
        "wecom outbound frame"
    );
    ws_write
        .send(Message::Text(frame.to_string().into()))
        .await?;
    Ok(out.chat_id)
}

async fn upload_wecom_media(
    attachment: &OutboundAttachment,
    ws_write: &mut WeComWsWrite,
    ws_read: &mut WeComWsRead,
    msg_tx: &mpsc::Sender<InboundMessage>,
    dedup: &mut MessageDeduplicator,
    last_recv: &mut tokio::time::Instant,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let path = attachment
        .required_local_path("wecom")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let bytes = tokio::fs::read(path).await?;
    let filename = attachment.filename_or_default(path, "attachment");
    let filename = crate::inbound_attachments::ensure_extension(
        &filename,
        attachment.mime_type.as_deref(),
        attachment.kind,
        &bytes,
    );
    let spec = WeComAttachmentSpec::for_kind(attachment.kind);
    let total_chunks = bytes.len().div_ceil(MEDIA_UPLOAD_CHUNK_BYTES);
    if total_chunks == 0 {
        return Err("cannot upload empty attachment".into());
    }
    if total_chunks > MEDIA_UPLOAD_MAX_CHUNKS {
        return Err(format!("attachment too large for WeCom upload: {total_chunks} chunks").into());
    }
    let md5 = hex::encode(Md5::digest(&bytes));
    tracing::info!(
        media_type = spec.name,
        filename,
        bytes = bytes.len(),
        chunks = total_chunks,
        "uploading wecom media"
    );

    let init_req_id = format!("aibot_upload_media_init-{}", uuid::Uuid::new_v4());
    let init = json!({
        "cmd": "aibot_upload_media_init",
        "headers": {"req_id": init_req_id},
        "body": {
            "type": spec.name,
            "filename": &filename,
            "total_size": bytes.len(),
            "total_chunks": total_chunks,
            "md5": md5,
        }
    });
    let init_reply = send_frame_and_wait(
        ws_write,
        ws_read,
        init,
        &init_req_id,
        msg_tx,
        dedup,
        last_recv,
    )
    .await?;
    let upload_id = init_reply["upload_id"]
        .as_str()
        .ok_or_else(|| format!("upload init missing upload_id: {init_reply}"))?
        .to_string();

    for (chunk_index, chunk) in bytes.chunks(MEDIA_UPLOAD_CHUNK_BYTES).enumerate() {
        let req_id = format!("aibot_upload_media_chunk-{}", uuid::Uuid::new_v4());
        let frame = json!({
            "cmd": "aibot_upload_media_chunk",
            "headers": {"req_id": req_id},
            "body": {
                "upload_id": upload_id,
                "chunk_index": chunk_index,
                "base64_data": base64::engine::general_purpose::STANDARD.encode(chunk),
            }
        });
        let _ = send_frame_and_wait(ws_write, ws_read, frame, &req_id, msg_tx, dedup, last_recv)
            .await?;
    }

    let finish_req_id = format!("aibot_upload_media_finish-{}", uuid::Uuid::new_v4());
    let finish = json!({
        "cmd": "aibot_upload_media_finish",
        "headers": {"req_id": finish_req_id},
        "body": {"upload_id": upload_id}
    });
    let finish_reply = send_frame_and_wait(
        ws_write,
        ws_read,
        finish,
        &finish_req_id,
        msg_tx,
        dedup,
        last_recv,
    )
    .await?;
    let media_id = finish_reply["media_id"]
        .as_str()
        .ok_or_else(|| format!("upload finish missing media_id: {finish_reply}"))?
        .to_string();
    tracing::info!(media_type = spec.name, "wecom media upload complete");
    Ok(media_id)
}

async fn send_frame_and_wait(
    ws_write: &mut WeComWsWrite,
    ws_read: &mut WeComWsRead,
    frame: Value,
    target_req_id: &str,
    msg_tx: &mpsc::Sender<InboundMessage>,
    dedup: &mut MessageDeduplicator,
    last_recv: &mut tokio::time::Instant,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let cmd = frame["cmd"].as_str().unwrap_or("?").to_string();
    ws_write
        .send(Message::Text(frame.to_string().into()))
        .await?;

    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                return Err(format!("wecom {cmd} timed out waiting for req_id {target_req_id}").into());
            }
            msg = ws_read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        *last_recv = tokio::time::Instant::now();
                        let Ok(data) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        let req_id = data["headers"]["req_id"].as_str().unwrap_or("");
                        if req_id == target_req_id {
                            let body = data["body"].clone();
                            let errcode = body["errcode"]
                                .as_i64()
                                .or_else(|| data["errcode"].as_i64())
                                .unwrap_or(0);
                            if errcode != 0 {
                                let errmsg = body["errmsg"]
                                    .as_str()
                                    .or_else(|| data["errmsg"].as_str())
                                    .unwrap_or("unknown");
                                return Err(format!("wecom {cmd} failed: {errcode}: {errmsg}").into());
                            }
                            return Ok(body);
                        }
                        handle_wecom_message(&data, msg_tx, dedup).await;
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Err("wecom websocket closed while waiting for upload ack".into());
                    }
                    Some(Err(e)) => return Err(format!("wecom ws error while waiting for upload ack: {e}").into()),
                    _ => {}
                }
            }
        }
    }
}

async fn wait_reconnect_delay(
    delay: std::time::Duration,
    shutdown: &mut tokio::sync::broadcast::Receiver<()>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        _ = shutdown.recv() => true,
    }
}

fn build_send_frame(bot_id: &str, out: &OutboundMessage) -> Value {
    if let Some(attachment) = out.attachment.as_ref() {
        return build_attachment_send_frame(bot_id, out, attachment);
    }
    if let (Some(req_id), Some(stream_id)) = (&out.reply_token, &out.stream_id) {
        // Streaming reply (full-replacement semantics)
        let mut stream = json!({
            "id": stream_id,
            "finish": out.stream_finish,
            "content": &out.text
        });
        if let Some(feedback_id) = out.feedback_id.as_deref() {
            stream["feedback"] = json!({ "id": feedback_id });
        }
        json!({
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": req_id},
            "body": {
                "msgtype": "stream",
                "stream": stream
            }
        })
    } else if let Some(ref req_id) = out.reply_token {
        // Non-stream reply (one-shot markdown via respond)
        let mut frame = json!({
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": req_id},
            "body": {
                "msgtype": "markdown",
                "markdown": {"content": &out.text}
            }
        });
        attach_body_feedback(&mut frame, out.feedback_id.as_deref());
        frame
    } else if !out.chat_id.is_empty() {
        // Proactive send (DM or group without req_id)
        let mut frame = json!({
            "cmd": "aibot_send_msg",
            "headers": {"req_id": format!("send-{}", uuid::Uuid::new_v4())},
            "body": {
                "bot_id": bot_id,
                "chatid": &out.chat_id,
                "msgtype": "markdown",
                "markdown": {"content": &out.text}
            }
        });
        attach_body_feedback(&mut frame, out.feedback_id.as_deref());
        frame
    } else {
        tracing::warn!(
            text_len = out.text.len(),
            "wecom: dropping message, no chat_id or reply_token"
        );
        json!({"cmd": "noop"})
    }
}

fn build_attachment_send_frame(
    bot_id: &str,
    out: &OutboundMessage,
    attachment: &OutboundAttachment,
) -> Value {
    let Some(media_id) = attachment.media_id.as_deref() else {
        tracing::warn!("wecom: dropping attachment send without media_id");
        return json!({"cmd": "noop"});
    };
    let spec = WeComAttachmentSpec::for_kind(attachment.kind);

    let mut body = json!({
        "msgtype": spec.name,
        spec.name: {"media_id": media_id}
    });
    if let AttachmentKind::Video = attachment.kind
        && let Some(name) = attachment.name.as_deref()
    {
        body[spec.name]["title"] = json!(name);
    }

    if let Some(ref req_id) = out.reply_token {
        json!({
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": req_id},
            "body": body
        })
    } else if !out.chat_id.is_empty() {
        body["bot_id"] = json!(bot_id);
        body["chatid"] = json!(&out.chat_id);
        json!({
            "cmd": "aibot_send_msg",
            "headers": {"req_id": format!("send-{}", uuid::Uuid::new_v4())},
            "body": body
        })
    } else {
        tracing::warn!("wecom: dropping attachment, no chat_id or reply_token");
        json!({"cmd": "noop"})
    }
}

#[derive(Clone, Copy)]
struct WeComAttachmentSpec {
    name: &'static str,
}

impl WeComAttachmentSpec {
    const fn for_kind(kind: AttachmentKind) -> Self {
        match kind {
            AttachmentKind::Image => Self { name: "image" },
            AttachmentKind::File | AttachmentKind::Unknown => Self { name: "file" },
            AttachmentKind::Video => Self { name: "video" },
            AttachmentKind::Audio => Self { name: "voice" },
        }
    }
}

fn attach_body_feedback(frame: &mut Value, feedback_id: Option<&str>) {
    if let Some(feedback_id) = feedback_id {
        frame["body"]["feedback"] = json!({ "id": feedback_id });
    }
}

async fn handle_wecom_message(
    data: &Value,
    msg_tx: &mpsc::Sender<InboundMessage>,
    dedup: &mut MessageDeduplicator,
) {
    let cmd = data["cmd"].as_str().unwrap_or("");
    let body = &data["body"];
    tracing::trace!(raw = %data, "wecom inbound raw");

    if let Some(feedback) = parse_feedback_event(body) {
        let msg_id = body["msgid"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| feedback.feedback_id.clone());
        let dedup_key = format!(
            "feedback:{}:{}:{}",
            msg_id, feedback.feedback_id, feedback.feedback_type
        );
        if !dedup.check(&dedup_key) {
            tracing::debug!(
                feedback_id = %feedback.feedback_id,
                feedback_type = feedback.feedback_type,
                "wecom duplicate feedback event skipped"
            );
            return;
        }

        let user_id = body["from"]["userid"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
        let chat_id = body["chatid"]
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(&user_id)
            .to_string();
        let chat_type = if body["chattype"].as_str() == Some("group") {
            ChatType::Group
        } else {
            ChatType::DirectMessage
        };
        tracing::info!(
            feedback_id = %feedback.feedback_id,
            feedback_type = feedback.feedback_type,
            user = %user_id,
            chat_id = %chat_id,
            "wecom feedback event received"
        );
        let msg = InboundMessage {
            platform: "wecom",
            chat_id,
            user_id,
            text: String::new(),
            attachments: Vec::new(),
            msg_id,
            chat_type,
            reply_token: data["headers"]["req_id"].as_str().map(String::from),
            route_override: None,
            feedback: Some(feedback),
        };
        if msg_tx.send(msg).await.is_err() {
            tracing::warn!("wecom message channel closed, dropping feedback event");
        }
        return;
    }

    if let Some(eventtype) = body["event"]["eventtype"].as_str() {
        tracing::info!(eventtype, cmd, "wecom event callback ignored");
    }

    if cmd != "aibot_msg_callback" && cmd != "aibot_callback" {
        if let Some((event_type, detail)) = classify_wecom_control_message(data) {
            emit_adapter_health(AdapterHealthEvent::new("wecom", event_type, detail));
        }
        return;
    }

    let msg_id = body["msgid"].as_str().unwrap_or("").to_string();
    if msg_id.is_empty() || !dedup.check(&msg_id) {
        return;
    }

    let text = extract_wecom_text(body);
    let attachments = extract_wecom_attachments(body);

    if text.is_empty() && attachments.is_empty() {
        tracing::debug!(
            msg_id,
            msgtype = body["msgtype"].as_str(),
            keys = ?body.as_object().map(|object| object.keys().cloned().collect::<Vec<_>>()),
            "wecom callback ignored because it contained no text or attachment"
        );
        return;
    }

    let user_id = body["from"]["userid"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    // DM messages don't carry chatid; fall back to userid so chat_id is always non-empty
    let chat_id = body["chatid"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or(&user_id)
        .to_string();
    let chat_type = if body["chattype"].as_str() == Some("group") {
        ChatType::Group
    } else {
        ChatType::DirectMessage
    };
    let reply_token = data["headers"]["req_id"].as_str().map(String::from);

    let msg = InboundMessage {
        platform: "wecom",
        chat_id,
        user_id,
        text,
        attachments,
        msg_id,
        chat_type,
        reply_token,
        route_override: None,
        feedback: None,
    };

    if msg_tx.send(msg).await.is_err() {
        tracing::warn!("wecom message channel full, dropping message");
    }
}

fn extract_wecom_text(body: &Value) -> String {
    let mut parts = Vec::new();

    if let Some(text) = body["text"]["content"].as_str() {
        push_nonempty_text(&mut parts, text);
    }
    if let Some(text) = body["voice"]["content"].as_str() {
        push_nonempty_text(&mut parts, text);
    }

    if let Some(items) = wecom_mixed_items(body) {
        for item in items {
            if let Some(text) = wecom_mixed_item_text(item) {
                push_nonempty_text(&mut parts, &text);
                continue;
            }
            if wecom_mixed_kind(item) == Some(WeComMixedKind::Voice)
                && let Some(section) = first_object_section(item, &["voice_item", "voice", "audio"])
                && let Some(text) = first_text_string(section)
            {
                push_nonempty_text(&mut parts, &format!("voice: {}", text.trim()));
            }
        }
    }

    parts.join("\n")
}

fn push_nonempty_text(parts: &mut Vec<String>, text: &str) {
    let text = text.trim();
    if !text.is_empty() {
        parts.push(text.to_string());
    }
}

fn extract_wecom_attachments(body: &Value) -> Vec<InboundAttachment> {
    let mut attachments = Vec::new();
    for (key, kind) in [
        ("image", AttachmentKind::Image),
        ("file", AttachmentKind::File),
        ("video", AttachmentKind::Video),
        ("voice", AttachmentKind::Audio),
    ] {
        let Some(section) = body.get(key).filter(|value| value.is_object()) else {
            continue;
        };
        let url = first_string(section, &url_keys());
        if is_transcribed_voice_without_download(kind, section, url.as_deref(), &["content"]) {
            continue;
        }
        let attachment = InboundAttachment {
            kind,
            name: first_string(section, ATTACHMENT_NAME_KEYS)
                .or_else(|| crate::inbound_attachments::infer_name_from_url(url.as_deref())),
            media_id: first_string(section, ATTACHMENT_MEDIA_ID_KEYS),
            url,
            local_path: None,
            mime_type: first_string(section, ATTACHMENT_MIME_KEYS),
            size_bytes: first_u64(section, ATTACHMENT_SIZE_KEYS),
            raw: section.clone(),
        };
        if attachment.media_id.is_some()
            || attachment.url.is_some()
            || attachment.name.is_some()
            || kind != AttachmentKind::Audio
        {
            attachments.push(attachment);
        }
    }
    if let Some(items) = wecom_mixed_items(body) {
        for item in items {
            if let Some(attachment) = extract_wecom_item_attachment(item) {
                attachments.push(attachment);
            }
        }
    }
    dedupe_inbound_attachments(&mut attachments);
    attachments
}

fn wecom_mixed_items(body: &Value) -> Option<&Vec<Value>> {
    body["item_list"]
        .as_array()
        .or_else(|| body["mixed"].as_array())
        .or_else(|| body["mixed"]["item_list"].as_array())
        .or_else(|| body["mixed"]["items"].as_array())
        .or_else(|| body["mixed"]["itemlist"].as_array())
        .or_else(|| body["mixed"]["item"].as_array())
        .or_else(|| body["mixed"]["msg_item"].as_array())
        .or_else(|| body["mixed"]["message_items"].as_array())
}

fn wecom_mixed_item_text(item: &Value) -> Option<String> {
    (wecom_mixed_kind(item) == Some(WeComMixedKind::Text))
        .then(|| {
            first_object_section(item, &["text_item", "text", "content_item"])
                .and_then(first_text_string)
                .or_else(|| first_text_string(item))
        })
        .flatten()
}

fn extract_wecom_item_attachment(item: &Value) -> Option<InboundAttachment> {
    let (kind, item_keys, name_fallbacks) = wecom_item_attachment_spec(item)?;
    let section = first_object_section(item, item_keys).unwrap_or(item);
    let url =
        first_string(&section["media"], &url_keys()).or_else(|| first_string(section, &url_keys()));
    if is_transcribed_voice_without_download(kind, section, url.as_deref(), &["text"]) {
        return None;
    }
    let attachment = attachment_from_wecom_item(item, kind, section, name_fallbacks);
    if inbound_attachment_has_identity(&attachment) {
        Some(attachment)
    } else {
        None
    }
}

fn wecom_item_attachment_spec(
    item: &Value,
) -> Option<(
    AttachmentKind,
    &'static [&'static str],
    &'static [&'static str],
)> {
    if wecom_mixed_kind(item) == Some(WeComMixedKind::Image)
        || first_object_section(item, &["image_item", "image", "pic"]).is_some()
    {
        return Some((
            AttachmentKind::Image,
            &["image_item", "image", "pic"],
            &["image"],
        ));
    }
    if wecom_mixed_kind(item) == Some(WeComMixedKind::File)
        || first_object_section(item, &["file_item", "file", "document"]).is_some()
    {
        return Some((
            AttachmentKind::File,
            &["file_item", "file", "document"],
            &["file"],
        ));
    }
    if wecom_mixed_kind(item) == Some(WeComMixedKind::Video)
        || first_object_section(item, &["video_item", "video"]).is_some()
    {
        return Some((AttachmentKind::Video, &["video_item", "video"], &["video"]));
    }
    if wecom_mixed_kind(item) == Some(WeComMixedKind::Voice)
        || first_object_section(item, &["voice_item", "voice", "audio"]).is_some()
    {
        return Some((
            AttachmentKind::Audio,
            &["voice_item", "voice", "audio"],
            &["voice"],
        ));
    }
    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WeComMixedKind {
    Text,
    Image,
    Voice,
    File,
    Video,
}

fn wecom_mixed_kind(item: &Value) -> Option<WeComMixedKind> {
    for key in ["type", "msgtype", "msg_type"] {
        if let Some(code) = item[key].as_i64() {
            return match code {
                1 => Some(WeComMixedKind::Text),
                2 => Some(WeComMixedKind::Image),
                3 => Some(WeComMixedKind::Voice),
                4 => Some(WeComMixedKind::File),
                5 => Some(WeComMixedKind::Video),
                _ => None,
            };
        }
        let Some(value) = item[key].as_str() else {
            continue;
        };
        let value = value.trim().to_ascii_lowercase();
        if let Ok(code) = value.parse::<i64>() {
            return match code {
                1 => Some(WeComMixedKind::Text),
                2 => Some(WeComMixedKind::Image),
                3 => Some(WeComMixedKind::Voice),
                4 => Some(WeComMixedKind::File),
                5 => Some(WeComMixedKind::Video),
                _ => None,
            };
        }
        let kind = match value.as_str() {
            "text" => Some(WeComMixedKind::Text),
            "image" | "pic" | "picture" => Some(WeComMixedKind::Image),
            "voice" | "audio" => Some(WeComMixedKind::Voice),
            "file" | "document" | "doc" => Some(WeComMixedKind::File),
            "video" => Some(WeComMixedKind::Video),
            _ => None,
        };
        if kind.is_some() {
            return kind;
        }
        for (suffix, kind) in [
            ("text", WeComMixedKind::Text),
            ("image", WeComMixedKind::Image),
            ("pic", WeComMixedKind::Image),
            ("picture", WeComMixedKind::Image),
            ("voice", WeComMixedKind::Voice),
            ("audio", WeComMixedKind::Voice),
            ("file", WeComMixedKind::File),
            ("document", WeComMixedKind::File),
            ("doc", WeComMixedKind::File),
            ("video", WeComMixedKind::Video),
        ] {
            if value.ends_with(suffix) {
                return Some(kind);
            }
        }
    }
    None
}

fn first_object_section<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .find_map(|key| value.get(*key).filter(|section| section.is_object()))
}

fn first_text_string(value: &Value) -> Option<String> {
    if let Some(text) = value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(text.to_string());
    }
    first_string(
        value,
        &[
            "text",
            "content",
            "plain_text",
            "msg",
            "message",
            "caption",
            "description",
        ],
    )
    .or_else(|| first_object_section(value, &["text", "content"]).and_then(first_text_string))
}

fn is_transcribed_voice_without_download(
    kind: AttachmentKind,
    section: &Value,
    url: Option<&str>,
    text_keys: &[&str],
) -> bool {
    kind == AttachmentKind::Audio && url.is_none() && first_string(section, text_keys).is_some()
}

fn inbound_attachment_has_identity(attachment: &InboundAttachment) -> bool {
    attachment.media_id.is_some()
        || attachment.url.is_some()
        || attachment.name.is_some()
        || attachment.mime_type.is_some()
}

fn dedupe_inbound_attachments(attachments: &mut Vec<InboundAttachment>) {
    let mut seen = std::collections::HashSet::new();
    attachments.retain(|attachment| {
        let key = attachment
            .media_id
            .as_ref()
            .map(|value| format!("media:{value}"))
            .or_else(|| attachment.url.as_ref().map(|value| format!("url:{value}")));
        match key {
            Some(key) => seen.insert(key),
            None => true,
        }
    });
}

fn attachment_from_wecom_item(
    item: &Value,
    kind: AttachmentKind,
    section: &Value,
    name_fallbacks: &[&str],
) -> InboundAttachment {
    let media = &section["media"];
    let url = first_string(media, &url_keys()).or_else(|| first_string(section, &url_keys()));
    let name = first_string(section, ATTACHMENT_NAME_KEYS)
        .or_else(|| first_string(item, name_fallbacks))
        .or_else(|| crate::inbound_attachments::infer_name_from_url(url.as_deref()));
    InboundAttachment {
        kind,
        name,
        media_id: first_string(media, ATTACHMENT_MEDIA_ID_KEYS)
            .or_else(|| first_string(section, ATTACHMENT_MEDIA_ID_KEYS)),
        url,
        local_path: None,
        mime_type: first_string(media, ATTACHMENT_MIME_KEYS)
            .or_else(|| first_string(section, ATTACHMENT_MIME_KEYS)),
        size_bytes: first_u64(media, ATTACHMENT_SIZE_KEYS)
            .or_else(|| first_u64(section, ATTACHMENT_SIZE_KEYS)),
        raw: item.clone(),
    }
}

pub(crate) async fn prepare_inbound_attachments(
    attachments: &mut Vec<InboundAttachment>,
    msg_id: &str,
) -> Option<String> {
    let mut dir_guard =
        crate::inbound_attachments::PrepareDirGuard::new(crate::inbound_attachments::dir(msg_id));
    let mut failures = Vec::new();
    for (idx, attachment) in attachments.iter().enumerate() {
        if attachment.local_path.is_none() && attachment.url.is_none() {
            failures.push(crate::inbound_attachments::missing_url_failure(
                attachment, idx,
            ));
        }
    }
    download_wecom_attachments(attachments, msg_id, &mut failures).await;
    let before = attachments.len();
    attachments.retain(|attachment| attachment.local_path.is_some());
    if !attachments.is_empty() {
        dir_guard.disarm();
    }
    crate::inbound_attachments::prepare_response(
        before - attachments.len(),
        attachments.is_empty(),
        &failures,
    )
}

async fn download_wecom_attachments(
    attachments: &mut [InboundAttachment],
    msg_id: &str,
    failures: &mut Vec<String>,
) {
    if attachments
        .iter()
        .all(|attachment| attachment.local_path.is_some() || attachment.url.is_none())
    {
        return;
    }

    let dir = crate::inbound_attachments::dir(msg_id);
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!(error = %e, "wecom attachment http client unavailable");
            failures.push("下载客户端不可用".into());
            return;
        }
    };

    for (idx, attachment) in attachments.iter_mut().enumerate() {
        if attachment.local_path.is_some() {
            continue;
        }
        let Some(url) = attachment.url.as_deref() else {
            continue;
        };
        if !is_safe_attachment_url(url) {
            failures.push(format!(
                "{} 链接地址不安全或协议不支持",
                crate::inbound_attachments::label(attachment, idx)
            ));
            continue;
        }

        let response = match client.get(url).send().await {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(url, error = %e, "wecom attachment download failed");
                failures.push(format!(
                    "{} 下载失败",
                    crate::inbound_attachments::label(attachment, idx)
                ));
                continue;
            }
        };
        if !response.status().is_success() {
            let status = response.status();
            tracing::warn!(
                url,
                status = %status,
                "wecom attachment download returned non-success status"
            );
            failures.push(format!(
                "{} 下载返回 HTTP {}",
                crate::inbound_attachments::label(attachment, idx),
                status.as_u16()
            ));
            continue;
        }
        if let Some(len) = response.content_length()
            && len > crate::inbound_attachments::MAX_INBOUND_ATTACHMENT_BYTES
        {
            tracing::warn!(
                url,
                content_length = len,
                max_bytes = crate::inbound_attachments::MAX_INBOUND_ATTACHMENT_BYTES,
                "wecom attachment too large"
            );
            failures.push(crate::inbound_attachments::too_large_failure(
                attachment, idx,
            ));
            continue;
        }
        let response_mime = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(normalize_content_type);

        let raw_bytes = match response.bytes().await {
            Ok(bytes)
                if bytes.len() as u64
                    <= crate::inbound_attachments::MAX_INBOUND_ATTACHMENT_BYTES =>
            {
                bytes.to_vec()
            }
            Ok(_) => {
                failures.push(crate::inbound_attachments::too_large_failure(
                    attachment, idx,
                ));
                continue;
            }
            Err(e) => {
                tracing::warn!(url, error = %e, "wecom attachment body read failed");
                failures.push(format!(
                    "{} 读取失败",
                    crate::inbound_attachments::label(attachment, idx)
                ));
                continue;
            }
        };

        if attachment.mime_type.is_none() {
            attachment.mime_type = response_mime;
        }
        let aes_key = find_string_deep(&attachment.raw, &["aeskey", "aes_key", "aesKey"]);
        let bytes = if let Some(aes_key) = aes_key.as_deref() {
            match decrypt_wecom_media(&raw_bytes, aes_key) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(url, error = %e, "wecom attachment decrypt failed");
                    failures.push(format!(
                        "{} 解密失败",
                        crate::inbound_attachments::label(attachment, idx)
                    ));
                    continue;
                }
            }
        } else {
            raw_bytes
        };
        if let Some(mime_type) = crate::inbound_attachments::detect_mime_from_bytes(
            &bytes,
            attachment.mime_type.as_deref(),
        ) {
            attachment.mime_type = Some(mime_type.to_string());
        }

        if let Err(e) = crate::inbound_attachments::ensure_private_dir(&dir).await {
            tracing::warn!(dir = %dir.display(), error = %e, "wecom attachment dir create failed");
            failures.push("附件目录创建失败".into());
            return;
        }
        let filename = crate::inbound_attachments::filename(attachment, idx, &bytes);
        let path = dir.join(filename);
        if let Err(e) = crate::inbound_attachments::write_private_file(&path, &bytes).await {
            tracing::warn!(path = %path.display(), error = %e, "wecom attachment write failed");
            failures.push(format!(
                "{} 写入失败",
                crate::inbound_attachments::label(attachment, idx)
            ));
            continue;
        }
        attachment.size_bytes.get_or_insert(bytes.len() as u64);
        attachment.local_path = Some(path.to_string_lossy().to_string());
    }
}

fn is_safe_attachment_url(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    if parsed.host_str().is_none() {
        return false;
    }
    #[cfg(not(test))]
    {
        let Some(host) = parsed.host_str() else {
            return false;
        };
        if host.eq_ignore_ascii_case("localhost") {
            return false;
        }
        if let Ok(ip) = host.parse::<IpAddr>()
            && is_private_attachment_ip(ip)
        {
            return false;
        }
    }
    true
}

#[cfg(not(test))]
fn is_private_attachment_ip(ip: IpAddr) -> bool {
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || match ip {
            IpAddr::V4(ip) => ip.is_private() || ip.is_link_local(),
            IpAddr::V6(ip) => ip.is_unique_local() || ip.is_unicast_link_local(),
        }
}

fn decrypt_wecom_media(encrypted: &[u8], aes_key: &str) -> Result<Vec<u8>, String> {
    if encrypted.is_empty() {
        return Err("empty encrypted body".into());
    }
    if !encrypted.len().is_multiple_of(16) {
        return Err(format!(
            "encrypted body length {} is not AES block aligned",
            encrypted.len()
        ));
    }

    let key = decode_wecom_aes_key(aes_key)?;
    if key.len() != 32 {
        return Err(format!(
            "aeskey decoded to {} bytes, expected 32",
            key.len()
        ));
    }

    let cipher = Aes256::new_from_slice(&key).map_err(|e| format!("invalid aes key: {e}"))?;
    let mut prev = key[..16].to_vec();
    let mut out = Vec::with_capacity(encrypted.len());
    for chunk in encrypted.chunks_exact(16) {
        let mut block = aes::cipher::generic_array::GenericArray::clone_from_slice(chunk);
        cipher.decrypt_block(&mut block);
        for i in 0..16 {
            out.push(block[i] ^ prev[i]);
        }
        prev.copy_from_slice(chunk);
    }

    let pad_len = *out
        .last()
        .ok_or_else(|| "decrypted body is empty".to_string())? as usize;
    if pad_len == 0 || pad_len > 32 || pad_len > out.len() {
        return Err(format!("invalid PKCS#7 padding value: {pad_len}"));
    }
    if out[out.len() - pad_len..]
        .iter()
        .any(|byte| *byte as usize != pad_len)
    {
        return Err("invalid PKCS#7 padding bytes".into());
    }
    out.truncate(out.len() - pad_len);
    Ok(out)
}

fn decode_wecom_aes_key(aes_key: &str) -> Result<Vec<u8>, String> {
    for engine in [
        &base64::engine::general_purpose::STANDARD,
        &base64::engine::general_purpose::STANDARD_NO_PAD,
        &base64::engine::general_purpose::URL_SAFE,
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
    ] {
        if let Ok(key) = engine.decode(aes_key.trim()) {
            return Ok(key);
        }
    }

    let mut padded = aes_key.trim().to_string();
    let remainder = padded.len() % 4;
    if remainder != 0 {
        padded.extend(std::iter::repeat_n('=', 4 - remainder));
    }
    base64::engine::general_purpose::STANDARD
        .decode(&padded)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(&padded))
        .map_err(|e| format!("invalid base64 aeskey: {e}"))
}

fn first_string(value: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(s) = value.get(*key).and_then(Value::as_str) {
            let s = s.trim();
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}

fn find_string_deep(value: &Value, keys: &[&str]) -> Option<String> {
    if let Some(text) = first_string(value, keys) {
        return Some(text);
    }
    match value {
        Value::Array(items) => items.iter().find_map(|item| find_string_deep(item, keys)),
        Value::Object(object) => object
            .values()
            .find_map(|child| find_string_deep(child, keys)),
        _ => None,
    }
}

fn normalize_content_type(value: &str) -> Option<String> {
    let mime = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if mime.is_empty() || mime == "application/octet-stream" {
        None
    } else {
        Some(mime)
    }
}

fn first_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    for key in keys {
        if let Some(n) = value.get(*key).and_then(Value::as_u64) {
            return Some(n);
        }
        if let Some(s) = value.get(*key).and_then(Value::as_str)
            && let Ok(n) = s.trim().parse()
        {
            return Some(n);
        }
    }
    None
}

fn url_keys() -> [&'static str; 7] {
    [
        "url",
        "download_url",
        "file_url",
        "full_url",
        "pic_url",
        "picurl",
        "image_url",
    ]
}

fn parse_feedback_event(body: &Value) -> Option<FeedbackEvent> {
    let event = &body["event"];
    if event["eventtype"].as_str()? != "feedback_event" {
        return None;
    }
    let feedback = event
        .get("feedback_event")
        .or_else(|| event.get("feedback"))?;
    let feedback_id = feedback["id"].as_str()?.trim().to_string();
    if feedback_id.is_empty() {
        return None;
    }
    let inaccurate_reason_list = feedback["inaccurate_reason_list"]
        .as_array()
        .map(|items| items.iter().filter_map(Value::as_i64).collect())
        .unwrap_or_default();
    Some(FeedbackEvent {
        feedback_id,
        feedback_type: feedback["type"].as_i64().unwrap_or(0),
        content: feedback["content"].as_str().map(str::to_string),
        inaccurate_reason_list,
        raw: feedback.clone(),
    })
}

fn classify_wecom_control_message(
    data: &Value,
) -> Option<(AdapterHealthEventType, Option<String>)> {
    let cmd = data["cmd"].as_str().unwrap_or("");
    match cmd {
        "aibot_subscribe" => {
            let errcode = data["body"]["errcode"].as_i64().unwrap_or(-1);
            if errcode == 0 {
                tracing::info!("wecom subscription confirmed");
                Some((AdapterHealthEventType::SubscribeAck, None))
            } else {
                tracing::error!(errcode, "wecom subscription failed");
                Some((
                    AdapterHealthEventType::SubscribeError,
                    Some(format!("errcode={errcode}")),
                ))
            }
        }
        "aibot_send_msg" | "aibot_respond_msg" => {
            let errcode = data["body"]["errcode"]
                .as_i64()
                .or_else(|| data["errcode"].as_i64())
                .unwrap_or(0);
            if errcode == 0 {
                Some((AdapterHealthEventType::SendAck, None))
            } else {
                let errmsg = data["body"]["errmsg"]
                    .as_str()
                    .or_else(|| data["errmsg"].as_str())
                    .unwrap_or("unknown");
                Some((
                    AdapterHealthEventType::SendError,
                    Some(format!("{errcode}: {errmsg}")),
                ))
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tokio::net::TcpListener;
    use tokio::sync::{broadcast, oneshot};
    use tokio::time::{Duration, timeout};

    #[test]
    fn parse_wecom_callback() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-123"},
            "body": {
                "msgid": "msg-001",
                "msgtype": "text",
                "from": {"userid": "user-1"},
                "chatid": "chat-1",
                "chattype": "single",
                "text": {"content": "hello world"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.platform, "wecom");
            assert_eq!(msg.chat_id, "chat-1");
            assert_eq!(msg.user_id, "user-1");
            assert_eq!(msg.text, "hello world");
            assert!(msg.attachments.is_empty());
            assert_eq!(msg.chat_type, ChatType::DirectMessage);
            assert_eq!(msg.reply_token, Some("req-123".to_string()));
        });
    }

    #[test]
    fn parse_image_attachment_message() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-image"},
            "body": {
                "msgid": "image-1",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "image",
                "image": {"media_id": "media-image-1", "filename": "shot.png", "mime_type": "image/png"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert!(msg.text.is_empty());
            assert_eq!(msg.attachments.len(), 1);
            let attachment = &msg.attachments[0];
            assert_eq!(attachment.kind, AttachmentKind::Image);
            assert_eq!(attachment.media_id.as_deref(), Some("media-image-1"));
            assert_eq!(attachment.name.as_deref(), Some("shot.png"));
            assert_eq!(attachment.mime_type.as_deref(), Some("image/png"));
        });
    }

    #[test]
    fn parse_file_attachment_with_text() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-file"},
            "body": {
                "msgid": "file-1",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "file",
                "text": {"content": "please read this"},
                "file": {"fileid": "file-media-1", "file_name": "report.pdf", "file_size": "1024"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "please read this");
            assert_eq!(msg.attachments.len(), 1);
            let attachment = &msg.attachments[0];
            assert_eq!(attachment.kind, AttachmentKind::File);
            assert_eq!(attachment.media_id.as_deref(), Some("file-media-1"));
            assert_eq!(attachment.name.as_deref(), Some("report.pdf"));
            assert_eq!(attachment.size_bytes, Some(1024));
        });
    }

    #[test]
    fn parse_mixed_text_and_image_items() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-mixed"},
            "body": {
                "msgid": "mixed-1",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "mixed",
                "mixed": {
                    "item_list": [
                        {"type": 1, "text_item": {"text": "看一下这张图"}},
                        {"type": 2, "image_item": {
                            "media": {
                                "full_url": "https://cdn.example.com/shot.png",
                                "media_id": "media-image-2",
                                "mime_type": "image/png",
                                "size": 2048
                            }
                        }}
                    ]
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "看一下这张图");
            assert_eq!(msg.attachments.len(), 1);
            let attachment = &msg.attachments[0];
            assert_eq!(attachment.kind, AttachmentKind::Image);
            assert_eq!(attachment.media_id.as_deref(), Some("media-image-2"));
            assert_eq!(
                attachment.url.as_deref(),
                Some("https://cdn.example.com/shot.png")
            );
            assert_eq!(attachment.mime_type.as_deref(), Some("image/png"));
            assert_eq!(attachment.size_bytes, Some(2048));
        });
    }

    #[test]
    fn mixed_empty_image_item_is_ignored() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-empty-image"},
            "body": {
                "msgid": "mixed-empty-image",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "mixed",
                "mixed": {
                    "items": [
                        {"type": 1, "text_item": {"text": "只有文本有效"}},
                        {"type": 2, "image_item": {"media": {}}}
                    ]
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "只有文本有效");
            assert!(msg.attachments.is_empty());
        });
    }

    #[test]
    fn parse_mixed_array_with_string_typed_items() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-mixed-array"},
            "body": {
                "msgid": "mixed-array",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "mixed",
                "mixed": [
                    {"type": "text", "text": {"content": "看一下这个文件"}},
                    {"type": "file", "file": {
                        "media": {
                            "url": "https://cdn.example.com/report.html",
                            "media_id": "media-file-2",
                            "mime_type": "text/html",
                            "size": "4096"
                        },
                        "file_name": "report.html"
                    }}
                ]
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "看一下这个文件");
            assert_eq!(msg.attachments.len(), 1);
            let attachment = &msg.attachments[0];
            assert_eq!(attachment.kind, AttachmentKind::File);
            assert_eq!(attachment.name.as_deref(), Some("report.html"));
            assert_eq!(attachment.media_id.as_deref(), Some("media-file-2"));
            assert_eq!(attachment.mime_type.as_deref(), Some("text/html"));
            assert_eq!(attachment.size_bytes, Some(4096));
        });
    }

    #[test]
    fn parse_mixed_text_from_msgtype_and_direct_string_field() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-mixed-msgtype"},
            "body": {
                "msgid": "mixed-msgtype",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "mixed",
                "mixed": {
                    "items": [
                        {"msgtype": "text", "text": "这张图里有什么？"},
                        {"msgtype": "image", "image": {
                            "media": {
                                "url": "https://cdn.example.com/shot.png",
                                "media_id": "media-image-3",
                                "mime_type": "image/png"
                            }
                        }}
                    ]
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "这张图里有什么？");
            assert_eq!(msg.attachments.len(), 1);
            assert_eq!(msg.attachments[0].kind, AttachmentKind::Image);
        });
    }

    #[test]
    fn parse_mixed_item_container_and_prefixed_msgtype() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-mixed-prefixed"},
            "body": {
                "msgid": "mixed-prefixed",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "msgtype": "mixed",
                "mixed": {
                    "item": [
                        {"msgtype": "mixed_text", "text": {"content": "这是什么菜"}},
                        {"msgtype": "mixed_image", "image": {
                            "media": {
                                "url": "https://cdn.example.com/dish.jpg",
                                "media_id": "media-dish",
                                "mime_type": "image/jpeg"
                            }
                        }}
                    ]
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "这是什么菜");
            assert_eq!(msg.attachments.len(), 1);
            assert_eq!(msg.attachments[0].media_id.as_deref(), Some("media-dish"));
        });
    }

    #[test]
    fn attachment_filename_prefixes_index_to_avoid_collisions() {
        let attachment = InboundAttachment {
            kind: AttachmentKind::Image,
            name: Some("shot.png".into()),
            media_id: None,
            url: None,
            local_path: None,
            mime_type: Some("image/png".into()),
            size_bytes: None,
            raw: Value::Null,
        };

        assert_eq!(
            crate::inbound_attachments::filename(&attachment, 0, b"\x89PNG\r\n\x1a\n"),
            "1-shot.png"
        );
        assert_eq!(
            crate::inbound_attachments::filename(&attachment, 1, b"\x89PNG\r\n\x1a\n"),
            "2-shot.png"
        );
    }

    #[test]
    fn attachment_filename_replaces_untrusted_image_extension() {
        let attachment = InboundAttachment {
            kind: AttachmentKind::Image,
            name: Some("7655903806783167482.image".into()),
            media_id: None,
            url: None,
            local_path: None,
            mime_type: None,
            size_bytes: None,
            raw: Value::Null,
        };

        assert_eq!(
            crate::inbound_attachments::filename(&attachment, 0, b"\x89PNG\r\n\x1a\nrest"),
            "1-7655903806783167482.png"
        );
    }

    #[test]
    fn nested_wecom_aes_key_is_found() {
        let raw = json!({
            "image": {
                "media": {
                    "aeskey": "abc123"
                }
            }
        });

        assert_eq!(
            find_string_deep(&raw, &["aeskey", "aes_key", "aesKey"]).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn decrypt_wecom_media_uses_aes_256_cbc_and_pkcs7_padding() {
        use aes::cipher::BlockEncrypt;

        let key = [7u8; 32];
        let aes_key = base64::engine::general_purpose::STANDARD.encode(key);
        let plaintext = b"%PDF-test\n";
        let mut padded = plaintext.to_vec();
        let pad_len = 32 - (padded.len() % 32);
        padded.extend(std::iter::repeat_n(pad_len as u8, pad_len));

        let cipher = Aes256::new_from_slice(&key).unwrap();
        let mut prev = key[..16].to_vec();
        let mut encrypted = Vec::new();
        for chunk in padded.chunks_exact(16) {
            let mut block_bytes = [0u8; 16];
            for i in 0..16 {
                block_bytes[i] = chunk[i] ^ prev[i];
            }
            let mut block =
                aes::cipher::generic_array::GenericArray::clone_from_slice(&block_bytes);
            cipher.encrypt_block(&mut block);
            encrypted.extend_from_slice(&block);
            prev.copy_from_slice(&block);
        }

        assert_eq!(
            decrypt_wecom_media(&encrypted, &aes_key).unwrap(),
            plaintext
        );
        assert_eq!(
            decrypt_wecom_media(&encrypted, aes_key.trim_end_matches('=')).unwrap(),
            plaintext
        );
    }

    #[test]
    fn dedup_skips_duplicate_callback() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgid": "msg-dup",
                "from": {"userid": "u"},
                "chatid": "c",
                "chattype": "single",
                "text": {"content": "hi"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            handle_wecom_message(&data, &tx, &mut dedup).await;
            assert!(rx.recv().await.is_some());
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn empty_text_ignored() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "r"},
            "body": {
                "msgid": "m",
                "from": {"userid": "u"},
                "chatid": "c",
                "chattype": "single",
                "text": {"content": "  "}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn bare_group_mention_is_preserved_for_runner() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "r"},
            "body": {
                "msgid": "bare-mention",
                "from": {"userid": "u"},
                "chatid": "group-1",
                "chattype": "group",
                "text": {"content": "@BisectBot"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "@BisectBot");
            assert_eq!(msg.chat_type, ChatType::Group);
        });
    }

    #[test]
    fn build_send_frame_dm() {
        let out = OutboundMessage {
            chat_id: "chat-123".into(),
            text: "hello".into(),
            attachment: None,
            reply_token: None,
            stream_id: None,
            feedback_id: None,
            stream_finish: true,
        };
        let frame = build_send_frame("bot-1", &out);
        assert_eq!(frame["cmd"], "aibot_send_msg");
        assert_eq!(frame["body"]["chatid"], "chat-123");
        assert_eq!(frame["body"]["markdown"]["content"], "hello");
    }

    #[test]
    fn build_send_frame_group_respond() {
        let out = OutboundMessage {
            chat_id: "group-456".into(),
            text: "response".into(),
            attachment: None,
            reply_token: Some("req-original".into()),
            stream_id: Some("stream-1".into()),
            feedback_id: Some("feedback-1".into()),
            stream_finish: true,
        };
        let frame = build_send_frame("bot-1", &out);
        assert_eq!(frame["cmd"], "aibot_respond_msg");
        assert_eq!(frame["headers"]["req_id"], "req-original");
        assert_eq!(frame["body"]["stream"]["content"], "response");
        assert_eq!(frame["body"]["stream"]["finish"], true);
        assert_eq!(frame["body"]["stream"]["feedback"]["id"], "feedback-1");
    }

    #[test]
    fn build_send_frame_markdown_respond_with_feedback() {
        let out = OutboundMessage {
            chat_id: "chat-1".into(),
            text: "response".into(),
            attachment: None,
            reply_token: Some("req-original".into()),
            stream_id: None,
            feedback_id: Some("feedback-1".into()),
            stream_finish: true,
        };
        let frame = build_send_frame("bot-1", &out);
        assert_eq!(frame["cmd"], "aibot_respond_msg");
        assert_eq!(frame["headers"]["req_id"], "req-original");
        assert_eq!(frame["body"]["msgtype"], "markdown");
        assert_eq!(frame["body"]["markdown"]["content"], "response");
        assert_eq!(frame["body"]["feedback"]["id"], "feedback-1");
    }

    #[test]
    fn parse_feedback_callback() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-feedback"},
            "body": {
                "msgid": "feedback-msg-1",
                "create_time": 1700000000,
                "aibotid": "bot-1",
                "chatid": "chat-1",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "event",
                "event": {
                    "eventtype": "feedback_event",
                    "feedback_event": {
                        "id": "request-1",
                        "type": 1,
                        "content": "good",
                        "inaccurate_reason_list": [2, 4]
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            let feedback = msg.feedback.unwrap();
            assert_eq!(msg.platform, "wecom");
            assert_eq!(msg.chat_id, "chat-1");
            assert_eq!(msg.user_id, "user-1");
            assert_eq!(feedback.feedback_id, "request-1");
            assert_eq!(feedback.feedback_type, 1);
            assert_eq!(feedback.content.as_deref(), Some("good"));
            assert_eq!(feedback.inaccurate_reason_list, vec![2, 4]);
        });
    }

    #[test]
    fn parse_feedback_callback_before_cmd_filter() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-feedback"},
            "body": {
                "msgid": "feedback-msg-2",
                "chatid": "chat-1",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "event",
                "event": {
                    "eventtype": "feedback_event",
                    "feedback_event": {
                        "id": "request-2",
                        "type": 2
                    }
                }
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            let feedback = msg.feedback.unwrap();
            assert_eq!(feedback.feedback_id, "request-2");
            assert_eq!(feedback.feedback_type, 2);
        });
    }

    #[test]
    fn classify_send_ack_and_error_health() {
        let ack: Value =
            serde_json::from_str(r#"{"cmd":"aibot_send_msg","body":{"errcode":0}}"#).unwrap();
        let err: Value = serde_json::from_str(
            r#"{"cmd":"aibot_respond_msg","body":{"errcode":45009,"errmsg":"rate limited"}}"#,
        )
        .unwrap();

        let (event, detail) = classify_wecom_control_message(&ack).unwrap();
        assert_eq!(event, AdapterHealthEventType::SendAck);
        assert!(detail.is_none());

        let (event, detail) = classify_wecom_control_message(&err).unwrap();
        assert_eq!(event, AdapterHealthEventType::SendError);
        assert!(detail.unwrap().contains("rate limited"));
    }

    #[tokio::test]
    async fn outbound_send_wakes_without_waiting_for_heartbeat() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (seen_tx, seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

            let subscribe = timeout(Duration::from_millis(500), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let subscribe = subscribe.into_text().unwrap();
            assert!(subscribe.contains("aibot_subscribe"));

            let outbound = timeout(Duration::from_millis(500), ws.next())
                .await
                .expect("outbound send should wake immediately")
                .unwrap()
                .unwrap();
            let outbound = outbound.into_text().unwrap();
            assert!(outbound.contains("aibot_send_msg"));
            assert!(outbound.contains("wake now"));
            let _ = seen_tx.send(());
            let _ = timeout(Duration::from_secs(1), ws.next()).await;
        });

        let config = WeComConfig {
            enabled: true,
            bot_id: "bot".into(),
            secret: "secret".into(),
            websocket_url: format!("ws://{addr}"),
        };
        let (msg_tx, _msg_rx) = mpsc::channel(4);
        let (out_tx, out_rx) = mpsc::channel(4);
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel(1);
        let out_rx = std::sync::Arc::new(tokio::sync::Mutex::new(out_rx));

        let client = tokio::spawn(async move {
            run_wecom_connection(&config, &msg_tx, out_rx, &mut shutdown_rx).await
        });

        out_tx
            .send(OutboundMessage {
                chat_id: "chat-1".into(),
                text: "wake now".into(),
                attachment: None,
                reply_token: None,
                stream_id: None,
                feedback_id: None,
                stream_finish: true,
            })
            .await
            .unwrap();

        timeout(Duration::from_millis(500), seen_rx)
            .await
            .expect("server should see outbound frame")
            .unwrap();
        let _ = shutdown_tx.send(());
        server.await.unwrap();
        let result = timeout(Duration::from_secs(1), client)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn reconnect_delay_is_shutdown_interruptible() {
        let (tx, mut rx) = broadcast::channel(1);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = tx.send(());
        });

        let interrupted = timeout(
            Duration::from_millis(200),
            wait_reconnect_delay(Duration::from_secs(60), &mut rx),
        )
        .await
        .unwrap();

        assert!(interrupted);
    }

    #[test]
    fn parse_voice_message() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "r1"},
            "body": {
                "msgid": "voice-1",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "voice": {"content": "transcribed text from voice", "media_id": "voice-media-1"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.text, "transcribed text from voice");
            assert!(msg.attachments.is_empty());
        });
    }

    #[test]
    fn parse_deduplicates_repeated_attachment_identity() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "r1"},
            "body": {
                "msgid": "image-1",
                "from": {"userid": "u1"},
                "chatid": "c1",
                "chattype": "single",
                "image": {"media_id": "media-1", "url": "https://example.com/image.png"},
                "item_list": [
                    {"type": 2, "image_item": {"media": {"media_id": "media-1", "url": "https://example.com/image.png"}}}
                ]
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.attachments.len(), 1);
            assert_eq!(msg.attachments[0].media_id.as_deref(), Some("media-1"));
        });
    }

    #[test]
    fn parse_group_message() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-group"},
            "body": {
                "msgid": "g1",
                "from": {"userid": "u1"},
                "chatid": "group-123",
                "chattype": "group",
                "text": {"content": "group message"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            let msg = rx.recv().await.unwrap();
            assert_eq!(msg.chat_type, ChatType::Group);
            assert_eq!(msg.chat_id, "group-123");
            assert_eq!(msg.reply_token, Some("req-group".into()));
        });
    }

    #[test]
    fn parse_subscribe_success() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_subscribe",
            "headers": {},
            "body": {"errcode": 0}
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            // Subscribe responses don't produce InboundMessages
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn unknown_cmd_ignored() {
        let data: Value =
            serde_json::from_str(r#"{"cmd": "pong", "headers": {}, "body": {}}"#).unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            assert!(rx.try_recv().is_err());
        });
    }

    #[test]
    fn missing_msgid_ignored() {
        let data: Value = serde_json::from_str(
            r#"{
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "r"},
            "body": {
                "from": {"userid": "u"},
                "chatid": "c",
                "text": {"content": "no msgid"}
            }
        }"#,
        )
        .unwrap();

        let (tx, mut rx) = mpsc::channel(10);
        let mut dedup = MessageDeduplicator::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            handle_wecom_message(&data, &tx, &mut dedup).await;
            assert!(rx.try_recv().is_err());
        });
    }
}
