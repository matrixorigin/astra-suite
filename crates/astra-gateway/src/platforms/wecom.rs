//! WeCom (企业微信) AI Bot WebSocket adapter.
//!
//! Protocol: connect → aibot_subscribe → heartbeat loop + message receive + outbound send.
//! Inbound: aibot_msg_callback. Outbound: aibot_send_msg / aibot_respond_msg.

use super::{
    AdapterCapability, AdapterHealthEvent, AdapterHealthEventType, ChatType, FeedbackEvent,
    InboundAttachment, InboundAttachmentKind, InboundMessage, OutboundAttachment, PlatformAdapter,
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
use reqwest::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use serde_json::{Value, json};
use std::path::PathBuf;
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
        .local_path
        .as_deref()
        .ok_or("attachment has no media_id or local_path")?;
    let bytes = tokio::fs::read(path).await?;
    let filename = attachment
        .name
        .clone()
        .or_else(|| {
            std::path::Path::new(path)
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "attachment".to_string());
    let filename = ensure_attachment_extension(
        &filename,
        attachment.mime_type.as_deref(),
        attachment.kind,
        &bytes,
    );
    let media_type = wecom_media_type(attachment.kind);
    let total_chunks = bytes.len().div_ceil(MEDIA_UPLOAD_CHUNK_BYTES);
    if total_chunks == 0 {
        return Err("cannot upload empty attachment".into());
    }
    if total_chunks > MEDIA_UPLOAD_MAX_CHUNKS {
        return Err(format!("attachment too large for WeCom upload: {total_chunks} chunks").into());
    }
    let md5 = hex::encode(Md5::digest(&bytes));
    tracing::info!(
        media_type,
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
            "type": media_type,
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
    tracing::info!(media_type, "wecom media upload complete");
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

fn wecom_media_type(kind: InboundAttachmentKind) -> &'static str {
    match kind {
        InboundAttachmentKind::Image => "image",
        InboundAttachmentKind::File | InboundAttachmentKind::Unknown => "file",
        InboundAttachmentKind::Video => "video",
        InboundAttachmentKind::Audio => "voice",
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
    let (msgtype, field) = match attachment.kind {
        InboundAttachmentKind::Image => ("image", "image"),
        InboundAttachmentKind::File | InboundAttachmentKind::Unknown => ("file", "file"),
        InboundAttachmentKind::Video => ("video", "video"),
        InboundAttachmentKind::Audio => ("voice", "voice"),
    };

    let mut body = json!({
        "msgtype": msgtype,
        field: {"media_id": media_id}
    });
    if let InboundAttachmentKind::Video = attachment.kind
        && let Some(name) = attachment.name.as_deref()
    {
        body[field]["title"] = json!(name);
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

    let raw_text = body["text"]["content"]
        .as_str()
        .or_else(|| body["voice"]["content"].as_str())
        .unwrap_or("")
        .trim();

    let text = raw_text.to_string();
    let mut attachments = extract_wecom_attachments(body);
    download_wecom_attachments(&mut attachments, &msg_id).await;

    if text.is_empty() && attachments.is_empty() {
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

fn extract_wecom_attachments(body: &Value) -> Vec<InboundAttachment> {
    let mut attachments = Vec::new();
    for (key, kind) in [
        ("image", InboundAttachmentKind::Image),
        ("file", InboundAttachmentKind::File),
        ("video", InboundAttachmentKind::Video),
        ("voice", InboundAttachmentKind::Audio),
    ] {
        let Some(section) = body.get(key) else {
            continue;
        };
        if !section.is_object() {
            continue;
        }
        let attachment = InboundAttachment {
            kind,
            name: first_string(
                section,
                &[
                    "filename",
                    "file_name",
                    "name",
                    "title",
                    "display_name",
                    "origin_name",
                ],
            )
            .or_else(|| infer_name_from_url(first_string(section, &url_keys()).as_deref())),
            media_id: first_string(section, &["media_id", "mediaid", "file_id", "fileid"]),
            url: first_string(section, &url_keys()),
            local_path: None,
            mime_type: first_string(section, &["mime_type", "mimetype", "content_type"]),
            size_bytes: first_u64(section, &["size", "file_size", "filesize", "size_bytes"]),
            raw: section.clone(),
        };
        if attachment.media_id.is_some()
            || attachment.url.is_some()
            || attachment.name.is_some()
            || kind != InboundAttachmentKind::Audio
        {
            attachments.push(attachment);
        }
    }
    attachments
}

async fn download_wecom_attachments(attachments: &mut [InboundAttachment], msg_id: &str) {
    const MAX_ATTACHMENT_BYTES: u64 = 25 * 1024 * 1024;

    if attachments.iter().all(|a| a.url.is_none()) {
        return;
    }

    let dir = attachment_dir(msg_id);
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!(error = %e, "wecom attachment http client unavailable");
            return;
        }
    };

    for (idx, attachment) in attachments.iter_mut().enumerate() {
        let Some(url) = attachment.url.as_deref() else {
            continue;
        };
        if !url.starts_with("http://") && !url.starts_with("https://") {
            continue;
        }

        let response = match client.get(url).send().await {
            Ok(response) => response,
            Err(e) => {
                tracing::warn!(url, error = %e, "wecom attachment download failed");
                continue;
            }
        };
        if !response.status().is_success() {
            tracing::warn!(
                url,
                status = %response.status(),
                "wecom attachment download returned non-success status"
            );
            continue;
        }
        if let Some(len) = response.content_length()
            && len > MAX_ATTACHMENT_BYTES
        {
            tracing::warn!(
                url,
                content_length = len,
                max_bytes = MAX_ATTACHMENT_BYTES,
                "wecom attachment too large"
            );
            continue;
        }

        let filename = content_disposition_filename(
            response
                .headers()
                .get(CONTENT_DISPOSITION)
                .and_then(|v| v.to_str().ok()),
        );
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let raw_bytes = match response.bytes().await {
            Ok(bytes) if bytes.len() as u64 <= MAX_ATTACHMENT_BYTES => bytes,
            Ok(bytes) => {
                tracing::warn!(
                    url,
                    bytes = bytes.len(),
                    max_bytes = MAX_ATTACHMENT_BYTES,
                    "wecom attachment body too large"
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(url, error = %e, "wecom attachment body read failed");
                continue;
            }
        };
        let aes_key = first_string(&attachment.raw, &["aeskey", "aes_key", "aesKey"]);
        let bytes = if let Some(aes_key) = aes_key.as_deref() {
            match decrypt_wecom_media(&raw_bytes, aes_key) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(url, error = %e, "wecom attachment decrypt failed");
                    continue;
                }
            }
        } else {
            raw_bytes.to_vec()
        };

        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            tracing::warn!(dir = %dir.display(), error = %e, "wecom attachment dir create failed");
            return;
        }
        if attachment.name.is_none() {
            attachment.name = filename;
        }
        if attachment.mime_type.is_none() {
            attachment.mime_type = content_type;
        }
        let filename = attachment_filename(attachment, idx);
        let path = dir.join(filename);
        if let Err(e) = tokio::fs::write(&path, &bytes).await {
            tracing::warn!(path = %path.display(), error = %e, "wecom attachment write failed");
            continue;
        }
        attachment.size_bytes.get_or_insert(bytes.len() as u64);
        attachment.local_path = Some(path.to_string_lossy().to_string());
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
        if let Ok(key) = engine.decode(aes_key) {
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

fn attachment_dir(msg_id: &str) -> PathBuf {
    run_dir()
        .join(".attachments")
        .join(sanitize_path_part(msg_id))
}

fn run_dir() -> PathBuf {
    std::env::var_os("GATEWAY_RUN_DIR")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".astra-gateway")))
        .unwrap_or_else(|| PathBuf::from(".astra-gateway"))
}

fn attachment_filename(attachment: &InboundAttachment, idx: usize) -> String {
    let inferred_name = infer_name_from_url(attachment.url.as_deref());
    let base = attachment
        .name
        .as_deref()
        .or(inferred_name.as_deref())
        .map(sanitize_path_part)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("attachment-{idx}"));
    if base.contains('.') {
        return base;
    }
    if let Some(ext) = attachment_extension(attachment.mime_type.as_deref(), attachment.kind, &[]) {
        return format!("{base}.{ext}");
    }
    match attachment.kind {
        InboundAttachmentKind::Image => format!("{base}.image"),
        InboundAttachmentKind::File => base,
        InboundAttachmentKind::Video => format!("{base}.video"),
        InboundAttachmentKind::Audio => format!("{base}.audio"),
        InboundAttachmentKind::Unknown => base,
    }
}

fn ensure_attachment_extension(
    filename: &str,
    mime_type: Option<&str>,
    kind: InboundAttachmentKind,
    bytes: &[u8],
) -> String {
    if filename.contains('.') {
        return filename.to_string();
    }
    match attachment_extension(mime_type, kind, bytes) {
        Some(ext) => format!("{filename}.{ext}"),
        None => filename.to_string(),
    }
}

fn attachment_extension(
    mime_type: Option<&str>,
    kind: InboundAttachmentKind,
    bytes: &[u8],
) -> Option<&'static str> {
    let mime = mime_type.unwrap_or("").trim().to_ascii_lowercase();
    match mime.as_str() {
        "text/html" | "application/xhtml+xml" => return Some("html"),
        "application/pdf" => return Some("pdf"),
        "image/png" => return Some("png"),
        "image/jpeg" | "image/jpg" => return Some("jpg"),
        "image/gif" => return Some("gif"),
        "image/webp" => return Some("webp"),
        "text/plain" => return Some("txt"),
        _ => {}
    }
    if bytes.starts_with(b"%PDF-") {
        return Some("pdf");
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("png");
    }
    if bytes.starts_with(b"\xff\xd8\xff") {
        return Some("jpg");
    }
    let sample_len = bytes.len().min(512);
    let sample = String::from_utf8_lossy(&bytes[..sample_len]).to_ascii_lowercase();
    if sample.contains("<!doctype html") || sample.contains("<html") {
        return Some("html");
    }
    match kind {
        InboundAttachmentKind::Image => Some("image"),
        InboundAttachmentKind::Video => Some("video"),
        InboundAttachmentKind::Audio => Some("audio"),
        InboundAttachmentKind::File | InboundAttachmentKind::Unknown => None,
    }
}

fn sanitize_path_part(input: &str) -> String {
    input
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => ch,
            _ => '_',
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
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

fn infer_name_from_url(url: Option<&str>) -> Option<String> {
    let url = url?;
    let path = url.split('?').next().unwrap_or(url);
    let name = path.rsplit('/').next()?.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn content_disposition_filename(value: Option<&str>) -> Option<String> {
    let value = value?;
    if let Some(rest) = value.split("filename*=UTF-8''").nth(1) {
        let encoded = rest.split(';').next().unwrap_or(rest).trim();
        if !encoded.is_empty() {
            return urlencoding::decode(encoded)
                .ok()
                .map(|s| s.trim_matches('"').to_string())
                .filter(|s| !s.is_empty());
        }
    }
    for part in value.split(';') {
        let part = part.trim();
        if let Some(name) = part.strip_prefix("filename=") {
            let name = name.trim().trim_matches('"');
            if !name.is_empty() {
                return urlencoding::decode(name)
                    .ok()
                    .map(|s| s.to_string())
                    .filter(|s| !s.is_empty());
            }
        }
    }
    None
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
            assert_eq!(msg.chat_type, ChatType::DirectMessage);
            assert_eq!(msg.reply_token, Some("req-123".to_string()));
        });
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
    fn build_send_frame_attachment_respond() {
        let out = OutboundMessage {
            chat_id: "chat-1".into(),
            text: String::new(),
            attachment: Some(OutboundAttachment {
                kind: InboundAttachmentKind::File,
                name: Some("report.pdf".into()),
                media_id: Some("media-file-1".into()),
                local_path: None,
                mime_type: Some("application/pdf".into()),
            }),
            reply_token: Some("req-original".into()),
            stream_id: None,
            feedback_id: None,
            stream_finish: true,
        };
        let frame = build_send_frame("bot-1", &out);
        assert_eq!(frame["cmd"], "aibot_respond_msg");
        assert_eq!(frame["headers"]["req_id"], "req-original");
        assert_eq!(frame["body"]["msgtype"], "file");
        assert_eq!(frame["body"]["file"]["media_id"], "media-file-1");
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
                "voice": {"content": "transcribed text from voice"}
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
            assert_eq!(attachment.kind, InboundAttachmentKind::Image);
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
            assert_eq!(attachment.kind, InboundAttachmentKind::File);
            assert_eq!(attachment.media_id.as_deref(), Some("file-media-1"));
            assert_eq!(attachment.name.as_deref(), Some("report.pdf"));
            assert_eq!(attachment.size_bytes, Some(1024));
        });
    }

    #[test]
    fn decrypt_wecom_media_uses_aes_256_cbc_and_32_byte_pkcs7_padding() {
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

        let decrypted = decrypt_wecom_media(&encrypted, &aes_key).unwrap();
        assert_eq!(decrypted, plaintext);

        let no_pad_key = aes_key.trim_end_matches('=');
        let decrypted = decrypt_wecom_media(&encrypted, no_pad_key).unwrap();
        assert_eq!(decrypted, plaintext);
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
