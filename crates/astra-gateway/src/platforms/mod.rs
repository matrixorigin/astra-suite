pub mod wecom;
pub mod weixin;
pub mod whatsapp;
pub mod whatsapp_web;

use async_trait::async_trait;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterCapability {
    ReceiveText,
    SendText,
    SendAttachment,
    SendTyping,
    GroupReply,
    #[serde(rename = "websocket")]
    WebSocket,
    LongPoll,
    PersistentState,
}

impl AdapterCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReceiveText => "receive_text",
            Self::SendText => "send_text",
            Self::SendAttachment => "send_attachment",
            Self::SendTyping => "send_typing",
            Self::GroupReply => "group_reply",
            Self::WebSocket => "websocket",
            Self::LongPoll => "long_poll",
            Self::PersistentState => "persistent_state",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterHealthEventType {
    Capability,
    Connected,
    Disconnected,
    Reconnecting,
    Shutdown,
    SubscribeAck,
    SubscribeError,
    SendAck,
    SendError,
    CredentialRestored,
    CredentialInvalid,
    StateRestored,
    StateInvalid,
    PollError,
    PollBackoff,
    InboundDropped,
}

impl AdapterHealthEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Capability => "capability",
            Self::Connected => "connected",
            Self::Disconnected => "disconnected",
            Self::Reconnecting => "reconnecting",
            Self::Shutdown => "shutdown",
            Self::SubscribeAck => "subscribe_ack",
            Self::SubscribeError => "subscribe_error",
            Self::SendAck => "send_ack",
            Self::SendError => "send_error",
            Self::CredentialRestored => "credential_restored",
            Self::CredentialInvalid => "credential_invalid",
            Self::StateRestored => "state_restored",
            Self::StateInvalid => "state_invalid",
            Self::PollError => "poll_error",
            Self::PollBackoff => "poll_backoff",
            Self::InboundDropped => "inbound_dropped",
        }
    }

    const fn is_error(self) -> bool {
        matches!(
            self,
            Self::Disconnected
                | Self::SubscribeError
                | Self::SendError
                | Self::CredentialInvalid
                | Self::StateInvalid
                | Self::InboundDropped
        )
    }

    const fn is_warn(self) -> bool {
        matches!(
            self,
            Self::Reconnecting | Self::PollError | Self::PollBackoff
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct AdapterHealthEvent {
    pub platform: &'static str,
    pub event_type: AdapterHealthEventType,
    pub capability: Option<AdapterCapability>,
    pub detail: Option<String>,
}

impl AdapterHealthEvent {
    pub fn new(
        platform: &'static str,
        event_type: AdapterHealthEventType,
        detail: Option<String>,
    ) -> Self {
        Self {
            platform,
            event_type,
            capability: None,
            detail,
        }
    }

    pub fn capability(platform: &'static str, capability: AdapterCapability) -> Self {
        Self {
            platform,
            event_type: AdapterHealthEventType::Capability,
            capability: Some(capability),
            detail: None,
        }
    }
}

pub fn emit_adapter_health(event: AdapterHealthEvent) -> AdapterHealthEvent {
    let event_type = event.event_type.as_str();
    let capability = event.capability.map(AdapterCapability::as_str);
    let detail = event.detail.as_deref();
    if event.event_type.is_error() {
        tracing::error!(
            platform = event.platform,
            event_type,
            capability,
            detail,
            "platform adapter health"
        );
    } else if event.event_type.is_warn() {
        tracing::warn!(
            platform = event.platform,
            event_type,
            capability,
            detail,
            "platform adapter health"
        );
    } else {
        tracing::info!(
            platform = event.platform,
            event_type,
            capability,
            detail,
            "platform adapter health"
        );
    }
    event
}

/// Normalized inbound message from any chat platform.
#[derive(Debug, Clone)]
pub struct InboundMessage {
    pub platform: &'static str,
    pub chat_id: String,
    pub user_id: String,
    pub text: String,
    pub msg_id: String,
    pub chat_type: ChatType,
    /// WeCom: the inbound req_id, needed for group responds.
    pub reply_token: Option<String>,
    /// When the router wants to enqueue this message under a
    /// non-default cli_profile (e.g. `/manage` routes through the
    /// `_manage` virtual profile so it does NOT queue behind the user's
    /// stuck tasks). None = use resolve_cli_profile's normal result.
    pub route_override: Option<String>,
    /// Platform-side feedback for a previous AI response. Feedback messages are
    /// recorded by the runner and do not go through the CLI slow path.
    pub feedback: Option<FeedbackEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundAttachment {
    pub kind: AttachmentKind,
    pub name: Option<String>,
    pub media_id: Option<String>,
    pub local_path: Option<String>,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    File,
    Video,
    Audio,
    Unknown,
}

impl AttachmentKind {
    pub fn from_hint(
        kind: Option<&str>,
        mime: Option<&str>,
        path: Option<&std::path::Path>,
    ) -> Self {
        match kind
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "image" => return Self::Image,
            "file" => return Self::File,
            "video" => return Self::Video,
            "audio" | "voice" => return Self::Audio,
            _ => {}
        }

        let mime = mime.unwrap_or_default().to_ascii_lowercase();
        if mime.starts_with("image/") {
            return Self::Image;
        }
        if mime.starts_with("video/") {
            return Self::Video;
        }
        if mime.starts_with("audio/") {
            return Self::Audio;
        }

        match path
            .and_then(|p| p.extension())
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "png" | "jpg" | "jpeg" | "gif" | "webp" => Self::Image,
            "mp4" | "mov" | "webm" => Self::Video,
            "mp3" | "wav" | "m4a" | "ogg" => Self::Audio,
            _ => Self::File,
        }
    }
}

impl OutboundAttachment {
    pub fn required_local_path(&self, platform: &str) -> Result<&std::path::Path, String> {
        self.local_path
            .as_deref()
            .map(std::path::Path::new)
            .ok_or_else(|| format!("{platform} attachment requires local_path"))
    }

    pub fn filename_or_default(&self, path: &std::path::Path, default: &str) -> String {
        self.name
            .clone()
            .or_else(|| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackEvent {
    /// Identifier previously sent with the AI response. For WeCom this is
    /// `stream.feedback.id`, and gateway sets it to the trace request id.
    pub feedback_id: String,
    /// Platform-native feedback type. WeCom: 1=positive, 2=negative, 3=cancel.
    pub feedback_type: i64,
    pub content: Option<String>,
    pub inaccurate_reason_list: Vec<i64>,
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatType {
    DirectMessage,
    Group,
}

impl InboundMessage {
    pub fn session_key(&self) -> String {
        format!("{}:{}", self.platform, self.chat_id)
    }
}

/// Platform adapter trait — implemented by each chat platform.
#[async_trait]
pub trait PlatformAdapter: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> &'static [AdapterCapability] {
        &[]
    }
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn stop(&mut self);
    async fn send_text(
        &self,
        chat_id: &str,
        text: &str,
        reply_token: Option<&str>,
    ) -> Result<(), String>;
    async fn send_attachment(
        &self,
        _chat_id: &str,
        _attachment: &OutboundAttachment,
        _reply_token: Option<&str>,
    ) -> Result<(), String> {
        Err(format!(
            "{} does not support sending attachments",
            self.name()
        ))
    }
    /// Send a streaming chunk. Platforms that support streaming (e.g. WeCom) should
    /// override this; others fall back to send_text.
    async fn send_stream_chunk(
        &self,
        chat_id: &str,
        text: &str,
        reply_token: Option<&str>,
        _stream_id: Option<&str>,
        _feedback_id: Option<&str>,
        _finish: bool,
    ) -> Result<(), String> {
        self.send_text(chat_id, text, reply_token).await
    }
    /// Send typing indicator (start). No-op for platforms that don't support it.
    async fn send_typing(&self, _chat_id: &str) -> Result<(), String> {
        Ok(())
    }
    /// Receive the next inbound message (blocking).
    async fn recv(&self) -> Option<InboundMessage>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_event_serializes() {
        let event = AdapterHealthEvent::capability("wecom", AdapterCapability::WebSocket);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("capability"));
        assert!(json.contains("websocket"));
    }

    #[test]
    fn emit_helper_returns_event() {
        let event = emit_adapter_health(AdapterHealthEvent::new(
            "weixin",
            AdapterHealthEventType::StateInvalid,
            Some("bad sync cursor".to_string()),
        ));
        assert_eq!(event.platform, "weixin");
        assert_eq!(event.event_type, AdapterHealthEventType::StateInvalid);
    }

    #[test]
    fn attachment_kind_from_hint_prefers_explicit_kind() {
        assert_eq!(
            AttachmentKind::from_hint(Some("voice"), Some("image/png"), None),
            AttachmentKind::Audio
        );
        assert_eq!(
            AttachmentKind::from_hint(Some("file"), Some("image/png"), None),
            AttachmentKind::File
        );
    }

    #[test]
    fn attachment_kind_from_hint_uses_mime_then_extension() {
        assert_eq!(
            AttachmentKind::from_hint(None, Some("image/png"), None),
            AttachmentKind::Image
        );
        assert_eq!(
            AttachmentKind::from_hint(None, None, Some(std::path::Path::new("clip.webm"))),
            AttachmentKind::Video
        );
        assert_eq!(
            AttachmentKind::from_hint(None, None, Some(std::path::Path::new("report.pdf"))),
            AttachmentKind::File
        );
    }

    #[test]
    fn outbound_attachment_derives_filename_from_path() {
        let attachment = OutboundAttachment {
            kind: AttachmentKind::File,
            name: None,
            media_id: None,
            local_path: Some("/tmp/report.pdf".into()),
            mime_type: None,
        };
        let path = attachment.required_local_path("test").unwrap();
        assert_eq!(
            attachment.filename_or_default(path, "attachment"),
            "report.pdf"
        );
    }
}
