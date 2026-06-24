//! WhatsApp Web sidecar adapter.
//!
//! This adapter pairs with `bridges/whatsapp-baileys`: inbound messages are
//! pushed from the sidecar over a local Unix socket JSONL stream, while outbound
//! commands are sent to the sidecar over the same JSONL command protocol.

use super::{
    AdapterCapability, AdapterHealthEvent, AdapterHealthEventType, ChatType, InboundMessage,
    PlatformAdapter, emit_adapter_health,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::process::Child;
use tokio::sync::{Mutex, mpsc};
use uuid::Uuid;

const WHATSAPP_WEB_CAPABILITIES: &[AdapterCapability] = &[
    AdapterCapability::ReceiveText,
    AdapterCapability::SendText,
    AdapterCapability::SendTyping,
];
const COMMAND_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, serde::Deserialize)]
pub struct WhatsAppWebConfig {
    #[serde(default)]
    pub enabled: bool,
}

fn default_socket_path() -> String {
    crate::whatsapp_bridge::socket_path()
        .to_string_lossy()
        .into_owned()
}

pub struct WhatsAppWebAdapter {
    socket_path: String,
    sidecar: Option<Child>,
    msg_tx: mpsc::Sender<InboundMessage>,
    msg_rx: Mutex<mpsc::Receiver<InboundMessage>>,
}

impl WhatsAppWebAdapter {
    pub fn new(_config: WhatsAppWebConfig) -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            socket_path: default_socket_path(),
            sidecar: None,
            msg_tx: tx,
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
        self.sidecar = Some(spawn_sidecar().await?);
        spawn_jsonl_reader(self.socket_path.clone(), self.msg_tx.clone());
        Ok(())
    }

    async fn stop(&mut self) {
        if let Some(mut child) = self.sidecar.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
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
        send_command(
            &self.socket_path,
            SidecarCommand::SendText {
                id: Uuid::new_v4().to_string(),
                to: chat_id.to_string(),
                text: text.to_string(),
            },
        )
        .await
        .map(|_| {
            emit_adapter_health(AdapterHealthEvent::new(
                "whatsapp_web",
                AdapterHealthEventType::SendAck,
                Some(chat_id.to_string()),
            ));
        })
    }

    async fn send_typing(&self, chat_id: &str) -> Result<(), String> {
        send_command(
            &self.socket_path,
            SidecarCommand::Typing {
                id: Uuid::new_v4().to_string(),
                to: chat_id.to_string(),
                state: "composing",
            },
        )
        .await
    }

    async fn recv(&self) -> Option<InboundMessage> {
        self.msg_rx.lock().await.recv().await
    }
}

async fn spawn_sidecar() -> Result<Child, Box<dyn std::error::Error + Send + Sync>> {
    let run_dir = crate::whatsapp_bridge::run_dir();
    let runtime_dir = crate::whatsapp_bridge::prepare_runtime(false)?;
    let child = tokio::process::Command::new("node")
        .arg("index.js")
        .current_dir(&runtime_dir)
        .env("GATEWAY_RUN_DIR", &run_dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| {
            format!(
                "failed to start WhatsApp sidecar from {}: {e}",
                runtime_dir.display()
            )
        })?;
    tracing::info!(
        platform = "whatsapp_web",
        pid = child.id(),
        runtime_dir = %runtime_dir.display(),
        "started WhatsApp sidecar"
    );
    Ok(child)
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SidecarEvent {
    InboundMessage {
        chat_id: String,
        user_id: String,
        text: String,
        #[serde(default)]
        group: bool,
        #[serde(default)]
        msg_id: String,
    },
    Connection {
        state: String,
    },
    Response {
        ok: bool,
        #[serde(default)]
        error: Option<String>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SidecarCommand<'a> {
    Subscribe {
        id: String,
    },
    SendText {
        id: String,
        to: String,
        text: String,
    },
    Typing {
        id: String,
        to: String,
        state: &'a str,
    },
}

fn spawn_jsonl_reader(socket_path: String, msg_tx: mpsc::Sender<InboundMessage>) {
    tokio::spawn(async move {
        loop {
            match run_jsonl_reader(&socket_path, msg_tx.clone()).await {
                Ok(()) => {
                    emit_adapter_health(AdapterHealthEvent::new(
                        "whatsapp_web",
                        AdapterHealthEventType::Disconnected,
                        Some("JSONL socket closed".into()),
                    ));
                }
                Err(e) => {
                    emit_adapter_health(AdapterHealthEvent::new(
                        "whatsapp_web",
                        AdapterHealthEventType::Reconnecting,
                        Some(e),
                    ));
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}

async fn run_jsonl_reader(
    socket_path: &str,
    msg_tx: mpsc::Sender<InboundMessage>,
) -> Result<(), String> {
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|e| format!("connect {socket_path}: {e}"))?;
    write_json_line(
        &mut stream,
        &SidecarCommand::Subscribe {
            id: Uuid::new_v4().to_string(),
        },
    )
    .await?;

    emit_adapter_health(AdapterHealthEvent::new(
        "whatsapp_web",
        AdapterHealthEventType::Connected,
        Some(socket_path.to_string()),
    ));

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader
            .read_line(&mut line)
            .await
            .map_err(|e| format!("read JSONL event: {e}"))?;
        if n == 0 {
            return Ok(());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<SidecarEvent>(trimmed) {
            Ok(SidecarEvent::InboundMessage {
                chat_id,
                user_id,
                text,
                group,
                msg_id,
            }) => {
                let inbound = InboundMessage {
                    platform: "whatsapp_web",
                    chat_id,
                    user_id,
                    text,
                    msg_id: if msg_id.is_empty() {
                        format!("whatsapp-web-{}", Uuid::new_v4())
                    } else {
                        msg_id
                    },
                    chat_type: if group {
                        ChatType::Group
                    } else {
                        ChatType::DirectMessage
                    },
                    reply_token: None,
                    route_override: None,
                    attachments: Vec::new(),
                    feedback: None,
                };
                if msg_tx.send(inbound).await.is_err() {
                    return Ok(());
                }
            }
            Ok(SidecarEvent::Connection { state }) => {
                tracing::info!(platform = "whatsapp_web", state, "sidecar connection state");
            }
            Ok(SidecarEvent::Response { ok, error }) => {
                if !ok {
                    tracing::warn!(
                        platform = "whatsapp_web",
                        error,
                        "sidecar subscription response"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(platform = "whatsapp_web", error = %e, "invalid sidecar JSONL event");
            }
        }
    }
}

async fn send_command(socket_path: &str, command: SidecarCommand<'_>) -> Result<(), String> {
    let mut stream = tokio::time::timeout(COMMAND_TIMEOUT, UnixStream::connect(socket_path))
        .await
        .map_err(|_| "whatsapp_web socket connect timed out".to_string())?
        .map_err(|e| format!("whatsapp_web socket connect failed: {e}"))?;
    tokio::time::timeout(COMMAND_TIMEOUT, write_json_line(&mut stream, &command))
        .await
        .map_err(|_| "whatsapp_web command write timed out".to_string())??;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let n = tokio::time::timeout(COMMAND_TIMEOUT, reader.read_line(&mut line))
        .await
        .map_err(|_| "whatsapp_web response timed out".to_string())?
        .map_err(|e| format!("whatsapp_web response read failed: {e}"))?;
    if n == 0 {
        return Err("whatsapp_web sidecar closed without response".into());
    }
    match serde_json::from_str::<SidecarEvent>(line.trim()) {
        Ok(SidecarEvent::Response { ok: true, .. }) => Ok(()),
        Ok(SidecarEvent::Response {
            ok: false,
            error: Some(error),
        }) => {
            emit_adapter_health(AdapterHealthEvent::new(
                "whatsapp_web",
                AdapterHealthEventType::SendError,
                Some(error.clone()),
            ));
            Err(format!("whatsapp_web command failed: {error}"))
        }
        Ok(other) => Err(format!(
            "unexpected whatsapp_web sidecar response: {other:?}"
        )),
        Err(e) => Err(format!("invalid whatsapp_web sidecar response: {e}")),
    }
}

async fn write_json_line<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<(), String> {
    let mut encoded = serde_json::to_vec(value)
        .map_err(|e| format!("serialize whatsapp_web command failed: {e}"))?;
    encoded.push(b'\n');
    stream
        .write_all(&encoded)
        .await
        .map_err(|e| format!("write whatsapp_web command failed: {e}"))
}
