//! Conversation history: Anthropic-shaped `Message` / `ContentBlock` types
//! plus persistence helpers. Full message arrays land in the
//! `gw_session_messages` table so a new gateway process resumes verbatim
//! from where the previous one left off.

use serde::{Deserialize, Serialize};

use crate::store::{GatewayStore, StoreError};

use super::SessionKey;

/// Conversation role. Matches Bedrock Converse input/output shape.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// A single block inside a message. Mirrors Anthropic's content block union
/// (text / tool_use / tool_result / thinking).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default)]
        is_error: bool,
    },
    Thinking {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
}

/// One turn in the conversation. Vectors of these are what gets persisted
/// and what we send to Bedrock on each request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }
}

/// Load the persisted `Vec<Message>` for a session, or `None` if there's no
/// row yet (fresh session or a non-NativeRust profile previously occupied
/// this session_id).
pub async fn load_history(
    store: &dyn GatewayStore,
    key: &SessionKey,
    session_id: &str,
) -> Result<Option<Vec<Message>>, StoreError> {
    let blob = match store
        .load_session_messages(&key.platform, &key.chat_id, &key.cli_profile, session_id)
        .await?
    {
        Some(b) => b,
        None => return Ok(None),
    };
    let msgs: Vec<Message> = serde_json::from_slice(&blob)?;
    Ok(Some(msgs))
}

/// Serialize and upsert the message array for a session.
pub async fn save_history(
    store: &dyn GatewayStore,
    key: &SessionKey,
    session_id: &str,
    messages: &[Message],
) -> Result<(), StoreError> {
    let blob = serde_json::to_vec(messages)?;
    store
        .save_session_messages(
            &key.platform,
            &key.chat_id,
            &key.cli_profile,
            session_id,
            &blob,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: &Message) -> Message {
        let bytes = serde_json::to_vec(msg).expect("serialize");
        serde_json::from_slice(&bytes).expect("deserialize")
    }

    #[test]
    fn text_message_round_trip() {
        let m = Message::user_text("hi");
        assert_eq!(round_trip(&m), m);
    }

    #[test]
    fn tool_use_round_trip() {
        let m = Message {
            role: Role::Assistant,
            content: vec![
                ContentBlock::Text {
                    text: "let me run it".into(),
                },
                ContentBlock::ToolUse {
                    id: "toolu_01".into(),
                    name: "bash".into(),
                    input: serde_json::json!({"command": "echo hi"}),
                },
            ],
        };
        assert_eq!(round_trip(&m), m);
    }

    #[test]
    fn tool_result_round_trip_with_error_flag() {
        let m = Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: "toolu_01".into(),
                content: "timeout".into(),
                is_error: true,
            }],
        };
        assert_eq!(round_trip(&m), m);
    }

    #[test]
    fn tool_result_default_is_error_false() {
        // Historical rows may have been written before is_error existed.
        let json = r#"{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":"ok"}]}"#;
        let m: Message = serde_json::from_str(json).unwrap();
        match &m.content[0] {
            ContentBlock::ToolResult { is_error, .. } => assert!(!is_error),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn thinking_round_trip_with_signature() {
        let m = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Thinking {
                text: "hmm".into(),
                signature: Some("sig_abc".into()),
            }],
        };
        assert_eq!(round_trip(&m), m);
    }

    #[test]
    fn thinking_without_signature_omits_field() {
        let m = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Thinking {
                text: "hmm".into(),
                signature: None,
            }],
        };
        let serialized = serde_json::to_string(&m).unwrap();
        assert!(
            !serialized.contains("signature"),
            "signature=None should be omitted, got: {serialized}"
        );
        assert_eq!(round_trip(&m), m);
    }

    #[tokio::test]
    async fn save_and_load_history_roundtrip_via_sqlite() {
        use crate::store::sqlite::SqliteGatewayStore;
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store = SqliteGatewayStore::new(pool);
        store.ensure_schema().await.unwrap();

        let key = SessionKey {
            platform: "test".into(),
            chat_id: "chat_hist".into(),
            cli_profile: "claude-direct".into(),
        };
        let sid = "sid-abc";
        let msgs = vec![
            Message::user_text("hi"),
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::Text {
                    text: "hello".into(),
                }],
            },
        ];

        // Before save: None.
        assert!(load_history(&store, &key, sid).await.unwrap().is_none());

        save_history(&store, &key, sid, &msgs).await.unwrap();

        // After save: Some(msgs).
        let loaded = load_history(&store, &key, sid).await.unwrap().unwrap();
        assert_eq!(loaded, msgs);

        // Upsert: second save with different content overwrites.
        let msgs2 = vec![Message::user_text("try again")];
        save_history(&store, &key, sid, &msgs2).await.unwrap();
        let loaded2 = load_history(&store, &key, sid).await.unwrap().unwrap();
        assert_eq!(loaded2, msgs2);
    }
}
