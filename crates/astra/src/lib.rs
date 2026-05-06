//! HTTP + SSE client for the astra agent server.
//!
//! This crate is transport-only — it has no knowledge of the cognitive engine
//! running behind the server, so any frontend (CLI, gateway, IDE plugin) can
//! depend on it without pulling heavier runtime crates.
//!
//! ## Layers
//! - [`paths`] — URL paths shared by all clients.
//! - [`protocol`] — JSON request bodies and [`protocol::StreamEvent`] classification.
//! - [`edge`] — edge executor metadata (capability presets, advertisement).
//! - [`sse`] — incremental `data: …\n\n` parser matching the server's SSE framing.
//! - [`Client`](client::Client) — `reqwest`-based transport.

pub mod client;
pub mod edge;
pub mod error;
pub mod paths;
pub mod protocol;
pub mod sse;

pub use client::Client;
pub use edge::{
    ASTRA_EDGE_ID_HEADER, advertise_executor, builtin_capability_preset,
    edge_register_with_capabilities,
};
pub use error::Error;
pub use protocol::{
    ApprovalDecision, ApprovalKind, ApprovalRespondRequest, ChatStreamRequest,
    EdgeHeartbeatRequest, EdgeRegisterRequest, SessionCreateRequest, SessionUpdateRequest,
    StreamEvent, TaskLeaseMutationRequest, ToolResultRequest, classify_stream_event,
};
/// SSE / buffered HTTP response from [`Client::post_chat_turn`].
pub use reqwest::Response as HttpResponse;

#[deprecated(since = "0.2.0", note = "renamed to Client")]
pub type ThinClient = Client;
#[deprecated(since = "0.2.0", note = "renamed to Error")]
pub type ThinClientError = Error;
