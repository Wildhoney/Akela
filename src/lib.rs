//! Akela &mdash; distributed Server-Sent Events with tag-based fan-out.
//!
//! A [`Hub`] accepts SSE clients, each holding a mutable set of tags, and
//! publishes events through Redis pub/sub so any number of instances behave
//! as one. An event sent without tags is public and reaches every client; an
//! event sent with tags reaches only the clients holding **all** of those
//! tags (clients may hold extras). A send attributed to a client via
//! [`Hub::send_from`] is delivered to everyone matching except that client.

mod hub;
mod protocol;
mod router;

pub use hub::{Hub, Subscription, Tag};

/// Everything that can go wrong when talking to the hub.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Redis was unreachable or rejected a command.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// An event payload could not be serialised or deserialised.
    #[error("serialisation error: {0}")]
    Serialise(#[from] serde_json::Error),

    /// The Redis subscription stream ended and will be re-established.
    #[error("the redis subscription ended unexpectedly")]
    Disconnected,
}
