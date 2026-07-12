use std::{
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    pin::Pin,
    sync::{Arc, RwLock, Weak},
    task::{Context, Poll},
    time::Duration,
};

use axum::response::sse::Event;
use futures::{Stream, StreamExt};
use redis::{AsyncCommands, aio::ConnectionManager};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

use crate::{
    Error,
    protocol::{Envelope, matches},
};

const BUFFER: usize = 256;

/// A distributed Server-Sent Events hub.
///
/// Every `Hub` publishes to &mdash; and subscribes on &mdash; a single Redis
/// pub/sub channel, so any number of instances behind a load balancer form
/// one logical hub: an event sent through any instance reaches the matching
/// clients on every instance. Delivery always routes through Redis, even for
/// clients connected to the publishing instance, which keeps ordering
/// consistent across the fleet.
///
/// Cloning a `Hub` is cheap and shares the same underlying state.
#[derive(Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

struct HubInner {
    clients: RwLock<HashMap<Uuid, Client>>,
    publisher: ConnectionManager,
    channel: String,
}

struct Client {
    tags: BTreeSet<String>,
    sender: mpsc::Sender<Event>,
}

impl Hub {
    /// Connects to Redis on the default `akela:events` channel.
    pub async fn connect(redis_url: impl Into<String>) -> Result<Self, Error> {
        Self::connect_on(redis_url, "akela:events").await
    }

    /// Connects to Redis on a custom pub/sub channel, allowing several
    /// independent hubs to share one Redis deployment.
    pub async fn connect_on(
        redis_url: impl Into<String>,
        channel: impl Into<String>,
    ) -> Result<Self, Error> {
        let client = redis::Client::open(redis_url.into())?;
        let publisher = client.get_connection_manager().await?;
        let inner = Arc::new(HubInner {
            clients: RwLock::new(HashMap::new()),
            publisher,
            channel: channel.into(),
        });
        tokio::spawn(listen(client, Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    /// Registers a new SSE client holding the given tags and returns its
    /// event stream. The stream opens with a `connected` event carrying the
    /// client id, and the client is deregistered automatically when the
    /// stream is dropped.
    pub fn subscribe(&self, tags: BTreeSet<String>) -> Subscription {
        let id = Uuid::new_v4();
        let (sender, receiver) = mpsc::channel(BUFFER);
        let connected = Event::default()
            .event("connected")
            .json_data(serde_json::json!({ "client": id, "tags": tags }));
        match connected {
            Ok(event) => drop(sender.try_send(event)),
            Err(error) => tracing::warn!(%error, "unable to serialise the connected event"),
        }
        self.inner
            .clients
            .write()
            .expect("clients lock poisoned")
            .insert(id, Client { tags, sender });
        Subscription {
            id,
            receiver: ReceiverStream::new(receiver),
            _guard: Guard {
                hub: Arc::downgrade(&self.inner),
                client: id,
            },
        }
    }

    /// Publishes an event to every instance. Without tags (or with an empty
    /// list) the event is public and reaches every client; with tags it
    /// reaches only the clients holding **all** of them, extras permitted.
    pub async fn send(&self, data: impl Serialize, tags: Option<Vec<String>>) -> Result<(), Error> {
        self.publish_send(data, tags, None).await
    }

    /// As [`Hub::send`], but attributed to the given client: every matching
    /// client receives the event except the sender itself.
    pub async fn send_from(
        &self,
        sender: Uuid,
        data: impl Serialize,
        tags: Option<Vec<String>>,
    ) -> Result<(), Error> {
        self.publish_send(data, tags, Some(sender)).await
    }

    /// Returns a handle for mutating the given client's tags. Mutations
    /// travel over Redis, so they apply regardless of which instance owns
    /// the client's connection.
    pub fn tag(&self, client: Uuid) -> Tag<'_> {
        Tag { hub: self, client }
    }

    async fn publish_send(
        &self,
        data: impl Serialize,
        tags: Option<Vec<String>>,
        sender: Option<Uuid>,
    ) -> Result<(), Error> {
        let tags = tags
            .map(|tags| tags.into_iter().collect::<BTreeSet<_>>())
            .filter(|tags| !tags.is_empty());
        self.publish(&Envelope::Send {
            data: serde_json::to_value(data)?,
            tags,
            sender,
        })
        .await
    }

    async fn publish(&self, envelope: &Envelope) -> Result<(), Error> {
        let payload = serde_json::to_string(envelope)?;
        let mut publisher = self.inner.publisher.clone();
        let _: () = publisher.publish(&self.inner.channel, payload).await?;
        Ok(())
    }
}

/// Handle for mutating one client's tag set, obtained via [`Hub::tag`].
pub struct Tag<'hub> {
    hub: &'hub Hub,
    client: Uuid,
}

impl Tag<'_> {
    /// Adds a tag to the client, widening the events it receives.
    pub async fn add(&self, tag: impl Into<String>) -> Result<(), Error> {
        self.hub
            .publish(&Envelope::TagAdd {
                client: self.client,
                tag: tag.into(),
            })
            .await
    }

    /// Removes a tag from the client, narrowing the events it receives.
    pub async fn remove(&self, tag: impl Into<String>) -> Result<(), Error> {
        self.hub
            .publish(&Envelope::TagRemove {
                client: self.client,
                tag: tag.into(),
            })
            .await
    }
}

/// One client's event stream, produced by [`Hub::subscribe`]. Dropping the
/// subscription deregisters the client from its hub.
pub struct Subscription {
    id: Uuid,
    receiver: ReceiverStream<Event>,
    _guard: Guard,
}

impl Subscription {
    /// The identifier other parties use to target this client's tags or to
    /// attribute sends for sender exclusion.
    pub fn id(&self) -> Uuid {
        self.id
    }
}

impl Stream for Subscription {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .receiver
            .poll_next_unpin(context)
            .map(|event| event.map(Ok))
    }
}

struct Guard {
    hub: Weak<HubInner>,
    client: Uuid,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(inner) = self.hub.upgrade() {
            inner
                .clients
                .write()
                .expect("clients lock poisoned")
                .remove(&self.client);
        }
    }
}

impl HubInner {
    fn apply(&self, envelope: Envelope) {
        match envelope {
            Envelope::Send { data, tags, sender } => self.deliver(&data, tags.as_ref(), sender),
            Envelope::TagAdd { client, tag } => self.retag(client, |tags| {
                tags.insert(tag);
            }),
            Envelope::TagRemove { client, tag } => self.retag(client, |tags| {
                tags.remove(&tag);
            }),
        }
    }

    fn deliver(
        &self,
        data: &serde_json::Value,
        required: Option<&BTreeSet<String>>,
        sender: Option<Uuid>,
    ) {
        let payload = match serde_json::to_string(data) {
            Ok(payload) => payload,
            Err(error) => return tracing::warn!(%error, "unable to serialise the event payload"),
        };
        let clients = self.clients.read().expect("clients lock poisoned");
        for (id, client) in clients.iter() {
            if sender.is_some_and(|sender| sender == *id) {
                continue;
            }
            if !matches(required, &client.tags) {
                continue;
            }
            let event = Event::default().event("message").data(&payload);
            if let Err(error) = client.sender.try_send(event) {
                tracing::debug!(%error, client = %id, "dropping event for a slow or closed client");
            }
        }
    }

    fn retag(&self, client: Uuid, mutate: impl FnOnce(&mut BTreeSet<String>)) {
        let mut clients = self.clients.write().expect("clients lock poisoned");
        let Some(entry) = clients.get_mut(&client) else {
            return;
        };
        mutate(&mut entry.tags);
        match Event::default().event("tags").json_data(&entry.tags) {
            Ok(event) => {
                if let Err(error) = entry.sender.try_send(event) {
                    tracing::debug!(%error, client = %client, "dropping tags event for a slow or closed client");
                }
            }
            Err(error) => tracing::warn!(%error, "unable to serialise the tags event"),
        }
    }
}

async fn listen(client: redis::Client, hub: Weak<HubInner>) {
    loop {
        let Some(channel) = hub.upgrade().map(|inner| inner.channel.clone()) else {
            return;
        };
        match run(&client, &channel, &hub).await {
            Ok(()) => return,
            Err(error) => tracing::warn!(%error, "redis subscription lost; reconnecting"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run(client: &redis::Client, channel: &str, hub: &Weak<HubInner>) -> Result<(), Error> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    let mut messages = pubsub.on_message();
    while let Some(message) = messages.next().await {
        let Some(inner) = hub.upgrade() else {
            return Ok(());
        };
        let payload: String = message.get_payload()?;
        match serde_json::from_str::<Envelope>(&payload) {
            Ok(envelope) => inner.apply(envelope),
            Err(error) => tracing::warn!(%error, "discarding a malformed envelope"),
        }
    }
    Err(Error::Disconnected)
}
