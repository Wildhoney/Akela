use std::{collections::BTreeSet, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{
        IntoResponse,
        sse::{KeepAlive, Sse},
    },
    routing::{get, post, put},
};
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

use crate::Hub;

#[derive(Deserialize)]
struct SseQuery {
    #[serde(default)]
    tags: Option<String>,
}

#[derive(Deserialize)]
struct SendBody {
    data: serde_json::Value,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    client: Option<Uuid>,
}

impl Hub {
    /// Builds an axum router exposing the hub over HTTP:
    ///
    /// - `GET /sse?tags=a,b` &mdash; connect a client, optionally with
    ///   initial tags; the first event is `connected` with the client id.
    /// - `POST /send` &mdash; publish `{ "data": ..., "tags": [...]?,
    ///   "client": "..."? }`; `client` attributes the send so the sender is
    ///   excluded from delivery.
    /// - `PUT /clients/{client}/tags/{tag}` &mdash; add a tag.
    /// - `DELETE /clients/{client}/tags/{tag}` &mdash; remove a tag.
    /// - `GET /health` &mdash; liveness probe.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/sse", get(sse))
            .route("/send", post(send))
            .route("/clients/{client}/tags/{tag}", put(add).delete(remove))
            .route("/health", get(async || StatusCode::OK))
            .layer(CorsLayer::permissive())
            .with_state(self.clone())
    }
}

async fn sse(State(hub): State<Hub>, Query(query): Query<SseQuery>) -> impl IntoResponse {
    let tags: BTreeSet<String> = query
        .tags
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(str::to_string)
        .collect();
    Sse::new(hub.subscribe(tags)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn send(State(hub): State<Hub>, Json(body): Json<SendBody>) -> StatusCode {
    let published = match body.client {
        Some(client) => hub.send_from(client, body.data, body.tags).await,
        None => hub.send(body.data, body.tags).await,
    };
    match published {
        Ok(()) => StatusCode::ACCEPTED,
        Err(error) => {
            tracing::error!(%error, "unable to publish the event");
            StatusCode::BAD_GATEWAY
        }
    }
}

async fn add(State(hub): State<Hub>, Path((client, tag)): Path<(Uuid, String)>) -> StatusCode {
    match hub.tag(client).add(tag).await {
        Ok(()) => StatusCode::ACCEPTED,
        Err(error) => {
            tracing::error!(%error, "unable to publish the tag addition");
            StatusCode::BAD_GATEWAY
        }
    }
}

async fn remove(State(hub): State<Hub>, Path((client, tag)): Path<(Uuid, String)>) -> StatusCode {
    match hub.tag(client).remove(tag).await {
        Ok(()) => StatusCode::ACCEPTED,
        Err(error) => {
            tracing::error!(%error, "unable to publish the tag removal");
            StatusCode::BAD_GATEWAY
        }
    }
}
