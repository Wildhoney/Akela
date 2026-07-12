<div align="center">
  <img src="/media/logo.png" width="475" />

<i>❝Look well, look well, O Wolves!❞</i>

</div>

# Akela

Distributed Server-Sent Events with tag-based fan-out, built on axum and Redis pub/sub.

Any number of Akela instances behind a load balancer form one logical hub: every event is published to a Redis channel and each instance delivers it to its own connected clients. Tag mutations travel over the same channel, so they work no matter which instance owns the client's connection.

## Semantics

- **Clients** connect over SSE and hold a mutable set of string tags.
- **`send(data)`** &mdash; no tags &mdash; is public: every client receives it.
- **`send(data, tags)`** reaches only the clients holding **all** of the supplied tags. Clients may hold any number of extra tags and still receive the event; an empty tag list is treated as public.
- **Sender exclusion** &mdash; a send attributed to a client (`send_from` in Rust, `"client"` in the HTTP body) is delivered to every matching client **except** the sender itself.
- Delivery always routes through Redis, even to clients on the publishing instance, so ordering is consistent across the fleet.

## Quick start

```sh
docker compose up --build
```

That starts Redis and one Akela instance on `http://localhost:8080`. Or run it directly against your own Redis:

```sh
REDIS_URL=redis://127.0.0.1:6379 PORT=8080 cargo run --release
```

## HTTP API

| Route | Method | Purpose |
| --- | --- | --- |
| `/sse?tags=vip,beta` | `GET` | Connect a client, optionally with initial tags. |
| `/send` | `POST` | Publish `{ "data": ..., "tags": ["vip"]?, "client": "<uuid>"? }`. |
| `/clients/{client}/tags/{tag}` | `PUT` | Add a tag to a client. |
| `/clients/{client}/tags/{tag}` | `DELETE` | Remove a tag from a client. |
| `/healthz` | `GET` | Liveness probe. |

The SSE stream emits three named events:

- `connected` &mdash; first event on the stream; carries `{ "client": "<uuid>", "tags": [...] }`. Keep the id: it is how you mutate tags and attribute sends.
- `tags` &mdash; emitted whenever the client's tag set changes; carries the full current set.
- `message` &mdash; a delivered event; carries the `data` value verbatim.

```sh
curl -N 'http://localhost:8080/sse?tags=vip'

curl -X POST http://localhost:8080/send \
  -H 'content-type: application/json' \
  -d '{"data": {"hello": "everyone"}}'

curl -X POST http://localhost:8080/send \
  -H 'content-type: application/json' \
  -d '{"data": {"hello": "vips"}, "tags": ["vip"]}'

curl -X PUT http://localhost:8080/clients/<uuid>/tags/beta
curl -X DELETE http://localhost:8080/clients/<uuid>/tags/beta
```

The router ships with permissive CORS so browser `EventSource` clients on other origins can connect; put it behind your own gateway if you need something stricter.

## Rust API

```rust,no_run
use akela::Hub;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hub = Hub::connect("redis://127.0.0.1:6379").await?;

    let subscription = hub.subscribe(["vip".to_string()].into());
    let client = subscription.id();

    hub.tag(client).add("beta").await?;
    hub.tag(client).remove("beta").await?;

    hub.send(serde_json::json!({ "hello": "everyone" }), None).await?;
    hub.send(serde_json::json!({ "hello": "vips" }), Some(vec!["vip".into()])).await?;
    hub.send_from(client, serde_json::json!({ "from": "me" }), None).await?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", 8080)).await?;
    axum::serve(listener, hub.router()).await?;
    Ok(())
}
```

`Hub::subscribe` returns a `Subscription` &mdash; a `Stream` of SSE events you can mount in your own axum routes if you do not use the bundled `hub.router()`. Dropping the subscription deregisters the client. `Hub::connect_on` selects a custom Redis channel so several independent hubs can share one Redis deployment.

## Scaling out

Run as many instances as you like against the same Redis; clients can connect to any of them. To try it locally, start a second instance on another port (`PORT=8081 cargo run`) and send through either &mdash; clients on both receive the event.

## Delivery notes

- Per-client buffers hold 256 events; a client that cannot keep up has further events dropped (logged at `debug` level) rather than back-pressuring the hub.
- The Redis subscription reconnects automatically with a one-second backoff; events published while an instance is disconnected are not replayed &mdash; SSE is a live notification layer, not a durable queue.
