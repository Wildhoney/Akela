<div align="center">
  <img src="/media/logo.png" width="475" />

<i>❝Look well, look well, O Wolves!❞</i>

</div>

# Akela

A standalone Server-Sent Events hub with tag-based fan-out, distributed over Redis pub/sub.

Akela is **infrastructure, not a library** &mdash; think nginx rather than a crate you embed. It happens to be written in Rust, but what you deploy is an interface: pull the image, mount a YAML config, point it at Redis, and speak its HTTP protocol from your application. The first-class client is [March Hare](https://github.com/Wildhoney/MarchHare)'s **omnicast** distribution, which drives every route below for you &mdash; connection lifecycle, tag mutation, sender exclusion, and payload validation included.

Any number of Akela instances behind a load balancer form one logical hub: every event is published to a Redis channel and each instance delivers it to its own connected clients. Tag mutations travel over the same channel, so they work no matter which instance owns the client's connection.

## Semantics

- **Clients** connect over SSE and hold a mutable set of string tags.
- **Sends without tags** are public: every client receives them.
- **Sends with tags** reach only the clients holding **all** of the supplied tags. Clients may hold any number of extra tags and still receive the event; an empty tag list is treated as public.
- **Sender exclusion** &mdash; a send attributed to a client id is delivered to every matching client **except** the sender itself.
- Delivery always routes through Redis, even to clients on the publishing instance, so ordering is consistent across the fleet &mdash; a tag mutation acknowledged before a send was published is always applied before that send is delivered.

## Deploying

```sh
docker compose up --build
```

That starts Redis and one Akela instance on `http://localhost:8080`, configured by [`akela.example.yaml`](./akela.example.yaml) mounted at `/etc/akela/akela.yaml`.

Configuration resolves nginx-style: an explicit `AKELA_CONFIG` path wins, then `./akela.yaml`, then `/etc/akela/akela.yaml`. Every key is optional &mdash; environment variables (`PORT`, `REDIS_URL`, `CHANNEL`) override the file, and built-in defaults cover anything left unset, so Akela also runs with no file at all:

```yaml
port: 8080
redis: redis://redis:6379
channel: akela:events
```

Give each independent hub its own `channel` to share one Redis deployment between several applications. Scale out by running as many instances as you like against the same Redis &mdash; clients can connect to any of them.

## The client: March Hare

Applications never call Akela's routes by hand. Declare omnicast actions, point the App at the endpoint, and dispatch &mdash; March Hare owns the connection and the protocol:

```ts
import { Action, App, Audience, Distribution } from "march-hare";
import { z } from "zod";

export namespace Omnicast {
  export class Room {
    static Joined = Action(
      "Room.Joined",
      Distribution.Omnicast(z.object({ member: z.string() })),
    );
  }
}

export const app = App({
  sse: { url: "http://localhost:8080", actions: Omnicast },
});

// In a handler — tags and dispatches, all through March Hare:
actions.useAction(Actions.JoinRoom, async (context, room) => {
  await context.actions.tag.add(`room-${room.id}`);

  await context.actions.dispatch(
    Actions.Omnicast.Room.Joined,
    Audience.Private([`room-${room.id}`]),
    { member: "Adam" },
  );
});
```

See the [March Hare SSE recipe](https://github.com/Wildhoney/MarchHare/blob/main/recipes/sse.md) for audiences, channels, schema validation, and race semantics.

## The protocol

For debugging, or for writing a client in another language, the interface is four routes:

| Route                          | Method   | Purpose                                                       |
| ------------------------------ | -------- | ------------------------------------------------------------- |
| `/sse?tags=vip,beta`           | `GET`    | Connect a client, optionally with initial tags.                |
| `/send`                        | `POST`   | Publish `{ "data": ..., "tags": ["vip"]?, "client": "<uuid>"? }`. |
| `/clients/{client}/tags/{tag}` | `PUT` / `DELETE` | Add or remove a tag on a connection.                   |
| `/healthz`                     | `GET`    | Liveness probe.                                                |

The SSE stream emits three named events:

- `connected` &mdash; first event on the stream; carries `{ "client": "<uuid>", "tags": [...] }`. The id is what attributes sends and addresses tag mutations.
- `tags` &mdash; emitted whenever the client's tag set changes; carries the full current set.
- `message` &mdash; a delivered event; carries the `data` value verbatim.

```sh
curl -N 'http://localhost:8080/sse?tags=vip'

curl -X POST http://localhost:8080/send \
  -H 'content-type: application/json' \
  -d '{"data": {"hello": "vips"}, "tags": ["vip"]}'
```

The router ships with permissive CORS so browser `EventSource` clients on other origins can connect; put it behind your own gateway if you need something stricter.

## Delivery notes

- Per-client buffers hold 256 events; a client that cannot keep up has further events dropped (logged at `debug` level) rather than back-pressuring the hub.
- The Redis subscription reconnects automatically with a one-second backoff; events published while an instance is disconnected are not replayed &mdash; SSE is a live notification layer, not a durable queue.
