# durable-streams-effect

An [Effect](https://effect.website)-based server implementation of the [Durable Streams Protocol](https://durablestreams.com).

Passes all 299 conformance tests.

## Features

- Full protocol coverage: catch-up reads, long-poll, SSE, stream creation/deletion, forking, TTL/expiry
- Idempotent producers with epoch fencing and sequence deduplication
- CDN cache-collapsing via interval-based cursor generation
- In-memory store with sliding-window TTL and cascading fork garbage collection
- gzip/deflate compression for catch-up responses
- `application/json` streams with array flattening and batching

## Getting started

```bash
pnpm install
pnpm dev        # start on http://127.0.0.1:4437
```

Or build and run:

```bash
pnpm build
pnpm start
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `4437` | Listening port |
| `HOST` | `127.0.0.1` | Bind address |
| `LONG_POLL_TIMEOUT_MS` | `60000` | Long-poll / SSE keep-alive interval (ms) |
| `COMPRESSION` | `true` | Enable gzip/deflate on catch-up responses |

## Development

```bash
pnpm test          # run conformance suite (299 tests)
pnpm test:watch    # watch mode
pnpm bench         # latency and throughput benchmarks
pnpm typecheck     # TypeScript type check without emit
```

### Benchmark results (local loopback)

| Metric | Mean |
|---|---|
| Baseline ping | ~0.14 ms |
| Long-poll round-trip | ~1 ms |
| Small message throughput (100 B) | ~166k msg/sec |
| Large message throughput (1 MB) | ~80 msg/sec |

## Architecture

```
src/
  index.ts    Entry point; wires layers and starts the Node.js HTTP server
  Server.ts   HTTP request handlers (PUT/HEAD/GET/POST/DELETE/OPTIONS)
  Store.ts    In-memory stream store as an Effect service
  Domain.ts   Shared types and tagged errors
  Cursor.ts   Interval-based cursor generation for CDN cache collapsing
```

**Store** holds all stream state in a `Ref<Map<string, DurableStream>>`. Long-poll waiters are tracked as `Deferred` values; `waitForMessages` registers a waiter before re-checking for messages to avoid the notification race. A per-producer `Semaphore` serialises concurrent appends from the same producer.

**Server** maps each HTTP method to a handler. SSE responses are driven by a forked fiber writing into a `Queue`; when the client disconnects the scope closes, the fiber is cancelled, and the queue shuts down cleanly.

**Cursor** divides time into 20-second intervals from a fixed epoch (2024-10-09). When a client echoes back a cursor that is already at or ahead of the current interval, jitter (1–3600 s) is added to guarantee strictly-monotonic progression and prevent CDN cache loops.

## Embedding

The server can be embedded in another Effect application:

```typescript
import { makeApp, makeDefaultConfig, StoreLayer } from "durable-streams-effect"
import { HttpServer } from "@effect/platform"
import { NodeHttpServer } from "@effect/platform-node"
import { Layer } from "effect"
import { createServer } from "node:http"

const config = makeDefaultConfig({ port: 4437 })

const ServerLayer = HttpServer.serve(makeApp(config)).pipe(
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: config.port })),
  Layer.provide(StoreLayer)
)
```

You can also supply a custom `StoreLayer` by implementing the `StoreService` interface and providing it via `Store` context tag.

## Protocol compliance

The implementation targets the [Durable Streams Protocol](https://durablestreams.com) specification. Known limitations of this in-memory implementation:

- **No persistence** — all stream data is lost on restart.
- **Single process** — the in-memory store is not shared across instances. For multi-node deployments a persistent store implementation would be required.
- **No authentication** — access control is not part of the protocol; add it at the reverse-proxy or middleware layer.
