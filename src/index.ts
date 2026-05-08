/**
 * Durable Streams – Effect server
 *
 * Entry point. Builds the HTTP app, provides the Store layer, and starts
 * a Node.js HTTP server on the configured port.
 *
 * Usage:
 *   pnpm dev            # tsx src/index.ts
 *   pnpm build && pnpm start
 */

import { createServer } from "node:http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { HttpServer } from "@effect/platform"
import { Layer } from "effect"
import { makeApp, makeDefaultConfig } from "./Server.js"
import { StoreLayer } from "./Store.js"

const config = makeDefaultConfig({
  port: Number(process.env["PORT"] ?? 4437),
  host: process.env["HOST"] ?? "127.0.0.1",
  longPollTimeout: Number(process.env["LONG_POLL_TIMEOUT_MS"] ?? 30_000),
  compression: process.env["COMPRESSION"] !== "false",
})

const app = makeApp(config)

const ServerLayer = HttpServer.serve(app).pipe(
  Layer.provide(
    NodeHttpServer.layer(() => createServer(), {
      port: config.port,
      host: config.host,
    })
  ),
  Layer.provide(StoreLayer)
)

NodeRuntime.runMain(Layer.launch(ServerLayer))

// ---------------------------------------------------------------------------
// Public exports (for embedding in other projects)
// ---------------------------------------------------------------------------

export { makeApp, makeDefaultConfig, type ServerConfig } from "./Server.js"
export { Store, StoreLayer, type StoreService } from "./Store.js"
export { generateResponseCursor, calculateCursor, type CursorOptions } from "./Cursor.js"
export * from "./Domain.js"
