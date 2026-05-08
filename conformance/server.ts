/**
 * Test helper: start the Effect server on a random port and return its URL.
 *
 * Effect.runFork(Layer.launch(FullLayer)) keeps the Effect scheduler alive so
 * that Effect.sleep, Deferred.await, and other async operations work correctly
 * inside request handlers. ManagedRuntime does not, because Effect.runPromise
 * resolves once the layer builds and does not keep scheduler ticks running.
 */

import { createServer } from "node:http"
import type { AddressInfo } from "node:net"
import { NodeHttpServer } from "@effect/platform-node"
import { HttpServer } from "@effect/platform"
import { Effect, Layer } from "effect"
import { makeApp, makeDefaultConfig } from "../src/Server.js"
import { StoreLayer } from "../src/Store.js"

export interface TestServer {
  url: string
  stop: () => Promise<void>
}

export async function startEffectServer(opts: {
  longPollTimeout?: number
} = {}): Promise<TestServer> {
  let nodeServer: ReturnType<typeof createServer> | undefined

  const config = makeDefaultConfig({
    port: 0,
    host: "127.0.0.1",
    longPollTimeout: opts.longPollTimeout ?? 500,
    compression: false,
  })

  const FullLayer = HttpServer.serve(makeApp(config)).pipe(
    Layer.provide(
      NodeHttpServer.layer(
        () => {
          nodeServer = createServer()
          return nodeServer
        },
        { port: 0, host: "127.0.0.1" }
      )
    ),
    Layer.provide(StoreLayer)
  )

  // runFork keeps the Effect scheduler alive for the lifetime of the server
  const fiber = Effect.runFork(Layer.launch(FullLayer))

  // Poll until Node.js server.listen() has completed and the port is bound
  await new Promise<void>((resolve) => {
    const check = () => {
      if (nodeServer?.listening) resolve()
      else setTimeout(check, 10)
    }
    check()
  })

  const addr = nodeServer!.address() as AddressInfo
  const url = `http://127.0.0.1:${addr.port}`

  return {
    url,
    stop: async () => {
      await Effect.runPromise(fiber.interruptAsFork(fiber.id()))
    },
  }
}
