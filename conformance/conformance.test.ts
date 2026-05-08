import { afterAll, beforeAll, describe } from "vitest"
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import { startEffectServer, type TestServer } from "./server.js"

describe("Effect Server", () => {
  let server: TestServer
  const config = { baseUrl: "" }

  beforeAll(async () => {
    server = await startEffectServer({ longPollTimeout: 500 })
    config.baseUrl = server.url
  })

  afterAll(async () => {
    await server.stop()
  })

  runConformanceTests(config)
})
