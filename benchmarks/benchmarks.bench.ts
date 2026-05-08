import { afterAll } from "vitest"
import { runBenchmarks } from "@durable-streams/benchmarks"
import { startEffectServer } from "../conformance/server.js"

const server = await startEffectServer({ longPollTimeout: 30_000 })

afterAll(async () => {
  await server.stop()
})

runBenchmarks({ baseUrl: server.url, environment: "local" })
