import { defineConfig } from "vitest/config"

export default defineConfig({
  test: {
    include: ["conformance/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 15_000,
    benchmark: {
      include: ["benchmarks/**/*.bench.ts"],
    },
  },
})
