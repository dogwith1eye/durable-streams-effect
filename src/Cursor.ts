/**
 * Interval-based cursor generation for CDN cache collapsing.
 *
 * Divides time into fixed intervals from an epoch and returns the interval
 * number as the cursor. When a client echoes back a cursor that is at or
 * ahead of the current interval, jitter is added to guarantee the returned
 * cursor always strictly advances (prevents CDN cache loops).
 */

export const DEFAULT_CURSOR_EPOCH = new Date("2024-10-09T00:00:00.000Z")
export const DEFAULT_CURSOR_INTERVAL_SECONDS = 20

const MIN_JITTER_SECONDS = 1
const MAX_JITTER_SECONDS = 3600

export interface CursorOptions {
  readonly intervalSeconds?: number
  readonly epoch?: Date
}

export function calculateCursor(options: CursorOptions = {}): string {
  const intervalSeconds = options.intervalSeconds ?? DEFAULT_CURSOR_INTERVAL_SECONDS
  const epoch = options.epoch ?? DEFAULT_CURSOR_EPOCH
  const intervalMs = intervalSeconds * 1000
  const intervalNumber = Math.floor((Date.now() - epoch.getTime()) / intervalMs)
  return String(intervalNumber)
}

function jitterIntervals(intervalSeconds: number): number {
  const jitterSeconds =
    MIN_JITTER_SECONDS +
    Math.floor(Math.random() * (MAX_JITTER_SECONDS - MIN_JITTER_SECONDS + 1))
  return Math.max(1, Math.ceil(jitterSeconds / intervalSeconds))
}

/**
 * Generate a cursor for a response, guaranteeing monotonic progression.
 *
 * - If no client cursor: return current interval
 * - If client cursor < current interval: return current interval
 * - If client cursor >= current interval: return client cursor + jitter
 */
export function generateResponseCursor(
  clientCursor: string | undefined,
  options: CursorOptions = {}
): string {
  const intervalSeconds = options.intervalSeconds ?? DEFAULT_CURSOR_INTERVAL_SECONDS
  const currentCursor = calculateCursor(options)
  const currentInterval = parseInt(currentCursor, 10)

  if (!clientCursor) return currentCursor

  const clientInterval = parseInt(clientCursor, 10)
  if (isNaN(clientInterval) || clientInterval < currentInterval) return currentCursor

  return String(clientInterval + jitterIntervals(intervalSeconds))
}
