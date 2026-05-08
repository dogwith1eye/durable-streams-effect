/**
 * HTTP server implementing the Durable Streams Protocol using @effect/platform.
 *
 * Each HTTP method maps to a stream operation:
 *   PUT    → create stream
 *   HEAD   → stream metadata
 *   GET    → read (catch-up, long-poll, SSE)
 *   POST   → append / close
 *   DELETE → delete stream
 *
 * SSE responses are driven by a Queue that a forked Effect fiber writes into.
 * When the client disconnects the underlying stream is interrupted, the Scope
 * closes, the forked fiber is cancelled, and the queue is shut down.
 */

import { deflateSync, gzipSync } from "node:zlib"
import {
  Headers,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform"
import { Effect, Option, Queue, Stream } from "effect"
import type { CursorOptions } from "./Cursor.js"
import { generateResponseCursor } from "./Cursor.js"
import { Store, type StoreService } from "./Store.js"
import type { AppendResult, DurableStream, StreamMessage } from "./Domain.js"

// ---------------------------------------------------------------------------
// Protocol header names  (all lower-case for req.headers lookup)
// ---------------------------------------------------------------------------

const H_NEXT_OFFSET = "stream-next-offset"
const H_CURSOR = "stream-cursor"
const H_UP_TO_DATE = "stream-up-to-date"
const H_CLOSED = "stream-closed"
const H_CONTENT_TYPE = "content-type"
const H_SEQ = "stream-seq"
const H_TTL = "stream-ttl"
const H_EXPIRES_AT = "stream-expires-at"
const H_FORKED_FROM = "stream-forked-from"
const H_FORK_OFFSET = "stream-fork-offset"
const H_SSE_DATA_ENCODING = "stream-sse-data-encoding"
const H_PRODUCER_ID = "producer-id"
const H_PRODUCER_EPOCH = "producer-epoch"
const H_PRODUCER_SEQ = "producer-seq"
const H_PRODUCER_EXPECTED_SEQ = "producer-expected-seq"
const H_PRODUCER_RECEIVED_SEQ = "producer-received-seq"

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const COMPRESSION_THRESHOLD = 1024
const VALID_OFFSET = /^(-1|now|\d+_\d+)$/
const VALID_FORK_OFFSET = /^\d+_\d+$/
const VALID_TTL = /^(0|[1-9]\d*)$/
const STRICT_INT = /^\d+$/

// ---------------------------------------------------------------------------
// Server config
// ---------------------------------------------------------------------------

export interface ServerConfig {
  port?: number
  host?: string
  longPollTimeout?: number
  cursorOptions?: CursorOptions
  compression?: boolean
}

export function makeDefaultConfig(overrides: ServerConfig = {}): Required<ServerConfig> {
  return {
    port: overrides.port ?? 4437,
    host: overrides.host ?? "127.0.0.1",
    longPollTimeout: overrides.longPollTimeout ?? 60_000,
    cursorOptions: overrides.cursorOptions ?? {},
    compression: overrides.compression ?? true,
  }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/** Read the full request body as Uint8Array. */
const readBody = (req: HttpServerRequest.HttpServerRequest): Effect.Effect<Uint8Array, never> =>
  req.arrayBuffer.pipe(
    Effect.map((buf) => new Uint8Array(buf)),
    Effect.catchAll(() => Effect.succeed(new Uint8Array(0)))
  )

/** Plain-text error response. */
function textErr(status: number, message: string): HttpServerResponse.HttpServerResponse {
  return HttpServerResponse.text(message, { status })
}

/** Encode an SSE event for the wire. */
function sseEvent(eventType: string, payload: string): Uint8Array {
  const lines = payload.split(/\r\n|\r|\n/)
  const data = lines.map((l) => `data:${l}`).join("\n") + "\n\n"
  return new TextEncoder().encode(`event: ${eventType}\n${data}`)
}

/** Apply gzip/deflate if the client accepts it and the response is large enough. */
function maybeCompress(
  data: Uint8Array,
  acceptEncoding: string | undefined
): { data: Uint8Array; encoding: string | null } {
  if (!acceptEncoding || data.length < COMPRESSION_THRESHOLD) return { data, encoding: null }
  const lower = acceptEncoding.toLowerCase()
  if (lower.includes("gzip")) return { data: gzipSync(data), encoding: "gzip" }
  if (lower.includes("deflate")) return { data: deflateSync(data), encoding: "deflate" }
  return { data, encoding: null }
}

/** CORS and browser-security headers added to every response. */
function withBaseHeaders(res: HttpServerResponse.HttpServerResponse): HttpServerResponse.HttpServerResponse {
  return HttpServerResponse.setHeaders(res, {
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET, POST, PUT, DELETE, HEAD, OPTIONS",
    "access-control-allow-headers":
      "content-type, authorization, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, " +
      "Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset",
    "access-control-expose-headers":
      "Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, " +
      "Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, " +
      "etag, content-type, content-encoding, vary",
    "x-content-type-options": "nosniff",
    "cross-origin-resource-policy": "cross-origin",
  })
}

// ---------------------------------------------------------------------------
// Handler: OPTIONS  (CORS preflight)
// ---------------------------------------------------------------------------

const handleOptions = (): HttpServerResponse.HttpServerResponse =>
  withBaseHeaders(HttpServerResponse.empty({ status: 204 }))

// ---------------------------------------------------------------------------
// Handler: PUT – create stream
// ---------------------------------------------------------------------------

const handleCreate = (
  path: string,
  req: HttpServerRequest.HttpServerRequest
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, Store> =>
  Effect.gen(function* () {
    const store = yield* Store

    const rawCT = req.headers[H_CONTENT_TYPE] as string | undefined
    const forkedFrom = req.headers[H_FORKED_FROM] as string | undefined
    const forkOffset = req.headers[H_FORK_OFFSET] as string | undefined
    const ttlHeader = req.headers[H_TTL] as string | undefined
    const expiresAtHeader = req.headers[H_EXPIRES_AT] as string | undefined
    const closedHeader = req.headers[H_CLOSED]
    const createClosed = closedHeader === "true"

    // Sanitise content-type
    let contentType: string | undefined =
      rawCT && rawCT.trim() !== "" && /^[\w-]+\/[\w-]+/.test(rawCT) ? rawCT : undefined
    if (!contentType && !forkedFrom) contentType = "application/octet-stream"

    if (ttlHeader && expiresAtHeader)
      return withBaseHeaders(textErr(400, "Cannot specify both Stream-TTL and Stream-Expires-At"))

    let ttlSeconds: number | undefined
    if (ttlHeader) {
      if (!VALID_TTL.test(ttlHeader)) return withBaseHeaders(textErr(400, "Invalid Stream-TTL value"))
      ttlSeconds = parseInt(ttlHeader, 10)
    }

    if (expiresAtHeader && isNaN(new Date(expiresAtHeader).getTime()))
      return withBaseHeaders(textErr(400, "Invalid Stream-Expires-At timestamp"))

    if (forkOffset && !VALID_FORK_OFFSET.test(forkOffset))
      return withBaseHeaders(textErr(400, "Invalid Stream-Fork-Offset format"))

    const body = yield* readBody(req)
    const isNew = !(yield* store.has(path))

    const createResult = yield* store
      .create(path, {
        contentType,
        ttlSeconds,
        expiresAt: expiresAtHeader,
        initialData: body.length > 0 ? body : undefined,
        closed: createClosed,
        forkedFrom,
        forkOffset,
      })
      .pipe(
        Effect.catchTags({
          StreamConflictError: (e) =>
            Effect.succeed(
              withBaseHeaders(textErr(e.reason.includes("Invalid fork offset") ? 400 : 409, e.reason))
            ),
          StreamNotFoundError: () =>
            Effect.succeed(withBaseHeaders(textErr(404, "Source stream not found"))),
          StreamGoneError: () =>
            Effect.succeed(withBaseHeaders(textErr(409, "Source stream was soft-deleted"))),
        })
      )

    // If catchTags returned an error response, surface it
    if ("status" in createResult)
      return createResult as HttpServerResponse.HttpServerResponse

    const stream = createResult as DurableStream
    const resolvedCT = stream.contentType ?? contentType ?? "application/octet-stream"
    const responseHeaders: Record<string, string> = {
      [H_CONTENT_TYPE]: resolvedCT,
      [H_NEXT_OFFSET]: stream.currentOffset,
    }
    if (isNew) {
      const reqHost = req.headers["host"] as string | undefined
      responseHeaders["location"] = reqHost ? `http://${reqHost}${path}` : path
    }
    if (stream.closed) responseHeaders[H_CLOSED] = "true"

    return withBaseHeaders(
      HttpServerResponse.empty({
        status: isNew ? 201 : 200,
        headers: Headers.unsafeFromRecord(responseHeaders),
      })
    )
  }).pipe(Effect.catchAll((e) => Effect.succeed(withBaseHeaders(textErr(500, String(e))))))

// ---------------------------------------------------------------------------
// Handler: HEAD – metadata
// ---------------------------------------------------------------------------

const handleHead = (
  path: string
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, Store> =>
  Effect.gen(function* () {
    const store = yield* Store
    const opt = yield* store.get(path)

    if (Option.isNone(opt)) return withBaseHeaders(HttpServerResponse.empty({ status: 404 }))
    const stream = opt.value
    if (stream.softDeleted) return withBaseHeaders(HttpServerResponse.empty({ status: 410 }))

    const closedSuffix = stream.closed ? ":c" : ""
    const etag = `"${Buffer.from(path).toString("base64")}:-1:${stream.currentOffset}${closedSuffix}"`

    const headers: Record<string, string> = {
      [H_NEXT_OFFSET]: stream.currentOffset,
      "cache-control": "no-store",
      etag,
    }
    if (stream.contentType) headers[H_CONTENT_TYPE] = stream.contentType
    if (stream.closed) headers[H_CLOSED] = "true"
    if (stream.ttlSeconds !== undefined) headers[H_TTL] = String(stream.ttlSeconds)
    if (stream.expiresAt) headers[H_EXPIRES_AT] = stream.expiresAt

    return withBaseHeaders(
      HttpServerResponse.empty({ status: 200, headers: Headers.unsafeFromRecord(headers) })
    )
  })

// ---------------------------------------------------------------------------
// Handler: GET – catch-up / long-poll / SSE
// ---------------------------------------------------------------------------

const handleRead = (
  path: string,
  url: URL,
  req: HttpServerRequest.HttpServerRequest,
  config: Required<ServerConfig>
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, Store> =>
  Effect.gen(function* () {
    const store = yield* Store

    const opt = yield* store.get(path)
    if (Option.isNone(opt)) return withBaseHeaders(textErr(404, "Stream not found"))
    const stream = opt.value
    if (stream.softDeleted) return withBaseHeaders(textErr(410, "Stream is gone"))

    const offset = url.searchParams.get("offset") ?? undefined
    const live = url.searchParams.get("live")
    const cursor = url.searchParams.get("cursor") ?? undefined

    // Validate offset
    if (offset !== undefined) {
      if (offset === "") return withBaseHeaders(textErr(400, "Empty offset parameter"))
      if (url.searchParams.getAll("offset").length > 1)
        return withBaseHeaders(textErr(400, "Multiple offset parameters not allowed"))
      if (!VALID_OFFSET.test(offset)) return withBaseHeaders(textErr(400, "Invalid offset format"))
    }
    if ((live === "long-poll" || live === "sse") && !offset)
      return withBaseHeaders(textErr(400, `${live === "sse" ? "SSE" : "Long-poll"} requires offset parameter`))

    // Binary SSE streams need base64 encoding
    const useBase64 =
      live === "sse" &&
      !(
        stream.contentType?.toLowerCase().startsWith("text/") === true ||
        stream.contentType?.toLowerCase().includes("application/json") === true
      )

    // ── SSE mode ─────────────────────────────────────────────────────────────
    if (live === "sse") {
      const sseOffset = offset === "now" ? stream.currentOffset : offset!
      const sseStream = makeSseStream(sseOffset, path, store, cursor, useBase64, config)
      const sseHeaders: Record<string, string> = {
        [H_CONTENT_TYPE]: "text/event-stream",
        "cache-control": "no-cache",
        connection: "keep-alive",
        "access-control-allow-origin": "*",
        "x-content-type-options": "nosniff",
        "cross-origin-resource-policy": "cross-origin",
      }
      if (useBase64) sseHeaders[H_SSE_DATA_ENCODING] = "base64"
      return HttpServerResponse.stream(sseStream, {
        headers: Headers.unsafeFromRecord(sseHeaders),
        status: 200,
      })
    }

    // Resolve sentinel "now" to the current tail offset
    const effectiveOffset = offset === "now" ? stream.currentOffset : offset

    // ── Catch-up offset=now ───────────────────────────────────────────────────
    if (offset === "now" && live !== "long-poll") {
      const isJson = stream.contentType?.includes("application/json") === true
      const headers: Record<string, string> = {
        [H_NEXT_OFFSET]: stream.currentOffset,
        [H_UP_TO_DATE]: "true",
        "cache-control": "no-store",
      }
      if (stream.contentType) headers[H_CONTENT_TYPE] = stream.contentType
      if (stream.closed) headers[H_CLOSED] = "true"
      return withBaseHeaders(
        HttpServerResponse.uint8Array(
          isJson ? new TextEncoder().encode("[]") : new Uint8Array(0),
          { status: 200, headers: Headers.unsafeFromRecord(headers) }
        )
      )
    }

    // ── Catch-up / long-poll ─────────────────────────────────────────────────
    let { messages, upToDate } = yield* store
      .read(path, effectiveOffset)
      .pipe(Effect.catchAll(() => Effect.succeed({ messages: [] as StreamMessage[], upToDate: true })))
    yield* store.touchAccess(path)

    const clientCaughtUp =
      (effectiveOffset !== undefined && effectiveOffset === stream.currentOffset) || offset === "now"

    if (live === "long-poll" && clientCaughtUp && messages.length === 0) {
      // Already closed at tail – respond immediately
      if (stream.closed) {
        return withBaseHeaders(
          HttpServerResponse.empty({
            status: 204,
            headers: Headers.unsafeFromRecord({
              [H_NEXT_OFFSET]: stream.currentOffset,
              [H_UP_TO_DATE]: "true",
              [H_CLOSED]: "true",
            }),
          })
        )
      }

      const waitResult = yield* store
        .waitForMessages(path, effectiveOffset ?? stream.currentOffset, config.longPollTimeout)
        .pipe(
          Effect.catchAll(() =>
            Effect.succeed({ messages: [] as StreamMessage[], timedOut: true, streamClosed: false })
          )
        )
      yield* store.touchAccess(path)

      const responseCursor = generateResponseCursor(cursor, config.cursorOptions)

      if (waitResult.streamClosed) {
        return withBaseHeaders(
          HttpServerResponse.empty({
            status: 204,
            headers: Headers.unsafeFromRecord({
              [H_NEXT_OFFSET]: effectiveOffset ?? stream.currentOffset,
              [H_UP_TO_DATE]: "true",
              [H_CLOSED]: "true",
            }),
          })
        )
      }

      if (waitResult.timedOut) {
        const streamNowOpt = yield* store.get(path)
        const timeoutHeaders: Record<string, string> = {
          [H_NEXT_OFFSET]: effectiveOffset ?? stream.currentOffset,
          [H_UP_TO_DATE]: "true",
          [H_CURSOR]: responseCursor,
        }
        if (Option.isSome(streamNowOpt) && streamNowOpt.value.closed)
          timeoutHeaders[H_CLOSED] = "true"
        return withBaseHeaders(
          HttpServerResponse.empty({ status: 204, headers: Headers.unsafeFromRecord(timeoutHeaders) })
        )
      }

      messages = waitResult.messages
      upToDate = true
    }

    // ── Build response ────────────────────────────────────────────────────────
    const responseHeaders: Record<string, string> = {}
    if (stream.contentType) responseHeaders[H_CONTENT_TYPE] = stream.contentType
    if (live !== "long-poll") responseHeaders["cache-control"] = "public, max-age=60, stale-while-revalidate=300"

    const lastMsg = messages[messages.length - 1]
    const responseOffset = lastMsg?.offset ?? stream.currentOffset
    responseHeaders[H_NEXT_OFFSET] = responseOffset
    if (live === "long-poll") responseHeaders[H_CURSOR] = generateResponseCursor(cursor, config.cursorOptions)
    if (upToDate) responseHeaders[H_UP_TO_DATE] = "true"

    const currentStreamOpt = yield* store.get(path)
    const currentStream = Option.isSome(currentStreamOpt) ? currentStreamOpt.value : stream
    const atTail = responseOffset === currentStream.currentOffset
    if (currentStream.closed && atTail && upToDate) responseHeaders[H_CLOSED] = "true"

    const startOffset = offset ?? "-1"
    const closedSuffix = currentStream.closed && atTail && upToDate ? ":c" : ""
    const etag = `"${Buffer.from(path).toString("base64")}:${startOffset}:${responseOffset}${closedSuffix}"`
    responseHeaders["etag"] = etag

    if (req.headers["if-none-match"] === etag)
      return withBaseHeaders(
        HttpServerResponse.empty({ status: 304, headers: Headers.unsafeFromRecord({ etag }) })
      )

    let responseData = yield* store
      .formatResponse(path, messages)
      .pipe(Effect.catchAll(() => Effect.succeed(new Uint8Array(0))))

    if (config.compression) {
      const { data, encoding } = maybeCompress(responseData, req.headers["accept-encoding"] as string | undefined)
      if (encoding) {
        responseData = data
        responseHeaders["content-encoding"] = encoding
        responseHeaders["vary"] = "accept-encoding"
      }
    }

    return withBaseHeaders(
      HttpServerResponse.uint8Array(responseData, {
        status: 200,
        headers: Headers.unsafeFromRecord(responseHeaders),
      })
    )
  }).pipe(Effect.catchAll((e) => Effect.succeed(withBaseHeaders(textErr(500, String(e))))))

// ---------------------------------------------------------------------------
// SSE stream builder
// ---------------------------------------------------------------------------

/**
 * Build a Stream<Uint8Array> that emits SSE-encoded bytes.
 *
 * A forked Effect fiber drives writes into an unbounded Queue. The fiber is
 * cancelled when the wrapping Scope closes (i.e. client disconnects), which
 * shuts down the queue and ends the stream.
 */
function makeSseStream(
  initialOffset: string,
  path: string,
  store: StoreService,
  clientCursor: string | undefined,
  useBase64: boolean,
  config: Required<ServerConfig>
): Stream.Stream<Uint8Array> {
  return Stream.unwrapScoped(
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<Uint8Array>()
      const decoder = new TextDecoder()

      // Fork the producer into the current Scope so it is cancelled on scope close
      yield* Effect.forkScoped(
        Effect.gen(function* () {
          let currentOffset = initialOffset

          try {
            // eslint-disable-next-line no-constant-condition
            while (true) {
              const { messages, upToDate } = yield* store
                .read(path, currentOffset)
                .pipe(
                  Effect.catchAll(() =>
                    Effect.succeed({ messages: [] as StreamMessage[], upToDate: true })
                  )
                )

              const streamOpt = yield* store.get(path)
              if (Option.isNone(streamOpt)) break
              const currentStream = streamOpt.value

              // Emit a data event per message
              for (const msg of messages) {
                let payload: string
                if (useBase64) {
                  payload = Buffer.from(msg.data).toString("base64")
                } else if (currentStream.contentType?.includes("application/json") === true) {
                  const formatted = yield* store
                    .formatResponse(path, [msg])
                    .pipe(Effect.catchAll(() => Effect.succeed(msg.data)))
                  payload = decoder.decode(formatted)
                } else {
                  payload = decoder.decode(msg.data)
                }
                yield* Queue.offer(queue, sseEvent("data", payload))
                currentOffset = msg.offset
              }

              // Control event
              const controlOffset = messages[messages.length - 1]?.offset ?? currentStream.currentOffset
              const atTail = controlOffset === currentStream.currentOffset
              const closed = currentStream.closed

              const control: Record<string, string | boolean> = { streamNextOffset: controlOffset }
              if (closed && atTail) {
                control.streamClosed = true
              } else {
                control.streamCursor = generateResponseCursor(clientCursor, config.cursorOptions)
                if (upToDate) control.upToDate = true
              }
              yield* Queue.offer(queue, sseEvent("control", JSON.stringify(control)))

              if (closed && atTail) break

              currentOffset = controlOffset

              if (upToDate) {
                // Check closure before blocking
                if (currentStream.closed) {
                  yield* Queue.offer(
                    queue,
                    sseEvent("control", JSON.stringify({ streamNextOffset: currentOffset, streamClosed: true }))
                  )
                  break
                }

                const waitResult = yield* store
                  .waitForMessages(path, currentOffset, config.longPollTimeout)
                  .pipe(
                    Effect.catchAll(() =>
                      Effect.succeed({ messages: [] as StreamMessage[], timedOut: true, streamClosed: false })
                    )
                  )
                yield* store.touchAccess(path)

                if (waitResult.streamClosed) {
                  yield* Queue.offer(
                    queue,
                    sseEvent("control", JSON.stringify({ streamNextOffset: currentOffset, streamClosed: true }))
                  )
                  break
                }

                if (waitResult.timedOut) {
                  const afterOpt = yield* store.get(path)
                  if (Option.isSome(afterOpt) && afterOpt.value.closed) {
                    yield* Queue.offer(
                      queue,
                      sseEvent("control", JSON.stringify({ streamNextOffset: currentOffset, streamClosed: true }))
                    )
                    break
                  }
                  // Keep-alive
                  yield* Queue.offer(
                    queue,
                    sseEvent(
                      "control",
                      JSON.stringify({
                        streamNextOffset: currentOffset,
                        streamCursor: generateResponseCursor(clientCursor, config.cursorOptions),
                        upToDate: true,
                      })
                    )
                  )
                }
              }
            }
          } finally {
            yield* Queue.shutdown(queue)
          }
        }).pipe(Effect.catchAll(() => Queue.shutdown(queue)))
      )

      return Stream.fromQueue(queue)
    })
  )
}

// ---------------------------------------------------------------------------
// Handler: POST – append / close
// ---------------------------------------------------------------------------

const handleAppend = (
  path: string,
  req: HttpServerRequest.HttpServerRequest
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, Store> =>
  Effect.gen(function* () {
    const store = yield* Store

    const contentType = req.headers[H_CONTENT_TYPE] as string | undefined
    const seq = req.headers[H_SEQ] as string | undefined
    const closeStream = req.headers[H_CLOSED] === "true"

    // Producer headers
    const producerId = req.headers[H_PRODUCER_ID] as string | undefined
    const producerEpochStr = req.headers[H_PRODUCER_EPOCH] as string | undefined
    const producerSeqStr = req.headers[H_PRODUCER_SEQ] as string | undefined

    const hasAny = producerId !== undefined || producerEpochStr !== undefined || producerSeqStr !== undefined
    const hasAll = producerId !== undefined && producerEpochStr !== undefined && producerSeqStr !== undefined

    if (hasAny && !hasAll)
      return withBaseHeaders(textErr(400, "All producer headers must be provided together"))
    if (hasAll && producerId === "")
      return withBaseHeaders(textErr(400, "Invalid Producer-Id: must not be empty"))

    let producerEpoch: number | undefined
    let producerSeq: number | undefined
    if (hasAll) {
      if (!STRICT_INT.test(producerEpochStr!) || !Number.isSafeInteger(Number(producerEpochStr)))
        return withBaseHeaders(textErr(400, "Invalid Producer-Epoch"))
      producerEpoch = Number(producerEpochStr)
      if (!STRICT_INT.test(producerSeqStr!) || !Number.isSafeInteger(Number(producerSeqStr)))
        return withBaseHeaders(textErr(400, "Invalid Producer-Seq"))
      producerSeq = Number(producerSeqStr)
    }

    const body = yield* readBody(req)

    // ── Close-only (empty body + Stream-Closed: true) ─────────────────────────
    if (body.length === 0 && closeStream) {
      if (!hasAll) {
        // Simple close without producer headers
        const closeResult = yield* store.closeStream(path).pipe(
          Effect.catchTags({
            StreamNotFoundError: () => Effect.succeed(null as null),
            StreamGoneError: () => Effect.succeed(null as null),
          })
        )
        if (!closeResult) return withBaseHeaders(textErr(404, "Stream not found"))
        return withBaseHeaders(
          HttpServerResponse.empty({
            status: 204,
            headers: Headers.unsafeFromRecord({
              [H_NEXT_OFFSET]: closeResult.finalOffset,
              [H_CLOSED]: "true",
            }),
          })
        )
      }
      // Close with producer headers – go through append path for deduplication
    }

    if (body.length === 0 && !closeStream)
      return withBaseHeaders(textErr(400, "Empty body"))
    if (body.length > 0 && !contentType)
      return withBaseHeaders(textErr(400, "Content-Type header is required"))

    const appendResult = yield* store
      .append(path, body, { seq, contentType, producerId, producerEpoch, producerSeq, close: closeStream })
      .pipe(
        Effect.catchTags({
          StreamNotFoundError: () =>
            Effect.succeed({ _tag: "error" as const, status: 404, message: "Stream not found" }),
          StreamGoneError: () =>
            Effect.succeed({ _tag: "error" as const, status: 410, message: "Stream is gone" }),
          StreamConflictError: (e) =>
            Effect.succeed({ _tag: "error" as const, status: 409, message: e.reason }),
          InvalidJsonError: () =>
            Effect.succeed({ _tag: "error" as const, status: 400, message: "Invalid JSON" }),
          EmptyArrayError: () =>
            Effect.succeed({ _tag: "error" as const, status: 400, message: "Empty arrays are not allowed" }),
          ContentTypeMismatchError: () =>
            Effect.succeed({ _tag: "error" as const, status: 409, message: "Content-type mismatch" }),
          SequenceConflictError: () =>
            Effect.succeed({ _tag: "error" as const, status: 409, message: "Sequence conflict" }),
        })
      )

    if ("_tag" in appendResult && appendResult._tag === "error") {
      return withBaseHeaders(textErr(appendResult.status, appendResult.message))
    }

    return buildAppendResponse(appendResult as AppendResult, producerEpoch, producerSeq, closeStream)
  }).pipe(Effect.catchAll((e) => Effect.succeed(withBaseHeaders(textErr(500, String(e))))))

/** Synchronously build the HTTP response for a successful / deduplicated append. */
function buildAppendResponse(
  result: AppendResult,
  producerEpoch: number | undefined,
  producerSeq: number | undefined,
  closeStream: boolean
): HttpServerResponse.HttpServerResponse {
  const { message, producerResult, streamClosed, finalOffset } = result

  // Close-only success: stream was just closed by this operation (empty body + producer accepted)
  if (streamClosed && message === null && producerResult?.status === "accepted") {
    const h: Record<string, string> = { [H_CLOSED]: "true", [H_NEXT_OFFSET]: finalOffset! }
    if (producerEpoch !== undefined) h[H_PRODUCER_EPOCH] = String(producerEpoch)
    if (producerSeq !== undefined) h[H_PRODUCER_SEQ] = String(producerSeq)
    return withBaseHeaders(HttpServerResponse.empty({ status: 204, headers: Headers.unsafeFromRecord(h) }))
  }

  // Append to already-closed stream
  if (streamClosed && message === null) {
    if (producerResult?.status === "duplicate") {
      const h: Record<string, string> = {
        [H_CLOSED]: "true",
        [H_NEXT_OFFSET]: finalOffset!,
        [H_PRODUCER_EPOCH]: String(producerEpoch!),
        [H_PRODUCER_SEQ]: String((producerResult as { lastSeq: number }).lastSeq),
      }
      return withBaseHeaders(HttpServerResponse.empty({ status: 204, headers: Headers.unsafeFromRecord(h) }))
    }
    return withBaseHeaders(
      HttpServerResponse.text("Stream is closed", {
        status: 409,
        headers: Headers.unsafeFromRecord({ [H_CLOSED]: "true", [H_NEXT_OFFSET]: finalOffset! }),
      })
    )
  }

  // Producer validation failures (stream was open)
  if (producerResult && producerResult.status !== "accepted") {
    switch (producerResult.status) {
      case "duplicate": {
        const h: Record<string, string> = {
          [H_PRODUCER_EPOCH]: String(producerEpoch!),
          [H_PRODUCER_SEQ]: String(producerResult.lastSeq),
        }
        if (streamClosed) h[H_CLOSED] = "true"
        return withBaseHeaders(HttpServerResponse.empty({ status: 204, headers: Headers.unsafeFromRecord(h) }))
      }
      case "stale_epoch":
        return withBaseHeaders(
          HttpServerResponse.text("Stale producer epoch", {
            status: 403,
            headers: Headers.unsafeFromRecord({ [H_PRODUCER_EPOCH]: String(producerResult.currentEpoch) }),
          })
        )
      case "invalid_epoch_seq":
        return withBaseHeaders(textErr(400, "New epoch must start with sequence 0"))
      case "sequence_gap":
        return withBaseHeaders(
          HttpServerResponse.text("Producer sequence gap", {
            status: 409,
            headers: Headers.unsafeFromRecord({
              [H_PRODUCER_EXPECTED_SEQ]: String(producerResult.expectedSeq),
              [H_PRODUCER_RECEIVED_SEQ]: String(producerResult.receivedSeq),
            }),
          })
        )
      case "stream_closed":
        return withBaseHeaders(
          HttpServerResponse.text("Stream is closed", {
            status: 409,
            headers: Headers.unsafeFromRecord({ [H_CLOSED]: "true" }),
          })
        )
    }
  }

  // Success
  const h: Record<string, string> = { [H_NEXT_OFFSET]: message!.offset }
  if (producerEpoch !== undefined) h[H_PRODUCER_EPOCH] = String(producerEpoch)
  if (producerSeq !== undefined) h[H_PRODUCER_SEQ] = String(producerSeq)
  if (closeStream || streamClosed) h[H_CLOSED] = "true"

  // 200 for producer appends (echo headers), 204 for plain appends
  const status = producerEpoch !== undefined ? 200 : 204
  return withBaseHeaders(HttpServerResponse.empty({ status, headers: Headers.unsafeFromRecord(h) }))
}

// ---------------------------------------------------------------------------
// Handler: DELETE – delete stream
// ---------------------------------------------------------------------------

const handleDelete = (
  path: string
): Effect.Effect<HttpServerResponse.HttpServerResponse, never, Store> =>
  Effect.gen(function* () {
    const store = yield* Store
    const opt = yield* store.get(path)
    if (Option.isSome(opt) && opt.value.softDeleted) return withBaseHeaders(textErr(410, "Stream is gone"))

    const deleted = yield* store.delete(path)
    if (!deleted) return withBaseHeaders(textErr(404, "Stream not found"))

    return withBaseHeaders(HttpServerResponse.empty({ status: 204 }))
  })

// ---------------------------------------------------------------------------
// Main app factory
// ---------------------------------------------------------------------------

export function makeApp(
  config: Required<ServerConfig>
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  never,
  Store | HttpServerRequest.HttpServerRequest
> {
  return Effect.gen(function* () {
    const req = yield* HttpServerRequest.HttpServerRequest
    const url = new URL(req.url, `http://localhost:${config.port}`)
    const path = url.pathname
    const method = req.method.toUpperCase()

    switch (method) {
      case "OPTIONS": return handleOptions()
      case "PUT":     return yield* handleCreate(path, req)
      case "HEAD":    return yield* handleHead(path)
      case "GET":     return yield* handleRead(path, url, req, config)
      case "POST":    return yield* handleAppend(path, req)
      case "DELETE":  return yield* handleDelete(path)
      default:        return withBaseHeaders(textErr(405, "Method not allowed"))
    }
  })
}
