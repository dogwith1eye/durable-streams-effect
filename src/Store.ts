/**
 * In-memory stream store as an Effect service.
 *
 * All mutable state lives in a single Ref<Map<string, DurableStream>>.
 * Long-poll waiters are tracked as an array of Deferreds (one per request).
 * Producer serialisation uses a per-producer Semaphore (1 permit = mutex).
 *
 * Concurrency note: between any two `yield*` points in Effect.gen another
 * fiber may be scheduled.  The waitForMessages implementation therefore
 * registers its Deferred *before* re-checking for existing messages to
 * eliminate the "message arrives after check, before registration" race.
 */

import {
  Context,
  Deferred,
  Duration,
  Effect,
  Layer,
  Option,
  Ref,
} from "effect"
import {
  PRODUCER_STATE_TTL_MS,
  ZERO_OFFSET,
  ContentTypeMismatchError,
  EmptyArrayError,
  InvalidJsonError,
  SequenceConflictError,
  StreamConflictError,
  StreamGoneError,
  StreamNotFoundError,
  type AppendOptions,
  type AppendResult,
  type CloseResult,
  type CreateOptions,
  type DurableStream,
  type ProducerState,
  type ProducerValidationResult,
  type ReadResult,
  type StreamMessage,
  type WaitResult,
} from "./Domain.js"

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

export function normalizeContentType(ct: string | undefined): string {
  if (!ct) return ""
  return ct.split(";")[0]!.trim().toLowerCase()
}

function processJsonAppend(data: Uint8Array, isInitialCreate = false): Uint8Array {
  const text = new TextDecoder().decode(data)
  let parsed: unknown
  try { parsed = JSON.parse(text) } catch { throw new InvalidJsonError() }

  if (Array.isArray(parsed)) {
    if (parsed.length === 0) {
      if (isInitialCreate) return new Uint8Array(0)
      throw new EmptyArrayError()
    }
    const result = parsed.map((item) => JSON.stringify(item)).join(",") + ","
    return new TextEncoder().encode(result)
  }
  return new TextEncoder().encode(JSON.stringify(parsed) + ",")
}

function formatJsonResponse(data: Uint8Array): Uint8Array {
  if (data.length === 0) return new TextEncoder().encode("[]")
  let text = new TextDecoder().decode(data).trimEnd()
  if (text.endsWith(",")) text = text.slice(0, -1)
  return new TextEncoder().encode(`[${text}]`)
}

// ---------------------------------------------------------------------------
// Offset / stream helpers (pure, synchronous)
// ---------------------------------------------------------------------------

function appendToStream(
  stream: DurableStream,
  data: Uint8Array,
  isInitialCreate = false
): StreamMessage | null {
  let processedData = data
  if (normalizeContentType(stream.contentType) === "application/json") {
    processedData = processJsonAppend(data, isInitialCreate)
    if (processedData.length === 0) return null
  }

  const parts = stream.currentOffset.split("_").map(Number)
  const newByteOffset = parts[1]! + processedData.length
  const newOffset =
    `${String(parts[0]!).padStart(16, "0")}_${String(newByteOffset).padStart(16, "0")}`

  const message: StreamMessage = { data: processedData, offset: newOffset, timestamp: Date.now() }
  stream.messages.push(message)
  stream.currentOffset = newOffset
  return message
}

function findOffsetIndex(stream: DurableStream, offset: string): number {
  for (let i = 0; i < stream.messages.length; i++) {
    if (stream.messages[i]!.offset > offset) return i
  }
  return -1
}

// ---------------------------------------------------------------------------
// Expiry helpers
// ---------------------------------------------------------------------------

function isExpired(stream: DurableStream): boolean {
  const now = Date.now()
  if (stream.expiresAt) {
    const t = new Date(stream.expiresAt).getTime()
    if (!Number.isFinite(t) || now >= t) return true
  }
  if (stream.ttlSeconds !== undefined) {
    if (now >= stream.lastAccessedAt + stream.ttlSeconds * 1000) return true
  }
  return false
}

function resolveForkExpiry(
  opts: { ttlSeconds?: number; expiresAt?: string },
  source: DurableStream
): { ttlSeconds?: number; expiresAt?: string } {
  if (opts.ttlSeconds !== undefined) return { ttlSeconds: opts.ttlSeconds }
  if (opts.expiresAt) return { expiresAt: opts.expiresAt }
  if (source.ttlSeconds !== undefined) return { ttlSeconds: source.ttlSeconds }
  if (source.expiresAt) return { expiresAt: source.expiresAt }
  return {}
}

// ---------------------------------------------------------------------------
// Stream access helper (returns undefined for expired/missing)
// ---------------------------------------------------------------------------

function getStreamIfLive(
  streams: Map<string, DurableStream>,
  path: string
): DurableStream | undefined {
  const stream = streams.get(path)
  if (!stream) return undefined
  if (isExpired(stream)) {
    if (stream.refCount > 0) { stream.softDeleted = true; return stream }
    return undefined
  }
  return stream
}

// ---------------------------------------------------------------------------
// Fork-aware read (pure, throws StreamNotFoundError)
// ---------------------------------------------------------------------------

function readMessages(
  streams: Map<string, DurableStream>,
  path: string,
  offset?: string
): ReadResult {
  const stream = getStreamIfLive(streams, path)
  if (!stream) throw new StreamNotFoundError({ path })

  const noOffset = !offset || offset === "-1"
  if (noOffset) {
    if (stream.forkedFrom) {
      const inherited = inheritedMessages(streams, stream.forkedFrom, undefined, stream.forkOffset!)
      return { messages: [...inherited, ...stream.messages], upToDate: true }
    }
    return { messages: [...stream.messages], upToDate: true }
  }

  if (stream.forkedFrom) {
    const msgs: StreamMessage[] = []
    if (offset < stream.forkOffset!) {
      msgs.push(...inheritedMessages(streams, stream.forkedFrom, offset, stream.forkOffset!))
    }
    const idx = findOffsetIndex(stream, offset)
    if (idx !== -1) msgs.push(...stream.messages.slice(idx))
    return { messages: msgs, upToDate: true }
  }

  const idx = findOffsetIndex(stream, offset)
  if (idx === -1) return { messages: [], upToDate: true }
  return { messages: stream.messages.slice(idx), upToDate: true }
}

function inheritedMessages(
  streams: Map<string, DurableStream>,
  sourcePath: string,
  offset: string | undefined,
  capOffset: string
): StreamMessage[] {
  const source = streams.get(sourcePath)
  if (!source) return []
  const msgs: StreamMessage[] = []
  if (source.forkedFrom && (!offset || offset < source.forkOffset!)) {
    const cap = source.forkOffset! < capOffset ? source.forkOffset! : capOffset
    msgs.push(...inheritedMessages(streams, source.forkedFrom, offset, cap))
  }
  for (const msg of source.messages) {
    if (offset && msg.offset <= offset) continue
    if (msg.offset > capOffset) break
    msgs.push(msg)
  }
  return msgs
}

// ---------------------------------------------------------------------------
// Producer validation (pure, returns proposed state without mutating)
// ---------------------------------------------------------------------------

function validateProducer(
  stream: DurableStream,
  producerId: string,
  epoch: number,
  seq: number
): ProducerValidationResult {
  if (!stream.producers) stream.producers = new Map()
  const now = Date.now()
  for (const [id, s] of stream.producers) {
    if (now - s.lastUpdated > PRODUCER_STATE_TTL_MS) stream.producers.delete(id)
  }

  const state = stream.producers.get(producerId)
  const newState: ProducerState = { epoch, lastSeq: seq, lastUpdated: now }

  if (!state) {
    if (seq !== 0) return { status: "sequence_gap", expectedSeq: 0, receivedSeq: seq }
    return { status: "accepted", isNew: true, producerId, proposedState: { ...newState, lastSeq: 0 } }
  }

  if (epoch < state.epoch) return { status: "stale_epoch", currentEpoch: state.epoch }
  if (epoch > state.epoch) {
    if (seq !== 0) return { status: "invalid_epoch_seq" }
    return { status: "accepted", isNew: true, producerId, proposedState: { ...newState, lastSeq: 0 } }
  }
  if (seq <= state.lastSeq) return { status: "duplicate", lastSeq: state.lastSeq }
  if (seq === state.lastSeq + 1)
    return { status: "accepted", isNew: false, producerId, proposedState: newState }

  return { status: "sequence_gap", expectedSeq: state.lastSeq + 1, receivedSeq: seq }
}

function commitProducerState(stream: DurableStream, result: ProducerValidationResult): void {
  if (result.status !== "accepted") return
  if (!stream.producers) stream.producers = new Map()
  stream.producers.set(result.producerId, result.proposedState)
}

// ---------------------------------------------------------------------------
// Cascading delete (pure, mutates the Map)
// ---------------------------------------------------------------------------

function deleteWithCascade(streams: Map<string, DurableStream>, path: string): void {
  const stream = streams.get(path)
  if (!stream) return
  const parent = stream.forkedFrom
  streams.delete(path)
  if (parent) {
    const p = streams.get(parent)
    if (p) {
      p.refCount = Math.max(0, p.refCount - 1)
      if (p.refCount === 0 && p.softDeleted) deleteWithCascade(streams, parent)
    }
  }
}

// ---------------------------------------------------------------------------
// Service interface
// ---------------------------------------------------------------------------

export interface StoreService {
  create(
    path: string,
    opts: CreateOptions
  ): Effect.Effect<DurableStream, StreamConflictError | StreamNotFoundError | StreamGoneError>

  get(path: string): Effect.Effect<Option.Option<DurableStream>>
  has(path: string): Effect.Effect<boolean>

  append(
    path: string,
    data: Uint8Array,
    opts: AppendOptions
  ): Effect.Effect<
    AppendResult,
    | StreamNotFoundError
    | StreamGoneError
    | StreamConflictError
    | InvalidJsonError
    | EmptyArrayError
    | ContentTypeMismatchError
    | SequenceConflictError
  >

  read(path: string, offset?: string): Effect.Effect<ReadResult, StreamNotFoundError>
  waitForMessages(path: string, offset: string, timeoutMs: number): Effect.Effect<WaitResult, StreamNotFoundError>
  closeStream(path: string): Effect.Effect<CloseResult, StreamNotFoundError | StreamGoneError>
  delete(path: string): Effect.Effect<boolean>
  touchAccess(path: string): Effect.Effect<void>
  formatResponse(path: string, messages: StreamMessage[]): Effect.Effect<Uint8Array, StreamNotFoundError>
  clear(): Effect.Effect<void>
  cancelAllWaits(): Effect.Effect<void>
}

// ---------------------------------------------------------------------------
// Context tag
// ---------------------------------------------------------------------------

export class Store extends Context.Tag("@durable-streams/effect/Store")<Store, StoreService>() {}

// ---------------------------------------------------------------------------
// Long-poll waiter type
// ---------------------------------------------------------------------------

interface Waiter {
  path: string
  offset: string
  deferred: Deferred.Deferred<{ messages: StreamMessage[]; streamClosed: boolean }>
}

// ---------------------------------------------------------------------------
// Service implementation
// ---------------------------------------------------------------------------

function makeStoreService(): Effect.Effect<StoreService> {
  return Effect.gen(function* () {
    const streamsRef = yield* Ref.make(new Map<string, DurableStream>())
    const waitersRef = yield* Ref.make<Waiter[]>([])
    const locksRef = yield* Ref.make(new Map<string, Effect.Semaphore>())

    // ── Producer lock ──────────────────────────────────────────────────────

    const acquireLock = (path: string, producerId: string): Effect.Effect<Effect.Semaphore> =>
      Effect.gen(function* () {
        const key = `${path}:${producerId}`
        const locks = yield* Ref.get(locksRef)
        const existing = locks.get(key)
        if (existing) return existing
        const sem = yield* Effect.makeSemaphore(1)
        yield* Ref.update(locksRef, (m) => new Map([...m, [key, sem]]))
        return sem
      })

    // ── Waiter management ─────────────────────────────────────────────────

    const removeWaiter = (waiter: Waiter): Effect.Effect<void> =>
      Ref.update(waitersRef, (ws) => ws.filter((w) => w !== waiter))

    // Notify waiters after an append.  Only wakes a waiter when messages
    // exist *after its offset* so spurious wake-ups are impossible.
    const notifyWaiters = (path: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const [streams, waiters] = yield* Effect.all([Ref.get(streamsRef), Ref.get(waitersRef)])
        for (const w of waiters) {
          if (w.path !== path) continue
          try {
            const { messages } = readMessages(streams, path, w.offset)
            if (messages.length > 0) {
              yield* Deferred.succeed(w.deferred, { messages, streamClosed: false })
            }
          } catch { /* stream may have been removed */ }
        }
      })

    const notifyWaitersClosed = (path: string): Effect.Effect<void> =>
      Effect.gen(function* () {
        const waiters = yield* Ref.get(waitersRef)
        for (const w of waiters.filter((w) => w.path === path)) {
          yield* Deferred.succeed(w.deferred, { messages: [], streamClosed: true })
        }
      })

    // ── create ────────────────────────────────────────────────────────────

    const create = (
      path: string,
      opts: CreateOptions
    ): Effect.Effect<DurableStream, StreamConflictError | StreamNotFoundError | StreamGoneError> =>
      Effect.gen(function* () {
        const streams = yield* Ref.get(streamsRef)

        // Build the new stream state synchronously; throw tagged errors on conflict.
        const { stream, updatedStreams } = yield* Effect.try({
          try: () => {
            const existingRaw = streams.get(path)
            if (existingRaw) {
              if (isExpired(existingRaw)) {
                // Expired: remove and re-create below
                streams.delete(path)
              } else if (existingRaw.softDeleted) {
                throw new StreamConflictError({ reason: `Stream has active forks — path cannot be reused: ${path}` })
              } else {
                const ctMatch =
                  (normalizeContentType(opts.contentType) || "application/octet-stream") ===
                  (normalizeContentType(existingRaw.contentType) || "application/octet-stream")
                const ok =
                  ctMatch &&
                  opts.ttlSeconds === existingRaw.ttlSeconds &&
                  opts.expiresAt === existingRaw.expiresAt &&
                  (opts.closed ?? false) === existingRaw.closed &&
                  (opts.forkedFrom ?? undefined) === existingRaw.forkedFrom &&
                  (opts.forkOffset === undefined || opts.forkOffset === existingRaw.forkOffset)
                if (ok) return { stream: existingRaw, updatedStreams: streams }
                throw new StreamConflictError({ reason: `Stream already exists with different configuration: ${path}` })
              }
            }

            const isFork = !!opts.forkedFrom
            let forkOffset = ZERO_OFFSET
            let sourceStream: DurableStream | undefined

            if (isFork) {
              sourceStream = streams.get(opts.forkedFrom!)
              if (!sourceStream) throw new StreamNotFoundError({ path: opts.forkedFrom! })
              if (sourceStream.softDeleted) throw new StreamGoneError({ path: opts.forkedFrom! })
              if (isExpired(sourceStream)) throw new StreamNotFoundError({ path: opts.forkedFrom! })

              forkOffset = opts.forkOffset ?? sourceStream.currentOffset
              if (forkOffset < ZERO_OFFSET || sourceStream.currentOffset < forkOffset)
                throw new StreamConflictError({ reason: `Invalid fork offset: ${forkOffset}` })

              if (
                opts.contentType &&
                normalizeContentType(opts.contentType) !== normalizeContentType(sourceStream.contentType)
              ) throw new StreamConflictError({ reason: "Content type mismatch with source stream" })

              sourceStream.refCount++
            }

            let contentType = opts.contentType
            if (!contentType || contentType.trim() === "") {
              contentType = isFork ? sourceStream?.contentType : undefined
            }

            const { ttlSeconds, expiresAt } = isFork
              ? resolveForkExpiry(opts, sourceStream!)
              : { ttlSeconds: opts.ttlSeconds, expiresAt: opts.expiresAt }

            const newStream: DurableStream = {
              path,
              contentType,
              messages: [],
              currentOffset: isFork ? forkOffset : ZERO_OFFSET,
              ttlSeconds,
              expiresAt,
              createdAt: Date.now(),
              lastAccessedAt: Date.now(),
              closed: opts.closed ?? false,
              refCount: 0,
              softDeleted: false,
              forkedFrom: isFork ? opts.forkedFrom : undefined,
              forkOffset: isFork ? forkOffset : undefined,
            }

            if (opts.initialData && opts.initialData.length > 0) {
              try {
                appendToStream(newStream, opts.initialData, true)
              } catch (err) {
                if (isFork && sourceStream) sourceStream.refCount--
                throw err
              }
            }

            const updated = new Map(streams)
            updated.set(path, newStream)
            return { stream: newStream, updatedStreams: updated }
          },
          catch: (e) => e as StreamConflictError | StreamNotFoundError | StreamGoneError,
        })

        yield* Ref.set(streamsRef, updatedStreams)
        return stream
      })

    // ── get / has ─────────────────────────────────────────────────────────

    const get = (path: string): Effect.Effect<Option.Option<DurableStream>> =>
      Ref.get(streamsRef).pipe(
        Effect.map((streams) => {
          const s = getStreamIfLive(streams, path)
          return s ? Option.some(s) : Option.none()
        })
      )

    const has = (path: string): Effect.Effect<boolean> =>
      get(path).pipe(Effect.map((opt) => Option.isSome(opt) && !opt.value.softDeleted))

    // ── read ──────────────────────────────────────────────────────────────

    const read = (path: string, offset?: string): Effect.Effect<ReadResult, StreamNotFoundError> =>
      Ref.get(streamsRef).pipe(
        Effect.flatMap((streams) =>
          Effect.try({
            try: () => readMessages(streams, path, offset),
            catch: (e) => e as StreamNotFoundError,
          })
        )
      )

    // ── waitForMessages ───────────────────────────────────────────────────

    // 1. Register Deferred first (before any check) to avoid the race where a
    //    notification fires between the "no messages" check and registration.
    // 2. Re-check for messages after registration; if found, cancel the wait.
    const waitForMessages = (
      path: string,
      offset: string,
      timeoutMs: number
    ): Effect.Effect<WaitResult, StreamNotFoundError> =>
      Effect.gen(function* () {
        const streams = yield* Ref.get(streamsRef)
        const stream = getStreamIfLive(streams, path)
        if (!stream) return yield* Effect.fail(new StreamNotFoundError({ path }))

        // Forked stream: inherited range is immediately available
        if (stream.forkedFrom && offset < stream.forkOffset!) {
          const result = readMessages(streams, path, offset)
          return { messages: result.messages, timedOut: false, streamClosed: false }
        }

        // Register deferred BEFORE the freshness check
        const deferred = yield* Deferred.make<{ messages: StreamMessage[]; streamClosed: boolean }>()
        const waiter: Waiter = { path, offset, deferred }
        yield* Ref.update(waitersRef, (ws) => [...ws, waiter])

        // Re-check now that we're registered (handles messages that arrived
        // between the first Ref.get above and registration)
        const fresh = yield* Ref.get(streamsRef)
        const freshStream = getStreamIfLive(fresh, path)

        if (!freshStream) {
          yield* removeWaiter(waiter)
          return yield* Effect.fail(new StreamNotFoundError({ path }))
        }
        const current = readMessages(fresh, path, offset)
        if (current.messages.length > 0) {
          yield* removeWaiter(waiter)
          return { messages: current.messages, timedOut: false, streamClosed: false }
        }
        if (freshStream.closed && offset === freshStream.currentOffset) {
          yield* removeWaiter(waiter)
          return { messages: [], timedOut: false, streamClosed: true }
        }

        // Block until notified or timeout.
        // Effect.interruptible restores interruption so the internal timer
        // fiber can fire even though App.toHandled wraps handlers in
        // Effect.uninterruptible to guarantee the response is always sent.
        const notified = yield* Effect.interruptible(
          Deferred.await(deferred).pipe(Effect.timeoutOption(Duration.millis(timeoutMs)))
        ).pipe(Effect.ensuring(removeWaiter(waiter)))

        if (notified._tag === "Some") {
          return { ...notified.value, timedOut: false }
        }

        // Timed out – check whether the stream closed during the wait
        const streamsAfter = yield* Ref.get(streamsRef)
        const streamAfter = getStreamIfLive(streamsAfter, path)
        return { messages: [], timedOut: true, streamClosed: streamAfter?.closed ?? false }
      })

    // ── append ────────────────────────────────────────────────────────────

    const doAppend = (
      path: string,
      data: Uint8Array,
      opts: AppendOptions
    ): Effect.Effect<
      AppendResult,
      | StreamNotFoundError
      | StreamGoneError
      | StreamConflictError
      | InvalidJsonError
      | EmptyArrayError
      | ContentTypeMismatchError
      | SequenceConflictError
    > =>
      Effect.gen(function* () {
        const streams = yield* Ref.get(streamsRef)
        const stream = getStreamIfLive(streams, path)
        if (!stream) return yield* Effect.fail(new StreamNotFoundError({ path }))
        if (stream.softDeleted) return yield* Effect.fail(new StreamGoneError({ path }))

        // Closed stream
        if (stream.closed) {
          const isDupe =
            opts.producerId &&
            stream.closedBy?.producerId === opts.producerId &&
            stream.closedBy.epoch === opts.producerEpoch &&
            stream.closedBy.seq === opts.producerSeq
          if (isDupe) {
            return { message: null, streamClosed: true, finalOffset: stream.currentOffset, producerResult: { status: "duplicate", lastSeq: opts.producerSeq! } }
          }
          return { message: null, streamClosed: true, finalOffset: stream.currentOffset }
        }

        // Content-type check
        if (opts.contentType && stream.contentType) {
          const got = normalizeContentType(opts.contentType)
          const expected = normalizeContentType(stream.contentType)
          if (got !== expected)
            return yield* Effect.fail(new ContentTypeMismatchError({ expected, got }))
        }

        // Producer validation (non-mutating)
        let producerResult: ProducerValidationResult | undefined
        if (
          opts.producerId !== undefined &&
          opts.producerEpoch !== undefined &&
          opts.producerSeq !== undefined
        ) {
          producerResult = validateProducer(stream, opts.producerId, opts.producerEpoch, opts.producerSeq)
          if (producerResult.status !== "accepted")
            return { message: null, producerResult, streamClosed: false }
        }

        // Stream-Seq check
        if (opts.seq !== undefined && stream.lastSeq !== undefined && opts.seq <= stream.lastSeq)
          return yield* Effect.fail(new SequenceConflictError({ got: opts.seq, last: stream.lastSeq }))

        // Close-only (empty body): skip append, just close
        if (data.length === 0 && opts.close) {
          if (producerResult) commitProducerState(stream, producerResult)
          stream.closed = true
          if (opts.producerId !== undefined) {
            stream.closedBy = { producerId: opts.producerId, epoch: opts.producerEpoch!, seq: opts.producerSeq! }
          }
          yield* notifyWaitersClosed(path)
          return { message: null, producerResult, streamClosed: true, finalOffset: stream.currentOffset }
        }

        // Append (can throw InvalidJsonError / EmptyArrayError)
        const message = yield* Effect.try({
          try: () => appendToStream(stream, data),
          catch: (e) => e as InvalidJsonError | EmptyArrayError,
        })

        // Commit mutations after successful append
        if (producerResult) commitProducerState(stream, producerResult)
        if (opts.seq !== undefined) stream.lastSeq = opts.seq
        if (opts.close) {
          stream.closed = true
          if (opts.producerId !== undefined) {
            stream.closedBy = { producerId: opts.producerId, epoch: opts.producerEpoch!, seq: opts.producerSeq! }
          }
          yield* notifyWaitersClosed(path)
        }

        stream.lastAccessedAt = Date.now()
        yield* notifyWaiters(path)
        return { message, producerResult, streamClosed: opts.close ?? false }
      })

    const append = (
      path: string,
      data: Uint8Array,
      opts: AppendOptions
    ): Effect.Effect<AppendResult, StreamNotFoundError | StreamGoneError | StreamConflictError | InvalidJsonError | EmptyArrayError | ContentTypeMismatchError | SequenceConflictError> => {
      if (!opts.producerId) return doAppend(path, data, opts)
      return Effect.flatMap(
        acquireLock(path, opts.producerId),
        (sem) => sem.withPermits(1)(doAppend(path, data, opts))
      )
    }

    // ── closeStream ───────────────────────────────────────────────────────

    const closeStream = (
      path: string
    ): Effect.Effect<CloseResult, StreamNotFoundError | StreamGoneError> =>
      Effect.gen(function* () {
        const streams = yield* Ref.get(streamsRef)
        const stream = getStreamIfLive(streams, path)
        if (!stream) return yield* Effect.fail(new StreamNotFoundError({ path }))
        if (stream.softDeleted) return yield* Effect.fail(new StreamGoneError({ path }))
        const alreadyClosed = stream.closed
        stream.closed = true
        yield* notifyWaitersClosed(path)
        return { finalOffset: stream.currentOffset, alreadyClosed }
      })

    // ── delete ────────────────────────────────────────────────────────────

    const deleteStream = (path: string): Effect.Effect<boolean> =>
      Effect.gen(function* () {
        const streams = yield* Ref.get(streamsRef)
        const existing = streams.get(path)
        if (!existing) return false
        if (existing.softDeleted) return true

        if (existing.refCount > 0) {
          existing.softDeleted = true
          return true
        }

        yield* Ref.update(streamsRef, (m) => {
          const updated = new Map(m)
          deleteWithCascade(updated, path)
          return updated
        })

        // Cancel any pending waiters for this path
        const waiters = yield* Ref.get(waitersRef)
        for (const w of waiters.filter((w) => w.path === path)) {
          yield* Deferred.succeed(w.deferred, { messages: [], streamClosed: false })
        }
        yield* Ref.update(waitersRef, (ws) => ws.filter((w) => w.path !== path))
        return true
      })

    // ── touchAccess ───────────────────────────────────────────────────────

    const touchAccess = (path: string): Effect.Effect<void> =>
      Ref.update(streamsRef, (streams) => {
        const s = streams.get(path)
        if (s) s.lastAccessedAt = Date.now()
        return streams
      })

    // ── formatResponse ────────────────────────────────────────────────────

    const formatResponse = (
      path: string,
      messages: StreamMessage[]
    ): Effect.Effect<Uint8Array, StreamNotFoundError> =>
      Ref.get(streamsRef).pipe(
        Effect.flatMap((streams) => {
          const stream = getStreamIfLive(streams, path)
          if (!stream) return Effect.fail(new StreamNotFoundError({ path }))

          const totalSize = messages.reduce((sum, m) => sum + m.data.length, 0)
          const concatenated = new Uint8Array(totalSize)
          let pos = 0
          for (const msg of messages) { concatenated.set(msg.data, pos); pos += msg.data.length }

          if (normalizeContentType(stream.contentType) === "application/json") {
            return Effect.succeed(formatJsonResponse(concatenated))
          }
          return Effect.succeed(concatenated)
        })
      )

    // ── clear / cancelAllWaits ────────────────────────────────────────────

    const clear = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const waiters = yield* Ref.get(waitersRef)
        for (const w of waiters) {
          yield* Deferred.succeed(w.deferred, { messages: [], streamClosed: false })
        }
        yield* Ref.set(waitersRef, [])
        yield* Ref.set(streamsRef, new Map())
      })

    const cancelAllWaits = (): Effect.Effect<void> =>
      Effect.gen(function* () {
        const waiters = yield* Ref.get(waitersRef)
        for (const w of waiters) {
          yield* Deferred.succeed(w.deferred, { messages: [], streamClosed: false })
        }
        yield* Ref.set(waitersRef, [])
      })

    return {
      create,
      get,
      has,
      read,
      waitForMessages,
      append,
      closeStream,
      delete: deleteStream,
      touchAccess,
      formatResponse,
      clear,
      cancelAllWaits,
    } satisfies StoreService
  })
}

// ---------------------------------------------------------------------------
// Layer
// ---------------------------------------------------------------------------

export const StoreLayer: Layer.Layer<Store> = Layer.effect(Store, makeStoreService())
