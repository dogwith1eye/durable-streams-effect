/**
 * Domain types and tagged errors for the Durable Streams protocol.
 */

import { Data } from "effect"

export const ZERO_OFFSET = "0000000000000000_0000000000000000"
export const PRODUCER_STATE_TTL_MS = 7 * 24 * 60 * 60 * 1000

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

export interface StreamMessage {
  readonly data: Uint8Array
  readonly offset: string
  readonly timestamp: number
}

export interface ProducerState {
  readonly epoch: number
  readonly lastSeq: number
  readonly lastUpdated: number
}

export interface DurableStream {
  path: string
  contentType?: string
  messages: StreamMessage[]
  currentOffset: string
  lastSeq?: string
  ttlSeconds?: number
  expiresAt?: string
  createdAt: number
  lastAccessedAt: number
  producers?: Map<string, ProducerState>
  closed: boolean
  closedBy?: { producerId: string; epoch: number; seq: number }
  forkedFrom?: string
  forkOffset?: string
  refCount: number
  softDeleted: boolean
}

// ---------------------------------------------------------------------------
// Operation options
// ---------------------------------------------------------------------------

export interface CreateOptions {
  contentType?: string
  ttlSeconds?: number
  expiresAt?: string
  initialData?: Uint8Array
  closed?: boolean
  forkedFrom?: string
  forkOffset?: string
}

export interface AppendOptions {
  seq?: string
  contentType?: string
  producerId?: string
  producerEpoch?: number
  producerSeq?: number
  close?: boolean
}

// ---------------------------------------------------------------------------
// Operation results
// ---------------------------------------------------------------------------

export interface ReadResult {
  messages: StreamMessage[]
  upToDate: boolean
}

export interface WaitResult {
  messages: StreamMessage[]
  timedOut: boolean
  streamClosed: boolean
}

export interface CloseResult {
  finalOffset: string
  alreadyClosed: boolean
}

export type ProducerValidationResult =
  | { status: "accepted"; isNew: boolean; producerId: string; proposedState: ProducerState }
  | { status: "duplicate"; lastSeq: number }
  | { status: "stale_epoch"; currentEpoch: number }
  | { status: "invalid_epoch_seq" }
  | { status: "sequence_gap"; expectedSeq: number; receivedSeq: number }
  | { status: "stream_closed" }

export interface AppendResult {
  message: StreamMessage | null
  producerResult?: ProducerValidationResult
  streamClosed: boolean
  finalOffset?: string
}

// ---------------------------------------------------------------------------
// Tagged errors
// ---------------------------------------------------------------------------

export class StreamNotFoundError extends Data.TaggedError("StreamNotFoundError")<{
  readonly path: string
}> {}

export class StreamConflictError extends Data.TaggedError("StreamConflictError")<{
  readonly reason: string
}> {}

export class StreamGoneError extends Data.TaggedError("StreamGoneError")<{
  readonly path: string
}> {}

export class InvalidJsonError extends Data.TaggedError("InvalidJsonError")<{}> {}

export class EmptyArrayError extends Data.TaggedError("EmptyArrayError")<{}> {}

export class ContentTypeMismatchError extends Data.TaggedError("ContentTypeMismatchError")<{
  readonly expected: string
  readonly got: string
}> {}

export class SequenceConflictError extends Data.TaggedError("SequenceConflictError")<{
  readonly got: string
  readonly last: string
}> {}
