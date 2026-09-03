# Architecture & Design Documentation

## Table of Contents

1. [How the Node.js Event Loop Works](#1-how-the-nodejs-event-loop-works)
2. [Potential Performance Bottlenecks](#2-potential-performance-bottlenecks)
3. [Error-Handling Strategy](#3-error-handling-strategy)
4. [Architectural Decisions](#4-architectural-decisions)

---

## 1. How the Node.js Event Loop Works

The Node.js event loop is the core mechanism that enables non-blocking I/O in a single-threaded runtime. It's an implementation of the **libuv** library's event loop, which processes callbacks in a specific order each "tick."

### Event Loop Phases (in order)

```
   ┌───────────────────────────┐
┌─>│        timers              │  ← setTimeout(), setInterval() callbacks
│  └─────────────┬─────────────┘
│  ┌─────────────┴─────────────┐
│  │     pending callbacks     │  ← I/O callbacks deferred from previous tick
│  └─────────────┬─────────────┘
│  ┌─────────────┴─────────────┐
│  │       idle, prepare       │  ← internal only
│  └─────────────┬─────────────┘
│  ┌─────────────┴─────────────┐
│  │         poll              │  ← new I/O events, execute I/O callbacks
│  └─────────────┬─────────────┘
│  ┌─────────────┴─────────────┐
│  │         check             │  ← setImmediate() callbacks
│  └─────────────┬─────────────┘
│  ┌─────────────┴─────────────┐
│  │     close callbacks       │  ← socket.on('close', ...)
│  └─────────────┬─────────────┘
└────────────────┘
```

### Microtask Queue (between each phase)

After **every phase** completes, Node.js drains the **microtask queue**:
- `Promise.then()` callbacks
- `queueMicrotask()` callbacks
- `process.nextTick()` callbacks (highest priority microtask)

### How This Service Uses the Event Loop

| Component | Event Loop Phase | Why |
|-----------|-----------------|-----|
| `setInterval` poll loop | **timers** | Periodically checks if jobs can start |
| `setTimeout` for retry delays | **timers** | Schedules retry after backoff delay |
| `Promise.race` for timeout | **microtask** | Timeout rejection flows through microtask queue |
| `handler.handler(input, signal)` | **poll** | Most I/O-bound work (simulated network calls) |
| `AbortController.abort()` | **microtask** | Signal listeners fire as microtasks |

### Key Insight: This Service is I/O-Bound

All job handlers simulate **I/O-bound work** (network calls, database queries). The `delay()` function uses `setTimeout`, which registers in the timers phase — it does **not** block the event loop. This means:

- 5 concurrent email jobs don't block 5 data processing jobs
- The event loop can process all of them "simultaneously" through async scheduling
- CPU remains idle during the `delay()` — it's just waiting for timers

If we had **CPU-bound** work (image processing, crypto, heavy computation), we'd need `worker_threads` to avoid blocking the event loop.

---

## 2. Potential Performance Bottlenecks

### 2.1 Event Loop Starvation

**Risk**: If a handler does synchronous CPU work, the event loop blocks and all other jobs stall.

**Mitigation in this code**:
- All handlers are `async` and use `await` for yielding
- No synchronous loops over large datasets
- `performance.now()` for timing (not `Date.now()` — though both are fine for this use case)

**What to watch for in production**:
```typescript
// ❌ BAD: Blocks the event loop for 1 second
function heavyComputation(data: number[]): number {
  let result = 0;
  for (let i = 0; i < 1_000_000_000; i++) {
    result += Math.sin(data[i % data.length]);
  }
  return result;
}

// ✅ GOOD: Yields to the event loop periodically
async function heavyComputation(data: number[]): Promise<number> {
  let result = 0;
  for (let i = 0; i < 1_000_000_000; i++) {
    result += Math.sin(data[i % data.length]);
    if (i % 1_000_000 === 0) {
      await new Promise(resolve => setImmediate(resolve));
    }
  }
  return result;
}
```

### 2.2 Memory Growth in Job Store

**Risk**: The `jobs` Map in `JobQueue` grows unboundedly — completed/failed jobs are never removed.

**Mitigation for production**:
- Implement a TTL-based cleanup (e.g., remove completed jobs after 5 minutes)
- Or use a circular buffer / LRU cache for job metadata
- Monitor with `process.memoryUsage().heapUsed`

### 2.3 Timer Accumulation from Retries

**Risk**: Each retry schedules a `setTimeout`. With thousands of failing jobs, you accumulate many pending timers.

**Mitigation**:
- The current code creates one timer per retry — reasonable for moderate loads
- For extreme scale, consider a priority queue (min-heap) instead of individual timers

### 2.4 Backpressure Missing

**Risk**: There's no maximum queue size — enqueue can be called infinitely, consuming memory.

**Improvement**: Add a `maxQueueSize` to `ConcurrencyConfig` and throw `QueueFullError` when exceeded (the error class exists but isn't used yet).

### 2.5 Metrics Array Growth

**Risk**: `durations` array in `MetricsCollector` is capped at 1000 entries (then trimmed to 500). This is a reasonable trade-off but the `slice(-500)` creates a new array each time.

**Better approach**: Use a circular buffer or ring buffer for O(1) insertions.

---

## 3. Error-Handling Strategy

### 3.1 Layered Error Handling

```
┌─────────────────────────────────────┐
│  Application Layer (lifecycle.ts)   │  ← catches shutdown errors
│  ┌─────────────────────────────────┐│
│  │  Queue Layer (queue.ts)         ││  ← catches per-job errors
│  │  ┌─────────────────────────────┐││
│  │  │  Handler Layer (job defs)   │││  ← throws domain-specific errors
│  │  └─────────────────────────────┘││
│  └─────────────────────────────────┘│
└─────────────────────────────────────┘
```

### 3.2 Error Type Hierarchy

```
Error
└── AppError (abstract)
    ├── JobTimeoutError      (code: JOB_TIMEOUT)
    ├── JobValidationError   (code: JOB_VALIDATION)
    ├── JobHandlerError      (code: JOB_HANDLER)
    ├── QueueFullError       (code: QUEUE_FULL)
    └── GracefulShutdownError (code: SHUTDOWN_ERROR)
```

**Design choice**: We use an **abstract class** (not interface) for `AppError` because:
1. We need shared behavior (`toJSON()` serialization, `context` property)
2. We need `Object.setPrototypeOf` for correct `instanceof` checks
3. Interfaces can't enforce constructor signatures

### 3.3 Type Narrowing Pattern

```typescript
try {
  await handler.handler(input, signal);
} catch (err) {
  // err is `unknown` — we narrow it:
  if (isJobTimeoutError(err)) {
    // err narrowed to JobTimeoutError
    // → timeout is always retryable
  } else if (isJobHandlerError(err)) {
    // err narrowed to JobHandlerError
    // → check inner error
  } else {
    // err is still `unknown` — log everything
  }
}
```

### 3.4 Retry Strategy

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Backoff** | Exponential with jitter | Prevents thundering herd when many jobs fail simultaneously |
| **Formula** | `min(base × 2^attempt, max) ± 25% jitter` | Bounded delay with randomness |
| **Max retries** | Configurable per-job and globally | Different jobs have different tolerance |
| **Timeout** | Per-job via `Promise.race` | Prevents stuck handlers from consuming slots |

### 3.5 Error Propagation Rules

1. **Handler errors** → caught by queue → retry or fail
2. **Queue errors** → logged, job marked failed
3. **Shutdown errors** → logged, process exits anyway
4. **Event listener errors** → caught and logged, never crash the emitter
5. **Unhandled rejections** → caught by `main().catch()` at the top level

### 3.6 AbortSignal Pattern

```typescript
// The queue creates an AbortController per job
const controller = new AbortController();

// It races the handler against a timeout
await Promise.race([
  handler.handler(input, controller.signal),
  this.timeout(timeoutMs, jobId),
]);

// On shutdown, the queue aborts all active jobs
controller.abort(); // → signal.aborted = true
```

Handlers **must** check `signal.aborted` and throw `DOMException("Aborted", "AbortError")` when cancelled. This is the cooperative cancellation pattern.

---

## 4. Architectural Decisions

### 4.1 Composition Over Inheritance

The service is built by **composing** independent modules, not by creating deep class hierarchies:

```
LifecycleManager ──has──→ JobQueue ──has──→ TypedEventBus
                          JobQueue ──has──→ AppConfig
                          MetricsCollector ──has──→ TypedEventBus
```

- No class extends another class (except `AppError` hierarchy — where inheritance is justified for shared behavior)
- Handlers are objects conforming to `JobDefinition` interface — not subclasses
- The event bus is injected, not inherited

### 4.2 Dependency Inversion

High-level modules depend on abstractions:

```typescript
// JobQueue depends on TypedEventBus (interface), not a concrete logger
constructor(
  private readonly config: AppConfig,        // interface
  private readonly events: TypedEventBus     // interface
) {}

// LifecycleManager depends on JobQueue (class used as interface)
constructor(
  private readonly config: AppConfig,
  private readonly events: TypedEventBus,
  private readonly queue: JobQueue           // any queue-like object works
) {}
```

### 4.3 Separation of Concerns

| Module | Responsibility | Knows about |
|--------|---------------|-------------|
| `config.ts` | Environment parsing | Nothing (pure) |
| `types.ts` | Type contracts | Nothing (pure) |
| `errors.ts` | Error hierarchy | Nothing (pure) |
| `events.ts` | Pub/sub messaging | Nothing (pure) |
| `queue.ts` | Job scheduling + retry | config, events, errors, types |
| `lifecycle.ts` | Shutdown orchestration | config, events, queue |
| `metrics.ts` | Performance counters | events |
| `decorators.ts` | Cross-cutting concerns | types |
| `index.ts` | Wiring + sample handlers | everything |

### 4.4 Strongly Typed Boundaries

Every module boundary has explicit types — no `any`, no implicit `unknown`:

```typescript
// Queue emits strongly typed events
await this.events.emit("job:completed", {
  jobId: metadata.id,     // string — enforced by EventMap
  type: metadata.type,    // string — enforced by EventMap
  durationMs,             // number — enforced by EventMap
});
```

The `EventMap` interface acts as a **contract registry** — changing an event's payload type causes compile errors everywhere it's emitted or consumed.

### 4.5 Decorators for Cross-Cutting Concerns

Decorators (`@Handler`, `@LogTiming`, `@ValidateInput`) separate **what** a handler does from **how** it's observed:

```typescript
class EmailJobHandler {
  @Handler({ type: "send-email", maxRetries: 3, timeoutMs: 10_000 })
  @LogTiming                                          // ← cross-cutting
  @ValidateInput(["to", "subject", "body"])           // ← cross-cutting
  async handle(input: SendEmailInput, signal: AbortSignal) {
    // Pure business logic — no logging, no validation code here
  }
}
```

### 4.6 Technical Debt Acknowledged

| Debt | Impact | Mitigation |
|------|--------|------------|
| Job store grows unbounded | Memory leak over days | Add TTL cleanup |
| No persistence layer | Jobs lost on restart | Add SQLite/Redis backend |
| No distributed coordination | Single-instance only | Add Redis-based locking |
| Metrics use array slicing | O(n) on trim | Use ring buffer |
| No circuit breaker | Failing services get hammered | Add circuit breaker pattern |
| Decorators are experimental | TC39 stage 3 | Stable in TS, monitor TC39 |
| No structured logging | Console.log only | Add pino/winston |
| No tracing | No request correlation | Add OpenTelemetry |
