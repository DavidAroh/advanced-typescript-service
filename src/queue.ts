import { randomUUID } from "crypto";
import { AppConfig, Job, JobDefinition, JobMetadata, JobState, EnqueuedJob } from "./types";
import { TypedEventBus } from "./events";
import {
  JobTimeoutError,
  isJobTimeoutError,
  isJobHandlerError,
} from "./errors";

export class JobQueue {
  private pending: Job[] = [];
  private active = new Map<string, Job>();
  private jobs = new Map<string, Job>();
  private abortControllers = new Map<string, AbortController>();
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private draining = false;
  private handlers = new Map<string, JobDefinition>();

  constructor(
    private readonly config: AppConfig,
    private readonly events: TypedEventBus
  ) {}

  register<TInput>(definition: JobDefinition<TInput>): void {
    if (this.handlers.has(definition.type)) {
      throw new Error(`Handler already registered for type "${definition.type}"`);
    }
    this.handlers.set(definition.type, definition as JobDefinition);
    console.log(`[queue] Registered handler for "${definition.type}"`);
  }

  enqueue<TInput>(type: string, input: TInput, overrides?: Partial<JobMetadata>): EnqueuedJob {
    if (!this.handlers.has(type)) {
      throw new Error(`No handler registered for type "${type}"`);
    }

    if (this.draining) {
      throw new Error("Cannot enqueue jobs during shutdown");
    }

    const metadata: JobMetadata = {
      id: randomUUID(),
      type,
      enqueuedAt: Date.now(),
      maxRetries: overrides?.maxRetries ?? this.config.jobDefaults.maxRetries,
      timeoutMs: overrides?.timeoutMs ?? this.config.jobDefaults.timeoutMs,
    };

    const state: JobState = { status: "pending", enqueuedAt: metadata.enqueuedAt };
    const job: Job<TInput> = { metadata, input, state } as Job<TInput>;

    this.jobs.set(metadata.id, job as Job);
    this.pending.push(job);

    console.log(`[queue] Enqueued job ${metadata.id} (type="${type}", pending=${this.pending.length})`);

    void this.events.emit("job:enqueued", { jobId: metadata.id, type });

    this.scheduleNext();

    return { id: metadata.id, type: metadata.type };
  }

  start(): void {
    if (this.pollTimer) return;
    console.log(
      `[queue] Starting poll loop (maxConcurrent=${this.config.concurrency.maxConcurrent}, pollInterval=${this.config.concurrency.pollIntervalMs}ms)`
    );
    this.pollTimer = setInterval(() => this.drain(), this.config.concurrency.pollIntervalMs);
    this.drain();
  }

  stop(): void {
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private scheduleNext(): void {
  }

  private async drain(): Promise<void> {
    const slotsAvailable = this.config.concurrency.maxConcurrent - this.active.size;

    if (slotsAvailable <= 0 || this.pending.length === 0) return;

    const toStart = this.pending.splice(0, slotsAvailable);

    for (const job of toStart) {
      void this.executeJob(job);
    }
  }

  private async executeJob(job: Job): Promise<void> {
    const { metadata } = job;
    const handler = this.handlers.get(metadata.type);
    if (!handler) {
      this.moveToFailed(job, new Error(`No handler for type "${metadata.type}"`));
      return;
    }

    const attempt = this.getAttempt(job);
    const controller = new AbortController();
    this.abortControllers.set(metadata.id, controller);

    job.state = {
      status: "running",
      startedAt: Date.now(),
      attempt,
    };
    this.active.set(metadata.id, job);

    void this.events.emit("job:started", {
      jobId: metadata.id,
      type: metadata.type,
      attempt,
    });

    console.log(
      `[queue] Executing job ${metadata.id} (type="${metadata.type}", attempt=${attempt})`
    );

    try {
      const result = await Promise.race([
        handler.handler(job.input, controller.signal),
        this.timeout(metadata.timeoutMs, metadata.id),
      ]);

      const durationMs = Date.now() - (job.state as { startedAt: number }).startedAt;
      job.state = {
        status: "completed",
        startedAt: (job.state as { startedAt: number }).startedAt,
        completedAt: Date.now(),
        result,
      };

      this.active.delete(metadata.id);
      this.abortControllers.delete(metadata.id);

      console.log(
        `[queue] Job ${metadata.id} completed in ${durationMs}ms`
      );
      void this.events.emit("job:completed", {
        jobId: metadata.id,
        type: metadata.type,
        durationMs,
      });
    } catch (err) {
      this.handleError(job, err);
    }
  }

  private handleError(job: Job, err: unknown): void {
    const { metadata } = job;
    const error = err instanceof Error ? err : new Error(String(err));
    const durationMs = Date.now() - (job.state as { startedAt: number }).startedAt;

    this.active.delete(metadata.id);
    this.abortControllers.delete(metadata.id);

    if (isJobTimeoutError(err)) {
      console.warn(
        `[queue] Job ${metadata.id} timed out after ${durationMs}ms`
      );
    } else if (isJobHandlerError(err)) {
      console.warn(
        `[queue] Job ${metadata.id} handler error: ${error.message}`
      );
    } else {
      console.error(`[queue] Job ${metadata.id} unexpected error:`, error);
    }

    const currentAttempt = this.getAttempt(job);
    if (currentAttempt < metadata.maxRetries) {
      this.scheduleRetry(job, error);
    } else {
      this.moveToFailed(job, error);
    }
  }

  private scheduleRetry(job: Job, error: Error): void {
    const { metadata } = job;
    const attempt = this.getAttempt(job);
    const { baseDelayMs, maxDelayMs, backoffMultiplier } = this.config.retry;

    const exponentialDelay = Math.min(
      baseDelayMs * Math.pow(backoffMultiplier, attempt),
      maxDelayMs
    );
    const jitter = exponentialDelay * 0.25 * (Math.random() * 2 - 1);
    const delay = Math.max(0, Math.round(exponentialDelay + jitter));
    const nextRetryAt = Date.now() + delay;

    job.state = {
      status: "retrying",
      startedAt: (job.state as { startedAt: number }).startedAt,
      failedAt: Date.now(),
      error,
      attempt,
      nextRetryAt,
    };

    console.log(
      `[queue] Job ${metadata.id} scheduled for retry #${attempt + 1} in ${delay}ms`
    );

    void this.events.emit("job:retrying", {
      jobId: metadata.id,
      type: metadata.type,
      attempt: attempt + 1,
      nextRetryAt,
    });

    setTimeout(() => {
      if (!this.draining) {
        job.state = { status: "pending", enqueuedAt: Date.now() };
        this.pending.push(job);
        this.drain();
      }
    }, delay);
  }

  private moveToFailed(job: Job, error: Error): void {
    const { metadata } = job;
    job.state = {
      status: "failed",
      startedAt: (job.state as { startedAt: number }).startedAt,
      failedAt: Date.now(),
      error,
      attempts: this.getAttempt(job) + 1,
    };

    this.active.delete(metadata.id);
    this.abortControllers.delete(metadata.id);

    const attempts = this.getAttempt(job) + 1;
    console.error(
      `[queue] Job ${metadata.id} FAILED after ${attempts} attempts: ${error.message}`
    );

    void this.events.emit("job:failed", {
      jobId: metadata.id,
      type: metadata.type,
      error: error.message,
      attempts,
    });
  }

  private getAttempt(job: Job): number {
    const s = job.state;
    switch (s.status) {
      case "running":
        return s.attempt;
      case "retrying":
        return s.attempt;
      case "completed":
        return 0;
      case "failed":
        return s.attempts;
      case "pending":
        return 0;
      default:
        return 0;
    }
  }

  private timeout(timeoutMs: number, jobId: string): Promise<never> {
    return new Promise<never>((_resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new JobTimeoutError(timeoutMs, { jobId }));
      }, timeoutMs);

      const cleanup = () => clearTimeout(timer);
      const controller = this.abortControllers.get(jobId);
      if (controller) {
        controller.signal.addEventListener("abort", cleanup, { once: true });
      }
    });
  }

  async gracefulDrain(): Promise<void> {
    console.log(`[queue] Draining: ${this.active.size} active, ${this.pending.length} pending`);
    this.draining = true;
    this.stop();

    for (const [jobId, controller] of this.abortControllers) {
      controller.abort();
      console.log(`[queue] Aborted job ${jobId}`);
    }

    const drainStart = Date.now();
    const maxWait = this.config.gracefulShutdownTimeoutMs;

    while (this.active.size > 0 && Date.now() - drainStart < maxWait) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    if (this.active.size > 0) {
      console.warn(
        `[queue] ${this.active.size} jobs still active after ${maxWait}ms timeout`
      );
    }

    console.log(`[queue] Drain complete: ${this.pending.length} jobs discarded`);
  }

  getStats(): {
    pending: number;
    active: number;
    total: number;
  } {
    return {
      pending: this.pending.length,
      active: this.active.size,
      total: this.jobs.size,
    };
  }

  getJob(id: string): Job | undefined {
    return this.jobs.get(id);
  }
}
