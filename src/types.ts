export type JobStatus = "pending" | "running" | "completed" | "failed" | "retrying";

export interface JobPending {
  status: "pending";
  enqueuedAt: number;
}

export interface JobRunning {
  status: "running";
  startedAt: number;
  attempt: number;
}

export interface JobCompleted {
  status: "completed";
  startedAt: number;
  completedAt: number;
  result: unknown;
}

export interface JobFailed {
  status: "failed";
  startedAt: number;
  failedAt: number;
  error: Error;
  attempts: number;
}

export interface JobRetrying {
  status: "retrying";
  startedAt: number;
  failedAt: number;
  error: Error;
  attempt: number;
  nextRetryAt: number;
}

export type JobState =
  | JobPending
  | JobRunning
  | JobCompleted
  | JobFailed
  | JobRetrying;

export interface JobDefinition<TInput = unknown> {
  type: string;
  handler: (input: TInput, signal: AbortSignal) => Promise<unknown>;
}

export interface JobMetadata {
  id: string;
  type: string;
  enqueuedAt: number;
  maxRetries: number;
  timeoutMs: number;
}

export interface Job<TInput = unknown> {
  metadata: JobMetadata;
  input: TInput;
  state: JobState;
}

export type EnqueuedJob = Pick<JobMetadata, "id" | "type">;

export interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
}

export interface ConcurrencyConfig {
  maxConcurrent: number;
  pollIntervalMs: number;
}

export interface AppConfig {
  env: "development" | "staging" | "production";
  port: number;
  gracefulShutdownTimeoutMs: number;
  retry: RetryConfig;
  concurrency: ConcurrencyConfig;
  jobDefaults: Readonly<Pick<JobMetadata, "maxRetries" | "timeoutMs">>;
}

export interface JobHandler<TInput = unknown> {
  type: string;
  handle: (input: TInput, signal: AbortSignal) => Promise<unknown>;
}

export interface EventMap {
  "job:enqueued": { jobId: string; type: string };
  "job:started": { jobId: string; type: string; attempt: number };
  "job:completed": { jobId: string; type: string; durationMs: number };
  "job:failed": { jobId: string; type: string; error: string; attempts: number };
  "job:retrying": { jobId: string; type: string; attempt: number; nextRetryAt: number };
  "shutdown:initiated": { reason: string };
  "shutdown:draining": { pendingJobs: number };
  "shutdown:complete": {};
}

export type EventPayload<K extends keyof EventMap> = EventMap[K];

export interface HandlerMetadata {
  type: string;
  maxRetries?: number;
  timeoutMs?: number;
}
