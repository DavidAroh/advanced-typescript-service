export interface ErrorClass<T extends Error = Error> {
  new (...args: any[]): T;
  prototype: T;
}

export abstract class AppError extends Error {
  abstract readonly code: string;
  readonly timestamp: number;
  readonly context?: Record<string, unknown>;

  constructor(
    message: string,
    options?: { cause?: Error; context?: Record<string, unknown> }
  ) {
    super(message, { cause: options?.cause });
    this.name = this.constructor.name;
    this.timestamp = Date.now();
    this.context = options?.context;
    Object.setPrototypeOf(this, new.target.prototype);
  }

  toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      timestamp: this.timestamp,
      context: this.context,
      stack: this.stack,
      cause:
        this.cause instanceof Error
          ? { name: this.cause.name, message: this.cause.message }
          : this.cause ?? null,
    };
  }
}

export class JobTimeoutError extends AppError {
  readonly code = "JOB_TIMEOUT";
  readonly timeoutMs: number;

  constructor(timeoutMs: number, context?: Record<string, unknown>) {
    super(`Job timed out after ${timeoutMs}ms`, { context });
    this.timeoutMs = timeoutMs;
  }
}

export class JobValidationError extends AppError {
  readonly code = "JOB_VALIDATION";
  readonly field?: string;

  constructor(message: string, field?: string) {
    super(message, { context: { field } });
    this.field = field;
  }
}

export class JobHandlerError extends AppError {
  readonly code = "JOB_HANDLER";
  readonly jobType: string;
  readonly attempt: number;

  constructor(
    jobType: string,
    attempt: number,
    cause: Error
  ) {
    super(`Handler for "${jobType}" failed on attempt ${attempt}`, {
      cause,
      context: { jobType, attempt },
    });
    this.jobType = jobType;
    this.attempt = attempt;
  }
}

export class QueueFullError extends AppError {
  readonly code = "QUEUE_FULL";
  readonly queueSize: number;
  readonly maxSize: number;

  constructor(queueSize: number, maxSize: number) {
    super(`Job queue is full (${queueSize}/${maxSize})`, {
      context: { queueSize, maxSize },
    });
    this.queueSize = queueSize;
    this.maxSize = maxSize;
  }
}

export class GracefulShutdownError extends AppError {
  readonly code = "SHUTDOWN_ERROR";

  constructor(cause: Error) {
    super("Error during graceful shutdown", { cause });
  }
}

export function isErrorInstanceOf<T extends Error>(
  err: unknown,
  errorClass: ErrorClass<T>
): err is T {
  return err instanceof errorClass;
}

export const isJobTimeoutError = (e: unknown): e is JobTimeoutError =>
  isErrorInstanceOf(e, JobTimeoutError);

export const isJobValidationError = (e: unknown): e is JobValidationError =>
  isErrorInstanceOf(e, JobValidationError);

export const isJobHandlerError = (e: unknown): e is JobHandlerError =>
  isErrorInstanceOf(e, JobHandlerError);

export const isQueueFullError = (e: unknown): e is QueueFullError =>
  isErrorInstanceOf(e, QueueFullError);

export function assertNever(value: never, message?: string): never {
  throw new Error(
    message ?? `Unexpected value: ${JSON.stringify(value)}`
  );
}
