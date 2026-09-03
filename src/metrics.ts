import { TypedEventBus } from "./events";

interface MetricSnapshot {
  jobsEnqueued: number;
  jobsStarted: number;
  jobsCompleted: number;
  jobsFailed: number;
  jobsRetrying: number;
  avgDurationMs: number;
  uptimeMs: number;
  startedAt: number;
}

export class MetricsCollector {
  private counters = {
    enqueued: 0,
    started: 0,
    completed: 0,
    failed: 0,
    retrying: 0,
  };

  private durations: number[] = [];
  private readonly startedAt = Date.now();

  constructor(private readonly events: TypedEventBus) {
    this.subscribe();
  }

  private subscribe(): void {
    this.events.on("job:enqueued", () => {
      this.counters.enqueued++;
    });

    this.events.on("job:started", () => {
      this.counters.started++;
    });

    this.events.on("job:completed", (payload) => {
      this.counters.completed++;
      this.durations.push(payload.durationMs);
      if (this.durations.length > 1000) {
        this.durations = this.durations.slice(-500);
      }
    });

    this.events.on("job:failed", () => {
      this.counters.failed++;
    });

    this.events.on("job:retrying", () => {
      this.counters.retrying++;
    });
  }

  snapshot(): MetricSnapshot {
    const avg =
      this.durations.length > 0
        ? this.durations.reduce((a, b) => a + b, 0) / this.durations.length
        : 0;

    return {
      jobsEnqueued: this.counters.enqueued,
      jobsStarted: this.counters.started,
      jobsCompleted: this.counters.completed,
      jobsFailed: this.counters.failed,
      jobsRetrying: this.counters.retrying,
      avgDurationMs: Math.round(avg * 100) / 100,
      uptimeMs: Date.now() - this.startedAt,
      startedAt: this.startedAt,
    };
  }

  startPeriodicReporting(intervalMs: number = 10_000): ReturnType<typeof setInterval> {
    return setInterval(() => {
      const snap = this.snapshot();
      console.log(
        `[metrics] enqueued=${snap.jobsEnqueued} started=${snap.jobsStarted} ` +
        `completed=${snap.jobsCompleted} failed=${snap.jobsFailed} ` +
        `retrying=${snap.jobsRetrying} avgDuration=${snap.avgDurationMs}ms ` +
        `uptime=${Math.round(snap.uptimeMs / 1000)}s`
      );
    }, intervalMs);
  }
}
