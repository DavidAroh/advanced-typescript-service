import { getConfig } from "./config";
import { TypedEventBus } from "./events";
import { JobQueue } from "./queue";
import { LifecycleManager } from "./lifecycle";
import { MetricsCollector } from "./metrics";
import { Handler, LogTiming, ValidateInput, createJobDefinition } from "./decorators";

interface SendEmailInput {
  to: string;
  subject: string;
  body: string;
}

interface ProcessDataInput {
  datasetId: string;
  operations: string[];
}

interface NotifyUserInput {
  userId: string;
  message: string;
  channel: "email" | "sms" | "push";
}

class EmailJobHandler {
  @Handler({ type: "send-email", maxRetries: 3, timeoutMs: 10_000 })
  @LogTiming
  @ValidateInput(["to", "subject", "body"])
  async handle(input: SendEmailInput, signal: AbortSignal): Promise<{ delivered: boolean }> {
    console.log(`  \u2192 Sending email to ${input.to}: "${input.subject}"`);
    await delay(200 + Math.random() * 300, signal);
    if (Math.random() < 0.2) {
      throw new Error("SMTP server returned 503 temporarily unavailable");
    }
    return { delivered: true };
  }
}

class DataProcessorJobHandler {
  @Handler({ type: "process-data", maxRetries: 2, timeoutMs: 15_000 })
  @LogTiming
  async handle(input: ProcessDataInput, signal: AbortSignal): Promise<{ recordsProcessed: number }> {
    console.log(`  \u2192 Processing dataset "${input.datasetId}" with ${input.operations.length} operations`);
    let records = 0;
    for (const op of input.operations) {
      if (signal.aborted) throw new Error("Operation cancelled");
      await delay(100, signal);
      records += Math.floor(Math.random() * 100);
      console.log(`    \u00b7 Operation "${op}" processed ${records} records`);
    }
    if (Math.random() < 0.15) {
      throw new Error("Database connection pool exhausted");
    }
    return { recordsProcessed: records };
  }
}

class NotificationJobHandler {
  @Handler({ type: "notify-user", maxRetries: 1, timeoutMs: 5_000 })
  @LogTiming
  async handle(input: NotifyUserInput, signal: AbortSignal): Promise<{ sent: boolean }> {
    console.log(`  \u2192 Sending ${input.channel} notification to user ${input.userId}`);
    await delay(100 + Math.random() * 200, signal);
    if (input.channel === "push" && Math.random() < 0.3) {
      throw new Error("Push notification service timeout");
    }
    return { sent: true };
  }
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        reject(new DOMException("Aborted", "AbortError"));
      },
      { once: true }
    );
  });
}

async function main(): Promise<void> {
  const config = getConfig();
  console.log("\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557");
  console.log("\u2551  Job Processor Service                          \u2551");
  console.log("\u2560\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2563");
  console.log(`\u2551  Environment:  ${config.env.padEnd(33)}\u2551`);
  console.log(`\u2551  Concurrency:  ${String(config.concurrency.maxConcurrent).padEnd(33)}\u2551`);
  console.log(`\u2551  Max Retries:  ${String(config.retry.maxRetries).padEnd(33)}\u2551`);
  console.log(`\u2551  Shutdown:     ${(config.gracefulShutdownTimeoutMs + "ms").padEnd(33)}\u2551`);
  console.log("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d");

  const events = new TypedEventBus();
  const queue = new JobQueue(config, events);

  queue.register<SendEmailInput>(createJobDefinition(new EmailJobHandler(), "send-email"));
  queue.register<ProcessDataInput>(createJobDefinition(new DataProcessorJobHandler(), "process-data"));
  queue.register<NotifyUserInput>(createJobDefinition(new NotificationJobHandler(), "notify-user"));

  const metrics = new MetricsCollector(events);
  metrics.startPeriodicReporting(10_000);

  const lifecycle = new LifecycleManager(config, events, queue);

  lifecycle.registerHook({
    name: "final-metrics",
    priority: 0,
    teardown: async () => {
      const snap = metrics.snapshot();
      console.log("\n[final] Metrics summary:");
      console.log(JSON.stringify(snap, null, 2));
    },
  });

  lifecycle.start();

  console.log("\n[jobs] Enqueuing sample jobs...\n");

  for (let i = 0; i < 5; i++) {
    queue.enqueue<SendEmailInput>("send-email", {
      to: `user${i}@example.com`,
      subject: `Welcome email #${i}`,
      body: `Hello user${i}, welcome to the service!`,
    });
  }

  for (let i = 0; i < 3; i++) {
    queue.enqueue<ProcessDataInput>("process-data", {
      datasetId: `ds-${Date.now()}-${i}`,
      operations: ["filter", "transform", "aggregate"],
    });
  }

  for (let i = 0; i < 4; i++) {
    queue.enqueue<NotifyUserInput>("notify-user", {
      userId: `user-${i}`,
      message: `You have a new notification #${i}`,
      channel: (["email", "sms", "push"] as const)[i % 3],
    });
  }

  setTimeout(() => {
    console.log("\n[stats] Queue statistics:");
    console.log(JSON.stringify(queue.getStats(), null, 2));
    console.log("\n[stats] Metrics:");
    console.log(JSON.stringify(metrics.snapshot(), null, 2));
  }, 5000);

  setTimeout(() => {
    console.log("\n[demo] Simulating graceful shutdown...\n");
    process.emit("SIGTERM", "SIGTERM");
  }, 15_000);
}

main().catch((err) => {
  console.error("[fatal] Unhandled error:", err);
  process.exit(1);
});
