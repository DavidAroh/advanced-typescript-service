import { AppConfig } from "./types";
import { TypedEventBus } from "./events";
import { JobQueue } from "./queue";

export interface ShutdownHook {
  name: string;
  priority: number;
  teardown: () => Promise<void>;
}

export class LifecycleManager {
  private hooks: ShutdownHook[] = [];
  private isShuttingDown = false;
  private signalHandlers: Array<{ event: string; handler: () => void }> = [];

  constructor(
    private readonly config: AppConfig,
    private readonly events: TypedEventBus,
    private readonly queue: JobQueue
  ) {}

  registerHook(hook: ShutdownHook): void {
    this.hooks.push(hook);
    this.hooks.sort((a, b) => a.priority - b.priority);
    console.log(`[lifecycle] Registered shutdown hook: "${hook.name}" (priority=${hook.priority})`);
  }

  start(): void {
    console.log(`[lifecycle] Starting in ${this.config.env} mode`);

    this.registerSignal("SIGTERM", () => this.initiateShutdown("SIGTERM"));
    this.registerSignal("SIGINT", () => this.initiateShutdown("SIGINT"));

    this.registerHook({
      name: "job-queue-drain",
      priority: 10,
      teardown: async () => {
        await this.queue.gracefulDrain();
      },
    });

    this.registerHook({
      name: "event-bus-cleanup",
      priority: 100,
      teardown: async () => {
        this.events.clear();
        console.log("[lifecycle] Event bus cleared");
      },
    });

    this.queue.start();

    console.log("[lifecycle] Application started successfully");
  }

  private async initiateShutdown(reason: string): Promise<void> {
    if (this.isShuttingDown) {
      console.log("[lifecycle] Shutdown already in progress, ignoring");
      return;
    }

    this.isShuttingDown = true;
    console.log(`[lifecycle] Initiating graceful shutdown (reason: ${reason})`);

    for (const { event, handler } of this.signalHandlers) {
      process.removeListener(event, handler);
    }

    void this.events.emit("shutdown:initiated", { reason });

    const forceExitTimer = setTimeout(() => {
      console.error(
        `[lifecycle] Forced exit after ${this.config.gracefulShutdownTimeoutMs}ms`
      );
      process.exit(1);
    }, this.config.gracefulShutdownTimeoutMs);

    forceExitTimer.unref();

    try {
      for (const hook of this.hooks) {
        console.log(`[lifecycle] Running shutdown hook: "${hook.name}"`);
        try {
          await hook.teardown();
        } catch (err) {
          console.error(
            `[lifecycle] Hook "${hook.name}" failed:`,
            err instanceof Error ? err.message : err
          );
        }
      }

      console.log("[lifecycle] Graceful shutdown complete");
    } catch (err) {
      console.error("[lifecycle] Error during shutdown:", err);
    } finally {
      clearTimeout(forceExitTimer);
      process.exit(0);
    }
  }

  private registerSignal(signal: string, handler: () => void): void {
    process.on(signal, handler);
    this.signalHandlers.push({ event: signal, handler });
  }

  get isShutdownInProgress(): boolean {
    return this.isShuttingDown;
  }
}
