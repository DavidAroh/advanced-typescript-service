import { AppConfig } from "./types";

function envInt(name: string, fallback: number, min?: number, max?: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;

  const parsed = parseInt(raw, 10);
  if (Number.isNaN(parsed)) {
    console.warn(`[config] ${name}="${raw}" is not a valid integer, using default ${fallback}`);
    return fallback;
  }
  if (min !== undefined && parsed < min) {
    console.warn(`[config] ${name}=${parsed} is below minimum ${min}, clamping`);
    return min;
  }
  if (max !== undefined && parsed > max) {
    console.warn(`[config] ${name}=${parsed} is above maximum ${max}, clamping`);
    return max;
  }
  return parsed;
}

function envString<T extends string>(
  name: string,
  allowed: readonly T[],
  fallback: T
): T {
  const raw = process.env[name];
  if (raw === undefined) return fallback;

  if ((allowed as readonly string[]).includes(raw)) {
    return raw as T;
  }
  console.warn(
    `[config] ${name}="${raw}" is not one of [${allowed.join(", ")}], using default "${fallback}"`
  );
  return fallback;
}

export function createConfig(overrides?: Partial<AppConfig>): AppConfig {
  const env = envString("NODE_ENV", ["development", "staging", "production"] as const, "development");

  const defaults: AppConfig = {
    env,
    port: envInt("PORT", 3000, 1, 65535),
    gracefulShutdownTimeoutMs: envInt("SHUTDOWN_TIMEOUT_MS", 30_000, 1000, 300_000),
    retry: {
      maxRetries: envInt("RETRY_MAX", 3, 0, 10),
      baseDelayMs: envInt("RETRY_BASE_DELAY_MS", 1000, 100, 30_000),
      maxDelayMs: envInt("RETRY_MAX_DELAY_MS", 30_000, 1000, 300_000),
      backoffMultiplier: 2,
    },
    concurrency: {
      maxConcurrent: envInt("MAX_CONCURRENT", 5, 1, 100),
      pollIntervalMs: envInt("POLL_INTERVAL_MS", 100, 10, 5000),
    },
    jobDefaults: {
      maxRetries: envInt("JOB_MAX_RETRIES", 3, 0, 10),
      timeoutMs: envInt("JOB_TIMEOUT_MS", 30_000, 1000, 300_000),
    },
  };

  if (overrides) {
    return {
      ...defaults,
      ...overrides,
      retry: { ...defaults.retry, ...overrides.retry },
      concurrency: { ...defaults.concurrency, ...overrides.concurrency },
      jobDefaults: { ...defaults.jobDefaults, ...overrides.jobDefaults },
    };
  }

  return defaults;
}

let _config: AppConfig | null = null;

export function getConfig(): AppConfig {
  if (!_config) {
    _config = Object.freeze(createConfig());
  }
  return _config;
}

export function resetConfig(): void {
  _config = null;
}
