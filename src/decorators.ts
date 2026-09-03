import { JobDefinition } from "./types";

const metadataStore = new WeakMap<object, Map<symbol, unknown>>();

function defineMeta(target: object, key: symbol, value: unknown): void {
  if (!metadataStore.has(target)) metadataStore.set(target, new Map());
  metadataStore.get(target)!.set(key, value);
}

function getMeta(target: object, key: symbol): unknown {
  return metadataStore.get(target)?.get(key);
}

const HANDLER_TYPE = Symbol("handler:type");
const HANDLER_OPTIONS = Symbol("handler:options");
const LOGGED = Symbol("decorator:logged");

export function Handler(options: { type: string; maxRetries?: number; timeoutMs?: number }) {
  return function (
    target: object,
    _propertyKey: string,
    _descriptor: PropertyDescriptor
  ) {
    defineMeta(target, HANDLER_TYPE, options.type);
    defineMeta(target, HANDLER_OPTIONS, options);
  };
}

export function getHandlerType(target: object): string | undefined {
  return getMeta(target, HANDLER_TYPE) as string | undefined;
}

export function LogTiming(
  target: object,
  propertyKey: string,
  descriptor: PropertyDescriptor
) {
  if (getMeta(target, LOGGED)) return descriptor;

  const original = descriptor.value as (...args: unknown[]) => Promise<unknown>;
  const className = target.constructor.name;

  descriptor.value = async function (this: unknown, ...args: unknown[]) {
    const start = performance.now();
    try {
      const result = await original.apply(this, args);
      const duration = (performance.now() - start).toFixed(1);
      console.log(`[${className}.${propertyKey}] completed in ${duration}ms`);
      return result;
    } catch (err) {
      const duration = (performance.now() - start).toFixed(1);
      console.error(`[${className}.${propertyKey}] failed after ${duration}ms:`, err);
      throw err;
    }
  };

  defineMeta(target, LOGGED, true);
  return descriptor;
}

export function ValidateInput(requiredFields: string[]) {
  return function (
    target: object,
    propertyKey: string,
    descriptor: PropertyDescriptor
  ) {
    const original = descriptor.value as (...args: unknown[]) => Promise<unknown>;

    descriptor.value = async function (this: unknown, ...args: unknown[]) {
      const input = args[0];

      if (input === null || input === undefined) {
        throw new TypeError(
          `[${target.constructor.name}.${propertyKey}] Input is null/undefined`
        );
      }

      if (typeof input === "object") {
        for (const field of requiredFields) {
          if (!(field in (input as Record<string, unknown>))) {
            throw new TypeError(
              `[${target.constructor.name}.${propertyKey}] Missing required field: "${field}"`
            );
          }
        }
      }

      return original.apply(this, args);
    };

    return descriptor;
  };
}

export function createJobDefinition<TInput>(
  instance: { handle: (input: TInput, signal: AbortSignal) => Promise<unknown> },
  type: string,
  _options?: { maxRetries?: number; timeoutMs?: number }
): JobDefinition<TInput> {
  return {
    type,
    handler: (input: TInput, signal: AbortSignal) => instance.handle(input, signal),
  };
}
