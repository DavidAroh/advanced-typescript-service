import { EventMap, EventPayload } from "./types";

type Listener<K extends keyof EventMap> = (payload: EventMap[K]) => void | Promise<void>;

export class TypedEventBus {
  private listeners = new Map<string, Set<Listener<any>>>();

  on<K extends keyof EventMap>(event: K, listener: Listener<K>): () => void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(listener);

    return () => {
      this.listeners.get(event)?.delete(listener);
    };
  }

  async emit<K extends keyof EventMap>(event: K, payload: EventPayload<K>): Promise<void> {
    const listeners = this.listeners.get(event);
    if (!listeners || listeners.size === 0) return;

    const promises: Promise<void>[] = [];
    for (const listener of listeners) {
      promises.push(
        Promise.resolve()
          .then(() => listener(payload))
          .catch((err) => {
            console.error(`[events] Listener error for "${event}":`, err);
          })
      );
    }

    await Promise.allSettled(promises);
  }

  listenerCount<K extends keyof EventMap>(event: K): number {
    return this.listeners.get(event)?.size ?? 0;
  }

  clear(): void {
    this.listeners.clear();
  }
}
