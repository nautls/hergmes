/**
 * A `WebSocket` for the SDK that injects the faults of a network, and of a relay bug, into a real
 * socket: cuts, delays, and dropped or duplicated frames. It needs no server: the SDK is handed
 * the class, which opens the real socket itself.
 */

export type Verdict = "pass" | "drop" | "duplicate";

export interface FaultOptions {
  /** The run's seeded draws, for delays and for armed cuts. */
  draw?: () => bigint;
  /** Each frame, either way, is held a drawn 0 to this many ms, in order. */
  maxDelayMs?: number;
  /** Arms each socket to be cut after a drawn 1 to this many incoming frames. */
  cutWithin?: number;
  /** Decides each incoming frame's fate, for the relay-bug cases. */
  frame?: (text: string) => Verdict;
  /** Where the socket really connects, from the URL the SDK asked for. */
  route?: (url: string) => string;
}

/** The faults of one SDK `Relay`'s sockets, and what was done, for a failure's report. */
export class Faults {
  readonly WebSocket: typeof WebSocket;
  readonly log: string[] = [];
  /** No new cut is armed, and an armed one waits. */
  paused = false;
  #current: { cut(why: string): void } | undefined;

  constructor(options: FaultOptions = {}) {
    // The socket class reaches its `Faults` through these.
    const log = (line: string) => this.log.push(line);
    const paused = () => this.paused;
    const opened = (socket: { cut(why: string): void }) => (this.#current = socket);
    const draw = options.draw ?? (() => 0n);
    const maxDelay = BigInt((options.maxDelayMs ?? 0) + 1);
    const delay = () => Number(draw() % maxDelay);
    let sockets = 0;

    class FaultySocket extends EventTarget {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSING = 2;
      static readonly CLOSED = 3;
      readyState = 0;
      readonly url: string;
      readonly #real: WebSocket;
      readonly #n = ++sockets;
      /** Nothing more goes either way. */
      #dead = false;
      /** `close` was fired. */
      #closed = false;
      /** The frames held each way, let through in order. */
      readonly #in = new Held(delay);
      readonly #out = new Held(delay);
      #frames = 0;
      #cutAfter: number | undefined;

      constructor(url: string | URL) {
        super();
        this.url = String(url);
        opened(this);
        if (options.cutWithin !== undefined && !paused()) {
          this.#cutAfter = Number(draw() % BigInt(options.cutWithin)) + 1;
        }
        this.#real = new WebSocket(options.route ? options.route(this.url) : this.url);
        this.#real.addEventListener("open", () => {
          if (this.#dead) return;
          this.readyState = FaultySocket.OPEN;
          this.dispatchEvent(new Event("open"));
        });
        this.#real.addEventListener("message", (event: MessageEvent) => {
          const text = event.data as string;
          const verdict = options.frame?.(text) ?? "pass";
          if (verdict === "drop") log(`socket ${String(this.#n)}: dropped ${text}`);
          if (verdict === "duplicate") {
            log(`socket ${String(this.#n)}: duplicated ${text}`);
          }
          this.#in.push(() => {
            if (this.#dead || verdict === "drop") return;
            this.dispatchEvent(new MessageEvent("message", { data: text }));
            if (verdict === "duplicate") {
              this.dispatchEvent(new MessageEvent("message", { data: text }));
            }
            this.#frames++;
            // An armed cut due while paused fires at the first frame after.
            if (this.#cutAfter !== undefined && this.#frames >= this.#cutAfter && !paused()) {
              this.cut(`after ${String(this.#frames)} frames`);
            }
          });
        });
        this.#real.addEventListener("close", () => {
          this.#die();
        });
      }

      send(text: string): void {
        if (this.#dead) return;
        this.#out.push(() => {
          if (!this.#dead && this.#real.readyState === WebSocket.OPEN) this.#real.send(text);
        });
      }

      /** The SDK closes it. */
      close(code?: number): void {
        if (this.#dead) return;
        this.#dead = true;
        this.readyState = FaultySocket.CLOSED;
        this.#real.close(code);
        queueMicrotask(() => {
          this.#die();
        });
      }

      /** The network cuts it: the SDK hears at once, and what is in flight is lost. */
      cut(why: string): void {
        if (this.#dead) return;
        log(`socket ${String(this.#n)}: cut ${why}`);
        this.#real.close();
        this.#die();
      }

      #die(): void {
        this.#dead = true;
        this.readyState = FaultySocket.CLOSED;
        if (this.#closed) return;
        this.#closed = true;
        this.dispatchEvent(new Event("close"));
      }
    }

    this.WebSocket = FaultySocket as unknown as typeof WebSocket;
  }

  /** Cuts the socket opened last, if it is still up. */
  cut(why: string): void {
    this.#current?.cut(why);
  }
}

/**
 * Frames held for a drawn delay each, let through in the order they came, by one timer at a
 * time: two timers of different lengths due in the same millisecond may fire in either order.
 */
class Held {
  readonly #delay: () => number;
  readonly #queue: { at: number; deliver: () => void }[] = [];
  #timer: ReturnType<typeof setTimeout> | undefined;

  constructor(delay: () => number) {
    this.#delay = delay;
  }

  push(deliver: () => void): void {
    const last = this.#queue.at(-1)?.at ?? 0;
    this.#queue.push({ at: Math.max(last, Date.now() + this.#delay()), deliver });
    this.#schedule();
  }

  #schedule(): void {
    const head = this.#queue[0];
    if (this.#timer !== undefined || !head) return;
    this.#timer = setTimeout(
      () => {
        this.#timer = undefined;
        const now = Date.now();
        while (this.#queue[0] && this.#queue[0].at <= now) this.#queue.shift()?.deliver();
        this.#schedule();
      },
      Math.max(0, head.at - Date.now()),
    );
  }
}
