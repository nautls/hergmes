/** A socket the test feeds by hand; it opens at once and records what is sent. */
export class FakeSocket extends EventTarget {
  /** The class, typed as the `WebSocket` the SDK takes. */
  static get class(): typeof WebSocket {
    return FakeSocket as unknown as typeof WebSocket;
  }

  static readonly OPEN = 1;
  static last: FakeSocket | undefined;
  /** Whether new sockets open; one that doesn't stays connecting. */
  static opens = true;
  readyState = 0;
  readonly url: string;
  readonly sent: unknown[] = [];

  constructor(url: string) {
    super();
    this.url = url;
    FakeSocket.last = this;
    if (!FakeSocket.opens) return;
    queueMicrotask(() => {
      this.readyState = FakeSocket.OPEN;
      this.dispatchEvent(new Event("open"));
    });
  }

  send(text: string): void {
    this.sent.push(JSON.parse(text));
  }

  close(): void {
    this.readyState = 3;
  }

  /** Delivers messages one after another within the caller's task. */
  deliver(...messages: unknown[]): void {
    for (const m of messages) {
      const data = typeof m === "string" ? m : JSON.stringify(m);
      this.dispatchEvent(new MessageEvent("message", { data }));
    }
  }

  /** The socket closed under the relay. */
  drop(): void {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}
