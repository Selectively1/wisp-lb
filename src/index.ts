import { DurableObject } from "cloudflare:workers";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface Env {
  WISP_PROXY: DurableObjectNamespace<WispProxy>;
  UPSTREAMS: string;
}

/** State persisted on each client WebSocket via serializeAttachment. */
interface WsAttachment {
  upstreamUrl: string;
}

// ── Worker Entrypoint ──────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      const upstreams = parseUpstreams(env.UPSTREAMS);
      return Response.json({ status: "ok", upstreams: upstreams.length });
    }

    // Only handle /wisp (with optional trailing path for sub-protocols)
    if (!url.pathname.startsWith("/wisp")) {
      return new Response("Not Found", { status: 404 });
    }

    // Must be a WebSocket upgrade
    const upgrade = request.headers.get("Upgrade");
    if (!upgrade || upgrade.toLowerCase() !== "websocket") {
      return new Response("Expected Upgrade: websocket", { status: 426 });
    }

    // Parse upstream list
    const upstreams = parseUpstreams(env.UPSTREAMS);
    if (upstreams.length === 0) {
      return new Response("No upstreams configured", { status: 503 });
    }

    // Pick a random upstream
    const upstream = upstreams[Math.floor(Math.random() * upstreams.length)];

    // Each WebSocket connection gets its own unique Durable Object.
    // This gives us 1:1 client<->upstream affinity with no shared state needed.
    const id = env.WISP_PROXY.newUniqueId();
    const stub = env.WISP_PROXY.get(id);

    // Forward the request to the DO, passing the chosen upstream as a header
    const proxyRequest = new Request(request.url, {
      method: request.method,
      headers: [
        ...Array.from(request.headers.entries()),
        ["X-Upstream-Url", upstream],
      ],
    });

    return stub.fetch(proxyRequest);
  },
} satisfies ExportedHandler<Env>;

// ── Durable Object: WispProxy ──────────────────────────────────────────────────
//
// Uses the WebSocket Hibernation API for the client-facing connection.
// During idle periods the DO can be evicted from memory while keeping the
// client WebSocket alive on the Cloudflare edge.  When a message arrives
// from the client the DO is re-instantiated and the upstream connection is
// re-established transparently.
//
// The upstream (outgoing) WebSocket uses the standard API since hibernation
// only applies to server-side WebSockets.
// ────────────────────────────────────────────────────────────────────────────────

export class WispProxy extends DurableObject<Env> {
  private upstreamWs: WebSocket | null = null;
  private closed = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // On wake-from-hibernation the constructor runs again.
    // Restore any hibernating client WebSockets and reconnect their upstreams.
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment() as WsAttachment | null;
      if (attachment) {
        this.connectUpstream(ws, attachment.upstreamUrl);
      }
    }
  }

  // ── fetch: initial connection setup ────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    const upstreamUrl = request.headers.get("X-Upstream-Url");
    if (!upstreamUrl) {
      return new Response("Missing upstream URL", { status: 400 });
    }

    // 1. Create the client-facing WebSocket pair
    const pair = new WebSocketPair();
    const [clientSide, serverSide] = Object.values(pair);

    // Accept via Hibernation API — allows the DO to hibernate while keeping
    // the client WebSocket alive on the edge.
    this.ctx.acceptWebSocket(serverSide);

    // Persist the upstream URL so we can reconnect after hibernation
    serverSide.serializeAttachment({ upstreamUrl } satisfies WsAttachment);

    // 2. Connect to the upstream Wisp server
    const ok = await this.connectUpstream(serverSide, upstreamUrl);
    if (!ok) {
      // connectUpstream already closed serverSide on failure
      return new Response("Upstream connection failed", { status: 502 });
    }

    // Return the client side of the WebSocket pair to the caller
    return new Response(null, { status: 101, webSocket: clientSide });
  }

  // ── Hibernation event handlers ─────────────────────────────────────────────

  /**
   * Called when the client sends a WebSocket message.
   * If the DO was hibernating this is the wake-up trigger — the constructor
   * will have already re-established the upstream connection.
   */
  async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
    if (this.closed || !this.upstreamWs) return;
    try {
      this.upstreamWs.send(message);
    } catch {
      this.teardown(ws, 1011, "Failed to forward to upstream");
    }
  }

  /**
   * Called when the client closes the WebSocket.
   */
  async webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    this.teardown(ws, code, reason || "Client closed");
  }

  /**
   * Called when the client WebSocket errors.
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    this.teardown(ws, 1011, "Client error");
  }

  // ── Upstream connection ────────────────────────────────────────────────────

  /**
   * Open a WebSocket to the upstream Wisp server and wire up the
   * upstream→client message bridge.
   *
   * Returns `true` on success, `false` on failure (caller should respond 502).
   */
  private async connectUpstream(
    clientWs: WebSocket,
    upstreamUrl: string,
  ): Promise<boolean> {
    try {
      const resp = await fetch(upstreamUrl, {
        headers: { Upgrade: "websocket" },
      });

      const ws = resp.webSocket;
      if (!ws) {
        clientWs.close(1011, "Upstream did not accept WebSocket");
        return false;
      }

      ws.accept();
      this.upstreamWs = ws;

      // Bridge: upstream → client
      ws.addEventListener("message", (event: MessageEvent) => {
        if (this.closed) return;
        try {
          clientWs.send(event.data as ArrayBuffer | string);
        } catch {
          this.teardown(clientWs, 1011, "Failed to forward to client");
        }
      });

      // Upstream closed
      ws.addEventListener("close", (event: CloseEvent) => {
        this.teardown(clientWs, event.code, event.reason || "Upstream closed");
      });

      // Upstream error
      ws.addEventListener("error", () => {
        this.teardown(clientWs, 1011, "Upstream error");
      });

      return true;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      clientWs.close(1011, `Upstream connection failed: ${msg}`);
      return false;
    }
  }

  // ── Teardown ───────────────────────────────────────────────────────────────

  /**
   * Cleanly tear down both sides of the proxy.
   * Safe to call multiple times — only the first call takes effect.
   */
  private teardown(clientWs: WebSocket, code: number, reason: string): void {
    if (this.closed) return;
    this.closed = true;

    // Clamp code to valid WebSocket close range
    const safeCode = code >= 1000 && code <= 4999 ? code : 1000;

    try {
      clientWs.close(safeCode, reason);
    } catch {
      // already closed
    }

    try {
      this.upstreamWs?.close(safeCode, reason);
    } catch {
      // already closed
    }

    this.upstreamWs = null;
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function parseUpstreams(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}
