import { DurableObject } from "cloudflare:workers";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface Env {
  WISP_PROXY: DurableObjectNamespace<WispProxy>;
  UPSTREAMS: string;
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

export class WispProxy extends DurableObject<Env> {
  private clientWs: WebSocket | null = null;
  private upstreamWs: WebSocket | null = null;
  private closed = false;

  async fetch(request: Request): Promise<Response> {
    const upstreamUrl = request.headers.get("X-Upstream-Url");
    if (!upstreamUrl) {
      return new Response("Missing upstream URL", { status: 400 });
    }

    // 1. Create the client-facing WebSocket pair
    const pair = new WebSocketPair();
    const [clientSide, serverSide] = Object.values(pair);

    // Accept the server side (our end of the client connection)
    serverSide.accept();
    this.clientWs = serverSide;

    // 2. Connect to the upstream Wisp server
    try {
      const upstreamResp = await fetch(upstreamUrl, {
        headers: { Upgrade: "websocket" },
      });

      const ws = upstreamResp.webSocket;
      if (!ws) {
        serverSide.close(1011, "Upstream did not accept WebSocket");
        return new Response("Upstream rejected WebSocket upgrade", {
          status: 502,
        });
      }

      ws.accept();
      this.upstreamWs = ws;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      serverSide.close(1011, `Upstream connection failed: ${msg}`);
      return new Response(`Upstream connection failed: ${msg}`, {
        status: 502,
      });
    }

    // 3. Bridge: client -> upstream
    this.clientWs.addEventListener("message", (event: MessageEvent) => {
      if (this.closed) return;
      try {
        if (event.data instanceof ArrayBuffer) {
          this.upstreamWs?.send(event.data);
        } else {
          this.upstreamWs?.send(event.data as string);
        }
      } catch {
        this.teardown(1011, "Failed to forward to upstream");
      }
    });

    // 4. Bridge: upstream -> client
    this.upstreamWs.addEventListener("message", (event: MessageEvent) => {
      if (this.closed) return;
      try {
        if (event.data instanceof ArrayBuffer) {
          this.clientWs?.send(event.data);
        } else {
          this.clientWs?.send(event.data as string);
        }
      } catch {
        this.teardown(1011, "Failed to forward to client");
      }
    });

    // 5. Handle close: client closed
    this.clientWs.addEventListener("close", (event: CloseEvent) => {
      this.teardown(event.code, event.reason || "Client closed");
    });

    // 6. Handle close: upstream closed
    this.upstreamWs.addEventListener("close", (event: CloseEvent) => {
      this.teardown(event.code, event.reason || "Upstream closed");
    });

    // 7. Handle errors
    this.clientWs.addEventListener("error", () => {
      this.teardown(1011, "Client error");
    });

    this.upstreamWs.addEventListener("error", () => {
      this.teardown(1011, "Upstream error");
    });

    // Return the client side of the WebSocket pair to the caller
    return new Response(null, { status: 101, webSocket: clientSide });
  }

  /**
   * Cleanly tear down both sides of the proxy.
   * Safe to call multiple times -- only the first call takes effect.
   */
  private teardown(code: number, reason: string): void {
    if (this.closed) return;
    this.closed = true;

    // Clamp code to valid WebSocket close range
    const safeCode = code >= 1000 && code <= 4999 ? code : 1000;

    try {
      this.clientWs?.close(safeCode, reason);
    } catch {
      // already closed
    }

    try {
      this.upstreamWs?.close(safeCode, reason);
    } catch {
      // already closed
    }

    this.clientWs = null;
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
