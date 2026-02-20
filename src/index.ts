import { DurableObject } from "cloudflare:workers";

// ── Types ──────────────────────────────────────────────────────────────────────

export interface Env {
  WISP_PROXY: DurableObjectNamespace<WispProxy>;
  UPSTREAM_REGISTRY: DurableObjectNamespace<UpstreamRegistry>;
  UPSTREAMS: string;
}

/** Maximum time (ms) to wait for an upstream WebSocket connection. */
const UPSTREAM_CONNECT_TIMEOUT_MS = 10_000;

/**
 * WebSocket close reasons are limited to 123 bytes per the RFC 6455 spec.
 * We truncate to stay within this limit.
 */
const MAX_CLOSE_REASON_BYTES = 123;

/** How often the registry checks upstream health (ms). */
const HEALTH_CHECK_INTERVAL_MS = 30_000;

/** Timeout for each individual health check probe (ms). */
const HEALTH_CHECK_TIMEOUT_MS = 5_000;

/**
 * After this many consecutive health check failures, an upstream is
 * marked unhealthy and will not be selected for new connections.
 */
const UNHEALTHY_THRESHOLD = 3;

/**
 * Soft cap on in-flight messages per direction before we start dropping.
 * CF Workers WebSocket doesn't expose bufferedAmount, so this is a
 * coarse proxy for backpressure. 512 messages in flight ≈ 256 KB at
 * typical Wisp packet sizes, well within CF's 128 MB memory limit.
 */
const BACKPRESSURE_HIGH_WATER = 512;

// ── Upstream state tracked by the registry ──────────────────────────────────

interface UpstreamState {
  url: string;
  healthy: boolean;
  /** Number of consecutive failed health checks. */
  consecutiveFailures: number;
  /** Active connections routed to this upstream right now. */
  activeConnections: number;
  /** Lifetime counters (reset on registry eviction, not persistent). */
  totalConnections: number;
  totalBytesIn: number;
  totalBytesOut: number;
  totalErrors: number;
  /** ISO timestamp of last successful health check. */
  lastHealthCheck: string | null;
}

/** Metrics reported by a WispProxy DO when its connection tears down. */
interface ConnectionReport {
  upstreamUrl: string;
  bytesIn: number;
  bytesOut: number;
  hadError: boolean;
}

// ── Worker Entrypoint ──────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // ── Health / stats endpoint ───────────────────────────────────────────
    if (url.pathname === "/health") {
      const registry = getRegistry(env);
      const resp = await registry.fetch(
        new Request("https://internal/health", { method: "GET" }),
      );
      return resp;
    }

    // ── Detailed metrics endpoint ─────────────────────────────────────────
    if (url.pathname === "/metrics") {
      const registry = getRegistry(env);
      const resp = await registry.fetch(
        new Request("https://internal/metrics", { method: "GET" }),
      );
      return resp;
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

    // Ask the registry to pick the best upstream (least connections among
    // healthy upstreams). This also increments the active connection count.
    const registry = getRegistry(env);
    const pickResp = await registry.fetch(
      new Request("https://internal/pick", { method: "POST" }),
    );
    if (!pickResp.ok) {
      const body = await pickResp.text();
      return new Response(body, { status: pickResp.status });
    }
    const upstream = await pickResp.text();

    // Each WebSocket connection gets its own unique Durable Object.
    // This gives us 1:1 client<->upstream affinity with no shared state needed.
    const id = env.WISP_PROXY.newUniqueId();
    const stub = env.WISP_PROXY.get(id);

    // Forward the request to the DO, passing the chosen upstream as a header.
    const headers = new Headers(request.headers);
    headers.set("X-Upstream-Url", upstream);

    return stub.fetch(
      new Request(request.url, { method: request.method, headers }),
    );
  },
} satisfies ExportedHandler<Env>;

/** Singleton registry: always the same DO instance within a deployment. */
function getRegistry(env: Env): DurableObjectStub<UpstreamRegistry> {
  const id = env.UPSTREAM_REGISTRY.idFromName("singleton");
  return env.UPSTREAM_REGISTRY.get(id);
}

// ── Durable Object: UpstreamRegistry ────────────────────────────────────────
//
// A singleton DO that tracks upstream health, active connection counts, and
// aggregate metrics. It performs periodic health checks via ctx.setInterval()
// and provides least-connections upstream selection.
//
// This DO is lightweight — it holds no WebSocket connections, just counters.
// It stays alive as long as there are active connections (since workers keep
// calling into it) and the health check interval keeps it alive otherwise.
// ─────────────────────────────────────────────────────────────────────────────

export class UpstreamRegistry extends DurableObject<Env> {
  private upstreams: Map<string, UpstreamState> = new Map();
  private initialized = false;
  private healthCheckInterval: ReturnType<typeof setInterval> | null = null;

  /** Ensure the upstream list is loaded and health checks are running. */
  private ensureInitialized(): void {
    if (this.initialized) return;
    this.initialized = true;

    const urls = parseUpstreams(this.env.UPSTREAMS);
    for (const url of urls) {
      if (!this.upstreams.has(url)) {
        this.upstreams.set(url, {
          url,
          healthy: true, // assume healthy until proven otherwise
          consecutiveFailures: 0,
          activeConnections: 0,
          totalConnections: 0,
          totalBytesIn: 0,
          totalBytesOut: 0,
          totalErrors: 0,
          lastHealthCheck: null,
        });
      }
    }

    // Remove upstreams no longer in config
    for (const key of this.upstreams.keys()) {
      if (!urls.includes(key)) {
        this.upstreams.delete(key);
      }
    }

    // Start periodic health checks
    if (!this.healthCheckInterval) {
      this.healthCheckInterval = setInterval(() => {
        void this.runHealthChecks();
      }, HEALTH_CHECK_INTERVAL_MS);

      // Run an initial health check immediately (non-blocking)
      void this.runHealthChecks();
    }
  }

  async fetch(request: Request): Promise<Response> {
    this.ensureInitialized();

    const url = new URL(request.url);

    // ── Pick best upstream (least connections among healthy) ─────────────
    if (url.pathname === "/pick") {
      const picked = this.pickUpstream();
      if (!picked) {
        return new Response("No healthy upstreams available", { status: 503 });
      }
      // Increment active connections
      picked.activeConnections++;
      picked.totalConnections++;
      return new Response(picked.url, { status: 200 });
    }

    // ── Connection teardown report ──────────────────────────────────────
    if (url.pathname === "/report") {
      const report: ConnectionReport = await request.json();
      const state = this.upstreams.get(report.upstreamUrl);
      if (state) {
        state.activeConnections = Math.max(0, state.activeConnections - 1);
        state.totalBytesIn += report.bytesIn;
        state.totalBytesOut += report.bytesOut;
        if (report.hadError) state.totalErrors++;
      }
      return new Response("ok", { status: 200 });
    }

    // ── Health summary ──────────────────────────────────────────────────
    if (url.pathname === "/health") {
      const entries = [...this.upstreams.values()];
      const healthy = entries.filter((u) => u.healthy).length;
      return Response.json({
        status: healthy > 0 ? "ok" : "degraded",
        upstreams: {
          total: entries.length,
          healthy,
          unhealthy: entries.length - healthy,
        },
      });
    }

    // ── Detailed metrics ────────────────────────────────────────────────
    if (url.pathname === "/metrics") {
      const entries = [...this.upstreams.values()].map((u) => ({
        url: u.url,
        healthy: u.healthy,
        consecutiveFailures: u.consecutiveFailures,
        activeConnections: u.activeConnections,
        totalConnections: u.totalConnections,
        totalBytesIn: u.totalBytesIn,
        totalBytesOut: u.totalBytesOut,
        totalErrors: u.totalErrors,
        lastHealthCheck: u.lastHealthCheck,
      }));
      return Response.json({ upstreams: entries });
    }

    return new Response("Not found", { status: 404 });
  }

  /**
   * Pick the best upstream using least-connections strategy.
   * Among healthy upstreams, returns the one with the fewest active
   * connections. If all upstreams are unhealthy, falls back to the
   * one with the fewest consecutive failures (best-effort).
   */
  private pickUpstream(): UpstreamState | null {
    const all = [...this.upstreams.values()];
    if (all.length === 0) return null;

    const healthy = all.filter((u) => u.healthy);
    const candidates = healthy.length > 0 ? healthy : all;

    // Least connections, with random tie-breaking
    let best: UpstreamState[] = [candidates[0]];
    for (let i = 1; i < candidates.length; i++) {
      const c = candidates[i];
      if (c.activeConnections < best[0].activeConnections) {
        best = [c];
      } else if (c.activeConnections === best[0].activeConnections) {
        best.push(c);
      }
    }

    return best[Math.floor(Math.random() * best.length)];
  }

  /**
   * Probe all upstreams concurrently. Each probe attempts a WebSocket
   * upgrade to the upstream's /wisp path. A successful upgrade (101)
   * means the upstream is alive. We immediately close the WebSocket
   * after confirming the upgrade.
   */
  private async runHealthChecks(): Promise<void> {
    const checks = [...this.upstreams.entries()].map(
      async ([url, state]): Promise<void> => {
        try {
          const controller = new AbortController();
          const timer = setTimeout(
            () => controller.abort(),
            HEALTH_CHECK_TIMEOUT_MS,
          );

          const resp = await fetch(url, {
            headers: { Upgrade: "websocket" },
            signal: controller.signal,
          });
          clearTimeout(timer);

          if (resp.webSocket) {
            // Successfully connected — upstream is healthy
            resp.webSocket.accept();
            resp.webSocket.close(1000, "health check");
            state.consecutiveFailures = 0;
            state.healthy = true;
            state.lastHealthCheck = new Date().toISOString();
          } else {
            this.markCheckFailed(state);
          }
        } catch {
          this.markCheckFailed(state);
        }
      },
    );

    await Promise.allSettled(checks);
  }

  private markCheckFailed(state: UpstreamState): void {
    state.consecutiveFailures++;
    if (state.consecutiveFailures >= UNHEALTHY_THRESHOLD) {
      state.healthy = false;
    }
  }
}

// ── Durable Object: WispProxy ──────────────────────────────────────────────────
//
// Does NOT use the Hibernation API. The DO stays in memory for the full
// duration of the client WebSocket connection. This is deliberate:
//
// 1. The upstream WebSocket cannot survive DO eviction — when the DO is
//    evicted the upstream connection is closed and all Wisp streams die.
//
// 2. Wisp sessions carry server-side state (stream maps, per-stream send
//    buffers, CONTINUE flow-control counters) that the upstream cannot
//    restore after a reconnection. Reconnecting to a fresh upstream after
//    hibernation would silently corrupt the client's session.
//
// 3. Mercury Workshop's own Cloudflare Workers implementation
//    (wisp-server-workers) uses standard WebSocket accept — not hibernation.
//
// Using standard `serverSide.accept()` keeps the DO alive as long as the
// client WebSocket is open, which is exactly what we need for a transparent
// proxy.
// ────────────────────────────────────────────────────────────────────────────────

export class WispProxy extends DurableObject<Env> {
  private upstreamWs: WebSocket | null = null;
  private clientWs: WebSocket | null = null;
  private upstreamUrl: string | null = null;
  private closed = false;
  private hadError = false;

  /** Bytes received from client (forwarded to upstream). */
  private bytesIn = 0;
  /** Bytes received from upstream (forwarded to client). */
  private bytesOut = 0;

  /**
   * Coarse backpressure counters. We track messages that have been sent
   * but not yet acknowledged by the remote side. Since CF Workers
   * WebSocket doesn't expose `bufferedAmount`, we use message counts as
   * a rough proxy. If the count exceeds BACKPRESSURE_HIGH_WATER we drop
   * new messages rather than buffering unboundedly in memory.
   *
   * Each direction has its own counter. The counter is incremented on
   * send and decremented when the next message arrives from the same
   * direction (indicating the remote is still consuming).
   */
  private toUpstreamInFlight = 0;
  private toClientInFlight = 0;

  // ── fetch: initial connection setup ────────────────────────────────────────

  async fetch(request: Request): Promise<Response> {
    const upstreamUrl = request.headers.get("X-Upstream-Url");
    if (!upstreamUrl) {
      return new Response("Missing upstream URL", { status: 400 });
    }

    this.upstreamUrl = upstreamUrl;

    // 1. Connect to the upstream Wisp server BEFORE accepting the client.
    //    This lets us return a proper HTTP 502 if upstream is unreachable
    //    rather than accepting the WebSocket and immediately closing it.
    const upstreamHeaders: HeadersInit = { Upgrade: "websocket" };

    // Forward Sec-WebSocket-Protocol so the upstream can negotiate Wisp v2
    // extensions. The Wisp v2 handshake uses INFO packets exchanged over WS,
    // but some servers gate v2 on the presence of this header during upgrade.
    const protocol = request.headers.get("Sec-WebSocket-Protocol");
    if (protocol) {
      upstreamHeaders["Sec-WebSocket-Protocol"] = protocol;
    }

    let upstreamResp: Response;
    try {
      const controller = new AbortController();
      const timer = setTimeout(
        () => controller.abort(),
        UPSTREAM_CONNECT_TIMEOUT_MS,
      );
      upstreamResp = await fetch(upstreamUrl, {
        headers: upstreamHeaders,
        signal: controller.signal,
      });
      clearTimeout(timer);
    } catch (err) {
      // Connection failed — report the error and release the slot
      void this.reportToRegistry(true);
      const msg = err instanceof Error ? err.message : String(err);
      return new Response(`Upstream connection failed: ${msg}`, {
        status: 502,
      });
    }

    const upstreamWs = upstreamResp.webSocket;
    if (!upstreamWs) {
      void this.reportToRegistry(true);
      return new Response("Upstream did not upgrade to WebSocket", {
        status: 502,
      });
    }

    upstreamWs.accept();
    this.upstreamWs = upstreamWs;

    // 2. Create the client-facing WebSocket pair
    const pair = new WebSocketPair();
    const [clientSide, serverSide] = Object.values(pair);

    // Accept with the standard API — no hibernation. The DO remains in
    // memory for the lifetime of this WebSocket, ensuring the upstream
    // connection stays alive.
    serverSide.accept();
    this.clientWs = serverSide;

    // 3. Wire up bidirectional message bridge
    this.bridgeClientToUpstream(serverSide, upstreamWs);
    this.bridgeUpstreamToClient(upstreamWs, serverSide);

    return new Response(null, { status: 101, webSocket: clientSide });
  }

  // ── Bridge wiring ─────────────────────────────────────────────────────────

  /** Forward all client messages/events to the upstream WebSocket. */
  private bridgeClientToUpstream(
    client: WebSocket,
    upstream: WebSocket,
  ): void {
    client.addEventListener("message", (event: MessageEvent) => {
      if (this.closed) return;

      // The fact that we received a message from the client means the
      // client is alive and consuming — reset the client-direction counter.
      this.toClientInFlight = 0;

      // Backpressure check: if upstream can't keep up, drop the message.
      // This is a last resort — under normal operation flows stay well
      // below the high-water mark.
      if (this.toUpstreamInFlight >= BACKPRESSURE_HIGH_WATER) {
        return; // drop
      }

      try {
        const data = event.data as ArrayBuffer | string;
        this.bytesIn += byteLength(data);
        this.toUpstreamInFlight++;
        upstream.send(data);
      } catch {
        this.hadError = true;
        this.teardown(1011, "Failed to forward to upstream");
      }
    });

    client.addEventListener("close", (event: CloseEvent) => {
      this.teardown(event.code, event.reason || "Client closed");
    });

    client.addEventListener("error", () => {
      this.hadError = true;
      this.teardown(1011, "Client WebSocket error");
    });
  }

  /** Forward all upstream messages/events to the client WebSocket. */
  private bridgeUpstreamToClient(
    upstream: WebSocket,
    client: WebSocket,
  ): void {
    upstream.addEventListener("message", (event: MessageEvent) => {
      if (this.closed) return;

      // Upstream sent us a message — it's still consuming, reset counter.
      this.toUpstreamInFlight = 0;

      if (this.toClientInFlight >= BACKPRESSURE_HIGH_WATER) {
        return; // drop
      }

      try {
        const data = event.data as ArrayBuffer | string;
        this.bytesOut += byteLength(data);
        this.toClientInFlight++;
        client.send(data);
      } catch {
        this.hadError = true;
        this.teardown(1011, "Failed to forward to client");
      }
    });

    upstream.addEventListener("close", (event: CloseEvent) => {
      this.teardown(event.code, event.reason || "Upstream closed");
    });

    upstream.addEventListener("error", () => {
      this.hadError = true;
      this.teardown(1011, "Upstream WebSocket error");
    });
  }

  // ── Teardown ───────────────────────────────────────────────────────────────

  /**
   * Cleanly close both sides of the proxy.
   * Idempotent — only the first call takes effect.
   */
  private teardown(code: number, reason: string): void {
    if (this.closed) return;
    this.closed = true;

    // Clamp code to valid WebSocket close range (RFC 6455 §7.4)
    const safeCode = code >= 1000 && code <= 4999 ? code : 1000;

    // WebSocket close reasons must be <= 123 bytes (RFC 6455 §5.5)
    const safeReason = truncateUtf8(reason, MAX_CLOSE_REASON_BYTES);

    try {
      this.clientWs?.close(safeCode, safeReason);
    } catch {
      // already closed
    }

    try {
      this.upstreamWs?.close(safeCode, safeReason);
    } catch {
      // already closed
    }

    this.clientWs = null;
    this.upstreamWs = null;

    // Report metrics back to the registry (fire-and-forget)
    void this.reportToRegistry(this.hadError);
  }

  /**
   * Report connection metrics to the UpstreamRegistry so it can update
   * active connection counts and aggregate statistics.
   */
  private async reportToRegistry(hadError: boolean): Promise<void> {
    if (!this.upstreamUrl) return;

    try {
      const registry = getRegistry(this.env);
      const report: ConnectionReport = {
        upstreamUrl: this.upstreamUrl,
        bytesIn: this.bytesIn,
        bytesOut: this.bytesOut,
        hadError,
      };
      await registry.fetch(
        new Request("https://internal/report", {
          method: "POST",
          body: JSON.stringify(report),
          headers: { "Content-Type": "application/json" },
        }),
      );
    } catch {
      // Best-effort — if the registry is unreachable the metrics are lost
      // but the proxy still functions correctly.
    }
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function parseUpstreams(raw: string): string[] {
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * Truncate a string so its UTF-8 byte length does not exceed `maxBytes`.
 * Avoids splitting in the middle of a multi-byte character.
 */
function truncateUtf8(str: string, maxBytes: number): string {
  const encoder = new TextEncoder();
  const encoded = encoder.encode(str);
  if (encoded.byteLength <= maxBytes) return str;

  // Walk backwards from the limit to find a valid UTF-8 boundary
  let end = maxBytes;
  // Skip continuation bytes (10xxxxxx)
  while (end > 0 && (encoded[end] & 0xc0) === 0x80) {
    end--;
  }
  return new TextDecoder().decode(encoded.subarray(0, end));
}

/** Get the byte length of a WebSocket message payload. */
function byteLength(data: ArrayBuffer | string): number {
  if (typeof data === "string") {
    // Fast approximation — TextEncoder is expensive to call per-message.
    // For ASCII-heavy Wisp payloads this is close enough for metrics.
    return data.length;
  }
  return data.byteLength;
}
