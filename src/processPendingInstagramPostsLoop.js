require("dotenv").config();
const http = require("http");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runLoop() {
  const intervalMs = Math.max(5000, Number(process.env.IG_POSTS_POLL_MS || 30000) || 30000);
  console.log("[ig-worker-loop] started", JSON.stringify({ intervalMs }));
  let runPendingInstagramPostsOnce = null;

  while (true) {
    try {
      if (!runPendingInstagramPostsOnce) {
        ({ runPendingInstagramPostsOnce } = require("./processPendingInstagramPosts"));
        console.log("[ig-worker-loop] processor loaded");
      }

      await runPendingInstagramPostsOnce();
    } catch (error) {
      console.error("[ig-worker-loop] cycle error", error && error.message ? error.message : error);
    }

    await sleep(intervalMs);
  }
}

function isTruthy(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function shouldStartHealthServer() {
  if (isTruthy(process.env.IG_WORKER_DISABLE_HEALTHCHECK)) {
    return false;
  }

  // Keep health endpoint on by default for managed platforms with HTTP healthchecks.
  return true;
}

function startHealthServerIfNeeded() {
  if (!shouldStartHealthServer()) {
    return null;
  }

  const port = Number(process.env.PORT || 8080);
  if (!Number.isInteger(port) || port <= 0) {
    console.warn("[ig-worker-loop] health server skipped: invalid PORT", process.env.PORT || "");
    return null;
  }

  const server = http.createServer((req, res) => {
    if (req.method === "GET" && (req.url === "/health" || req.url === "/readiness")) {
      const body = JSON.stringify({ ok: true, service: "ig_worker_loop" });
      res.writeHead(200, {
        "content-type": "application/json",
        "content-length": Buffer.byteLength(body),
      });
      res.end(body);
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not_found");
  });

  server.listen(port, "0.0.0.0", () => {
    console.log("[ig-worker-loop] health server listening", JSON.stringify({ port }));
  });

  server.on("error", (error) => {
    console.error("[ig-worker-loop] health server error", error && error.message ? error.message : error);
    process.exit(1);
  });

  return server;
}

if (require.main === module) {
  startHealthServerIfNeeded();

  runLoop().catch((error) => {
    console.error("[ig-worker-loop] fatal", error);
    process.exit(1);
  });
}
