const express = require("express");
const crypto = require("crypto");
require("dotenv").config();

const { saveRawInstagramPosts } = require("./firestoreRepo");
const { fetchDatasetItems } = require("./apify");
const { initFirebase, getDb } = require("./firebase");
const { createLogger } = require("./logger");
const { recordHttpRequest, snapshot } = require("./metrics");

const app = express();
const logger = createLogger({ service: "node-api" });

app.use(express.json({ limit: "2mb" }));

app.use((req, res, next) => {
  const requestId = req.get("x-request-id") || crypto.randomUUID();
  req.requestId = requestId;

  const start = Date.now();
  res.on("finish", () => {
    const durationMs = Date.now() - start;
    recordHttpRequest(res.statusCode);

    logger.info("http_request", {
      requestId,
      method: req.method,
      path: req.originalUrl,
      statusCode: res.statusCode,
      durationMs,
      ip: req.ip,
      userAgent: req.get("user-agent") || null,
    });
  });

  next();
});

app.get("/health", (_req, res) => {
  return res.status(200).json({ ok: true });
});

function timeoutPromise(ms, message) {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), ms);
  });
}

async function checkReadiness() {
  const checks = {
    config: {
      ok: true,
      details: {
        hasApifyToken: Boolean(String(process.env.APIFY_TOKEN || "").trim()),
      },
    },
    firestore: {
      ok: true,
      details: {
        initialized: false,
        pingMs: null,
      },
    },
    metrics: {
      ok: true,
      details: snapshot(),
    },
  };

  if (!checks.config.details.hasApifyToken) {
    checks.config.ok = false;
  }

  try {
    const startedAt = Date.now();
    initFirebase();
    checks.firestore.details.initialized = true;

    const db = getDb();
    await Promise.race([
      db.collection("_health").limit(1).get(),
      timeoutPromise(2500, "firestore_ping_timeout"),
    ]);

    checks.firestore.details.pingMs = Date.now() - startedAt;
  } catch (error) {
    checks.firestore.ok = false;
    checks.firestore.details.error = error && error.message ? error.message : String(error || "unknown_error");
  }

  const ok = checks.config.ok && checks.firestore.ok;
  return { ok, checks };
}

app.get("/readiness", async (req, res) => {
  try {
    const readiness = await checkReadiness();
    if (!readiness.ok) {
      logger.warn("readiness_failed", {
        requestId: req.requestId,
        checks: readiness.checks,
      });
      return res.status(503).json(readiness);
    }

    return res.status(200).json(readiness);
  } catch (error) {
    logger.error("readiness_error", {
      requestId: req.requestId,
      error,
    });
    return res.status(503).json({ ok: false, error: "readiness_internal_error" });
  }
});

app.post("/webhooks/apify/instagram", async (req, res) => {
  const datasetId = req.body?.datasetId;
  const requestId = req.requestId;

  logger.info("apify_webhook_received", {
    requestId,
    hasDatasetId: Boolean(datasetId),
  });

  if (
    !datasetId ||
    typeof datasetId !== "string" ||
    datasetId.trim() === "" ||
    datasetId.includes("{{")
  ) {
    logger.warn("apify_webhook_invalid_dataset_id", {
      requestId,
      datasetId: typeof datasetId === "string" ? datasetId : null,
    });
    return res.status(400).json({ ok: false, error: "invalid datasetId" });
  }

  res.status(200).json({ ok: true });

  setImmediate(async () => {
    const startedAt = Date.now();
    const cleanDatasetId = datasetId.trim();
    try {
      const items = await fetchDatasetItems(cleanDatasetId);
      logger.info("apify_dataset_fetched", {
        requestId,
        datasetId: cleanDatasetId,
        items: items.length,
      });

      const summary = await saveRawInstagramPosts(items);
      const tookMs = Date.now() - startedAt;
      logger.info("apify_webhook_processed", {
        requestId,
        datasetId: cleanDatasetId,
        tookMs,
        summary,
      });

      if (summary.failed > 0 && summary.errors.length > 0) {
        logger.warn("apify_webhook_sample_errors", {
          requestId,
          datasetId: cleanDatasetId,
          sampleErrors: summary.errors.slice(0, 5),
        });
      }
    } catch (error) {
      logger.error("apify_webhook_processing_error", {
        requestId,
        datasetId: cleanDatasetId,
        error,
      });
    }
  });
});

app.use((err, req, res, _next) => {
  const requestId = req.requestId || null;

  if (err && err.type === "entity.parse.failed") {
    logger.warn("http_invalid_json", {
      requestId,
      path: req.originalUrl,
      method: req.method,
    });
    return res.status(400).json({
      ok: false,
      error: "invalid_json",
      message: "Malformed JSON body",
    });
  }

  logger.error("http_unhandled_error", {
    requestId,
    path: req.originalUrl,
    method: req.method,
    error: err,
  });
  return res.status(500).json({ ok: false, error: "internal_error" });
});

const port = Number(process.env.PORT || 8080);
app.listen(port, "0.0.0.0", () => {
  logger.info("server_started", {
    host: "0.0.0.0",
    port,
    nodeEnv: process.env.NODE_ENV || "development",
  });
});
