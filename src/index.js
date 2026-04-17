const express = require("express");
const crypto = require("crypto");

const { getConfig } = require("./config");
const { saveRawInstagramPosts } = require("./firestoreRepo");
const { fetchDatasetItems } = require("./apify");
const { initFirebase, getDb } = require("./firebase");
const { getIdempotencyRepository } = require("./idempotencyRepo");
const { createLogger } = require("./logger");
const { recordHttpRequest, snapshot } = require("./metrics");
const { getTelegramUseCase } = require("./telegramUseCase");
const { createTelegramClient } = require("./telegramClient");
const { createTelegramWebhookFlow } = require("./telegramWebhookFlow");

let config;
try {
  config = getConfig();
} catch (error) {
  const message = error && error.message ? error.message : String(error || "unknown_config_error");
  console.error(
    JSON.stringify({
      ts: new Date().toISOString(),
      level: "error",
      msg: "config_validation_failed",
      error: message,
    })
  );
  process.exit(1);
}

const app = express();
const logger = createLogger({ service: "node-api" });
const idempotencyRepo = getIdempotencyRepository();
const APIFY_WEBHOOK_SCOPE = "apify_webhook_instagram_dataset";
const TELEGRAM_SECRET_HEADER = "x-telegram-bot-api-secret-token";
const telegramWebhookPath = config.telegram.webhookPath;
const telegramWebhookFlow = config.telegram.enabled
  ? createTelegramWebhookFlow({
      telegramUseCase: getTelegramUseCase(),
      telegramClient: createTelegramClient({
        token: config.telegram.token,
      }),
      logger,
    })
  : null;

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
        nodeEnv: config.nodeEnv,
        port: config.port,
        apifyTokenConfigured: true,
        telegramEnabled: config.telegram.enabled,
        telegramWebhookPath: config.telegram.webhookPath,
        telegramWebhookSecretConfigured: Boolean(config.telegram.webhookSecret),
        firebaseCredentialsSource: config.firebase.source,
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
  const payloadRunId =
    (typeof req.body?.runId === "string" && req.body.runId.trim()) ||
    (typeof req.body?.resource?.id === "string" && req.body.resource.id.trim()) ||
    null;

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
    const idempotencyKey = payloadRunId ? `${cleanDatasetId}|${payloadRunId}` : cleanDatasetId;

    try {
      const claim = await idempotencyRepo.claimKey({
        scope: APIFY_WEBHOOK_SCOPE,
        key: idempotencyKey,
        allowRetryOnFailed: false,
        meta: {
          datasetId: cleanDatasetId,
          runId: payloadRunId,
          requestId,
        },
      });

      if (!claim.claimed) {
        logger.info("apify_webhook_replay_skipped", {
          requestId,
          datasetId: cleanDatasetId,
          runId: payloadRunId,
          idempotencyKey,
          idempotencyStatus: claim.status,
        });
        return;
      }

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
        runId: payloadRunId,
        idempotencyKey,
        tookMs,
        summary,
      });

      await idempotencyRepo.markCompleted({
        scope: APIFY_WEBHOOK_SCOPE,
        key: idempotencyKey,
        resultMeta: {
          datasetId: cleanDatasetId,
          runId: payloadRunId,
          tookMs,
          processed: summary.processed,
          inserted: summary.inserted,
          updated: summary.updated,
          failed: summary.failed,
        },
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
        runId: payloadRunId,
        idempotencyKey,
        error,
      });

      try {
        await idempotencyRepo.markFailed({
          scope: APIFY_WEBHOOK_SCOPE,
          key: idempotencyKey,
          error: error && error.message ? error.message : String(error || "unknown_error"),
        });
      } catch (markError) {
        logger.error("apify_webhook_idempotency_mark_failed_error", {
          requestId,
          datasetId: cleanDatasetId,
          runId: payloadRunId,
          idempotencyKey,
          error: markError,
        });
      }
    }
  });
});

app.post(telegramWebhookPath, async (req, res) => {
  const requestId = req.requestId;

  if (!config.telegram.enabled || !telegramWebhookFlow) {
    logger.warn("telegram_webhook_disabled", {
      requestId,
      path: req.originalUrl,
    });
    return res.status(503).json({ ok: false, error: "telegram_not_configured" });
  }

  if (config.telegram.webhookSecret) {
    const requestSecret = req.get(TELEGRAM_SECRET_HEADER);
    if (requestSecret !== config.telegram.webhookSecret) {
      logger.warn("telegram_webhook_unauthorized", {
        requestId,
        path: req.originalUrl,
      });
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }
  }

  logger.info("telegram_webhook_received", {
    requestId,
    path: req.originalUrl,
    updateId: req.body && typeof req.body.update_id === "number" ? req.body.update_id : null,
  });

  res.status(200).json({ ok: true });

  setImmediate(async () => {
    try {
      await telegramWebhookFlow.process(req.body, {
        requestId,
      });
    } catch (error) {
      logger.error("telegram_webhook_processing_error", {
        requestId,
        path: req.originalUrl,
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

app.listen(config.port, "0.0.0.0", () => {
  logger.info("server_started", {
    host: "0.0.0.0",
    port: config.port,
    nodeEnv: config.nodeEnv,
    firebaseCredentialsSource: config.firebase.source,
    telegramEnabled: config.telegram.enabled,
    telegramWebhookPath: config.telegram.webhookPath,
    telegramWebhookSecretConfigured: Boolean(config.telegram.webhookSecret),
  });
});
