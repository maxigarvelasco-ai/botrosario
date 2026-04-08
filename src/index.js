const express = require("express");
require("dotenv").config();

const { saveRawInstagramPosts } = require("./firestoreRepo");

const app = express();
app.use(express.json({ limit: "2mb" }));

function extractItemsFromBody(body) {
  if (Array.isArray(body)) {
    return body;
  }

  if (!body || typeof body !== "object") {
    return null;
  }

  if (Array.isArray(body.items)) {
    return body.items;
  }

  if (Array.isArray(body.data)) {
    return body.data;
  }

  if (body.data && typeof body.data === "object" && Array.isArray(body.data.items)) {
    return body.data.items;
  }

  if (body.item && typeof body.item === "object") {
    return [body.item];
  }

  return [body];
}

app.get("/health", (_req, res) => {
  return res.status(200).send("ok");
});

app.post("/webhooks/apify/instagram", (req, res) => {
  const items = extractItemsFromBody(req.body);
  if (!Array.isArray(items)) {
    return res.status(400).json({
      ok: false,
      error: "invalid_payload",
      message: "Expected payload as object or array",
    });
  }

  console.log("[apify-webhook] received", JSON.stringify({ received: items.length }));

  if (items.length === 0) {
    return res.status(200).json({
      ok: true,
      accepted: 0,
      message: "No items to process",
    });
  }

  res.status(200).json({
    ok: true,
    accepted: items.length,
    message: "Webhook received",
  });

  setImmediate(async () => {
    const startedAt = Date.now();
    try {
      const summary = await saveRawInstagramPosts(items);
      const tookMs = Date.now() - startedAt;
      console.log(
        "[apify-webhook] processed",
        JSON.stringify({
          ...summary,
          tookMs,
        })
      );

      if (summary.failed > 0 && summary.errors.length > 0) {
        console.error("[apify-webhook] sample errors", summary.errors.slice(0, 5));
      }
    } catch (error) {
      console.error("[apify-webhook] fatal processing error", error);
    }
  });
});

app.use((err, _req, res, _next) => {
  if (err && err.type === "entity.parse.failed") {
    return res.status(400).json({
      ok: false,
      error: "invalid_json",
      message: "Malformed JSON body",
    });
  }

  console.error("[server] unhandled error", err);
  return res.status(500).json({ ok: false, error: "internal_error" });
});

const port = Number(process.env.PORT || 8080);
app.listen(port, "0.0.0.0", () => {
  console.log(`[server] listening on 0.0.0.0:${port}`);
});
