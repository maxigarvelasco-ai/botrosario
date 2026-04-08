const express = require("express");
require("dotenv").config();

const { saveRawInstagramPosts } = require("./firestoreRepo");
const { fetchDatasetItems } = require("./apify");

const app = express();
app.use(express.json({ limit: "2mb" }));

app.get("/health", (_req, res) => {
  return res.status(200).send("ok");
});

app.post("/webhooks/apify/instagram", async (req, res) => {
  const datasetId = req.body?.datasetId;

  console.log("[apify-webhook] received", req.body);

  if (
    !datasetId ||
    typeof datasetId !== "string" ||
    datasetId.trim() === "" ||
    datasetId.includes("{{")
  ) {
    return res.status(400).json({ ok: false, error: "invalid datasetId" });
  }

  res.status(200).json({ ok: true });

  setImmediate(async () => {
    const startedAt = Date.now();
    try {
      const cleanDatasetId = datasetId.trim();
      const items = await fetchDatasetItems(cleanDatasetId);
      console.log(
        "[apify-webhook] fetched",
        JSON.stringify({ datasetId: cleanDatasetId, items: items.length })
      );

      const summary = await saveRawInstagramPosts(items);
      const tookMs = Date.now() - startedAt;
      console.log(
        "[apify-webhook] processed",
        JSON.stringify({
          datasetId: cleanDatasetId,
          ...summary,
          tookMs,
        })
      );

      if (summary.failed > 0 && summary.errors.length > 0) {
        console.error("[apify-webhook] sample errors", summary.errors.slice(0, 5));
      }
    } catch (error) {
      console.error(
        "[apify-webhook] fatal processing error",
        JSON.stringify({
          datasetId: typeof datasetId === "string" ? datasetId.trim() : null,
          error: error && error.message ? error.message : String(error || "unknown_error"),
        })
      );
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
