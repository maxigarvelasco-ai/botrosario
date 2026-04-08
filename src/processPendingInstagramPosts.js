require("dotenv").config();

const {
  getPendingInstagramPosts,
  markPostDone,
  markPostError,
  markPostProcessing,
  upsertEvents,
} = require("./firestoreRepo");
const { extractTextFromPostImages, shutdownOcrWorker } = require("./ocr");
const { extractEventsFromPost } = require("./eventExtractor");

function toPositiveInt(value, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) {
    return fallback;
  }
  return Math.floor(num);
}

async function runWithConcurrency(items, limit, workerFn) {
  const queue = [...items];
  const results = [];

  async function consumeOne() {
    while (queue.length > 0) {
      const item = queue.shift();
      if (!item) {
        continue;
      }
      const result = await workerFn(item);
      results.push(result);
    }
  }

  const workers = [];
  const safeLimit = Math.max(1, Math.min(limit, items.length || 1));
  for (let i = 0; i < safeLimit; i += 1) {
    workers.push(consumeOne());
  }

  await Promise.all(workers);
  return results;
}

async function processSinglePost(post) {
  const postId = String(post.postId || post.id || "").trim();
  if (!postId) {
    return {
      ok: false,
      skipped: true,
      reason: "missing_post_id",
      eventCount: 0,
    };
  }

  const locked = await markPostProcessing(postId);
  if (!locked) {
    return {
      ok: true,
      skipped: true,
      postId,
      reason: "already_processing_or_done",
      eventCount: 0,
    };
  }

  try {
    const ocrResult = await extractTextFromPostImages(post);
    const mergedPost = {
      ...post,
      postId,
      ocrText: ocrResult.text,
    };

    const extractedEvents = extractEventsFromPost(mergedPost);
    const upsertSummary = await upsertEvents(extractedEvents);

    await markPostDone(postId, {
      ocrText: ocrResult.text,
      parsedEventIds: upsertSummary.eventIds,
      ocrMeta: {
        attemptedImages: ocrResult.attempted,
        successfulImages: ocrResult.successful,
        failedImages: ocrResult.failed,
      },
    });

    return {
      ok: true,
      skipped: false,
      postId,
      eventCount: upsertSummary.upserted,
      createdEvents: upsertSummary.created,
      updatedEvents: upsertSummary.updated,
      failedEvents: upsertSummary.failed,
      ocrImagesAttempted: ocrResult.attempted,
      ocrImagesFailed: ocrResult.failed,
    };
  } catch (error) {
    await markPostError(postId, error);

    return {
      ok: false,
      skipped: false,
      postId,
      eventCount: 0,
      error: error && error.message ? error.message : String(error || "unknown_post_error"),
    };
  }
}

async function runPendingInstagramPostsOnce(options = {}) {
  const batchSize = toPositiveInt(options.batchSize || process.env.IG_POSTS_BATCH_SIZE, 5);
  const concurrency = toPositiveInt(options.concurrency || process.env.IG_POSTS_CONCURRENCY, 2);

  const pendingPosts = await getPendingInstagramPosts(batchSize);
  console.log("[ig-worker] pending posts", pendingPosts.length);

  if (pendingPosts.length === 0) {
    return {
      found: 0,
      processed: 0,
      success: 0,
      failed: 0,
      skipped: 0,
      eventsUpserted: 0,
      eventsCreated: 0,
      eventsUpdated: 0,
    };
  }

  const results = await runWithConcurrency(pendingPosts, concurrency, processSinglePost);

  const summary = {
    found: pendingPosts.length,
    processed: results.length,
    success: results.filter((x) => x.ok && !x.skipped).length,
    failed: results.filter((x) => !x.ok).length,
    skipped: results.filter((x) => x.skipped).length,
    eventsUpserted: results.reduce((acc, x) => acc + (x.eventCount || 0), 0),
    eventsCreated: results.reduce((acc, x) => acc + (x.createdEvents || 0), 0),
    eventsUpdated: results.reduce((acc, x) => acc + (x.updatedEvents || 0), 0),
  };

  console.log("[ig-worker] summary", JSON.stringify(summary));
  return summary;
}

if (require.main === module) {
  runPendingInstagramPostsOnce()
    .then(async () => {
      await shutdownOcrWorker();
      process.exit(0);
    })
    .catch(async (error) => {
      console.error("[ig-worker] fatal", error);
      await shutdownOcrWorker();
      process.exit(1);
    });
}

module.exports = {
  runPendingInstagramPostsOnce,
};
