require("dotenv").config();

const { runPendingInstagramPostsOnce } = require("./processPendingInstagramPosts");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runLoop() {
  const intervalMs = Math.max(5000, Number(process.env.IG_POSTS_POLL_MS || 30000) || 30000);
  console.log("[ig-worker-loop] started", JSON.stringify({ intervalMs }));

  while (true) {
    try {
      await runPendingInstagramPostsOnce();
    } catch (error) {
      console.error("[ig-worker-loop] cycle error", error && error.message ? error.message : error);
    }

    await sleep(intervalMs);
  }
}

if (require.main === module) {
  runLoop().catch((error) => {
    console.error("[ig-worker-loop] fatal", error);
    process.exit(1);
  });
}
