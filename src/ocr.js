const { createWorker } = require("tesseract.js");

const OCR_LANG = String(process.env.OCR_LANG || process.env.TESSERACT_LANG || "spa+eng").trim() || "spa+eng";
const IMAGE_FETCH_TIMEOUT_MS = Math.max(5000, Number(process.env.IMAGE_FETCH_TIMEOUT_MS || 15000) || 15000);

let workerPromise = null;

function asNullableString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length ? out : null;
}

function uniqueUrls(urls) {
  const out = [];
  const seen = new Set();

  for (const url of urls) {
    const clean = asNullableString(url);
    if (!clean) {
      continue;
    }

    let parsed;
    try {
      parsed = new URL(clean);
    } catch (_error) {
      continue;
    }

    if (!/^https?:$/i.test(parsed.protocol)) {
      continue;
    }

    const key = parsed.toString();
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    out.push(key);
  }

  return out;
}

function getImageUrlsFromPost(post) {
  const urls = [];
  if (post && typeof post === "object") {
    if (post.displayUrl) {
      urls.push(post.displayUrl);
    }

    if (Array.isArray(post.childDisplayUrls)) {
      urls.push(...post.childDisplayUrls);
    }
  }

  return uniqueUrls(urls);
}

async function getWorker() {
  if (!workerPromise) {
    workerPromise = createWorker(OCR_LANG);
  }
  return workerPromise;
}

async function fetchImageAsBuffer(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), IMAGE_FETCH_TIMEOUT_MS);

  try {
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        Accept: "image/*,*/*;q=0.8",
      },
    });

    if (!response.ok) {
      throw new Error(`image_fetch_http_${response.status}`);
    }

    const bytes = await response.arrayBuffer();
    return Buffer.from(bytes);
  } finally {
    clearTimeout(timeout);
  }
}

async function extractTextFromImageUrl(url) {
  const worker = await getWorker();
  const buffer = await fetchImageAsBuffer(url);
  const result = await worker.recognize(buffer);
  const text =
    result && result.data && typeof result.data.text === "string" ? result.data.text.trim() : "";

  return text;
}

async function extractTextFromPostImages(post) {
  const urls = getImageUrlsFromPost(post);
  const parts = [];
  const perImage = [];

  for (const url of urls) {
    try {
      const text = await extractTextFromImageUrl(url);
      const clean = String(text || "").trim();
      if (clean) {
        parts.push(clean);
      }
      perImage.push({
        url,
        ok: true,
        textLength: clean.length,
      });
    } catch (error) {
      perImage.push({
        url,
        ok: false,
        error: error && error.message ? error.message : String(error || "ocr_image_error"),
      });
    }
  }

  return {
    urls,
    attempted: urls.length,
    successful: perImage.filter((x) => x.ok).length,
    failed: perImage.filter((x) => !x.ok).length,
    perImage,
    text: parts.join("\n\n").trim(),
  };
}

async function shutdownOcrWorker() {
  if (!workerPromise) {
    return;
  }

  try {
    const worker = await workerPromise;
    await worker.terminate();
  } catch (_error) {
    // Best effort shutdown.
  } finally {
    workerPromise = null;
  }
}

module.exports = {
  extractTextFromImageUrl,
  extractTextFromPostImages,
  getImageUrlsFromPost,
  shutdownOcrWorker,
};
