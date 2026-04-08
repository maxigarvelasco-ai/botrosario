const { admin, db } = require("./firebase");

const IG_POSTS_COLLECTION = "ig_posts_raw";

function asNullableString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length ? out : null;
}

function asNullableNumber(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function toStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  const out = [];
  for (const item of value) {
    if (item === undefined || item === null) {
      continue;
    }

    if (typeof item === "string") {
      const clean = item.trim();
      if (clean) {
        out.push(clean);
      }
      continue;
    }

    if (typeof item === "object") {
      const candidate =
        item.username ||
        item.name ||
        item.tag ||
        item.text ||
        item.value ||
        item.hashtag;
      if (candidate) {
        const clean = String(candidate).trim();
        if (clean) {
          out.push(clean);
        }
      }
    }
  }

  return [...new Set(out)];
}

function extractHashtags(caption, sourceHashtags) {
  const fromSource = toStringArray(sourceHashtags).map((h) => h.replace(/^#/, ""));
  const fromCaption = [];

  if (typeof caption === "string" && caption.length) {
    const matches = caption.match(/#([\p{L}\p{N}_]+)/gu) || [];
    for (const match of matches) {
      fromCaption.push(match.replace(/^#/, ""));
    }
  }

  return [...new Set([...fromSource, ...fromCaption])];
}

function extractMentions(caption, sourceMentions) {
  const fromSource = toStringArray(sourceMentions).map((m) => m.replace(/^@/, ""));
  const fromCaption = [];

  if (typeof caption === "string" && caption.length) {
    const matches = caption.match(/@([A-Za-z0-9._]+)/g) || [];
    for (const match of matches) {
      fromCaption.push(match.replace(/^@/, ""));
    }
  }

  return [...new Set([...fromSource, ...fromCaption])];
}

function extractChildDisplayUrls(childPosts) {
  if (!Array.isArray(childPosts)) {
    return [];
  }

  const urls = [];
  for (const child of childPosts) {
    if (!child || typeof child !== "object") {
      continue;
    }

    const url =
      child.displayUrl ||
      child.display_url ||
      child.url ||
      child.imageUrl ||
      null;

    const clean = asNullableString(url);
    if (clean) {
      urls.push(clean);
    }
  }

  return [...new Set(urls)];
}

function getPostDocId(item) {
  if (!item || typeof item !== "object") {
    return null;
  }

  const id = asNullableString(item.id);
  if (id) {
    return id;
  }

  const shortCode = asNullableString(item.shortCode || item.shortcode);
  if (shortCode) {
    return shortCode;
  }

  return null;
}

function tryGetBestEffortDocHint(item) {
  if (!item || typeof item !== "object") {
    return null;
  }
  return asNullableString(item.id) || asNullableString(item.shortCode || item.shortcode) || null;
}

function normalizeInstagramItem(item) {
  const safe = item && typeof item === "object" ? item : {};

  const caption = asNullableString(safe.caption || safe.text || safe.description);
  const location = safe.location && typeof safe.location === "object" ? safe.location : {};
  const owner = safe.owner && typeof safe.owner === "object" ? safe.owner : {};

  return {
    id: asNullableString(safe.id),
    shortCode: asNullableString(safe.shortCode || safe.shortcode),
    type: asNullableString(safe.type || safe.typeName || safe.__typename),
    caption,
    hashtags: extractHashtags(caption, safe.hashtags),
    mentions: extractMentions(caption, safe.mentions),
    url: asNullableString(safe.url || safe.postUrl),
    commentsCount: asNullableNumber(safe.commentsCount),
    displayUrl: asNullableString(safe.displayUrl || safe.imageUrl),
    timestamp: asNullableString(safe.timestamp || safe.taken_at || safe.takenAt),
    locationName: asNullableString(safe.locationName || location.name),
    locationId: asNullableString(safe.locationId || location.id),
    ownerUsername: asNullableString(safe.ownerUsername || owner.username),
    ownerId: asNullableString(safe.ownerId || owner.id),
    productType: asNullableString(safe.productType),
    likesCount: asNullableNumber(safe.likesCount),
    videoUrl: asNullableString(safe.videoUrl),
    audioUrl: asNullableString(safe.audioUrl),
    inputUrl: asNullableString(safe.inputUrl),
    childDisplayUrls: extractChildDisplayUrls(safe.childPosts),
    ocrStatus: asNullableString(safe.ocrStatus) || "pending",
    ocrText: asNullableString(safe.ocrText),
    parsedEventIds: Array.isArray(safe.parsedEventIds) ? safe.parsedEventIds.filter(Boolean).map(String) : [],
  };
}

async function saveRawInstagramPost(item) {
  const docId = getPostDocId(item);
  if (!docId) {
    throw new Error("Cannot persist item without id or shortCode");
  }

  const normalized = normalizeInstagramItem(item);
  const ref = db.collection(IG_POSTS_COLLECTION).doc(docId);

  const snapshot = await ref.get();
  const payload = {
    ...normalized,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  if (!snapshot.exists) {
    payload.createdAt = admin.firestore.FieldValue.serverTimestamp();
  }

  await ref.set(payload, { merge: true });

  return {
    docId,
    created: !snapshot.exists,
  };
}

async function saveRawInstagramPosts(items) {
  if (!Array.isArray(items)) {
    throw new Error("saveRawInstagramPosts expects an array");
  }

  if (items.length === 0) {
    return {
      received: 0,
      processed: 0,
      upserted: 0,
      failed: 0,
      errors: [],
    };
  }

  const results = await Promise.allSettled(
    items.map(async (item, index) => {
      try {
        const saved = await saveRawInstagramPost(item);
        return {
          index,
          docId: saved.docId,
        };
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error || "unknown_error"));
        err.index = index;
        err.docHint = tryGetBestEffortDocHint(item);
        throw err;
      }
    })
  );

  const summary = {
    received: items.length,
    processed: items.length,
    upserted: 0,
    failed: 0,
    errors: [],
  };

  for (const result of results) {
    if (result.status === "fulfilled") {
      summary.upserted += 1;
      continue;
    }

    summary.failed += 1;
    const reason =
      result.reason && result.reason.message
        ? result.reason.message
        : String(result.reason || "unknown_error");
    const maybeIndex = typeof result.reason?.index === "number" ? result.reason.index : null;
    const maybeDocHint = typeof result.reason?.docHint === "string" ? result.reason.docHint : null;
    const prefix = [
      maybeIndex !== null ? `index=${maybeIndex}` : null,
      maybeDocHint ? `docHint=${maybeDocHint}` : null,
    ]
      .filter(Boolean)
      .join(" ");
    const message = prefix ? `${prefix} error=${reason}` : reason;
    summary.errors.push(message);
  }

  return summary;
}

module.exports = {
  getPostDocId,
  normalizeInstagramItem,
  saveRawInstagramPost,
  saveRawInstagramPosts,
  tryGetBestEffortDocHint,
};
