const { admin, getDb } = require("./firebase");
const crypto = require("crypto");

const IG_POSTS_COLLECTION = "ig_posts_raw";
const EVENTS_COLLECTION = String(process.env.FIRESTORE_EVENTS_COLLECTION || "events").trim() || "events";

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

function normalizeToken(value) {
  const base = asNullableString(value);
  if (!base) {
    return null;
  }
  return base
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || null;
}

function toUniqueStringArray(value) {
  if (!Array.isArray(value)) {
    return [];
  }
  const out = [];
  const seen = new Set();
  for (const item of value) {
    const clean = asNullableString(item);
    if (!clean) {
      continue;
    }
    const key = clean.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    out.push(clean);
  }
  return out;
}

function buildEventHash(event) {
  const signature = [
    normalizeToken(event.categoria) || "sin_categoria",
    normalizeToken(event.fecha_text) || "sin_fecha",
    normalizeToken(event.hora) || "sin_hora",
    normalizeToken(event.ciudad) || "sin_ciudad",
    normalizeToken(event.lugar) || "sin_lugar",
    normalizeToken(event.event_date) || "sin_event_date",
    normalizeToken(event.tipo_norm) || "sin_tipo",
    normalizeToken(event.artista_norm) || "sin_artista",
  ].join("|");

  return crypto.createHash("sha1").update(signature, "utf8").digest("hex");
}

function normalizeEventForStorage(event) {
  const payload = event && typeof event.payload === "object" && event.payload !== null ? event.payload : {};

  const categoria = asNullableString(event.categoria) || "sin_categoria";
  const ciudad = asNullableString(event.ciudad) || "Rosario";
  const lugar = asNullableString(event.lugar);
  const tipoNorm = normalizeToken(event.tipo_norm || event.tipo) || "evento";
  const artistaNorm = normalizeToken(event.artista_norm || event.artista || event.artista_o_show);
  const categoryNorm = normalizeToken(event.category_norm || categoria) || "sin_categoria";
  const ciudadNorm = normalizeToken(event.ciudad_norm || ciudad) || "rosario";
  const lugarNorm = normalizeToken(event.lugar_norm || lugar);

  return {
    categoria,
    fecha_text: asNullableString(event.fecha_text || event.fecha),
    hora: asNullableString(event.hora),
    ciudad,
    lugar,
    event_date: asNullableString(event.event_date),
    payload,
    category_norm: categoryNorm,
    ciudad_norm: ciudadNorm,
    lugar_norm: lugarNorm,
    tipo_norm: tipoNorm,
    artista_norm: artistaNorm,
    is_free: Boolean(event.is_free),
    tags: toUniqueStringArray(event.tags || []),
  };
}

async function saveRawInstagramPost(item) {
  const docId = getPostDocId(item);
  if (!docId) {
    throw new Error("Cannot persist item without id or shortCode");
  }

  const db = getDb();
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

async function getPendingInstagramPosts(limit = 10) {
  const db = getDb();
  const safeLimit = Math.max(1, Math.min(50, Number(limit) || 10));
  const query = db.collection(IG_POSTS_COLLECTION).where("ocrStatus", "==", "pending").limit(safeLimit);
  const snapshot = await query.get();

  return snapshot.docs.map((doc) => ({
    postId: doc.id,
    ...doc.data(),
  }));
}

async function markPostProcessing(postId) {
  const cleanPostId = asNullableString(postId);
  if (!cleanPostId) {
    throw new Error("markPostProcessing requires postId");
  }

  const db = getDb();
  const ref = db.collection(IG_POSTS_COLLECTION).doc(cleanPostId);

  return db.runTransaction(async (tx) => {
    const snapshot = await tx.get(ref);
    if (!snapshot.exists) {
      return false;
    }

    const data = snapshot.data() || {};
    const currentStatus = asNullableString(data.ocrStatus) || "pending";
    if (currentStatus !== "pending") {
      return false;
    }

    tx.set(
      ref,
      {
        ocrStatus: "processing",
        ocrError: null,
        ocrUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    return true;
  });
}

async function markPostError(postId, error) {
  const cleanPostId = asNullableString(postId);
  if (!cleanPostId) {
    throw new Error("markPostError requires postId");
  }

  const message =
    error && error.message ? String(error.message) : asNullableString(error) || "unknown_error";

  const db = getDb();
  const ref = db.collection(IG_POSTS_COLLECTION).doc(cleanPostId);
  await ref.set(
    {
      ocrStatus: "error",
      ocrError: message.slice(0, 1200),
      ocrUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function updatePostParsedEvents(postId, eventIds) {
  const cleanPostId = asNullableString(postId);
  if (!cleanPostId) {
    throw new Error("updatePostParsedEvents requires postId");
  }

  const ids = toUniqueStringArray(eventIds || []);

  const db = getDb();
  const ref = db.collection(IG_POSTS_COLLECTION).doc(cleanPostId);
  await ref.set(
    {
      parsedEventIds: ids,
      parsedEventsCount: ids.length,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );
}

async function markPostDone(postId, data = {}) {
  const cleanPostId = asNullableString(postId);
  if (!cleanPostId) {
    throw new Error("markPostDone requires postId");
  }

  const ids = toUniqueStringArray(data.parsedEventIds || []);
  const ocrText = asNullableString(data.ocrText);

  const payload = {
    ocrStatus: "done",
    ocrError: null,
    ocrText: ocrText || null,
    parsedEventIds: ids,
    parsedEventsCount: ids.length,
    ocrUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  if (data.ocrMeta && typeof data.ocrMeta === "object") {
    payload.ocrMeta = data.ocrMeta;
  }

  const db = getDb();
  const ref = db.collection(IG_POSTS_COLLECTION).doc(cleanPostId);
  await ref.set(payload, { merge: true });
}

async function upsertEvents(events) {
  if (!Array.isArray(events)) {
    throw new Error("upsertEvents expects an array");
  }

  const db = getDb();
  const summary = {
    received: events.length,
    upserted: 0,
    created: 0,
    updated: 0,
    failed: 0,
    eventIds: [],
    errors: [],
  };

  for (let index = 0; index < events.length; index += 1) {
    try {
      const normalized = normalizeEventForStorage(events[index] || {});
      const eventHash = buildEventHash(normalized);
      const ref = db.collection(EVENTS_COLLECTION).doc(eventHash);

      const isCreated = await db.runTransaction(async (tx) => {
        const snap = await tx.get(ref);

        const payload = {
          event_hash: eventHash,
          ...normalized,
          updated_at: admin.firestore.FieldValue.serverTimestamp(),
        };

        if (!snap.exists) {
          payload.created_at = admin.firestore.FieldValue.serverTimestamp();
        }

        tx.set(ref, payload, { merge: true });
        return !snap.exists;
      });

      summary.upserted += 1;
      summary.eventIds.push(eventHash);
      if (isCreated) {
        summary.created += 1;
      } else {
        summary.updated += 1;
      }
    } catch (error) {
      summary.failed += 1;
      const message =
        error && error.message ? error.message : String(error || "unknown_event_upsert_error");
      summary.errors.push(`index=${index} error=${message}`);
    }
  }

  return summary;
}

module.exports = {
  buildEventHash,
  getPostDocId,
  getPendingInstagramPosts,
  markPostDone,
  markPostError,
  markPostProcessing,
  normalizeInstagramItem,
  normalizeEventForStorage,
  saveRawInstagramPost,
  saveRawInstagramPosts,
  updatePostParsedEvents,
  upsertEvents,
  tryGetBestEffortDocHint,
};
