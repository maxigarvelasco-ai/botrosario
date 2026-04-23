const { getDb } = require("./firebase");
const { getConfig } = require("./config");
const { assertNormalizedEvent, enums } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function asOptionalBoolean(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== "boolean") {
    throw new Error("isFree must be a boolean when provided");
  }
  return value;
}

function asPositiveLimit(value, fallback = 20) {
  if (value === undefined || value === null) {
    return fallback;
  }
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    throw new Error("limit must be a positive integer");
  }
  return Math.min(num, 500);
}

function assertIsoDate(value, fieldName) {
  if (value === undefined || value === null) {
    return null;
  }
  const clean = asNonEmptyString(value);
  if (!clean) {
    throw new Error(`${fieldName} must be a non-empty YYYY-MM-DD string when provided`);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(clean)) {
    throw new Error(`${fieldName} must be YYYY-MM-DD when provided`);
  }
  return clean;
}

function createEventCatalogRepository({
  db,
  collectionName,
  now = () => new Date().toISOString(),
} = {}) {
  if (!db || typeof db.collection !== "function" || typeof db.runTransaction !== "function") {
    throw new Error("createEventCatalogRepository requires a Firestore-like db instance");
  }

  const resolvedCollection = asNonEmptyString(collectionName) || "event_catalog";

  function collection() {
    return db.collection(resolvedCollection);
  }

  async function upsertEvents(events) {
    if (!Array.isArray(events)) {
      throw new Error("upsertEvents expects an array");
    }

    const summary = {
      received: events.length,
      upserted: 0,
      created: 0,
      updated: 0,
      failed: 0,
      eventHashes: [],
      errors: [],
    };

    for (let index = 0; index < events.length; index += 1) {
      try {
        const validated = assertNormalizedEvent(events[index]);
        const eventHash = asNonEmptyString(validated.eventHash);
        const ref = collection().doc(eventHash);

        const created = await db.runTransaction(async (tx) => {
          const snapshot = await tx.get(ref);
          const timestamp = now();
          const payload = {
            ...validated,
            eventHash,
            updatedAt: asNonEmptyString(validated.updatedAt) || timestamp,
            catalogUpdatedAt: timestamp,
          };

          if (!snapshot.exists) {
            payload.createdAt = timestamp;
          }

          tx.set(ref, payload, { merge: true });
          return !snapshot.exists;
        });

        summary.upserted += 1;
        summary.eventHashes.push(eventHash);
        if (created) {
          summary.created += 1;
        } else {
          summary.updated += 1;
        }
      } catch (error) {
        summary.failed += 1;
        const message = error && error.message ? error.message : String(error || "unknown_catalog_upsert_error");
        summary.errors.push(`index=${index} error=${message}`);
      }
    }

    return summary;
  }

  async function getByEventHash(eventHash) {
    const cleanHash = asNonEmptyString(eventHash);
    if (!cleanHash) {
      throw new Error("getByEventHash requires eventHash");
    }

    const snapshot = await collection().doc(cleanHash).get();
    if (!snapshot.exists) {
      return null;
    }
    return snapshot.data() || null;
  }

  async function findEvents(query = {}) {
    const cleanQuery = query && typeof query === "object" ? query : {};
    const limit = asPositiveLimit(cleanQuery.limit, 20);

    const city = asNonEmptyString(cleanQuery.city);
    const category = asNonEmptyString(cleanQuery.category);
    const eventDate = assertIsoDate(cleanQuery.eventDate, "eventDate");
    const isFree = asOptionalBoolean(cleanQuery.isFree);

    if (category && !enums.eventCategories.includes(category)) {
      throw new Error(`category must be one of: ${enums.eventCategories.join(", ")}`);
    }

    let fsQuery = collection();

    if (city) {
      fsQuery = fsQuery.where("city", "==", city);
    }

    if (category) {
      fsQuery = fsQuery.where("category", "==", category);
    }

    if (eventDate) {
      fsQuery = fsQuery.where("eventDate", "==", eventDate);
    }

    if (isFree !== null) {
      fsQuery = fsQuery.where("isFree", "==", isFree);
    }

    const snapshot = await fsQuery.limit(limit).get();
    return snapshot.docs.map((doc) => ({
      eventHash: doc.id,
      ...(doc.data() || {}),
    }));
  }

  return {
    upsertEvents,
    findEvents,
    getByEventHash,
  };
}

let cachedRepository = null;

function getEventCatalogRepository() {
  if (!cachedRepository) {
    const config = getConfig();
    cachedRepository = createEventCatalogRepository({
      db: getDb(),
      collectionName: config.eventCatalogCollection,
    });
  }
  return cachedRepository;
}

function resetEventCatalogRepositoryForTests() {
  cachedRepository = null;
}

module.exports = {
  createEventCatalogRepository,
  getEventCatalogRepository,
  resetEventCatalogRepositoryForTests,
};
