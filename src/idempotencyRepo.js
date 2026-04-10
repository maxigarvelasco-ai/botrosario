const crypto = require("crypto");

const { getDb } = require("./firebase");
const { getConfig } = require("./config");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function hashDocId(scope, key) {
  return crypto.createHash("sha1").update(`${scope}:${key}`, "utf8").digest("hex");
}

function createIdempotencyRepository({
  db,
  collectionName,
  now = () => new Date().toISOString(),
} = {}) {
  if (!db || typeof db.runTransaction !== "function" || typeof db.collection !== "function") {
    throw new Error("createIdempotencyRepository requires a Firestore-like db instance");
  }

  const resolvedCollection = asNonEmptyString(collectionName) || "idempotency_keys";

  function refFor(scope, key) {
    return db.collection(resolvedCollection).doc(hashDocId(scope, key));
  }

  function assertInput(scope, key) {
    if (!asNonEmptyString(scope)) {
      throw new Error("scope is required");
    }
    if (!asNonEmptyString(key)) {
      throw new Error("key is required");
    }
  }

  async function claimKey({ scope, key, meta = null, allowRetryOnFailed = true }) {
    assertInput(scope, key);
    const cleanScope = asNonEmptyString(scope);
    const cleanKey = asNonEmptyString(key);
    const ref = refFor(cleanScope, cleanKey);

    const decision = await db.runTransaction(async (tx) => {
      const snapshot = await tx.get(ref);
      const timestamp = now();

      if (!snapshot.exists) {
        tx.set(ref, {
          scope: cleanScope,
          key: cleanKey,
          status: "processing",
          attempts: 1,
          createdAt: timestamp,
          updatedAt: timestamp,
          lastClaimedAt: timestamp,
          meta,
        });

        return {
          claimed: true,
          status: "processing",
          reason: "created",
        };
      }

      const current = snapshot.data() || {};
      const currentStatus = asNonEmptyString(current.status) || "unknown";

      if (currentStatus === "failed" && allowRetryOnFailed) {
        tx.update(ref, {
          status: "processing",
          updatedAt: timestamp,
          lastClaimedAt: timestamp,
          attempts: Number(current.attempts || 0) + 1,
          meta,
        });

        return {
          claimed: true,
          status: "processing",
          reason: "retry_after_failed",
        };
      }

      return {
        claimed: false,
        status: currentStatus,
        reason: "already_exists",
      };
    });

    return {
      scope: cleanScope,
      key: cleanKey,
      ...decision,
    };
  }

  async function markCompleted({ scope, key, resultMeta = null }) {
    assertInput(scope, key);
    const cleanScope = asNonEmptyString(scope);
    const cleanKey = asNonEmptyString(key);
    const ref = refFor(cleanScope, cleanKey);
    const timestamp = now();

    await ref.set(
      {
        scope: cleanScope,
        key: cleanKey,
        status: "completed",
        updatedAt: timestamp,
        completedAt: timestamp,
        resultMeta,
      },
      { merge: true }
    );
  }

  async function markFailed({ scope, key, error = null }) {
    assertInput(scope, key);
    const cleanScope = asNonEmptyString(scope);
    const cleanKey = asNonEmptyString(key);
    const ref = refFor(cleanScope, cleanKey);
    const timestamp = now();

    await ref.set(
      {
        scope: cleanScope,
        key: cleanKey,
        status: "failed",
        updatedAt: timestamp,
        failedAt: timestamp,
        lastError: asNonEmptyString(error) || String(error || "unknown_error"),
      },
      { merge: true }
    );
  }

  async function getKeyState({ scope, key }) {
    assertInput(scope, key);
    const cleanScope = asNonEmptyString(scope);
    const cleanKey = asNonEmptyString(key);
    const ref = refFor(cleanScope, cleanKey);
    const snapshot = await ref.get();

    if (!snapshot.exists) {
      return null;
    }

    return snapshot.data() || null;
  }

  return {
    claimKey,
    markCompleted,
    markFailed,
    getKeyState,
  };
}

let cachedRepository = null;

function getIdempotencyRepository() {
  if (!cachedRepository) {
    const config = getConfig();
    cachedRepository = createIdempotencyRepository({
      db: getDb(),
      collectionName: config.idempotencyCollection,
    });
  }
  return cachedRepository;
}

function resetIdempotencyRepositoryForTests() {
  cachedRepository = null;
}

module.exports = {
  createIdempotencyRepository,
  getIdempotencyRepository,
  resetIdempotencyRepositoryForTests,
};
