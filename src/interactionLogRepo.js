const crypto = require("crypto");

const { getDb } = require("./firebase");
const { getConfig } = require("./config");
const { assertInteractionLog } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function createInteractionLogRepository({
  db,
  collectionName,
  now = () => new Date().toISOString(),
  createId = () => crypto.randomUUID(),
} = {}) {
  if (!db || typeof db.collection !== "function") {
    throw new Error("createInteractionLogRepository requires a Firestore-like db instance");
  }

  const resolvedCollection = asNonEmptyString(collectionName) || "interaction_logs";

  async function saveInteraction(interaction) {
    if (!interaction || typeof interaction !== "object" || Array.isArray(interaction)) {
      throw new Error("saveInteraction requires an interaction object");
    }

    const payload = assertInteractionLog({
      ...interaction,
      id: asNonEmptyString(interaction.id) || createId(),
      createdAt: asNonEmptyString(interaction.createdAt) || now(),
    });

    await db.collection(resolvedCollection).doc(payload.id).set(payload);
    return payload;
  }

  return {
    saveInteraction,
  };
}

let cachedRepository = null;

function getInteractionLogRepository() {
  if (!cachedRepository) {
    const config = getConfig();
    cachedRepository = createInteractionLogRepository({
      db: getDb(),
      collectionName: config.interactionLogCollection,
    });
  }
  return cachedRepository;
}

function resetInteractionLogRepositoryForTests() {
  cachedRepository = null;
}

module.exports = {
  createInteractionLogRepository,
  getInteractionLogRepository,
  resetInteractionLogRepositoryForTests,
};
