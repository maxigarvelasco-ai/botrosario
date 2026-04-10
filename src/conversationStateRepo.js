const { getDb } = require("./firebase");
const { getConfig } = require("./config");
const { assertConversationState } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function asFiniteNumber(value) {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return null;
  }
  return value;
}

function assertChatId(chatId) {
  const clean = asFiniteNumber(chatId);
  if (clean === null) {
    throw new Error("chatId must be a valid number");
  }
  return clean;
}

function assertPatchObject(patch) {
  if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
    throw new Error("patchState requires a patch object");
  }
}

function createConversationStateRepository({
  db,
  collectionName,
  now = () => new Date().toISOString(),
} = {}) {
  if (!db || typeof db.collection !== "function" || typeof db.runTransaction !== "function") {
    throw new Error("createConversationStateRepository requires a Firestore-like db instance");
  }

  const resolvedCollection = asNonEmptyString(collectionName) || "conversation_state";

  function refForChatId(chatId) {
    return db.collection(resolvedCollection).doc(String(chatId));
  }

  async function getState(chatId) {
    const cleanChatId = assertChatId(chatId);
    const snapshot = await refForChatId(cleanChatId).get();
    if (!snapshot.exists) {
      return null;
    }

    const data = snapshot.data() || {};
    return assertConversationState(data);
  }

  async function saveState(state) {
    if (!state || typeof state !== "object" || Array.isArray(state)) {
      throw new Error("saveState requires a state object");
    }

    const cleanChatId = assertChatId(state.chatId);
    const payload = assertConversationState({
      ...state,
      chatId: cleanChatId,
      updatedAt: now(),
    });

    await refForChatId(cleanChatId).set(payload);
    return payload;
  }

  async function patchState(chatId, patch) {
    const cleanChatId = assertChatId(chatId);
    assertPatchObject(patch);

    if (patch.chatId !== undefined && patch.chatId !== cleanChatId) {
      throw new Error("patch.chatId must match chatId");
    }

    const ref = refForChatId(cleanChatId);

    return db.runTransaction(async (tx) => {
      const snapshot = await tx.get(ref);
      if (!snapshot.exists) {
        throw new Error(`ConversationState not found for chatId=${cleanChatId}`);
      }

      const current = snapshot.data() || {};
      const merged = {
        ...current,
        ...patch,
        chatId: cleanChatId,
        updatedAt: now(),
      };
      const validated = assertConversationState(merged);

      tx.set(ref, validated);
      return validated;
    });
  }

  return {
    getState,
    saveState,
    patchState,
  };
}

let cachedRepository = null;

function getConversationStateRepository() {
  if (!cachedRepository) {
    const config = getConfig();
    cachedRepository = createConversationStateRepository({
      db: getDb(),
      collectionName: config.conversationStateCollection,
    });
  }

  return cachedRepository;
}

function resetConversationStateRepositoryForTests() {
  cachedRepository = null;
}

module.exports = {
  createConversationStateRepository,
  getConversationStateRepository,
  resetConversationStateRepositoryForTests,
};
