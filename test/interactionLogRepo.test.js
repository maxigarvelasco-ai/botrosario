const test = require("node:test");
const assert = require("node:assert/strict");

const { createInteractionLogRepository } = require("../src/interactionLogRepo");

function createFakeDb() {
  const store = new Map();

  function storeKey(collection, id) {
    return `${collection}/${id}`;
  }

  class FakeDocRef {
    constructor(collection, id) {
      this.collection = collection;
      this.id = id;
    }

    async set(payload) {
      store.set(storeKey(this.collection, this.id), { ...payload });
    }
  }

  return {
    collection(name) {
      return {
        doc(id) {
          return new FakeDocRef(name, id);
        },
      };
    },
    read(collection, id) {
      return store.get(storeKey(collection, id)) || null;
    },
  };
}

function validInteraction(overrides = {}) {
  return {
    id: "int_1",
    chatId: 123,
    createdAt: "2026-04-10T00:00:00.000Z",
    query: "quiero museo para hoy",
    constraints: {
      rawText: "quiero museo para hoy",
      dateScope: "today",
      timeScope: "none",
      nearby: false,
      includeCategories: ["museo"],
      excludeCategories: [],
      mood: "none",
      styleHint: "none",
      splitPlan: false,
      noMixDays: false,
      source: "deterministic",
    },
    recommendationStatus: "ok",
    usedFallback: false,
    shortlistSummary: ["Museo Castagnino", "Macro"],
    metadata: {
      source: "test",
    },
    ...overrides,
  };
}

test("saveInteraction persists a valid InteractionLog", async () => {
  const db = createFakeDb();
  const repo = createInteractionLogRepository({
    db,
    collectionName: "interaction_logs_test",
  });

  const saved = await repo.saveInteraction(validInteraction());
  const stored = db.read("interaction_logs_test", saved.id);

  assert.equal(saved.id, "int_1");
  assert.equal(stored.chatId, 123);
  assert.equal(stored.recommendationStatus, "ok");
});

test("saveInteraction autocompletes id and createdAt when missing", async () => {
  const db = createFakeDb();
  const repo = createInteractionLogRepository({
    db,
    collectionName: "interaction_logs_test",
    now: () => "2026-04-10T00:00:00.000Z",
    createId: () => "int_generated",
  });

  const saved = await repo.saveInteraction(
    validInteraction({
      id: undefined,
      createdAt: undefined,
    })
  );

  const stored = db.read("interaction_logs_test", "int_generated");
  assert.equal(saved.id, "int_generated");
  assert.equal(saved.createdAt, "2026-04-10T00:00:00.000Z");
  assert.equal(stored.id, "int_generated");
});

test("saveInteraction rejects invalid shape", async () => {
  const db = createFakeDb();
  const repo = createInteractionLogRepository({
    db,
    collectionName: "interaction_logs_test",
  });

  await assert.rejects(
    () =>
      repo.saveInteraction(
        validInteraction({
          usedFallback: "no",
        })
      ),
    /InteractionLog validation failed/
  );
});
