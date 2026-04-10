const test = require("node:test");
const assert = require("node:assert/strict");

const { createConversationStateRepository } = require("../src/conversationStateRepo");

function createFakeDb() {
  const store = new Map();

  function makeStoreKey(collection, id) {
    return `${collection}/${id}`;
  }

  class FakeDocSnapshot {
    constructor(value) {
      this.exists = value !== undefined;
      this._value = value;
    }

    data() {
      return this._value;
    }
  }

  class FakeDocRef {
    constructor(collection, id) {
      this.collection = collection;
      this.id = id;
    }

    async get() {
      return new FakeDocSnapshot(store.get(makeStoreKey(this.collection, this.id)));
    }

    async set(payload) {
      store.set(makeStoreKey(this.collection, this.id), { ...payload });
    }
  }

  class FakeTransaction {
    async get(ref) {
      return ref.get();
    }

    set(ref, payload) {
      store.set(makeStoreKey(ref.collection, ref.id), { ...payload });
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
    async runTransaction(fn) {
      return fn(new FakeTransaction());
    },
  };
}

function validConversationState(overrides = {}) {
  return {
    chatId: 123,
    pendingLocationRequest: false,
    updatedAt: "2026-04-10T00:00:00.000Z",
    lastUserQuery: "museos en rosario",
    ...overrides,
  };
}

function buildRepo() {
  const db = createFakeDb();
  return createConversationStateRepository({
    db,
    collectionName: "conversation_state_test",
    now: () => "2026-04-10T00:00:00.000Z",
  });
}

test("getState returns null when chatId does not exist", async () => {
  const repo = buildRepo();
  const state = await repo.getState(999);
  assert.equal(state, null);
});

test("saveState persists valid ConversationState and normalizes updatedAt", async () => {
  const repo = buildRepo();

  await repo.saveState(
    validConversationState({
      updatedAt: "2020-01-01T00:00:00.000Z",
    })
  );

  const saved = await repo.getState(123);
  assert.equal(saved.chatId, 123);
  assert.equal(saved.pendingLocationRequest, false);
  assert.equal(saved.updatedAt, "2026-04-10T00:00:00.000Z");
});

test("patchState updates only provided fields and preserves valid shape", async () => {
  const repo = buildRepo();

  await repo.saveState(validConversationState());
  const patched = await repo.patchState(123, {
    pendingLocationRequest: true,
    pendingNearbyQuery: "cerca de pichincha",
  });

  assert.equal(patched.chatId, 123);
  assert.equal(patched.pendingLocationRequest, true);
  assert.equal(patched.pendingNearbyQuery, "cerca de pichincha");
  assert.equal(patched.lastUserQuery, "museos en rosario");
  assert.equal(patched.updatedAt, "2026-04-10T00:00:00.000Z");
});

test("patchState rejects invalid patch that breaks ConversationState", async () => {
  const repo = buildRepo();

  await repo.saveState(validConversationState());

  await assert.rejects(
    () => repo.patchState(123, { pendingLocationRequest: "si" }),
    /ConversationState validation failed/
  );
});

test("patchState rejects when state does not exist", async () => {
  const repo = buildRepo();

  await assert.rejects(
    () => repo.patchState(123, { pendingLocationRequest: true }),
    /ConversationState not found/
  );
});
