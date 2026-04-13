const test = require("node:test");
const assert = require("node:assert/strict");

const { createIdempotencyRepository } = require("../src/idempotencyRepo");

function createFakeDb() {
  const store = new Map();

  function mapKey(collection, id) {
    return `${collection}/${id}`;
  }

  class FakeDocSnapshot {
    constructor(data) {
      this.exists = data !== undefined;
      this._data = data;
    }

    data() {
      return this._data;
    }
  }

  class FakeDocRef {
    constructor(collection, id) {
      this.collection = collection;
      this.id = id;
    }

    async get() {
      return new FakeDocSnapshot(store.get(mapKey(this.collection, this.id)));
    }

    async set(data, options = {}) {
      const key = mapKey(this.collection, this.id);
      const prev = store.get(key);
      if (options && options.merge && prev && typeof prev === "object") {
        store.set(key, { ...prev, ...data });
        return;
      }
      store.set(key, { ...data });
    }
  }

  class FakeTransaction {
    async get(ref) {
      return ref.get();
    }

    set(ref, data) {
      const key = mapKey(ref.collection, ref.id);
      store.set(key, { ...data });
    }

    update(ref, patch) {
      const key = mapKey(ref.collection, ref.id);
      const prev = store.get(key);
      if (!prev) {
        throw new Error("cannot update missing document");
      }
      store.set(key, { ...prev, ...patch });
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

function createRepo() {
  const db = createFakeDb();
  return createIdempotencyRepository({
    db,
    collectionName: "idempotency_test",
    now: () => "2026-04-10T00:00:00.000Z",
  });
}

test("claimKey creates processing record on first claim", async () => {
  const repo = createRepo();
  const claim = await repo.claimKey({ scope: "apify", key: "dataset-1" });

  assert.equal(claim.claimed, true);
  assert.equal(claim.status, "processing");
  assert.equal(claim.reason, "created");
});

test("claimKey rejects duplicate while processing", async () => {
  const repo = createRepo();

  const first = await repo.claimKey({ scope: "apify", key: "dataset-1" });
  const second = await repo.claimKey({ scope: "apify", key: "dataset-1" });

  assert.equal(first.claimed, true);
  assert.equal(second.claimed, false);
  assert.equal(second.status, "processing");
});

test("markCompleted blocks future claims for same key", async () => {
  const repo = createRepo();

  await repo.claimKey({ scope: "apify", key: "dataset-1" });
  await repo.markCompleted({ scope: "apify", key: "dataset-1" });
  const second = await repo.claimKey({ scope: "apify", key: "dataset-1" });

  assert.equal(second.claimed, false);
  assert.equal(second.status, "completed");
});

test("markFailed allows claim retry by default", async () => {
  const repo = createRepo();

  await repo.claimKey({ scope: "apify", key: "dataset-1" });
  await repo.markFailed({ scope: "apify", key: "dataset-1", error: "boom" });
  const retryClaim = await repo.claimKey({ scope: "apify", key: "dataset-1" });

  assert.equal(retryClaim.claimed, true);
  assert.equal(retryClaim.reason, "retry_after_failed");
});

test("markFailed blocks claim when allowRetryOnFailed is false", async () => {
  const repo = createRepo();

  await repo.claimKey({ scope: "apify", key: "dataset-1" });
  await repo.markFailed({ scope: "apify", key: "dataset-1", error: "boom" });
  const retryClaim = await repo.claimKey({
    scope: "apify",
    key: "dataset-1",
    allowRetryOnFailed: false,
  });

  assert.equal(retryClaim.claimed, false);
  assert.equal(retryClaim.status, "failed");
});

test("claimKey validates required scope/key", async () => {
  const repo = createRepo();

  await assert.rejects(() => repo.claimKey({ scope: "", key: "x" }), /scope is required/);
  await assert.rejects(() => repo.claimKey({ scope: "a", key: "" }), /key is required/);
});
