const test = require("node:test");
const assert = require("node:assert/strict");

const { createEventCatalogRepository } = require("../src/eventCatalogRepo");

function createFakeDb() {
  const store = new Map();

  function makeStoreKey(collection, id) {
    return `${collection}/${id}`;
  }

  function listCollectionDocs(collection) {
    const prefix = `${collection}/`;
    const docs = [];
    for (const [key, value] of store.entries()) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      const id = key.slice(prefix.length);
      docs.push({ id, data: { ...value } });
    }
    return docs;
  }

  class FakeDocSnapshot {
    constructor(id, value) {
      this.id = id;
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
      const value = store.get(makeStoreKey(this.collection, this.id));
      return new FakeDocSnapshot(this.id, value);
    }

    async set(data, options = {}) {
      const key = makeStoreKey(this.collection, this.id);
      const prev = store.get(key);
      if (options && options.merge && prev && typeof prev === "object") {
        store.set(key, { ...prev, ...data });
        return;
      }
      store.set(key, { ...data });
    }
  }

  class FakeQuery {
    constructor(collection, filters = [], limitValue = null) {
      this.collection = collection;
      this.filters = filters;
      this.limitValue = limitValue;
    }

    doc(id) {
      return new FakeDocRef(this.collection, id);
    }

    where(field, operator, value) {
      if (operator !== "==") {
        throw new Error(`Unsupported operator: ${operator}`);
      }
      return new FakeQuery(this.collection, [...this.filters, { field, value }], this.limitValue);
    }

    limit(value) {
      return new FakeQuery(this.collection, this.filters, value);
    }

    async get() {
      const docs = listCollectionDocs(this.collection)
        .filter((doc) =>
          this.filters.every((filter) => {
            const candidate = doc.data[filter.field];
            return candidate === filter.value;
          })
        )
        .slice(0, this.limitValue || Number.MAX_SAFE_INTEGER)
        .map((doc) => new FakeDocSnapshot(doc.id, doc.data));

      return { docs };
    }
  }

  class FakeTransaction {
    async get(ref) {
      return ref.get();
    }

    set(ref, payload, options = {}) {
      const key = makeStoreKey(ref.collection, ref.id);
      const prev = store.get(key);
      if (options && options.merge && prev && typeof prev === "object") {
        store.set(key, { ...prev, ...payload });
        return;
      }
      store.set(key, { ...payload });
    }
  }

  return {
    collection(name) {
      return new FakeQuery(name);
    },
    async runTransaction(fn) {
      return fn(new FakeTransaction());
    },
  };
}

function validNormalizedEvent(overrides = {}) {
  return {
    eventHash: "evt_1",
    title: "Muestra en museo",
    category: "museo",
    dateText: "12 de abril",
    eventDate: "2026-04-12",
    timeText: "18:00",
    timeBucket: "afternoon",
    city: "Rosario",
    venue: "Museo Castagnino",
    isFree: true,
    tags: ["museo", "gratis"],
    source: {
      provider: "instagram",
      postId: "post-1",
    },
    quality: {
      completeness: 0.8,
      confidence: 0.7,
    },
    updatedAt: "2026-04-10T00:00:00.000Z",
    ...overrides,
  };
}

function buildRepo() {
  const db = createFakeDb();
  return createEventCatalogRepository({
    db,
    collectionName: "event_catalog_test",
    now: () => "2026-04-10T00:00:00.000Z",
  });
}

test("upsertEvents creates and updates by eventHash", async () => {
  const repo = buildRepo();

  const first = await repo.upsertEvents([validNormalizedEvent()]);
  const second = await repo.upsertEvents([
    validNormalizedEvent({
      title: "Muestra actualizada",
      eventHash: "evt_1",
    }),
  ]);

  const saved = await repo.getByEventHash("evt_1");

  assert.equal(first.created, 1);
  assert.equal(first.updated, 0);
  assert.equal(second.created, 0);
  assert.equal(second.updated, 1);
  assert.equal(saved.title, "Muestra actualizada");
});

test("upsertEvents reports validation errors for invalid NormalizedEvent", async () => {
  const repo = buildRepo();
  const invalid = validNormalizedEvent();
  delete invalid.quality;

  const result = await repo.upsertEvents([invalid]);

  assert.equal(result.upserted, 0);
  assert.equal(result.failed, 1);
  assert.equal(result.errors.length, 1);
  assert.match(result.errors[0], /NormalizedEvent validation failed/);
});

test("findEvents filters by city, category, eventDate and isFree", async () => {
  const repo = buildRepo();

  await repo.upsertEvents([
    validNormalizedEvent({
      eventHash: "evt_rosario_museo",
      city: "Rosario",
      category: "museo",
      eventDate: "2026-04-12",
      isFree: true,
    }),
    validNormalizedEvent({
      eventHash: "evt_rosario_teatro",
      city: "Rosario",
      category: "teatro",
      eventDate: "2026-04-12",
      isFree: false,
      title: "Obra de teatro",
    }),
    validNormalizedEvent({
      eventHash: "evt_funes_museo",
      city: "Funes",
      category: "museo",
      eventDate: "2026-04-13",
      title: "Muestra en Funes",
    }),
  ]);

  const result = await repo.findEvents({
    city: "Rosario",
    category: "museo",
    eventDate: "2026-04-12",
    isFree: true,
    limit: 10,
  });

  assert.equal(result.length, 1);
  assert.equal(result[0].eventHash, "evt_rosario_museo");
});
