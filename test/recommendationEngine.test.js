const test = require("node:test");
const assert = require("node:assert/strict");

const { validateRecommendationResult } = require("../src/contracts");
const { createRecommendationEngine } = require("../src/recommendationEngine");

function validIntent(overrides = {}) {
  return {
    rawText: "museos hoy",
    dateScope: "today",
    timeScope: "none",
    nearby: false,
    includeCategories: ["museo"],
    excludeCategories: [],
    mood: "none",
    styleHint: "none",
    splitPlan: false,
    noMixDays: false,
    confidence: 0.8,
    source: "deterministic",
    ...overrides,
  };
}

function validEvent(overrides = {}) {
  return {
    eventHash: "evt_1",
    title: "Muestra en museo",
    category: "museo",
    dateText: "13 de abril",
    eventDate: "2026-04-13",
    timeText: "18:00",
    timeBucket: "afternoon",
    city: "Rosario",
    venue: "Museo Castagnino",
    isFree: true,
    tags: ["museo"],
    source: {
      provider: "instagram",
      postId: "post-1",
    },
    quality: {
      completeness: 0.8,
      confidence: 0.9,
    },
    updatedAt: "2026-04-10T00:00:00.000Z",
    ...overrides,
  };
}

function buildEngine(eventsByKey = {}) {
  const receivedQueries = [];
  const repo = {
    async findEvents(query) {
      receivedQueries.push({ ...query });
      const key = JSON.stringify({
        city: query.city || null,
        eventDate: query.eventDate || null,
        isFree: query.isFree === undefined ? null : query.isFree,
      });
      return eventsByKey[key] ? [...eventsByKey[key]] : [];
    },
  };

  const engine = createRecommendationEngine({
    eventCatalogRepository: repo,
    now: () => new Date("2026-04-13T10:00:00.000Z"),
    defaultCity: "Rosario",
  });

  return {
    engine,
    receivedQueries,
  };
}

function keyFor(query) {
  return JSON.stringify({
    city: query.city || null,
    eventDate: query.eventDate || null,
    isFree: query.isFree === undefined ? null : query.isFree,
  });
}

test("devuelve RecommendationResult valido con shortlist filtrado por include/exclude", async () => {
  const strictKey = keyFor({ city: "Rosario", eventDate: "2026-04-13", isFree: null });
  const { engine } = buildEngine({
    [strictKey]: [
      validEvent({ eventHash: "evt_museo", category: "museo", title: "Museo A" }),
      validEvent({ eventHash: "evt_teatro", category: "teatro", title: "Teatro B" }),
      validEvent({ eventHash: "evt_cine", category: "cine", title: "Cine C" }),
    ],
  });

  const result = await engine.recommend(
    validIntent({
      includeCategories: ["museo", "teatro"],
      excludeCategories: ["teatro"],
    })
  );

  const validation = validateRecommendationResult(result);
  assert.equal(validation.ok, true);
  assert.equal(result.status, "ok");
  assert.equal(result.candidatesCount, 1);
  assert.equal(result.shortlist.length, 1);
  assert.equal(result.shortlist[0].event.category, "museo");
  assert.equal(result.fallbackUsed, false);
});

test("aplica fallback de fecha de forma controlada cuando dateScope no encuentra resultados", async () => {
  const strictKey = keyFor({ city: "Rosario", eventDate: "2026-04-13", isFree: null });
  const relaxedKey = keyFor({ city: "Rosario", eventDate: null, isFree: null });

  const { engine, receivedQueries } = buildEngine({
    [strictKey]: [],
    [relaxedKey]: [
      validEvent({ eventHash: "evt_relaxed", eventDate: "2026-04-14", title: "Museo Manana" }),
    ],
  });

  const result = await engine.recommend(validIntent({ includeCategories: ["museo"] }));

  assert.equal(result.status, "ok");
  assert.equal(result.fallbackUsed, true);
  assert.equal(result.shortlist.length, 1);
  assert.equal(result.shortlist[0].event.eventHash, "evt_relaxed");
  assert.equal(receivedQueries.length, 2);
});

test("si nearby es true sin ubicacion devuelve need_user_input/share_location", async () => {
  const { engine, receivedQueries } = buildEngine({});

  const result = await engine.recommend(validIntent({ nearby: true }));

  assert.equal(result.status, "need_user_input");
  assert.equal(result.needsAction, "share_location");
  assert.equal(result.shortlist.length, 0);
  assert.equal(receivedQueries.length, 0);
});

test("si no hay resultados devuelve empty y needsAction relax_filters cuando corresponde", async () => {
  const strictKey = keyFor({ city: "Rosario", eventDate: "2026-04-13", isFree: null });
  const relaxedKey = keyFor({ city: "Rosario", eventDate: null, isFree: null });

  const { engine } = buildEngine({
    [strictKey]: [],
    [relaxedKey]: [],
  });

  const result = await engine.recommend(validIntent({ includeCategories: ["museo"] }));

  assert.equal(result.status, "empty");
  assert.equal(result.candidatesCount, 0);
  assert.equal(result.shortlist.length, 0);
  assert.equal(result.needsAction, "relax_filters");
});

test("respeta filtro isFree cuando se informa por contexto", async () => {
  const strictKey = keyFor({ city: "Rosario", eventDate: "2026-04-13", isFree: true });
  const { engine, receivedQueries } = buildEngine({
    [strictKey]: [validEvent({ eventHash: "evt_free", isFree: true })],
  });

  const result = await engine.recommend(validIntent(), { isFree: true });

  assert.equal(result.status, "ok");
  assert.equal(result.shortlist.length, 1);
  assert.equal(result.shortlist[0].event.isFree, true);
  assert.equal(receivedQueries[0].isFree, true);
});
