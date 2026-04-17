const test = require("node:test");
const assert = require("node:assert/strict");

const { validateBotResponse } = require("../src/contracts");
const { createResponseRenderer } = require("../src/responseRenderer");

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

function validRecommendationResult(overrides = {}) {
  return {
    status: "ok",
    constraints: validIntent(),
    candidatesCount: 1,
    shortlist: [
      {
        event: validEvent(),
        score: 3,
        reasons: ["base_match"],
      },
    ],
    fallbackUsed: false,
    needsAction: null,
    ...overrides,
  };
}

test("renderiza status ok con shortlist legible y BotResponse valido", async () => {
  const renderer = createResponseRenderer();
  const input = validRecommendationResult({
    shortlist: [
      {
        event: validEvent({
          title: "Museo A",
          venue: "Castagnino",
          city: "Rosario",
          eventDate: "2026-04-13",
          timeText: "19:00",
        }),
        score: 4,
        reasons: ["match_include_category"],
      },
      {
        event: validEvent({
          eventHash: "evt_2",
          title: "Centro Cultural B",
          city: "Funes",
          venue: "Centro B",
          eventDate: "2026-04-14",
          timeText: "20:30",
        }),
        score: 3,
        reasons: ["base_match"],
      },
    ],
    candidatesCount: 2,
  });

  const response = await renderer.render(input, { chatId: 123 });
  const validation = validateBotResponse(response);

  assert.equal(validation.ok, true);
  assert.equal(response.chatId, 123);
  assert.equal(response.metadata.recommendationStatus, "ok");
  assert.equal(response.metadata.usedFallback, false);
  assert.match(response.text, /Museo A/);
  assert.match(response.text, /Centro Cultural B/);
  assert.match(response.text, /ciudad: Funes/);
  assert.ok(response.chunks.length >= 1);
});

test("renderiza status empty con mensaje honesto y sugerencia cuando needsAction=relax_filters", async () => {
  const renderer = createResponseRenderer();
  const input = validRecommendationResult({
    status: "empty",
    shortlist: [],
    candidatesCount: 0,
    fallbackUsed: true,
    needsAction: "relax_filters",
  });

  const response = await renderer.render(input, { chatId: 555 });

  assert.equal(response.metadata.recommendationStatus, "empty");
  assert.equal(response.metadata.usedFallback, true);
  assert.match(response.text, /No encontre eventos/);
  assert.match(response.text, /fallback controlado/);
  assert.match(response.text, /filtros mas flexibles/);
});

test("renderiza status need_user_input pidiendo ubicacion cuando corresponde", async () => {
  const renderer = createResponseRenderer();
  const input = validRecommendationResult({
    status: "need_user_input",
    shortlist: [],
    candidatesCount: 0,
    needsAction: "share_location",
  });

  const response = await renderer.render(input, { chatId: 999 });

  assert.equal(response.metadata.recommendationStatus, "need_user_input");
  assert.match(response.text, /compartime tu ubicacion/i);
  assert.equal(response.chunks.length, 1);
});

test("chunking divide mensajes largos en chunks coherentes", async () => {
  const renderer = createResponseRenderer({ maxChunkLength: 90 });
  const longTitle = "Muestra ".repeat(30);
  const input = validRecommendationResult({
    shortlist: [
      {
        event: validEvent({ title: longTitle, eventHash: "evt_long_1" }),
        score: 2,
        reasons: ["base_match"],
      },
      {
        event: validEvent({ title: longTitle, eventHash: "evt_long_2" }),
        score: 2,
        reasons: ["base_match"],
      },
    ],
    candidatesCount: 2,
  });

  const response = await renderer.render(input, { chatId: 1 });

  assert.ok(response.chunks.length > 1);
  assert.ok(response.chunks.every((chunk) => chunk.length <= 90));
});

test("mantiene metadata consistente con RecommendationResult", async () => {
  const renderer = createResponseRenderer();
  const input = validRecommendationResult({
    status: "need_user_input",
    shortlist: [],
    candidatesCount: 0,
    fallbackUsed: false,
    needsAction: "relax_filters",
  });

  const response = await renderer.render(input, { chatId: 7 });

  assert.equal(response.metadata.recommendationStatus, "need_user_input");
  assert.equal(response.metadata.usedFallback, false);
  assert.equal(response.metadata.needsAction, "relax_filters");
  assert.equal(response.metadata.candidatesCount, 0);
});
