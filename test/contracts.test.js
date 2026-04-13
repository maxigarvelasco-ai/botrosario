const test = require("node:test");
const assert = require("node:assert/strict");

const {
  validateTelegramUpdateNormalized,
  validateIntentConstraints,
  validateConversationState,
  validateInteractionLog,
  validateRawEvent,
  validateNormalizedEvent,
  validateRecommendationResult,
  validateBotResponse,
} = require("../src/contracts");

function validIntent() {
  return {
    rawText: "algo tranqui hoy",
    dateScope: "today",
    timeScope: "none",
    nearby: false,
    includeCategories: [],
    excludeCategories: ["musica"],
    mood: "tranquilo",
    styleHint: "none",
    splitPlan: false,
    noMixDays: false,
    confidence: 0.92,
    source: "deterministic",
  };
}

function validNormalizedEvent() {
  return {
    eventHash: "abc123",
    title: "Muestra en museo",
    category: "museo",
    dateText: "12 de abril",
    eventDate: "2026-04-12",
    timeText: "18:00",
    timeBucket: "afternoon",
    city: "Rosario",
    venue: "Museo Castagnino",
    lat: -32.95,
    lng: -60.64,
    isFree: true,
    tags: ["museo", "gratis"],
    description: "Exposicion",
    source: {
      provider: "instagram",
      postId: "post-1",
    },
    quality: {
      completeness: 0.8,
      confidence: 0.7,
    },
    updatedAt: new Date().toISOString(),
  };
}

test("TelegramUpdateNormalized valida caso valido", () => {
  const input = {
    updateId: 123,
    receivedAt: new Date().toISOString(),
    chatId: 100,
    userId: 100,
    messageId: 9,
    text: "hola",
    location: { lat: -32.9, lng: -60.6 },
    isEdited: false,
    raw: { update_id: 123 },
  };

  const result = validateTelegramUpdateNormalized(input);
  assert.equal(result.ok, true);
});

test("IntentConstraints rechaza dateScope invalido", () => {
  const input = validIntent();
  input.dateScope = "next_week";
  const result = validateIntentConstraints(input);

  assert.equal(result.ok, false);
  assert.ok(result.errors.some((x) => x.includes("dateScope")));
});

test("InteractionLog valida caso valido", () => {
  const result = validateInteractionLog({
    id: "int_1",
    chatId: 123,
    createdAt: "2026-04-10T12:00:00.000Z",
    query: "museos hoy",
    constraints: {
      dateScope: "today",
    },
    recommendationStatus: "ok",
    usedFallback: false,
    shortlistSummary: ["Museo Castagnino", "Macro"],
    metadata: {
      requestId: "req_1",
    },
  });

  assert.equal(result.ok, true);
  assert.equal(result.errors.length, 0);
});
test("ConversationState valida nested lastConstraints", () => {
  const input = {
    chatId: 1,
    lastUserQuery: "y para mañana?",
    lastAssistantReply: "opciones...",
    lastConstraints: validIntent(),
    pendingLocationRequest: false,
    pendingNearbyQuery: null,
    userLocation: { lat: -32.95, lng: -60.63 },
    updatedAt: new Date().toISOString(),
  };

  const result = validateConversationState(input);
  assert.equal(result.ok, true);
});

test("RawEvent valida caso valido", () => {
  const input = {
    sourcePostId: "post-1",
    sourceDatasetId: "dataset-1",
    sourceUrl: "https://instagram.com/p/post-1",
    caption: "caption",
    ocrText: "ocr",
    evidence: "12/04 18 hs museo",
    parsedTitle: "titulo",
    parsedDateText: "12/04",
    parsedTimeText: "18:00",
    parsedVenue: "museo",
    parsedCity: "Rosario",
    parsedCategory: "museo",
    parsedIsFree: true,
    tags: ["museo"],
  };

  const result = validateRawEvent(input);
  assert.equal(result.ok, true);
});

test("NormalizedEvent rechaza quality faltante", () => {
  const input = validNormalizedEvent();
  delete input.quality;

  const result = validateNormalizedEvent(input);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((x) => x.includes("quality")));
});

test("RecommendationResult valida caso valido", () => {
  const input = {
    status: "ok",
    constraints: validIntent(),
    candidatesCount: 2,
    shortlist: [
      {
        event: validNormalizedEvent(),
        score: 8.7,
        reasons: ["match_mood"],
      },
    ],
    fallbackUsed: false,
    needsAction: null,
    note: "ok",
    debug: { rule: "base" },
  };

  const result = validateRecommendationResult(input);
  assert.equal(result.ok, true);
});

test("BotResponse rechaza parseMode invalido", () => {
  const input = {
    chatId: 12,
    text: "hola",
    parseMode: "MARKDOWN_V2",
    chunks: ["hola"],
    metadata: {
      recommendationStatus: "ok",
      usedFallback: false,
    },
  };

  const result = validateBotResponse(input);
  assert.equal(result.ok, false);
  assert.ok(result.errors.some((x) => x.includes("parseMode")));
});
