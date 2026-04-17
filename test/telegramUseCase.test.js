const test = require("node:test");
const assert = require("node:assert/strict");

const { validateBotResponse, assertInteractionLog, assertConversationState } = require("../src/contracts");
const { parseIntentConstraints } = require("../src/intentParser");
const { createRecommendationEngine } = require("../src/recommendationEngine");
const { createResponseRenderer } = require("../src/responseRenderer");
const { createTelegramUseCase } = require("../src/telegramUseCase");

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

function keyForQuery(query) {
  return JSON.stringify({
    city: query.city || null,
    eventDate: query.eventDate || null,
    isFree: query.isFree === undefined ? null : query.isFree,
  });
}

function createUseCaseHarness({
  eventsByQuery = {},
  initialState = null,
  intentParser = { parseIntentConstraints },
  nowIso = "2026-04-13T10:00:00.000Z",
} = {}) {
  const stateStore = new Map();
  if (initialState) {
    stateStore.set(initialState.chatId, { ...initialState });
  }

  const interactionLogs = [];

  const conversationStateRepository = {
    async getState(chatId) {
      return stateStore.has(chatId) ? { ...stateStore.get(chatId) } : null;
    },
    async saveState(state) {
      const validated = assertConversationState({
        ...state,
        updatedAt: nowIso,
      });
      stateStore.set(validated.chatId, { ...validated });
      return validated;
    },
  };

  const interactionLogRepository = {
    async saveInteraction(interaction) {
      const validated = assertInteractionLog({
        ...interaction,
        id: "int_" + String(interactionLogs.length + 1),
        createdAt: nowIso,
      });
      interactionLogs.push(validated);
      return validated;
    },
  };

  const recommendationEngine = createRecommendationEngine({
    eventCatalogRepository: {
      async findEvents(query) {
        const key = keyForQuery(query);
        return eventsByQuery[key] ? [...eventsByQuery[key]] : [];
      },
    },
    now: () => new Date(nowIso),
  });

  const responseRenderer = createResponseRenderer();

  const useCase = createTelegramUseCase({
    intentParser,
    recommendationEngine,
    responseRenderer,
    conversationStateRepository,
    interactionLogRepository,
    now: () => nowIso,
  });

  return {
    useCase,
    getState(chatId) {
      return stateStore.get(chatId) || null;
    },
    interactionLogs,
  };
}

function validUpdate(overrides = {}) {
  return {
    updateId: 100,
    receivedAt: "2026-04-13T10:00:00.000Z",
    chatId: 123,
    userId: 999,
    messageId: 200,
    text: "quiero museos hoy",
    isEdited: false,
    raw: { update_id: 100 },
    ...overrides,
  };
}

test("flujo principal: parsea, recomienda, renderiza y persiste estado+log", async () => {
  const strictKey = keyForQuery({ city: "Rosario", eventDate: "2026-04-13", isFree: null });

  const harness = createUseCaseHarness({
    eventsByQuery: {
      [strictKey]: [validEvent()],
    },
  });

  const response = await harness.useCase.execute(validUpdate());
  const validation = validateBotResponse(response);

  assert.equal(validation.ok, true);
  assert.equal(response.chatId, 123);
  assert.equal(response.metadata.recommendationStatus, "ok");

  const state = harness.getState(123);
  assert.equal(state.chatId, 123);
  assert.equal(state.pendingLocationRequest, false);
  assert.equal(state.lastUserQuery, "quiero museos hoy");
  assert.equal(state.lastConstraints.dateScope, "today");

  assert.equal(harness.interactionLogs.length, 1);
  assert.equal(harness.interactionLogs[0].chatId, 123);
  assert.equal(harness.interactionLogs[0].recommendationStatus, "ok");
  assert.equal(harness.interactionLogs[0].query, "quiero museos hoy");
});

test("nearby sin ubicacion: devuelve need_user_input y deja estado pendiente", async () => {
  const harness = createUseCaseHarness();

  const response = await harness.useCase.execute(
    validUpdate({
      text: "quiero algo cerca mio hoy",
    })
  );

  assert.equal(response.metadata.recommendationStatus, "need_user_input");
  assert.equal(response.metadata.needsAction, "share_location");

  const state = harness.getState(123);
  assert.equal(state.pendingLocationRequest, true);
  assert.equal(state.pendingNearbyQuery, "quiero algo cerca mio hoy");

  assert.equal(harness.interactionLogs.length, 1);
  assert.equal(harness.interactionLogs[0].recommendationStatus, "need_user_input");
});

test("si llega location con nearby pendiente y texto vacio, reutiliza pendingNearbyQuery", async () => {
  const strictKey = keyForQuery({ city: "Rosario", eventDate: "2026-04-13", isFree: null });

  const harness = createUseCaseHarness({
    eventsByQuery: {
      [strictKey]: [validEvent({ eventHash: "evt_loc", title: "Plan cerca" })],
    },
    initialState: {
      chatId: 123,
      lastUserQuery: "quiero algo cerca mio hoy",
      lastAssistantReply: "Para recomendarte opciones cerca, compartime tu ubicacion.",
      lastConstraints: parseIntentConstraints("quiero algo cerca mio hoy"),
      pendingLocationRequest: true,
      pendingNearbyQuery: "quiero algo cerca mio hoy",
      updatedAt: "2026-04-13T09:00:00.000Z",
    },
  });

  const response = await harness.useCase.execute(
    validUpdate({
      text: "",
      location: { lat: -32.95, lng: -60.64 },
    })
  );

  assert.equal(response.metadata.recommendationStatus, "ok");

  const state = harness.getState(123);
  assert.equal(state.pendingLocationRequest, false);
  assert.equal(state.pendingNearbyQuery, null);
  assert.deepEqual(state.userLocation, { lat: -32.95, lng: -60.64 });
  assert.equal(state.lastUserQuery, "quiero algo cerca mio hoy");

  assert.equal(harness.interactionLogs.length, 1);
  assert.equal(harness.interactionLogs[0].metadata.reusedPendingNearbyQuery, true);
});

test("follow-up basico usa lastConstraints previo y actualiza dateScope", async () => {
  const strictKey = keyForQuery({ city: "Rosario", eventDate: "2026-04-14", isFree: null });

  const harness = createUseCaseHarness({
    eventsByQuery: {
      [strictKey]: [validEvent({ eventHash: "evt_follow", eventDate: "2026-04-14" })],
    },
    initialState: {
      chatId: 123,
      lastUserQuery: "quiero museos hoy",
      lastAssistantReply: "Te comparto opciones culturales",
      lastConstraints: parseIntentConstraints("quiero museos hoy"),
      pendingLocationRequest: false,
      pendingNearbyQuery: null,
      updatedAt: "2026-04-13T09:00:00.000Z",
    },
  });

  const response = await harness.useCase.execute(
    validUpdate({
      text: "y para manana?",
    })
  );

  assert.equal(response.metadata.recommendationStatus, "ok");

  const state = harness.getState(123);
  assert.equal(state.lastConstraints.dateScope, "tomorrow");
  assert.deepEqual(state.lastConstraints.includeCategories, ["museo"]);
});

test("texto vacio sin contexto devuelve BotResponse valido y lo registra", async () => {
  const relaxedKey = keyForQuery({ city: "Rosario", eventDate: null, isFree: null });

  const harness = createUseCaseHarness({
    eventsByQuery: {
      [relaxedKey]: [validEvent({ eventHash: "evt_any" })],
    },
  });

  const response = await harness.useCase.execute(
    validUpdate({
      text: "   ",
    })
  );

  const validation = validateBotResponse(response);
  assert.equal(validation.ok, true);

  const state = harness.getState(123);
  assert.equal(state.lastUserQuery, "consulta vacia");

  assert.equal(harness.interactionLogs.length, 1);
  assert.equal(harness.interactionLogs[0].query, "consulta vacia");
});

test("espera correctamente intentParser async (hibrido)", async () => {
  const strictKey = keyForQuery({ city: "Rosario", eventDate: "2026-04-13", isFree: null });
  let asyncParserCalled = false;

  const harness = createUseCaseHarness({
    eventsByQuery: {
      [strictKey]: [validEvent({ eventHash: "evt_async" })],
    },
    intentParser: {
      async parseIntentConstraints(rawText, conversationState) {
        asyncParserCalled = true;
        await new Promise((resolve) => setImmediate(resolve));
        return parseIntentConstraints(rawText, conversationState);
      },
    },
  });

  const response = await harness.useCase.execute(validUpdate({ text: "quiero museos hoy" }));

  assert.equal(asyncParserCalled, true);
  assert.equal(response.metadata.recommendationStatus, "ok");

  const state = harness.getState(123);
  assert.equal(state.lastConstraints.source, "deterministic");
});
