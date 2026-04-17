const {
  assertBotResponse,
  assertTelegramUpdateNormalized,
} = require("./contracts");
const { getHybridIntentParser } = require("./hybridIntentParser");
const { getRecommendationEngine } = require("./recommendationEngine");
const { getResponseRenderer } = require("./responseRenderer");
const { getConversationStateRepository } = require("./conversationStateRepo");
const { getInteractionLogRepository } = require("./interactionLogRepo");

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

function sanitizeQuery(value) {
  return asNonEmptyString(value) || "consulta vacia";
}

function cloneLocation(location) {
  if (!location || typeof location !== "object") {
    return null;
  }

  const lat = asFiniteNumber(location.lat);
  const lng = asFiniteNumber(location.lng);
  if (lat === null || lng === null) {
    return null;
  }

  return { lat, lng };
}

function hasLocation(location) {
  return Boolean(cloneLocation(location));
}

function shortlistSummary(shortlist) {
  if (!Array.isArray(shortlist)) {
    return [];
  }

  return shortlist
    .map((item) => {
      const title = asNonEmptyString(item && item.event && item.event.title);
      if (title) {
        return title;
      }
      return asNonEmptyString(item && item.event && item.event.eventHash) || "evento_sin_titulo";
    })
    .slice(0, 5);
}

function buildFallbackResponse(chatId, text) {
  const safeText = asNonEmptyString(text) || "No pude procesar tu consulta en este momento.";
  return assertBotResponse({
    chatId: asFiniteNumber(chatId) || 0,
    text: safeText,
    parseMode: null,
    chunks: [safeText],
    metadata: {
      recommendationStatus: "error",
      usedFallback: false,
      needsAction: null,
      candidatesCount: 0,
    },
  });
}

function mergeResponseMetadata(response, extraMetadata) {
  return assertBotResponse({
    ...response,
    metadata: {
      ...response.metadata,
      ...extraMetadata,
    },
  });
}

function createTelegramUseCase({
  intentParser = getHybridIntentParser(),
  recommendationEngine = getRecommendationEngine(),
  responseRenderer = getResponseRenderer(),
  conversationStateRepository = getConversationStateRepository(),
  interactionLogRepository = getInteractionLogRepository(),
  now = () => new Date().toISOString(),
} = {}) {
  if (!intentParser || typeof intentParser.parseIntentConstraints !== "function") {
    throw new Error("createTelegramUseCase requires intentParser.parseIntentConstraints");
  }
  if (!recommendationEngine || typeof recommendationEngine.recommend !== "function") {
    throw new Error("createTelegramUseCase requires recommendationEngine.recommend");
  }
  if (!responseRenderer || typeof responseRenderer.render !== "function") {
    throw new Error("createTelegramUseCase requires responseRenderer.render");
  }
  if (!conversationStateRepository || typeof conversationStateRepository.getState !== "function" || typeof conversationStateRepository.saveState !== "function") {
    throw new Error("createTelegramUseCase requires conversationStateRepository.getState/saveState");
  }
  if (!interactionLogRepository || typeof interactionLogRepository.saveInteraction !== "function") {
    throw new Error("createTelegramUseCase requires interactionLogRepository.saveInteraction");
  }

  async function execute(updateInput) {
    let update;
    try {
      update = assertTelegramUpdateNormalized(updateInput);
    } catch (error) {
      const fallbackChatId = asFiniteNumber(updateInput && updateInput.chatId) || 0;
      return buildFallbackResponse(fallbackChatId, "No pude interpretar la entrada de Telegram.");
    }

    const chatId = update.chatId;
    const incomingLocation = cloneLocation(update.location);

    let previousState = null;
    let stateReadError = null;

    try {
      previousState = await conversationStateRepository.getState(chatId);
    } catch (error) {
      stateReadError = error;
      previousState = null;
    }

    const previousLocation = cloneLocation(previousState && previousState.userLocation);
    const resolvedLocation = incomingLocation || previousLocation;

    const textInput = asNonEmptyString(update.text);
    const pendingNearbyQuery = asNonEmptyString(previousState && previousState.pendingNearbyQuery);
    const pendingLocationBefore = Boolean(previousState && previousState.pendingLocationRequest);

    const reusedPendingNearbyQuery = Boolean(
      incomingLocation &&
        !textInput &&
        pendingLocationBefore &&
        pendingNearbyQuery
    );

    const effectiveQuery = sanitizeQuery(reusedPendingNearbyQuery ? pendingNearbyQuery : textInput);

    let constraints;
    let recommendation;
    let response;

    try {
      constraints = intentParser.parseIntentConstraints(effectiveQuery, previousState);
      recommendation = await recommendationEngine.recommend(constraints, {
        userLocation: resolvedLocation || undefined,
        hasUserLocation: hasLocation(resolvedLocation),
      });
      response = await responseRenderer.render(recommendation, { chatId });
    } catch (error) {
      const fallbackResponse = buildFallbackResponse(chatId, "No pude generar una recomendacion en este momento.");
      return mergeResponseMetadata(fallbackResponse, {
        error: error && error.message ? error.message : String(error || "unknown_use_case_error"),
      });
    }

    const waitingForLocation =
      recommendation.status === "need_user_input" && recommendation.needsAction === "share_location";

    const nextState = {
      ...(previousState || {}),
      chatId,
      lastUserQuery: effectiveQuery,
      lastAssistantReply: response.text,
      lastConstraints: constraints,
      pendingLocationRequest: waitingForLocation,
      pendingNearbyQuery: waitingForLocation ? effectiveQuery : null,
      userLocation: resolvedLocation,
    };

    let statePersisted = true;
    let interactionLogged = true;
    let statePersistError = null;
    let interactionLogError = null;

    try {
      await conversationStateRepository.saveState(nextState);
    } catch (error) {
      statePersisted = false;
      statePersistError = error;
    }

    try {
      await interactionLogRepository.saveInteraction({
        chatId,
        query: effectiveQuery,
        constraints,
        recommendationStatus: recommendation.status,
        usedFallback: Boolean(recommendation.fallbackUsed),
        shortlistSummary: shortlistSummary(recommendation.shortlist),
        metadata: {
          updateId: update.updateId,
          messageId: update.messageId || null,
          hasIncomingLocation: Boolean(incomingLocation),
          reusedPendingNearbyQuery,
          pendingLocationRequestBefore: pendingLocationBefore,
          pendingLocationRequestAfter: waitingForLocation,
          needsAction: recommendation.needsAction,
          candidatesCount: recommendation.candidatesCount,
          textWasEmpty: !textInput,
        },
      });
    } catch (error) {
      interactionLogged = false;
      interactionLogError = error;
    }

    return mergeResponseMetadata(response, {
      statePersisted,
      interactionLogged,
      stateReadError: stateReadError ? stateReadError.message || String(stateReadError) : null,
      statePersistError: statePersistError ? statePersistError.message || String(statePersistError) : null,
      interactionLogError: interactionLogError ? interactionLogError.message || String(interactionLogError) : null,
    });
  }

  return {
    execute,
  };
}

let cachedUseCase = null;

function getTelegramUseCase() {
  if (!cachedUseCase) {
    cachedUseCase = createTelegramUseCase();
  }
  return cachedUseCase;
}

function resetTelegramUseCaseForTests() {
  cachedUseCase = null;
}

module.exports = {
  createTelegramUseCase,
  getTelegramUseCase,
  resetTelegramUseCaseForTests,
};
