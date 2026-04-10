function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isIsoDateTime(value) {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function isIsoDate(value) {
  if (typeof value !== "string") {
    return false;
  }
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function pushError(errors, path, message) {
  errors.push(`${path}: ${message}`);
}

function validateRequiredString(errors, value, path) {
  if (typeof value !== "string" || value.trim() === "") {
    pushError(errors, path, "must be a non-empty string");
  }
}

function validateOptionalString(errors, value, path) {
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== "string") {
    pushError(errors, path, "must be a string when provided");
  }
}

function validateRequiredNumber(errors, value, path) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    pushError(errors, path, "must be a number");
  }
}

function validateOptionalNumber(errors, value, path) {
  if (value === undefined || value === null) {
    return;
  }
  validateRequiredNumber(errors, value, path);
}

function validateOptionalBoolean(errors, value, path) {
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== "boolean") {
    pushError(errors, path, "must be a boolean when provided");
  }
}

function validateRequiredStringArray(errors, value, path) {
  if (!Array.isArray(value)) {
    pushError(errors, path, "must be an array of strings");
    return;
  }

  for (let i = 0; i < value.length; i += 1) {
    if (typeof value[i] !== "string") {
      pushError(errors, `${path}[${i}]`, "must be a string");
    }
  }
}

function validateEnum(errors, value, allowed, path) {
  if (!allowed.includes(value)) {
    pushError(errors, path, `must be one of: ${allowed.join(", ")}`);
  }
}

function buildResult(errors, data) {
  return {
    ok: errors.length === 0,
    errors,
    value: data,
  };
}

function assertValid(result, contractName) {
  if (!result.ok) {
    throw new Error(`${contractName} validation failed: ${result.errors.join(" | ")}`);
  }
  return result.value;
}

const intentDateScopes = ["none", "today", "tomorrow", "tonight"];
const intentTimeScopes = ["none", "afternoon", "night"];
const intentMoods = ["none", "movido", "tranquilo", "familiar"];
const intentStyleHints = ["none", "careta", "no_careta"];
const intentSources = ["deterministic", "llm", "hybrid"];
const eventCategories = [
  "teatro",
  "museo",
  "cine",
  "feria",
  "taller",
  "charla",
  "familiar",
  "aire_libre",
  "cultural",
];
const eventTimeBuckets = ["unknown", "afternoon", "night", "other"];
const recommendationStatuses = ["ok", "empty", "need_user_input", "error"];
const recommendationActions = ["share_location", "relax_filters", null];
const botParseModes = ["Markdown", "HTML", null];

const contractDefinitions = {
  TelegramUpdateNormalized: {
    requiredFields: ["updateId", "receivedAt", "chatId", "userId", "isEdited", "raw"],
    optionalFields: ["messageId", "text", "location"],
  },
  IntentConstraints: {
    requiredFields: [
      "rawText",
      "dateScope",
      "timeScope",
      "nearby",
      "includeCategories",
      "excludeCategories",
      "mood",
      "styleHint",
      "splitPlan",
      "noMixDays",
      "source",
    ],
    optionalFields: ["confidence"],
  },
  ConversationState: {
    requiredFields: ["chatId", "pendingLocationRequest", "updatedAt"],
    optionalFields: [
      "lastUserQuery",
      "lastAssistantReply",
      "lastConstraints",
      "pendingNearbyQuery",
      "userLocation",
    ],
  },
  RawEvent: {
    requiredFields: ["sourcePostId", "evidence", "tags"],
    optionalFields: [
      "sourceDatasetId",
      "sourceUrl",
      "caption",
      "ocrText",
      "parsedTitle",
      "parsedDateText",
      "parsedTimeText",
      "parsedVenue",
      "parsedCity",
      "parsedCategory",
      "parsedIsFree",
    ],
  },
  NormalizedEvent: {
    requiredFields: [
      "eventHash",
      "title",
      "category",
      "timeBucket",
      "city",
      "isFree",
      "tags",
      "source",
      "quality",
      "updatedAt",
    ],
    optionalFields: ["dateText", "eventDate", "timeText", "venue", "lat", "lng", "description"],
  },
  RecommendationResult: {
    requiredFields: [
      "status",
      "constraints",
      "candidatesCount",
      "shortlist",
      "fallbackUsed",
      "needsAction",
    ],
    optionalFields: ["note", "debug"],
  },
  BotResponse: {
    requiredFields: ["chatId", "text", "parseMode", "chunks", "metadata"],
    optionalFields: [],
  },
};

function validateTelegramUpdateNormalized(data) {
  const errors = [];
  const path = "TelegramUpdateNormalized";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredNumber(errors, data.updateId, `${path}.updateId`);
  validateRequiredNumber(errors, data.chatId, `${path}.chatId`);
  validateRequiredNumber(errors, data.userId, `${path}.userId`);

  if (!isIsoDateTime(data.receivedAt)) {
    pushError(errors, `${path}.receivedAt`, "must be an ISO date-time string");
  }

  if (typeof data.isEdited !== "boolean") {
    pushError(errors, `${path}.isEdited`, "must be a boolean");
  }

  if (!isPlainObject(data.raw)) {
    pushError(errors, `${path}.raw`, "must be an object");
  }

  validateOptionalNumber(errors, data.messageId, `${path}.messageId`);
  validateOptionalString(errors, data.text, `${path}.text`);

  if (data.location !== undefined && data.location !== null) {
    if (!isPlainObject(data.location)) {
      pushError(errors, `${path}.location`, "must be an object when provided");
    } else {
      validateRequiredNumber(errors, data.location.lat, `${path}.location.lat`);
      validateRequiredNumber(errors, data.location.lng, `${path}.location.lng`);
    }
  }

  return buildResult(errors, data);
}

function validateIntentConstraints(data) {
  const errors = [];
  const path = "IntentConstraints";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredString(errors, data.rawText, `${path}.rawText`);
  validateEnum(errors, data.dateScope, intentDateScopes, `${path}.dateScope`);
  validateEnum(errors, data.timeScope, intentTimeScopes, `${path}.timeScope`);

  if (typeof data.nearby !== "boolean") {
    pushError(errors, `${path}.nearby`, "must be a boolean");
  }

  validateRequiredStringArray(errors, data.includeCategories, `${path}.includeCategories`);
  validateRequiredStringArray(errors, data.excludeCategories, `${path}.excludeCategories`);

  validateEnum(errors, data.mood, intentMoods, `${path}.mood`);
  validateEnum(errors, data.styleHint, intentStyleHints, `${path}.styleHint`);

  if (typeof data.splitPlan !== "boolean") {
    pushError(errors, `${path}.splitPlan`, "must be a boolean");
  }
  if (typeof data.noMixDays !== "boolean") {
    pushError(errors, `${path}.noMixDays`, "must be a boolean");
  }

  validateEnum(errors, data.source, intentSources, `${path}.source`);

  if (data.confidence !== undefined && data.confidence !== null) {
    validateRequiredNumber(errors, data.confidence, `${path}.confidence`);
    if (typeof data.confidence === "number" && (data.confidence < 0 || data.confidence > 1)) {
      pushError(errors, `${path}.confidence`, "must be between 0 and 1");
    }
  }

  return buildResult(errors, data);
}

function validateConversationState(data) {
  const errors = [];
  const path = "ConversationState";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredNumber(errors, data.chatId, `${path}.chatId`);

  if (typeof data.pendingLocationRequest !== "boolean") {
    pushError(errors, `${path}.pendingLocationRequest`, "must be a boolean");
  }

  if (!isIsoDateTime(data.updatedAt)) {
    pushError(errors, `${path}.updatedAt`, "must be an ISO date-time string");
  }

  validateOptionalString(errors, data.lastUserQuery, `${path}.lastUserQuery`);
  validateOptionalString(errors, data.lastAssistantReply, `${path}.lastAssistantReply`);

  if (data.pendingNearbyQuery !== undefined && data.pendingNearbyQuery !== null && typeof data.pendingNearbyQuery !== "string") {
    pushError(errors, `${path}.pendingNearbyQuery`, "must be a string or null");
  }

  if (data.userLocation !== undefined && data.userLocation !== null) {
    if (!isPlainObject(data.userLocation)) {
      pushError(errors, `${path}.userLocation`, "must be an object when provided");
    } else {
      validateRequiredNumber(errors, data.userLocation.lat, `${path}.userLocation.lat`);
      validateRequiredNumber(errors, data.userLocation.lng, `${path}.userLocation.lng`);
    }
  }

  if (data.lastConstraints !== undefined && data.lastConstraints !== null) {
    const nested = validateIntentConstraints(data.lastConstraints);
    if (!nested.ok) {
      nested.errors.forEach((err) => pushError(errors, `${path}.lastConstraints`, err));
    }
  }

  return buildResult(errors, data);
}

function validateRawEvent(data) {
  const errors = [];
  const path = "RawEvent";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredString(errors, data.sourcePostId, `${path}.sourcePostId`);
  validateRequiredString(errors, data.evidence, `${path}.evidence`);
  validateRequiredStringArray(errors, data.tags, `${path}.tags`);

  validateOptionalString(errors, data.sourceDatasetId, `${path}.sourceDatasetId`);
  validateOptionalString(errors, data.sourceUrl, `${path}.sourceUrl`);
  validateOptionalString(errors, data.caption, `${path}.caption`);
  validateOptionalString(errors, data.ocrText, `${path}.ocrText`);
  validateOptionalString(errors, data.parsedTitle, `${path}.parsedTitle`);
  validateOptionalString(errors, data.parsedDateText, `${path}.parsedDateText`);
  validateOptionalString(errors, data.parsedTimeText, `${path}.parsedTimeText`);
  validateOptionalString(errors, data.parsedVenue, `${path}.parsedVenue`);
  validateOptionalString(errors, data.parsedCity, `${path}.parsedCity`);
  validateOptionalString(errors, data.parsedCategory, `${path}.parsedCategory`);
  validateOptionalBoolean(errors, data.parsedIsFree, `${path}.parsedIsFree`);

  return buildResult(errors, data);
}

function validateNormalizedEvent(data) {
  const errors = [];
  const path = "NormalizedEvent";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredString(errors, data.eventHash, `${path}.eventHash`);
  validateRequiredString(errors, data.title, `${path}.title`);
  validateEnum(errors, data.category, eventCategories, `${path}.category`);
  validateEnum(errors, data.timeBucket, eventTimeBuckets, `${path}.timeBucket`);
  validateRequiredString(errors, data.city, `${path}.city`);

  if (typeof data.isFree !== "boolean") {
    pushError(errors, `${path}.isFree`, "must be a boolean");
  }

  validateRequiredStringArray(errors, data.tags, `${path}.tags`);

  if (!isPlainObject(data.source)) {
    pushError(errors, `${path}.source`, "must be an object");
  }

  if (!isPlainObject(data.quality)) {
    pushError(errors, `${path}.quality`, "must be an object");
  } else {
    validateRequiredNumber(errors, data.quality.completeness, `${path}.quality.completeness`);
    validateRequiredNumber(errors, data.quality.confidence, `${path}.quality.confidence`);

    if (typeof data.quality.completeness === "number" && (data.quality.completeness < 0 || data.quality.completeness > 1)) {
      pushError(errors, `${path}.quality.completeness`, "must be between 0 and 1");
    }
    if (typeof data.quality.confidence === "number" && (data.quality.confidence < 0 || data.quality.confidence > 1)) {
      pushError(errors, `${path}.quality.confidence`, "must be between 0 and 1");
    }
  }

  if (!isIsoDateTime(data.updatedAt)) {
    pushError(errors, `${path}.updatedAt`, "must be an ISO date-time string");
  }

  validateOptionalString(errors, data.dateText, `${path}.dateText`);
  if (data.eventDate !== undefined && data.eventDate !== null && !isIsoDate(data.eventDate)) {
    pushError(errors, `${path}.eventDate`, "must be YYYY-MM-DD when provided");
  }
  validateOptionalString(errors, data.timeText, `${path}.timeText`);
  validateOptionalString(errors, data.venue, `${path}.venue`);
  validateOptionalNumber(errors, data.lat, `${path}.lat`);
  validateOptionalNumber(errors, data.lng, `${path}.lng`);
  validateOptionalString(errors, data.description, `${path}.description`);

  return buildResult(errors, data);
}

function validateRecommendationResult(data) {
  const errors = [];
  const path = "RecommendationResult";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateEnum(errors, data.status, recommendationStatuses, `${path}.status`);

  const constraintsResult = validateIntentConstraints(data.constraints);
  if (!constraintsResult.ok) {
    constraintsResult.errors.forEach((err) => pushError(errors, `${path}.constraints`, err));
  }

  validateRequiredNumber(errors, data.candidatesCount, `${path}.candidatesCount`);
  if (typeof data.candidatesCount === "number" && data.candidatesCount < 0) {
    pushError(errors, `${path}.candidatesCount`, "must be >= 0");
  }

  if (!Array.isArray(data.shortlist)) {
    pushError(errors, `${path}.shortlist`, "must be an array");
  } else {
    for (let i = 0; i < data.shortlist.length; i += 1) {
      const item = data.shortlist[i];
      const itemPath = `${path}.shortlist[${i}]`;
      if (!isPlainObject(item)) {
        pushError(errors, itemPath, "must be an object");
        continue;
      }

      const eventResult = validateNormalizedEvent(item.event);
      if (!eventResult.ok) {
        eventResult.errors.forEach((err) => pushError(errors, `${itemPath}.event`, err));
      }

      validateRequiredNumber(errors, item.score, `${itemPath}.score`);
      validateRequiredStringArray(errors, item.reasons, `${itemPath}.reasons`);
    }
  }

  if (typeof data.fallbackUsed !== "boolean") {
    pushError(errors, `${path}.fallbackUsed`, "must be a boolean");
  }

  if (!recommendationActions.includes(data.needsAction)) {
    pushError(errors, `${path}.needsAction`, `must be one of: share_location, relax_filters, null`);
  }

  validateOptionalString(errors, data.note, `${path}.note`);
  if (data.debug !== undefined && data.debug !== null && !isPlainObject(data.debug)) {
    pushError(errors, `${path}.debug`, "must be an object when provided");
  }

  return buildResult(errors, data);
}

function validateBotResponse(data) {
  const errors = [];
  const path = "BotResponse";

  if (!isPlainObject(data)) {
    pushError(errors, path, "must be an object");
    return buildResult(errors, data);
  }

  validateRequiredNumber(errors, data.chatId, `${path}.chatId`);
  validateRequiredString(errors, data.text, `${path}.text`);

  if (!botParseModes.includes(data.parseMode)) {
    pushError(errors, `${path}.parseMode`, "must be one of: Markdown, HTML, null");
  }

  validateRequiredStringArray(errors, data.chunks, `${path}.chunks`);

  if (!isPlainObject(data.metadata)) {
    pushError(errors, `${path}.metadata`, "must be an object");
  } else {
    validateRequiredString(errors, data.metadata.recommendationStatus, `${path}.metadata.recommendationStatus`);
    if (typeof data.metadata.usedFallback !== "boolean") {
      pushError(errors, `${path}.metadata.usedFallback`, "must be a boolean");
    }
  }

  return buildResult(errors, data);
}

function assertTelegramUpdateNormalized(data) {
  return assertValid(validateTelegramUpdateNormalized(data), "TelegramUpdateNormalized");
}

function assertIntentConstraints(data) {
  return assertValid(validateIntentConstraints(data), "IntentConstraints");
}

function assertConversationState(data) {
  return assertValid(validateConversationState(data), "ConversationState");
}

function assertRawEvent(data) {
  return assertValid(validateRawEvent(data), "RawEvent");
}

function assertNormalizedEvent(data) {
  return assertValid(validateNormalizedEvent(data), "NormalizedEvent");
}

function assertRecommendationResult(data) {
  return assertValid(validateRecommendationResult(data), "RecommendationResult");
}

function assertBotResponse(data) {
  return assertValid(validateBotResponse(data), "BotResponse");
}

module.exports = {
  contractDefinitions,
  enums: {
    intentDateScopes,
    intentTimeScopes,
    intentMoods,
    intentStyleHints,
    intentSources,
    eventCategories,
    eventTimeBuckets,
    recommendationStatuses,
    recommendationActions,
    botParseModes,
  },
  validateTelegramUpdateNormalized,
  validateIntentConstraints,
  validateConversationState,
  validateRawEvent,
  validateNormalizedEvent,
  validateRecommendationResult,
  validateBotResponse,
  assertTelegramUpdateNormalized,
  assertIntentConstraints,
  assertConversationState,
  assertRawEvent,
  assertNormalizedEvent,
  assertRecommendationResult,
  assertBotResponse,
};
