const { assertIntentConstraints } = require("./contracts");
const { parseIntentConstraints: parseDeterministicIntentConstraints } = require("./intentParser");
const { createLlmIntentParser } = require("./llmIntentParser");
const { getConfig } = require("./config");
const { createLogger } = require("./logger");

const FOLLOW_UP_PREFIXES = ["y", "pero", "ademas", "igual", "dale", "ok", "bueno"];

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function uniqueStrings(values) {
  return Array.from(new Set(values.filter((value) => typeof value === "string" && value.length > 0)));
}

function hasSignals(constraints) {
  if (!constraints || typeof constraints !== "object") {
    return 0;
  }

  let score = 0;
  if (constraints.dateScope && constraints.dateScope !== "none") {
    score += 1;
  }
  if (constraints.timeScope && constraints.timeScope !== "none") {
    score += 1;
  }
  if (constraints.nearby === true) {
    score += 1;
  }
  score += Math.min(2, Array.isArray(constraints.includeCategories) ? constraints.includeCategories.length : 0);
  score += Math.min(2, Array.isArray(constraints.excludeCategories) ? constraints.excludeCategories.length : 0);
  if (constraints.mood && constraints.mood !== "none") {
    score += 1;
  }
  if (constraints.styleHint && constraints.styleHint !== "none") {
    score += 1;
  }
  if (constraints.splitPlan === true) {
    score += 1;
  }
  if (constraints.noMixDays === true) {
    score += 1;
  }

  return score;
}

function evaluateNeedForLlm(rawText, conversationState, deterministicConstraints) {
  const text = normalizeText(rawText);
  const tokens = text.length > 0 ? text.split(" ").filter(Boolean) : [];
  const signalScore = hasSignals(deterministicConstraints);
  const confidence = typeof deterministicConstraints.confidence === "number"
    ? deterministicConstraints.confidence
    : 0;

  const lowConfidence = confidence < 0.55;
  const sparseConstraints = signalScore <= 1 && tokens.length >= 4;
  const longAndOpenText = text.length >= 140;
  const questionWithoutAnchors = text.includes("?") && signalScore <= 1;

  const negationMatches = text.match(/\b(no|sin|ni|evitar|que no)\b/g) || [];
  const connectiveMatches = text.match(/\b(pero|aunque|salvo|excepto|o)\b/g) || [];
  const mixedRestrictions = negationMatches.length >= 2 && connectiveMatches.length >= 1;

  const firstToken = tokens[0] || "";
  const difficultFollowUp = Boolean(
    conversationState &&
      conversationState.lastConstraints &&
      FOLLOW_UP_PREFIXES.includes(firstToken) &&
      (lowConfidence || sparseConstraints)
  );

  const shouldUseLlm =
    lowConfidence ||
    sparseConstraints ||
    longAndOpenText ||
    questionWithoutAnchors ||
    mixedRestrictions ||
    difficultFollowUp;

  return {
    shouldUseLlm,
    reasons: {
      lowConfidence,
      sparseConstraints,
      longAndOpenText,
      questionWithoutAnchors,
      mixedRestrictions,
      difficultFollowUp,
    },
  };
}

function mergeHybridConstraints(deterministicConstraints, llmConstraints) {
  const merged = {
    ...deterministicConstraints,
    rawText: deterministicConstraints.rawText,
    nearby: Boolean(deterministicConstraints.nearby || llmConstraints.nearby),
    includeCategories: uniqueStrings([
      ...(deterministicConstraints.includeCategories || []),
      ...(llmConstraints.includeCategories || []),
    ]),
    excludeCategories: uniqueStrings([
      ...(deterministicConstraints.excludeCategories || []),
      ...(llmConstraints.excludeCategories || []),
    ]),
    splitPlan: Boolean(deterministicConstraints.splitPlan || llmConstraints.splitPlan),
    noMixDays: Boolean(deterministicConstraints.noMixDays || llmConstraints.noMixDays),
    confidence: Math.max(deterministicConstraints.confidence || 0, llmConstraints.confidence || 0),
    source: "hybrid",
  };

  if (llmConstraints.dateScope !== "none" && deterministicConstraints.dateScope === "none") {
    merged.dateScope = llmConstraints.dateScope;
  }
  if (llmConstraints.timeScope !== "none" && deterministicConstraints.timeScope === "none") {
    merged.timeScope = llmConstraints.timeScope;
  }
  if (llmConstraints.mood !== "none" && deterministicConstraints.mood === "none") {
    merged.mood = llmConstraints.mood;
  }
  if (llmConstraints.styleHint !== "none" && deterministicConstraints.styleHint === "none") {
    merged.styleHint = llmConstraints.styleHint;
  }

  const excludedSet = new Set(merged.excludeCategories);
  merged.includeCategories = merged.includeCategories.filter((category) => !excludedSet.has(category));

  return assertIntentConstraints(merged);
}

function sameConstraints(a, b) {
  if (!a || !b) {
    return false;
  }

  const normalizeArray = (values) => [...(values || [])].sort();

  return (
    a.dateScope === b.dateScope &&
    a.timeScope === b.timeScope &&
    a.nearby === b.nearby &&
    a.mood === b.mood &&
    a.styleHint === b.styleHint &&
    a.splitPlan === b.splitPlan &&
    a.noMixDays === b.noMixDays &&
    JSON.stringify(normalizeArray(a.includeCategories)) === JSON.stringify(normalizeArray(b.includeCategories)) &&
    JSON.stringify(normalizeArray(a.excludeCategories)) === JSON.stringify(normalizeArray(b.excludeCategories))
  );
}

function createHybridIntentParser({
  deterministicParser = { parseIntentConstraints: parseDeterministicIntentConstraints },
  llmIntentParser = null,
  llmEnabled = false,
  logger = createLogger({ service: "hybrid-intent-parser" }),
} = {}) {
  if (!deterministicParser || typeof deterministicParser.parseIntentConstraints !== "function") {
    throw new Error("createHybridIntentParser requires deterministicParser.parseIntentConstraints");
  }

  if (llmEnabled && (!llmIntentParser || typeof llmIntentParser.parseIntentConstraints !== "function")) {
    throw new Error("createHybridIntentParser requires llmIntentParser.parseIntentConstraints when llmEnabled=true");
  }

  async function parseIntentConstraints(rawText, conversationState = null) {
    const deterministicConstraints = assertIntentConstraints(
      deterministicParser.parseIntentConstraints(rawText, conversationState)
    );

    if (!llmEnabled || !llmIntentParser) {
      return deterministicConstraints;
    }

    const llmDecision = evaluateNeedForLlm(rawText, conversationState, deterministicConstraints);
    if (!llmDecision.shouldUseLlm) {
      return deterministicConstraints;
    }

    try {
      const llmConstraints = assertIntentConstraints(
        await llmIntentParser.parseIntentConstraints(rawText, conversationState, {
          deterministicConstraints,
        })
      );

      const deterministicSignal = hasSignals(deterministicConstraints);
      const llmSignal = hasSignals(llmConstraints);

      if (deterministicSignal <= 1 && llmSignal >= 2) {
        return assertIntentConstraints({
          ...llmConstraints,
          rawText: asNonEmptyString(rawText) || deterministicConstraints.rawText,
          source: "llm",
        });
      }

      const merged = mergeHybridConstraints(deterministicConstraints, llmConstraints);
      if (sameConstraints(merged, deterministicConstraints)) {
        return deterministicConstraints;
      }

      return merged;
    } catch (error) {
      logger.warn("hybrid_intent_parser_llm_fallback", {
        error: error && error.message ? error.message : String(error || "unknown_llm_error"),
        reasons: llmDecision.reasons,
      });
      return deterministicConstraints;
    }
  }

  return {
    parseIntentConstraints,
  };
}

let cachedHybridParser = null;

function getHybridIntentParser() {
  if (!cachedHybridParser) {
    const config = getConfig();

    const llmIntentParser = config.intentLlm.enabled
      ? createLlmIntentParser({
          provider: config.intentLlm.provider,
          apiKey: config.intentLlm.apiKey,
          model: config.intentLlm.model,
          timeoutMs: config.intentLlm.timeoutMs,
        })
      : null;

    cachedHybridParser = createHybridIntentParser({
      llmEnabled: config.intentLlm.enabled,
      llmIntentParser,
    });
  }

  return cachedHybridParser;
}

function resetHybridIntentParserForTests() {
  cachedHybridParser = null;
}

module.exports = {
  createHybridIntentParser,
  getHybridIntentParser,
  resetHybridIntentParserForTests,
  evaluateNeedForLlm,
};
