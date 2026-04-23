const { assertIntentConstraints, assertRecommendationResult } = require("./contracts");

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function asOptionalBoolean(value, fieldName) {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value !== "boolean") {
    throw new Error(`${fieldName} must be a boolean when provided`);
  }
  return value;
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function wantsAllResults(rawText) {
  const normalized = normalizeText(rawText);
  if (!normalized) {
    return false;
  }

  if (/\b(todos?|todas?)\s+(los\s+|las\s+)?(eventos|planes|opciones|actividades)\b/.test(normalized)) {
    return true;
  }

  if (/\b(lista(do)?\s+complet[ao])\b/.test(normalized)) {
    return true;
  }

  if (/\b(sin\s+limite|sin\s+tope)\b/.test(normalized)) {
    return true;
  }

  return false;
}

function toIsoDate(value) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(baseDate, days) {
  const copy = new Date(baseDate.getTime());
  copy.setDate(copy.getDate() + days);
  return copy;
}

function mapDateScopeToEventDate(dateScope, nowDate) {
  if (dateScope === "today" || dateScope === "tonight") {
    return toIsoDate(nowDate);
  }
  if (dateScope === "tomorrow") {
    return toIsoDate(addDays(nowDate, 1));
  }
  return null;
}

function buildReasons(event, constraints, usedFallbackDate) {
  const reasons = [];

  if (constraints.includeCategories.includes(event.category)) {
    reasons.push("match_include_category");
  }
  if (constraints.timeScope !== "none" && event.timeBucket === constraints.timeScope) {
    reasons.push("match_time_scope");
  }
  if (constraints.mood !== "none") {
    reasons.push(`mood_${constraints.mood}`);
  }
  if (usedFallbackDate) {
    reasons.push("fallback_relaxed_date");
  }

  if (reasons.length === 0) {
    reasons.push("base_match");
  }

  return reasons;
}

function scoreEvent(event, constraints, usedFallbackDate) {
  let score = 1;

  if (constraints.includeCategories.includes(event.category)) {
    score += 2;
  }

  if (constraints.timeScope !== "none" && event.timeBucket === constraints.timeScope) {
    score += 1;
  }

  if (constraints.dateScope !== "none" && usedFallbackDate) {
    score -= 0.5;
  }

  if (constraints.nearby) {
    // Nearby scoring stays neutral until geospatial support is added.
    score += 0;
  }

  return Number(score.toFixed(2));
}

function filterEvents(events, constraints) {
  const includeSet = new Set(constraints.includeCategories || []);
  const excludeSet = new Set(constraints.excludeCategories || []);

  return events.filter((event) => {
    if (!event || typeof event !== "object") {
      return false;
    }

    if (excludeSet.has(event.category)) {
      return false;
    }

    if (includeSet.size > 0 && !includeSet.has(event.category)) {
      return false;
    }

    if (constraints.timeScope !== "none" && event.timeBucket !== constraints.timeScope) {
      return false;
    }

    return true;
  });
}

function normalizeShortlist(events, constraints, shortlistLimit, usedFallbackDate) {
  return events.slice(0, shortlistLimit).map((event) => ({
    event,
    score: scoreEvent(event, constraints, usedFallbackDate),
    reasons: buildReasons(event, constraints, usedFallbackDate),
  }));
}

function sortCandidates(candidates) {
  return [...candidates].sort((left, right) => {
    const leftDate = asNonEmptyString(left.eventDate) || "9999-99-99";
    const rightDate = asNonEmptyString(right.eventDate) || "9999-99-99";
    if (leftDate !== rightDate) {
      return leftDate.localeCompare(rightDate);
    }

    const leftTime = asNonEmptyString(left.timeText) || "99:99";
    const rightTime = asNonEmptyString(right.timeText) || "99:99";
    if (leftTime !== rightTime) {
      return leftTime.localeCompare(rightTime);
    }

    const leftTitle = asNonEmptyString(left.title) || "";
    const rightTitle = asNonEmptyString(right.title) || "";
    return leftTitle.localeCompare(rightTitle);
  });
}

function createRecommendationEngine({
  eventCatalogRepository,
  now = () => new Date(),
  defaultCity = "Rosario",
  shortlistLimit = 5,
  fetchLimit = 100,
  showAllFetchLimit = 500,
} = {}) {
  const catalogRepo = eventCatalogRepository;
  if (!catalogRepo || typeof catalogRepo.findEvents !== "function") {
    throw new Error("createRecommendationEngine requires an eventCatalogRepository with findEvents");
  }

  const cleanDefaultCity = asNonEmptyString(defaultCity) || "Rosario";
  const cleanShortlistLimit = Number.isInteger(shortlistLimit) && shortlistLimit > 0 ? shortlistLimit : 5;
  const cleanFetchLimit = Number.isInteger(fetchLimit) && fetchLimit > 0 ? Math.min(fetchLimit, 100) : 100;
  const cleanShowAllFetchLimit =
    Number.isInteger(showAllFetchLimit) && showAllFetchLimit > 0
      ? Math.min(showAllFetchLimit, 500)
      : 500;

  async function recommend(constraintsInput, context = {}) {
    const constraints = assertIntentConstraints(constraintsInput);
    const nowDate = now();
    const targetDate = mapDateScopeToEventDate(constraints.dateScope, nowDate);
    const showAllResults = Boolean(context.showAll) || wantsAllResults(constraints.rawText);
    const queryLimit = showAllResults ? cleanShowAllFetchLimit : cleanFetchLimit;

    const city = asNonEmptyString(context.city) || cleanDefaultCity;
    const isFree = asOptionalBoolean(context.isFree, "context.isFree");
    const hasUserLocation = Boolean(context.hasUserLocation || context.userLocation);

    if (constraints.nearby && !hasUserLocation) {
      return assertRecommendationResult({
        status: "need_user_input",
        constraints,
        candidatesCount: 0,
        shortlist: [],
        fallbackUsed: false,
        needsAction: "share_location",
        note: "Se necesita ubicacion para resolver la preferencia de cercania.",
        debug: {
          city,
          targetDate,
          filters: {
            includeCategories: constraints.includeCategories,
            excludeCategories: constraints.excludeCategories,
            timeScope: constraints.timeScope,
            isFree,
          },
        },
      });
    }

    const strictQuery = {
      city,
      eventDate: targetDate,
      isFree,
      limit: queryLimit,
    };

    const strictBaseEvents = await catalogRepo.findEvents(strictQuery);
    let strictCandidates = filterEvents(strictBaseEvents, constraints);
    strictCandidates = sortCandidates(strictCandidates);

    let usedFallback = false;
    let finalCandidates = strictCandidates;

    if (finalCandidates.length === 0 && targetDate) {
      const relaxedQuery = {
        city,
        isFree,
        limit: queryLimit,
      };

      const relaxedBaseEvents = await catalogRepo.findEvents(relaxedQuery);
      let relaxedCandidates = filterEvents(relaxedBaseEvents, constraints);
      relaxedCandidates = sortCandidates(relaxedCandidates);

      if (relaxedCandidates.length > 0) {
        usedFallback = true;
        finalCandidates = relaxedCandidates;
      }
    }

    if (finalCandidates.length === 0) {
      return assertRecommendationResult({
        status: "empty",
        constraints,
        candidatesCount: 0,
        shortlist: [],
        fallbackUsed: usedFallback,
        needsAction: constraints.includeCategories.length > 0 || constraints.dateScope !== "none" ? "relax_filters" : null,
        note: "No se encontraron eventos con los filtros solicitados.",
        debug: {
          city,
          targetDate,
          fallbackAttempted: Boolean(targetDate),
          filters: {
            includeCategories: constraints.includeCategories,
            excludeCategories: constraints.excludeCategories,
            timeScope: constraints.timeScope,
            isFree,
          },
        },
      });
    }

    const effectiveShortlistLimit = showAllResults ? finalCandidates.length : cleanShortlistLimit;
    const shortlist = normalizeShortlist(finalCandidates, constraints, effectiveShortlistLimit, usedFallback);

    return assertRecommendationResult({
      status: "ok",
      constraints,
      candidatesCount: finalCandidates.length,
      shortlist,
      fallbackUsed: usedFallback,
      needsAction: null,
      note: usedFallback ? "No hubo coincidencia exacta de fecha, se aplico fallback controlado." : undefined,
      debug: {
        city,
        targetDate,
        fallbackAttempted: Boolean(targetDate),
        filters: {
          includeCategories: constraints.includeCategories,
          excludeCategories: constraints.excludeCategories,
          timeScope: constraints.timeScope,
          isFree,
        },
      },
    });
  }

  return {
    recommend,
  };
}

let cachedEngine = null;

function getRecommendationEngine() {
  if (!cachedEngine) {
    const { getEventCatalogRepository } = require("./eventCatalogRepo");
    cachedEngine = createRecommendationEngine({
      eventCatalogRepository: getEventCatalogRepository(),
    });
  }
  return cachedEngine;
}

function resetRecommendationEngineForTests() {
  cachedEngine = null;
}

module.exports = {
  createRecommendationEngine,
  getRecommendationEngine,
  resetRecommendationEngineForTests,
};
