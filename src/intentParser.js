const { assertIntentConstraints, validateIntentConstraints } = require("./contracts");

const CATEGORY_SYNONYMS = {
  teatro: ["teatro", "teatros", "obra", "obras"],
  museo: ["museo", "museos", "muestra", "muestras", "exposicion", "exposiciones"],
  cine: ["cine", "cines", "pelicula", "peliculas", "film", "films"],
  feria: ["feria", "ferias"],
  taller: ["taller", "talleres"],
  charla: ["charla", "charlas", "conferencia", "conferencias"],
  familiar: ["familiar", "familia", "chicos", "chicas", "ninos", "ninas", "infantil"],
  aire_libre: ["aire libre", "al aire libre", "plaza", "parque"],
  cultural: ["cultural", "cultura", "centro cultural", "centros culturales"],
};

const FOLLOW_UP_PREFIXES = ["y", "pero", "para", "sin", "no", "algo"];

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s?]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function tokenize(normalizedText) {
  if (!normalizedText) {
    return [];
  }
  return normalizedText.split(" ").filter(Boolean);
}

function sanitizeRawText(value) {
  const text = String(value || "").trim();
  return text.length > 0 ? text : "consulta vacia";
}

function createDefaultConstraints(rawText) {
  return {
    rawText: sanitizeRawText(rawText),
    dateScope: "none",
    timeScope: "none",
    nearby: false,
    includeCategories: [],
    excludeCategories: [],
    mood: "none",
    styleHint: "none",
    splitPlan: false,
    noMixDays: false,
    confidence: 0.35,
    source: "deterministic",
  };
}

function cloneConstraints(constraints, rawText) {
  return {
    ...constraints,
    rawText: sanitizeRawText(rawText),
    includeCategories: [...(constraints.includeCategories || [])],
    excludeCategories: [...(constraints.excludeCategories || [])],
    source: "deterministic",
  };
}

function hasPhrase(normalizedText, phrases) {
  return phrases.some((phrase) => normalizedText.includes(phrase));
}

function detectDateScope(normalizedText) {
  if (hasPhrase(normalizedText, ["esta noche", "hoy a la noche"])) {
    return "tonight";
  }
  if (normalizedText.includes("manana") && !normalizedText.includes("pasado manana")) {
    return "tomorrow";
  }
  if (normalizedText.includes("hoy")) {
    return "today";
  }
  return null;
}

function detectTimeScope(normalizedText) {
  if (hasPhrase(normalizedText, ["a la noche", "por la noche", "noche"])) {
    return "night";
  }
  if (hasPhrase(normalizedText, ["a la tarde", "por la tarde", "tarde"])) {
    return "afternoon";
  }
  return null;
}

function detectNearby(normalizedText) {
  return hasPhrase(normalizedText, ["cerca", "cerca mio", "cerca mio", "a mano", "caminable", "caminando"]);
}

function detectMood(normalizedText) {
  if (hasPhrase(normalizedText, ["mas movido", "algo movido", "movido", "arriba"])) {
    return "movido";
  }
  if (hasPhrase(normalizedText, ["tranqui", "tranquilo", "calmo", "relajado"])) {
    return "tranquilo";
  }
  if (hasPhrase(normalizedText, ["familiar", "en familia", "con chicos", "para chicos", "infantil"])) {
    return "familiar";
  }
  return null;
}

function detectStyleHint(normalizedText) {
  if (hasPhrase(normalizedText, ["no careta", "sin careta"])) {
    return "no_careta";
  }
  if (hasPhrase(normalizedText, ["careta", "elegante", "formal"])) {
    return "careta";
  }
  return null;
}

function detectSplitPlan(normalizedText) {
  return hasPhrase(normalizedText, ["y despues", "despues", "luego", "arrancar con"]);
}

function detectNoMixDays(normalizedText) {
  return hasPhrase(normalizedText, ["sin mezclar", "no mezclar", "separado por dia", "por separado"]);
}

function findSequencePositions(tokens, sequence) {
  const positions = [];
  if (!tokens.length || !sequence.length || sequence.length > tokens.length) {
    return positions;
  }

  for (let i = 0; i <= tokens.length - sequence.length; i += 1) {
    let match = true;
    for (let j = 0; j < sequence.length; j += 1) {
      if (tokens[i + j] !== sequence[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      positions.push(i);
    }
  }

  return positions;
}

function isNegated(tokens, startIndex) {
  const from = Math.max(0, startIndex - 4);
  const window = tokens.slice(from, startIndex);

  if (window.includes("no") || window.includes("sin") || window.includes("ni") || window.includes("evitar")) {
    return true;
  }

  const previousToken = tokens[startIndex - 1];
  const twoBefore = tokens[startIndex - 2];

  if (twoBefore === "no" && previousToken === "quiero") {
    return true;
  }
  if (twoBefore === "que" && previousToken === "no") {
    return true;
  }
  if (twoBefore === "nada" && previousToken === "de") {
    return true;
  }

  return false;
}

function detectCategoryConstraints(tokens) {
  const include = new Set();
  const exclude = new Set();

  Object.entries(CATEGORY_SYNONYMS).forEach(([category, synonyms]) => {
    let hasPositiveMatch = false;
    let hasNegativeMatch = false;

    for (const synonym of synonyms) {
      const synonymTokens = synonym.split(" ");
      const positions = findSequencePositions(tokens, synonymTokens);
      positions.forEach((position) => {
        if (isNegated(tokens, position)) {
          hasNegativeMatch = true;
        } else {
          hasPositiveMatch = true;
        }
      });
    }

    if (hasPositiveMatch) {
      include.add(category);
    }
    if (hasNegativeMatch) {
      exclude.add(category);
    }
  });

  return {
    include: Array.from(include),
    exclude: Array.from(exclude),
  };
}

function looksLikeFollowUp(normalizedText, conversationState) {
  if (!conversationState || !conversationState.lastConstraints) {
    return false;
  }

  const firstToken = normalizedText.split(" ").filter(Boolean)[0];
  if (firstToken && FOLLOW_UP_PREFIXES.includes(firstToken)) {
    return true;
  }

  return hasPhrase(normalizedText, ["para manana", "algo mas", "algo mas movido"]);
}

function getBaseConstraints(rawText, normalizedText, conversationState) {
  if (!looksLikeFollowUp(normalizedText, conversationState)) {
    return createDefaultConstraints(rawText);
  }

  const candidate = conversationState.lastConstraints;
  const validation = validateIntentConstraints(candidate);
  if (!validation.ok) {
    return createDefaultConstraints(rawText);
  }

  return cloneConstraints(candidate, rawText);
}

function uniqueStrings(values) {
  return Array.from(new Set(values.filter((value) => typeof value === "string" && value.length > 0)));
}

function mergeCategories(baseValues, detectedValues) {
  return uniqueStrings([...(baseValues || []), ...(detectedValues || [])]);
}

function computeConfidence({
  usedLastConstraints,
  dateScopeDetected,
  timeScopeDetected,
  nearbyDetected,
  moodDetected,
  styleHintDetected,
  categorySignals,
}) {
  let score = 0.35;

  if (usedLastConstraints) {
    score += 0.1;
  }
  if (dateScopeDetected) {
    score += 0.14;
  }
  if (timeScopeDetected) {
    score += 0.1;
  }
  if (nearbyDetected) {
    score += 0.1;
  }
  if (moodDetected) {
    score += 0.1;
  }
  if (styleHintDetected) {
    score += 0.06;
  }
  if (categorySignals > 0) {
    score += 0.15;
  }

  return Math.max(0, Math.min(0.95, Number(score.toFixed(2))));
}

function parseIntentConstraints(rawText, conversationState = null) {
  const normalizedText = normalizeText(rawText);
  const tokens = tokenize(normalizedText);

  const usedLastConstraints = looksLikeFollowUp(normalizedText, conversationState);
  const constraints = getBaseConstraints(rawText, normalizedText, conversationState);

  const detectedDateScope = detectDateScope(normalizedText);
  const detectedTimeScope = detectTimeScope(normalizedText);
  const detectedNearby = detectNearby(normalizedText);
  const detectedMood = detectMood(normalizedText);
  const detectedStyleHint = detectStyleHint(normalizedText);
  const detectedSplitPlan = detectSplitPlan(normalizedText);
  const detectedNoMixDays = detectNoMixDays(normalizedText);
  const detectedCategories = detectCategoryConstraints(tokens);

  if (detectedDateScope) {
    constraints.dateScope = detectedDateScope;
    if (detectedDateScope === "tonight" && !detectedTimeScope) {
      constraints.timeScope = "night";
    }
  }

  if (detectedTimeScope) {
    constraints.timeScope = detectedTimeScope;
  }

  if (detectedNearby) {
    constraints.nearby = true;
  }

  if (detectedMood) {
    constraints.mood = detectedMood;
  }

  if (detectedStyleHint) {
    constraints.styleHint = detectedStyleHint;
  }

  if (detectedSplitPlan) {
    constraints.splitPlan = true;
  }

  if (detectedNoMixDays) {
    constraints.noMixDays = true;
  }

  constraints.includeCategories = mergeCategories(constraints.includeCategories, detectedCategories.include);
  constraints.excludeCategories = mergeCategories(constraints.excludeCategories, detectedCategories.exclude);

  // Exclusion wins over inclusion when both appear for the same category.
  const excludedSet = new Set(constraints.excludeCategories);
  constraints.includeCategories = constraints.includeCategories.filter((category) => !excludedSet.has(category));

  constraints.source = "deterministic";
  constraints.confidence = computeConfidence({
    usedLastConstraints,
    dateScopeDetected: Boolean(detectedDateScope),
    timeScopeDetected: Boolean(detectedTimeScope),
    nearbyDetected: detectedNearby,
    moodDetected: Boolean(detectedMood),
    styleHintDetected: Boolean(detectedStyleHint),
    categorySignals: detectedCategories.include.length + detectedCategories.exclude.length,
  });

  return assertIntentConstraints(constraints);
}

module.exports = {
  parseIntentConstraints,
};
