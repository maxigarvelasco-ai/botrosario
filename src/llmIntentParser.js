const { assertIntentConstraints, enums } = require("./contracts");

const DEFAULT_TIMEOUT_MS = 10000;
const GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions";

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function asBoolean(value, fallback = false) {
  if (typeof value === "boolean") {
    return value;
  }
  return fallback;
}

function asConfidence(value, fallback = 0.55) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return fallback;
  }
  return Math.max(0, Math.min(0.95, Number(value.toFixed(2))));
}

function sanitizeRawText(value) {
  return asNonEmptyString(value) || "consulta vacia";
}

function uniqueStrings(values) {
  return Array.from(new Set(values.filter((value) => typeof value === "string" && value.length > 0)));
}

function normalizeCategories(rawValue) {
  if (!Array.isArray(rawValue)) {
    return [];
  }

  const allowed = new Set(enums.eventCategories);
  return uniqueStrings(
    rawValue
      .map((item) => asNonEmptyString(item))
      .filter(Boolean)
      .map((item) => item.toLowerCase())
      .filter((item) => allowed.has(item))
  );
}

function normalizeEnum(rawValue, allowedValues, fallback) {
  const clean = asNonEmptyString(rawValue);
  if (!clean) {
    return fallback;
  }

  return allowedValues.includes(clean) ? clean : fallback;
}

function createDefaultConstraints(rawText, fallbackConstraints = null) {
  if (fallbackConstraints && typeof fallbackConstraints === "object") {
    return {
      ...fallbackConstraints,
      rawText: sanitizeRawText(rawText),
      includeCategories: [...(fallbackConstraints.includeCategories || [])],
      excludeCategories: [...(fallbackConstraints.excludeCategories || [])],
      source: "llm",
    };
  }

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
    confidence: 0.55,
    source: "llm",
  };
}

function stripFence(text) {
  const cleanText = asNonEmptyString(text) || "";
  if (!cleanText.startsWith("```") || !cleanText.endsWith("```")) {
    return cleanText;
  }

  return cleanText.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
}

function extractJsonPayload(content) {
  if (content && typeof content === "object") {
    return content;
  }

  const raw = stripFence(content);
  if (!raw) {
    throw new Error("LLM returned empty content");
  }

  try {
    return JSON.parse(raw);
  } catch (_error) {
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start >= 0 && end > start) {
      const candidate = raw.slice(start, end + 1);
      return JSON.parse(candidate);
    }
    throw new Error("LLM response is not valid JSON");
  }
}

function buildPrompt({ rawText, conversationState, deterministicConstraints }) {
  const payload = {
    userText: sanitizeRawText(rawText),
    previousConstraints:
      conversationState && conversationState.lastConstraints ? conversationState.lastConstraints : null,
    deterministicConstraints: deterministicConstraints || null,
  };

  return {
    system: [
      "Sos un parser de intenciones para un bot cultural de Rosario.",
      "Responde UNICAMENTE un JSON valido, sin markdown ni explicaciones.",
      "No inventes datos fuera del texto o contexto provisto.",
      "Respeta negaciones explicitamente (no/sin/ni/evitar/que no).",
      "No inventes ubicacion si el usuario no la dio.",
      "No mezcles dias si el texto no lo pide.",
      `Categorias permitidas: ${enums.eventCategories.join(", ")}.`,
      `dateScope permitido: ${enums.intentDateScopes.join(", ")}.`,
      `timeScope permitido: ${enums.intentTimeScopes.join(", ")}.`,
      `mood permitido: ${enums.intentMoods.join(", ")}.`,
      `styleHint permitido: ${enums.intentStyleHints.join(", ")}.`,
      "El JSON debe incluir exactamente estos campos:",
      "rawText,dateScope,timeScope,nearby,includeCategories,excludeCategories,mood,styleHint,splitPlan,noMixDays,confidence,source",
      'Setea source="llm".',
    ].join("\n"),
    user: JSON.stringify(payload),
  };
}

async function callGroq({ apiKey, model, timeoutMs, fetchImpl, prompt }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetchImpl(GROQ_CHAT_COMPLETIONS_URL, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        temperature: 0,
        response_format: {
          type: "json_object",
        },
        messages: [
          {
            role: "system",
            content: prompt.system,
          },
          {
            role: "user",
            content: prompt.user,
          },
        ],
      }),
    });

    let body = null;
    try {
      body = await response.json();
    } catch (_error) {
      body = null;
    }

    if (!response.ok) {
      const description = body && body.error && body.error.message
        ? body.error.message
        : `http_status_${response.status}`;
      throw new Error(`Groq API error: ${description}`);
    }

    const content = body && body.choices && body.choices[0] && body.choices[0].message
      ? body.choices[0].message.content
      : null;

    if (!content) {
      throw new Error("Groq API returned empty message content");
    }

    return content;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeLlmConstraints(rawText, llmDraft, deterministicConstraints) {
  const base = createDefaultConstraints(rawText, deterministicConstraints);
  const draft = llmDraft && typeof llmDraft === "object" ? llmDraft : {};

  base.rawText = sanitizeRawText(rawText);
  base.dateScope = normalizeEnum(draft.dateScope, enums.intentDateScopes, base.dateScope || "none");
  base.timeScope = normalizeEnum(draft.timeScope, enums.intentTimeScopes, base.timeScope || "none");
  base.nearby = asBoolean(draft.nearby, Boolean(base.nearby));
  base.includeCategories = normalizeCategories(
    Array.isArray(draft.includeCategories) ? draft.includeCategories : base.includeCategories
  );
  base.excludeCategories = normalizeCategories(
    Array.isArray(draft.excludeCategories) ? draft.excludeCategories : base.excludeCategories
  );
  base.mood = normalizeEnum(draft.mood, enums.intentMoods, base.mood || "none");
  base.styleHint = normalizeEnum(draft.styleHint, enums.intentStyleHints, base.styleHint || "none");
  base.splitPlan = asBoolean(draft.splitPlan, Boolean(base.splitPlan));
  base.noMixDays = asBoolean(draft.noMixDays, Boolean(base.noMixDays));
  base.confidence = asConfidence(draft.confidence, asConfidence(base.confidence, 0.55));
  base.source = "llm";

  const excludeSet = new Set(base.excludeCategories);
  base.includeCategories = base.includeCategories.filter((category) => !excludeSet.has(category));

  return assertIntentConstraints(base);
}

function createLlmIntentParser({
  provider,
  apiKey,
  model,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  fetchImpl = global.fetch,
} = {}) {
  const cleanProvider = asNonEmptyString(provider) || "groq";
  const cleanApiKey = asNonEmptyString(apiKey);
  const cleanModel = asNonEmptyString(model);

  if (cleanProvider !== "groq") {
    throw new Error(`Unsupported LLM provider: ${cleanProvider}`);
  }

  if (!cleanApiKey) {
    throw new Error("createLlmIntentParser requires apiKey");
  }

  if (!cleanModel) {
    throw new Error("createLlmIntentParser requires model");
  }

  if (typeof fetchImpl !== "function") {
    throw new Error("createLlmIntentParser requires fetch implementation");
  }

  const cleanTimeoutMs = Number.isInteger(timeoutMs) && timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;

  async function parseIntentConstraints(rawText, conversationState = null, options = {}) {
    const prompt = buildPrompt({
      rawText,
      conversationState,
      deterministicConstraints: options.deterministicConstraints || null,
    });

    const rawContent = await callGroq({
      apiKey: cleanApiKey,
      model: cleanModel,
      timeoutMs: cleanTimeoutMs,
      fetchImpl,
      prompt,
    });

    const llmDraft = extractJsonPayload(rawContent);

    return normalizeLlmConstraints(rawText, llmDraft, options.deterministicConstraints || null);
  }

  return {
    parseIntentConstraints,
  };
}

module.exports = {
  createLlmIntentParser,
};
