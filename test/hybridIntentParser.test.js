const test = require("node:test");
const assert = require("node:assert/strict");

const { parseIntentConstraints } = require("../src/intentParser");
const { createHybridIntentParser } = require("../src/hybridIntentParser");

function deterministicDraft(overrides = {}) {
  return {
    rawText: "consulta",
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
    ...overrides,
  };
}

test("hibrido usa deterministic cuando llm esta deshabilitado", async () => {
  const parser = createHybridIntentParser({
    llmEnabled: false,
    deterministicParser: {
      parseIntentConstraints,
    },
  });

  const out = await parser.parseIntentConstraints("quiero museos hoy");
  assert.equal(out.source, "deterministic");
  assert.equal(out.dateScope, "today");
});

test("hibrido usa llm cuando deterministic queda ambiguo y llm mejora", async () => {
  const parser = createHybridIntentParser({
    llmEnabled: true,
    deterministicParser: {
      parseIntentConstraints() {
        return deterministicDraft({
          rawText: "algo para salir no muy careta y sin cine",
          confidence: 0.25,
        });
      },
    },
    llmIntentParser: {
      async parseIntentConstraints() {
        return {
          rawText: "algo para salir no muy careta y sin cine",
          dateScope: "today",
          timeScope: "night",
          nearby: false,
          includeCategories: ["teatro"],
          excludeCategories: ["cine"],
          mood: "none",
          styleHint: "no_careta",
          splitPlan: false,
          noMixDays: false,
          confidence: 0.82,
          source: "llm",
        };
      },
    },
  });

  const out = await parser.parseIntentConstraints("algo para salir no muy careta y sin cine");
  assert.equal(out.source, "llm");
  assert.equal(out.timeScope, "night");
  assert.deepEqual(out.excludeCategories, ["cine"]);
});

test("hibrido devuelve source=hybrid cuando fusiona info", async () => {
  const parser = createHybridIntentParser({
    llmEnabled: true,
    deterministicParser: {
      parseIntentConstraints() {
        return deterministicDraft({
          rawText: "quiero museo para manana",
          dateScope: "tomorrow",
          includeCategories: ["museo"],
          confidence: 0.32,
        });
      },
    },
    llmIntentParser: {
      async parseIntentConstraints() {
        return {
          rawText: "museo para manana",
          dateScope: "tomorrow",
          timeScope: "none",
          nearby: false,
          includeCategories: ["museo"],
          excludeCategories: [],
          mood: "tranquilo",
          styleHint: "none",
          splitPlan: false,
          noMixDays: false,
          confidence: 0.78,
          source: "llm",
        };
      },
    },
  });

  const out = await parser.parseIntentConstraints("quiero museo para manana");
  assert.equal(out.source, "hybrid");
  assert.equal(out.dateScope, "tomorrow");
  assert.equal(out.mood, "tranquilo");
  assert.deepEqual(out.includeCategories, ["museo"]);
});

test("hibrido hace fallback seguro a deterministic si falla llm", async () => {
  const parser = createHybridIntentParser({
    llmEnabled: true,
    deterministicParser: {
      parseIntentConstraints,
    },
    llmIntentParser: {
      async parseIntentConstraints() {
        throw new Error("llm timeout");
      },
    },
    logger: {
      info() {},
      warn() {},
      error() {},
    },
  });

  const out = await parser.parseIntentConstraints("quiero museos hoy");
  assert.equal(out.source, "deterministic");
  assert.equal(out.dateScope, "today");
});
