const test = require("node:test");
const assert = require("node:assert/strict");

const { validateIntentConstraints } = require("../src/contracts");
const { parseIntentConstraints } = require("../src/intentParser");

function validLastConstraints(overrides = {}) {
  return {
    rawText: "base",
    dateScope: "today",
    timeScope: "none",
    nearby: false,
    includeCategories: ["museo"],
    excludeCategories: ["cine"],
    mood: "tranquilo",
    styleHint: "none",
    splitPlan: false,
    noMixDays: false,
    confidence: 0.8,
    source: "deterministic",
    ...overrides,
  };
}

test("parser devuelve IntentConstraints valido aunque el texto sea vacio", () => {
  const parsed = parseIntentConstraints("");

  const result = validateIntentConstraints(parsed);
  assert.equal(result.ok, true);
  assert.equal(parsed.dateScope, "none");
  assert.equal(parsed.timeScope, "none");
  assert.equal(parsed.nearby, false);
});

test("detecta dateScope, timeScope, nearby e includeCategories basicos", () => {
  const parsed = parseIntentConstraints("hoy a la noche cerca mio quiero museos");

  assert.equal(parsed.dateScope, "tonight");
  assert.equal(parsed.timeScope, "night");
  assert.equal(parsed.nearby, true);
  assert.deepEqual(parsed.includeCategories, ["museo"]);
  assert.deepEqual(parsed.excludeCategories, []);
});

test("detecta negaciones simples por categoria", () => {
  const parsed = parseIntentConstraints("sin teatro, no cine, no quiero museos");

  assert.deepEqual(parsed.excludeCategories.sort(), ["cine", "museo", "teatro"]);
  assert.deepEqual(parsed.includeCategories, []);
});

test("follow-up y para manana conserva constraints previos y cambia dateScope", () => {
  const parsed = parseIntentConstraints("y para manana?", {
    lastConstraints: validLastConstraints({
      dateScope: "today",
      nearby: true,
      includeCategories: ["museo", "feria"],
      excludeCategories: ["cine"],
    }),
  });

  assert.equal(parsed.dateScope, "tomorrow");
  assert.equal(parsed.nearby, true);
  assert.deepEqual(parsed.includeCategories.sort(), ["feria", "museo"]);
  assert.deepEqual(parsed.excludeCategories, ["cine"]);
});

test("follow-up algo mas movido conserva filtros y actualiza mood", () => {
  const parsed = parseIntentConstraints("algo mas movido", {
    lastConstraints: validLastConstraints({
      mood: "tranquilo",
      includeCategories: ["taller"],
      excludeCategories: ["cine"],
    }),
  });

  assert.equal(parsed.mood, "movido");
  assert.deepEqual(parsed.includeCategories, ["taller"]);
  assert.deepEqual(parsed.excludeCategories, ["cine"]);
});

test("follow-up pero sin teatro agrega exclusion y exclusion gana sobre inclusion previa", () => {
  const parsed = parseIntentConstraints("pero sin teatro", {
    lastConstraints: validLastConstraints({
      includeCategories: ["teatro", "museo"],
      excludeCategories: [],
    }),
  });

  assert.deepEqual(parsed.excludeCategories, ["teatro"]);
  assert.deepEqual(parsed.includeCategories, ["museo"]);
});

test("si lastConstraints es invalido, ignora follow-up y vuelve a base segura", () => {
  const parsed = parseIntentConstraints("y para manana?", {
    lastConstraints: {
      dateScope: "invalido",
    },
  });

  assert.equal(parsed.dateScope, "tomorrow");
  assert.deepEqual(parsed.includeCategories, []);
  assert.deepEqual(parsed.excludeCategories, []);
});
