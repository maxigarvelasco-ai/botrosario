const test = require("node:test");
const assert = require("node:assert/strict");

const { createLlmIntentParser } = require("../src/llmIntentParser");

function createFetchMock(content, { ok = true, status = 200 } = {}) {
  return async () => ({
    ok,
    status,
    async json() {
      return {
        ok,
        choices: [
          {
            message: {
              content,
            },
          },
        ],
        error: ok ? undefined : { message: content },
      };
    },
  });
}

test("llm parser devuelve IntentConstraints valido desde JSON", async () => {
  const parser = createLlmIntentParser({
    provider: "groq",
    apiKey: "api-key",
    model: "llama-test",
    fetchImpl: createFetchMock(
      JSON.stringify({
        rawText: "algo cultural cerca",
        dateScope: "today",
        timeScope: "none",
        nearby: true,
        includeCategories: ["museo"],
        excludeCategories: ["cine"],
        mood: "none",
        styleHint: "none",
        splitPlan: false,
        noMixDays: false,
        confidence: 0.8,
        source: "llm",
      })
    ),
  });

  const out = await parser.parseIntentConstraints("algo cultural cerca");

  assert.equal(out.source, "llm");
  assert.equal(out.nearby, true);
  assert.deepEqual(out.includeCategories, ["museo"]);
  assert.deepEqual(out.excludeCategories, ["cine"]);
});

test("llm parser rechaza proveedor no soportado", () => {
  assert.throws(
    () =>
      createLlmIntentParser({
        provider: "openai",
        apiKey: "key",
        model: "model",
        fetchImpl: async () => ({}),
      }),
    /Unsupported LLM provider/
  );
});

test("llm parser falla con error claro cuando API responde error", async () => {
  const parser = createLlmIntentParser({
    provider: "groq",
    apiKey: "api-key",
    model: "llama-test",
    fetchImpl: async () => ({
      ok: false,
      status: 401,
      async json() {
        return {
          error: {
            message: "unauthorized",
          },
        };
      },
    }),
  });

  await assert.rejects(() => parser.parseIntentConstraints("hola"), /Groq API error: unauthorized/);
});
