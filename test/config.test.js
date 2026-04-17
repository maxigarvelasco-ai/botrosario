const test = require("node:test");
const assert = require("node:assert/strict");

const { buildConfig } = require("../src/config");

function validEnv(overrides = {}) {
  return {
    NODE_ENV: "development",
    PORT: "8080",
    APIFY_TOKEN: "token_123",
    FIREBASE_SERVICE_ACCOUNT_JSON: JSON.stringify({
      project_id: "demo-project",
      client_email: "firebase-adminsdk@demo-project.iam.gserviceaccount.com",
      private_key: "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
    }),
    ...overrides,
  };
}

test("buildConfig returns normalized valid config", () => {
  const config = buildConfig(validEnv());
  assert.equal(config.nodeEnv, "development");
  assert.equal(config.port, 8080);
  assert.equal(config.apifyToken, "token_123");
  assert.equal(config.conversationStateCollection, "conversation_state");
  assert.equal(config.interactionLogCollection, "interaction_logs");
  assert.equal(config.firebase.source, "env_json");
  assert.match(config.firebase.serviceAccount.private_key, /BEGIN PRIVATE KEY/);
  assert.equal(config.intentLlm.enabled, false);
  assert.equal(config.intentLlm.provider, "groq");
  assert.equal(config.intentLlm.timeoutMs, 10000);
  assert.equal(config.telegram.enabled, false);
  assert.equal(config.telegram.webhookPath, "/webhooks/telegram");
});

test("buildConfig fails on invalid NODE_ENV", () => {
  assert.throws(() => buildConfig(validEnv({ NODE_ENV: "staging" })), /Invalid NODE_ENV/);
});

test("buildConfig fails on invalid PORT", () => {
  assert.throws(() => buildConfig(validEnv({ PORT: "99999" })), /Invalid PORT/);
});

test("buildConfig fails when APIFY_TOKEN is missing", () => {
  assert.throws(() => buildConfig(validEnv({ APIFY_TOKEN: "" })), /Missing APIFY_TOKEN/);
});

test("buildConfig fails when Firebase credentials are missing", () => {
  const env = validEnv({ FIREBASE_SERVICE_ACCOUNT_JSON: undefined, GOOGLE_APPLICATION_CREDENTIALS: undefined });
  delete env.FIREBASE_SERVICE_ACCOUNT_JSON;
  delete env.GOOGLE_APPLICATION_CREDENTIALS;
  assert.throws(() => buildConfig(env), /Missing Firebase credentials/);
});

test("buildConfig fails on invalid FIREBASE_SERVICE_ACCOUNT_JSON", () => {
  assert.throws(
    () => buildConfig(validEnv({ FIREBASE_SERVICE_ACCOUNT_JSON: "{invalid_json}" })),
    /Invalid FIREBASE_SERVICE_ACCOUNT_JSON/
  );
});

test("buildConfig enables telegram when TELEGRAM_TOKEN exists", () => {
  const config = buildConfig(
    validEnv({
      TELEGRAM_TOKEN: "tg_token_123",
      TELEGRAM_WEBHOOK_SECRET: "webhook-secret",
      TELEGRAM_WEBHOOK_PATH_SECRET: "abc123_secret",
    })
  );

  assert.equal(config.telegram.enabled, true);
  assert.equal(config.telegram.token, "tg_token_123");
  assert.equal(config.telegram.webhookSecret, "webhook-secret");
  assert.equal(config.telegram.webhookPathSecret, "abc123_secret");
  assert.equal(config.telegram.webhookPath, "/webhooks/telegram/abc123_secret");
});

test("buildConfig fails on invalid TELEGRAM_WEBHOOK_PATH_SECRET", () => {
  assert.throws(
    () => buildConfig(validEnv({ TELEGRAM_WEBHOOK_PATH_SECRET: "secret/with/slash" })),
    /Invalid TELEGRAM_WEBHOOK_PATH_SECRET/
  );
});

test("buildConfig enables LLM intent parser when configured", () => {
  const config = buildConfig(
    validEnv({
      LLM_INTENT_ENABLED: "true",
      LLM_INTENT_PROVIDER: "groq",
      LLM_INTENT_MODEL: "llama-3.3-70b-versatile",
      LLM_INTENT_API_KEY: "groq-key",
      LLM_INTENT_TIMEOUT_MS: "8000",
    })
  );

  assert.equal(config.intentLlm.enabled, true);
  assert.equal(config.intentLlm.provider, "groq");
  assert.equal(config.intentLlm.model, "llama-3.3-70b-versatile");
  assert.equal(config.intentLlm.apiKey, "groq-key");
  assert.equal(config.intentLlm.timeoutMs, 8000);
});

test("buildConfig fails when LLM intent is enabled without API key", () => {
  assert.throws(
    () =>
      buildConfig(
        validEnv({
          LLM_INTENT_ENABLED: "true",
          LLM_INTENT_PROVIDER: "groq",
          LLM_INTENT_MODEL: "llama-3.3-70b-versatile",
          LLM_INTENT_API_KEY: "",
        })
      ),
    /Missing LLM_INTENT_API_KEY/
  );
});
