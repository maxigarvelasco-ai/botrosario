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
