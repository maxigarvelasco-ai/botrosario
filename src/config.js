const fs = require("fs");
const path = require("path");
require("dotenv").config();

const ALLOWED_NODE_ENVS = new Set(["development", "test", "production"]);
const ALLOWED_LLM_PROVIDERS = new Set(["groq"]);

let cachedConfig = null;

function asNonEmptyString(value) {
  if (value === undefined || value === null) {
    return null;
  }
  const out = String(value).trim();
  return out.length > 0 ? out : null;
}

function parsePort(rawPort) {
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error("Invalid PORT: must be an integer between 1 and 65535");
  }
  return port;
}

function parseBoolean(rawValue, fallback = false) {
  const value = asNonEmptyString(rawValue);
  if (!value) {
    return fallback;
  }

  const normalized = value.toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(`Invalid boolean value: ${value}`);
}

function parsePositiveInt(rawValue, fieldName, fallback) {
  const value = asNonEmptyString(rawValue);
  if (!value) {
    return fallback;
  }

  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    throw new Error(`Invalid ${fieldName}: must be a positive integer`);
  }

  return num;
}

function parseWebhookPathSecret(rawValue) {
  const value = asNonEmptyString(rawValue);
  if (!value) {
    return null;
  }

  if (!/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new Error("Invalid TELEGRAM_WEBHOOK_PATH_SECRET: only letters, numbers, _ and - are allowed");
  }

  return value;
}

function resolveIntentLlmConfig(env) {
  const enabled = parseBoolean(env.LLM_INTENT_ENABLED, false);

  const provider = asNonEmptyString(env.LLM_INTENT_PROVIDER) || "groq";
  if (!ALLOWED_LLM_PROVIDERS.has(provider)) {
    throw new Error(`Invalid LLM_INTENT_PROVIDER: ${provider}. Allowed: groq`);
  }

  const model = asNonEmptyString(env.LLM_INTENT_MODEL);
  const apiKey = asNonEmptyString(env.LLM_INTENT_API_KEY);
  const timeoutMs = parsePositiveInt(env.LLM_INTENT_TIMEOUT_MS, "LLM_INTENT_TIMEOUT_MS", 10000);

  if (enabled && !apiKey) {
    throw new Error("Missing LLM_INTENT_API_KEY when LLM_INTENT_ENABLED=true");
  }

  if (enabled && !model) {
    throw new Error("Missing LLM_INTENT_MODEL when LLM_INTENT_ENABLED=true");
  }

  return {
    enabled,
    provider,
    model,
    apiKey,
    timeoutMs,
  };
}

function normalizeServiceAccount(serviceAccount) {
  if (!serviceAccount || typeof serviceAccount !== "object") {
    throw new Error("Invalid Firebase credentials: expected JSON object");
  }

  const normalized = { ...serviceAccount };
  if (typeof normalized.private_key === "string") {
    normalized.private_key = normalized.private_key.replace(/\\n/g, "\n");
  }

  if (!asNonEmptyString(normalized.project_id)) {
    throw new Error("Invalid Firebase credentials: project_id is required");
  }
  if (!asNonEmptyString(normalized.client_email)) {
    throw new Error("Invalid Firebase credentials: client_email is required");
  }
  if (!asNonEmptyString(normalized.private_key)) {
    throw new Error("Invalid Firebase credentials: private_key is required");
  }

  return normalized;
}

function resolveFirebaseCredentials(env) {
  const rawJson = asNonEmptyString(env.FIREBASE_SERVICE_ACCOUNT_JSON);
  if (rawJson) {
    let parsed;
    try {
      parsed = JSON.parse(rawJson);
    } catch (error) {
      throw new Error(`Invalid FIREBASE_SERVICE_ACCOUNT_JSON: ${error.message}`);
    }

    return {
      source: "env_json",
      serviceAccount: normalizeServiceAccount(parsed),
      credentialsPath: null,
    };
  }

  const credentialsPathRaw = asNonEmptyString(env.GOOGLE_APPLICATION_CREDENTIALS);
  if (!credentialsPathRaw) {
    throw new Error(
      "Missing Firebase credentials: set FIREBASE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS"
    );
  }

  const credentialsPath = path.resolve(credentialsPathRaw);
  if (!fs.existsSync(credentialsPath)) {
    throw new Error(`Invalid GOOGLE_APPLICATION_CREDENTIALS: file does not exist (${credentialsPath})`);
  }

  let parsed;
  try {
    const fileContent = fs.readFileSync(credentialsPath, "utf8");
    parsed = JSON.parse(fileContent);
  } catch (error) {
    throw new Error(`Invalid Firebase credentials file (${credentialsPath}): ${error.message}`);
  }

  return {
    source: "file",
    serviceAccount: normalizeServiceAccount(parsed),
    credentialsPath,
  };
}

function buildConfig(env = process.env) {
  const nodeEnv = asNonEmptyString(env.NODE_ENV) || "development";
  if (!ALLOWED_NODE_ENVS.has(nodeEnv)) {
    throw new Error(`Invalid NODE_ENV: ${nodeEnv}. Allowed: development, test, production`);
  }

  const port = parsePort(asNonEmptyString(env.PORT) || "8080");

  const apifyToken = asNonEmptyString(env.APIFY_TOKEN);
  if (!apifyToken) {
    throw new Error("Missing APIFY_TOKEN environment variable");
  }

  const firebase = resolveFirebaseCredentials(env);
  const idempotencyCollection = asNonEmptyString(env.IDEMPOTENCY_COLLECTION) || "idempotency_keys";
  const eventCatalogCollection = asNonEmptyString(env.EVENT_CATALOG_COLLECTION) || "event_catalog";
  const conversationStateCollection =
    asNonEmptyString(env.CONVERSATION_STATE_COLLECTION) || "conversation_state";
  const interactionLogCollection = asNonEmptyString(env.INTERACTION_LOG_COLLECTION) || "interaction_logs";
  const intentLlm = resolveIntentLlmConfig(env);

  const telegramToken = asNonEmptyString(env.TELEGRAM_TOKEN);
  const telegramWebhookSecret = asNonEmptyString(env.TELEGRAM_WEBHOOK_SECRET);
  const telegramWebhookPathSecret = parseWebhookPathSecret(env.TELEGRAM_WEBHOOK_PATH_SECRET);
  const telegramWebhookPath = telegramWebhookPathSecret
    ? `/webhooks/telegram/${telegramWebhookPathSecret}`
    : "/webhooks/telegram";
  const telegramEnabled = Boolean(telegramToken);

  return {
    nodeEnv,
    port,
    apifyToken,
    firebase,
    idempotencyCollection,
    eventCatalogCollection,
    conversationStateCollection,
    interactionLogCollection,
    intentLlm,
    telegram: {
      enabled: telegramEnabled,
      token: telegramToken,
      webhookSecret: telegramWebhookSecret,
      webhookPathSecret: telegramWebhookPathSecret,
      webhookPath: telegramWebhookPath,
    },
  };
}

function getConfig() {
  if (!cachedConfig) {
    cachedConfig = buildConfig(process.env);
  }
  return cachedConfig;
}

function resetConfigForTests() {
  cachedConfig = null;
}

module.exports = {
  buildConfig,
  getConfig,
  resetConfigForTests,
};
