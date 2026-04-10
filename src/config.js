const fs = require("fs");
const path = require("path");
require("dotenv").config();

const ALLOWED_NODE_ENVS = new Set(["development", "test", "production"]);

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

  return {
    nodeEnv,
    port,
    apifyToken,
    firebase,
    idempotencyCollection,
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
