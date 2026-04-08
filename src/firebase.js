const admin = require("firebase-admin");
const fs = require("fs");
const path = require("path");

function parseServiceAccountFromEnv() {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (raw && raw.trim()) {
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (error) {
      throw new Error(`Invalid FIREBASE_SERVICE_ACCOUNT_JSON: ${error.message}`);
    }

    if (parsed.private_key && typeof parsed.private_key === "string") {
      parsed.private_key = parsed.private_key.replace(/\\n/g, "\n");
    }

    return parsed;
  }

  const filePathRaw = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  const filePath = filePathRaw ? path.resolve(filePathRaw) : null;
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(
      "Missing Firebase credentials: set FIREBASE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS"
    );
  }

  const fileContent = fs.readFileSync(filePath, "utf8");
  let parsed;
  try {
    parsed = JSON.parse(fileContent);
  } catch (error) {
    throw new Error(`Invalid credentials JSON file (${filePath}): ${error.message}`);
  }

  if (parsed.private_key && typeof parsed.private_key === "string") {
    parsed.private_key = parsed.private_key.replace(/\\n/g, "\n");
  }

  return parsed;
}

function initFirebase() {
  if (admin.apps.length > 0) {
    return admin.app();
  }

  const serviceAccount = parseServiceAccountFromEnv();
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });

  return admin.app();
}

function getDb() {
  initFirebase();
  return admin.firestore();
}

module.exports = {
  admin,
  initFirebase,
  getDb,
};
