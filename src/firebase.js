const admin = require("firebase-admin");
const { getConfig } = require("./config");

function initFirebase() {
  if (admin.apps.length > 0) {
    return admin.app();
  }

  const serviceAccount = getConfig().firebase.serviceAccount;
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
