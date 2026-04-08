const { initializeApp, cert } = require('firebase-admin/app');
const { getFirestore } = require('firebase-admin/firestore');

const serviceAccount = require('./serviceAccountKey.json');

initializeApp({
  credential: cert(serviceAccount),
});

const db = getFirestore();

async function main() {
  await db.collection('users').doc('123456').set({
    nombre: 'Maxi',
    tier: 'free',
    intereses: ['musica', 'teatro'],
    interaction_count: 0,
    created_at: new Date(),
    last_seen: new Date(),
  });

  console.log('Escritura OK');
}

main().catch(console.error);