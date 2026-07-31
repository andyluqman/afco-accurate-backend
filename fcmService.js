require('dotenv').config();
const path = require('path');
const admin = require('firebase-admin');
const { pgPool } = require('./configPg');

let initialized = false;

function initFirebase() {
  if (initialized) return;

  const saJson = process.env.FCM_SERVICE_ACCOUNT_JSON;
  const saPath = process.env.FCM_SERVICE_ACCOUNT_PATH;

  let credential;
  if (saJson) {
    credential = admin.credential.cert(JSON.parse(saJson));
  } else if (saPath) {
    credential = admin.credential.cert(require(path.resolve(saPath)));
  } else {
    credential = admin.credential.applicationDefault();
  }
//console.log(credential)
  admin.initializeApp({ credential });
  initialized = true;
}

// Nama tabel & kolom token bisa diubah lewat .env
const TOKEN_TABLE = process.env.FCM_TOKEN_TABLE || 'user_device_tokens';
const TOKEN_USER_COL = process.env.FCM_TOKEN_USER_COL || 'user_id';
const TOKEN_COL = process.env.FCM_TOKEN_COL || 'fcm_token';

/**
 * Mengambil semua FCM token milik sebuah user dari tabel PostgreSQL.
 * @param {string|number} userId
 * @returns {Promise<string[]>}
 */
async function getTokensForUser(userId) {
  const res = await pgPool.query(
    `SELECT "${TOKEN_COL}" AS token FROM "${TOKEN_TABLE}" WHERE "${TOKEN_USER_COL}" = $1`,
    [userId]
  );
  return res.rows.map(r => r.token).filter(Boolean);
}

/**
 * Mengirim notifikasi FCM ke banyak token sekaligus.
 * Token yang sudah tidak valid (unregistered/invalid) akan otomatis dihapus
 * dari tabel agar tidak dikirim lagi di kemudian hari.
 *
 * @param {Object} opts
 * @param {string[]} opts.tokens - Daftar FCM token tujuan
 * @param {string} opts.title
 * @param {string} opts.body
 * @param {Object} [opts.data] - Payload data tambahan (harus string)
 * @returns {Promise<{success:number, failed:number}>}
 */
async function sendNotification({ tokens, title, body, data = {} }) {
  if (!tokens || tokens.length === 0) {
    return { success: 0, failed: 0 };
  }

  initFirebase();

  const message = {
    notification: { title, body },
    data,
    tokens,
  };

  const response = await admin.messaging().sendEachForMulticast(message);

  // Bersihkan token yang sudah tidak valid
  const invalid = [];
  response.responses.forEach((r, i) => {
    if (!r.success) {
      const code = r.error && r.error.code;
      if (
        code === 'messaging/registration-token-not-registered' ||
        code === 'messaging/invalid-registration-token'
      ) {
        invalid.push(tokens[i]);
      }
    }
  });

  if (invalid.length > 0) {
    await removeTokens(invalid);
  }

  return {
    success: response.successCount,
    failed: response.failureCount,
  };
}

/**
 * Menghapus token yang sudah tidak valid dari tabel.
 * @param {string[]} tokens
 */
async function removeTokens(tokens) {
  if (tokens.length === 0) return;
  const placeholders = tokens.map((_, i) => `$${i + 1}`).join(', ');
  try {
    await pgPool.query(
      `DELETE FROM "${TOKEN_TABLE}" WHERE "${TOKEN_COL}" IN (${placeholders})`,
      tokens
    );
  } catch (e) {
    console.error('Gagal menghapus FCM token tidak valid:', e.message);
  }
}

module.exports = {
  initFirebase,
  getTokensForUser,
  sendNotification,
  removeTokens,
};
