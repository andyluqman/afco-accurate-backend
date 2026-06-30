/**
 * dbPoolManager.js
 * Manajemen pool koneksi Firebird untuk multi-database.
 */

const Firebird = require('node-firebird');

// Cache pool per database
const poolCache = Object.create(null);

// Konfigurasi global
const MAX_CONNECTIONS_PER_DB = 5;
const IDLE_TIMEOUT_MS = 60 * 60 * 1000; // 30 menit
const CLEANUP_INTERVAL_MS = 10 * 60 * 1000; // 10 menit

/**
 * Ambil atau buat pool Firebird untuk database tertentu.
 * Pool akan disimpan dalam cache global agar tidak dibuat berulang.
 */
function getFirebirdPool(dbName) {
  const name = dbName.trim();

  if (poolCache[name]) {
    poolCache[name].lastUsed = Date.now();
    return poolCache[name].pool;
  }

  const { getDbConfig } = require('./configFb');
  const dbConfig = getDbConfig(name);
  if (!dbConfig) {
    throw new Error(`DbPoolFbManager : Konfigurasi DB ${name} tidak ditemukan`);
  }

  const pool = Firebird.pool(MAX_CONNECTIONS_PER_DB, dbConfig);
  pool.isdev = dbConfig.isdev;
  poolCache[name] = {
    pool,
    createdAt: Date.now(),
    lastUsed: Date.now(),
  };

  console.log(`✅ Pool Firebird dibuat untuk DB: ${name}`);
  return pool;
}

/**
 * Bersihkan pool yang idle terlalu lama.
 */
function cleanupIdlePools() {
  const now = Date.now();
  let cleaned = 0;

  for (const [name, info] of Object.entries(poolCache)) {
    if (now - info.lastUsed > IDLE_TIMEOUT_MS) {
      console.log(`🧹 Menutup pool Firebird idle: ${name}`);
      try {
        info.pool.destroy();
      } catch (e) {
        console.warn(`⚠️ Gagal menutup pool ${name}:`, e.message);
      }
      delete poolCache[name];
      cleaned++;
    }
  }

  if (cleaned > 0) {
    console.log(`🧾 ${cleaned} pool Firebird dibersihkan (idle > ${IDLE_TIMEOUT_MS / 60000} menit)`);
  }
}

/**
 * Bersihkan pool.
 */
function cleanupPools() {
  let cleaned = 0;

  for (const [name, info] of Object.entries(poolCache)) {
      console.log(`🧹 Konfigurasi baru, Menutup pool Firebird : ${name}`);
      try {
        info.pool.destroy();
      } catch (e) {
        console.warn(`⚠️ Gagal menutup pool ${name}:`, e.message);
      }
      delete poolCache[name];
      cleaned++;
    
  }

  if (cleaned > 0) {
    console.log(`🧾 ${cleaned} pool Firebird dibersihkan untuk konfigurasi baru.)`);
  }
}

/**
 * Cetak statistik pool yang aktif (opsional, untuk monitoring)
 */
function logPoolStats() {
  const entries = Object.entries(poolCache);
  if (entries.length === 0) {
    console.log('ℹ️ Tidak ada pool aktif saat ini.');
    return;
  }

  console.log('📊 Statistik Pool Firebird:');
  for (const [name, info] of entries) {
    console.log(`  - ${name}: dibuat ${timeAgo(info.createdAt)}, terakhir dipakai ${timeAgo(info.lastUsed)}`);
  }
}

/**
 * Utility untuk format waktu relatif
 */
function timeAgo(timestamp) {
  const diffSec = Math.floor((Date.now() - timestamp) / 1000);
  if (diffSec < 60) return `${diffSec}s lalu`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m lalu`;
  return `${Math.floor(diffSec / 3600)}h lalu`;
}

// Jalankan auto cleanup periodik
setInterval(() => {
  cleanupIdlePools();
  logPoolStats();
}, CLEANUP_INTERVAL_MS);

// Export fungsi utama
module.exports = {
  getFirebirdPool,
  logPoolStats,
  cleanupIdlePools,
  cleanupPools
};
