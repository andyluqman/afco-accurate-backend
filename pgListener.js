require('dotenv').config();
const { Client } = require('pg');

// Map channel -> { client, callbacks:Set, reconnectTimer }
const channels = new Map();

function createClient() {
  return new Client({
    host: process.env.PG_HOST,
    port: process.env.PG_PORT,
    user: process.env.PG_USER,
    password: process.env.PG_PASS,
    database: process.env.PG_DB,
  });
}

function safeParse(payload) {
  if (typeof payload !== 'string') return payload;
  try {
    return JSON.parse(payload);
  } catch {
    return payload;
  }
}

async function subscribe(channel) {
  const client = createClient();

  const entry = { client, callbacks: new Set(), reconnectTimer: null };
  channels.set(channel, entry);

  client.on('notification', (msg) => {
    if (msg.channel !== channel) return;
    const payload = safeParse(msg.payload);
    for (const cb of entry.callbacks) {
      try {
        cb(payload, msg);
      } catch (e) {
        console.error(`Error in pg LISTEN callback (${channel}):`, e);
      }
    }
  });

  client.on('error', (err) => {
    console.error(`pg LISTEN client error (${channel}):`, err.message);
  });

  client.on('end', () => {
    // Koneksi terputus, coba subscribe ulang kecuali sedang direstart
    if (!entry.reconnectTimer) {
      console.warn(`pg LISTEN connection ended (${channel}), reconnecting in 3s...`);
      entry.reconnectTimer = setTimeout(() => {
        entry.reconnectTimer = null;
        channels.delete(channel);
        listen(channel, ...entry.callbacks);
      }, 3000);
    }
  });

  await client.connect();
  await client.query(`LISTEN ${channel}`);
  console.log(`Listening to PostgreSQL channel: ${channel}`);
}

/**
 * Mendengarkan notifikasi PostgreSQL pada channel tertentu.
 *
 * @param {string} channel - Nama channel, misal 'new_order'
 * @param {Function} callback - Dipanggil dengan (payload, msg) saat notifikasi masuk
 * @returns {Function} - Fungsi untuk berhenti mendengarkan (unsubscribe)
 */
async function listen(channel, callback) {
  let entry = channels.get(channel);

  if (!entry) {
    await subscribe(channel);
    entry = channels.get(channel);
  }

  entry.callbacks.add(callback);

  return function unsubscribe() {
    const e = channels.get(channel);
    if (!e) return;
    e.callbacks.delete(callback);
    // Jika tidak ada listener lagi, tutup koneksi
    if (e.callbacks.size === 0) {
      if (e.reconnectTimer) clearTimeout(e.reconnectTimer);
      channels.delete(channel);
      e.client.end().catch(() => {});
    }
  };
}

async function closeAll() {
  for (const [channel, entry] of channels) {
    if (entry.reconnectTimer) clearTimeout(entry.reconnectTimer);
    channels.delete(channel);
    await entry.client.end().catch(() => {});
  }
}

module.exports = { listen, closeAll };
