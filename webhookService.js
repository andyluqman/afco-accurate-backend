require('dotenv').config();
const crypto = require('crypto');

const SECRET = process.env.WEBHOOK_SECRET || 'my-secret-key';
const WEBHOOK_URL = process.env.WEBHOOK_URL || 'http://localhost:3000/wagateway/webhook/send';
const WEBHOOK_ID = process.env.WEBHOOK_ID || 'abc123';

/**
 * Mengirim notifikasi ke webhook eksternal dengan signature HMAC-SHA256.
 *
 * @param {Object} payload - Object yang akan dikirim sebagai JSON body.
 * @param {Object} [opts]
 * @param {string} [opts.url] - Override URL webhook.
 * @param {string} [opts.webhookId] - Override header x-webhook-id.
 * @param {string} [opts.secret] - Override secret untuk signature.
 * @param {number} [opts.timeout] - Timeout dalam ms (default 10000).
 * @returns {Promise<{status:number, ok:boolean, body:string}>}
 */
async function sendWebhook(payload, opts = {}) {
  const url = opts.url || WEBHOOK_URL;
  const webhookId = opts.webhookId || WEBHOOK_ID;
  const secret = opts.secret || SECRET;
  const timeout = opts.timeout || 10000;

  // Body harus persis sama dengan yang dipakai membuat signature
  const body = JSON.stringify(payload);

  const signature = crypto
    .createHmac('sha256', secret)
    .update(body)
    .digest('hex');

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-webhook-id': webhookId,
        'x-signature': signature,
      },
      body,
      signal: controller.signal,
    });

    const text = await res.text();
    return { status: res.status, ok: res.ok, body: text };
  } finally {
    clearTimeout(timer);
  }
}

module.exports = {
  sendWebhook,
};
