const CryptoJS = require('crypto-js');

function decryptData(encryptedBase64, password) {
  try {
    const key = CryptoJS.SHA256(password);
    const decrypted = CryptoJS.AES.decrypt(encryptedBase64, key, {
      mode: CryptoJS.mode.ECB,
      padding: CryptoJS.pad.Pkcs7,
    });

    const decryptedBase64 = decrypted.toString(CryptoJS.enc.Utf8);

    if (!decryptedBase64) {
      throw new Error("Decryption failed or empty result");
    }

    // Decode base64 untuk dapatkan plaintext asli
    const plaintext = Buffer.from(decryptedBase64, 'base64').toString('utf8');

    return plaintext;
  } catch (error) {
    console.error("Decryption error:", error);
    throw error;
  }
}

module.exports = {
    decryptData
}