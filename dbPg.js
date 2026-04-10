const { pgPool } = require('./configPg');
const { activeResponses } = require("./connections");
const fbkey = process.env.FB_KEY;
/**
 * Fungsi untuk menulis baris JSON ke respons HTTP, dienkapsulasi dalam 'cork'
 * untuk kinerja I/O yang lebih baik (umum di lingkungan uWS/µWS).
 *
 * @param {Object} res - Objek respons HTTP (uWS/Express-like).
 * @param {Array|Object} data - Data untuk di-JSON.stringify.
 * @param {boolean} isError - Flag jika ini adalah pesan error.
 */
function writeNdjsonRow(res, data, isError = false) {
  if (isError) console.log('@@@@@@@@@@@@', data)
  const prefix = isError ? JSON.stringify(["__ERROR__", data.message || data]) : JSON.stringify(data);
  const jsonRow = prefix + "\n";
  // Gunakan res.cork() atau mekanisme penulisan non-blocking lain yang sesuai dengan framework Anda
  if (!res.aborted)
    res.cork(() => {
      res.write(jsonRow);
    });
}
async function safeCloseCursor(client, cursorName) {
  try {
    await client.query(`CLOSE "${cursorName}"`);
  } catch (e) {
    if (e.code !== '34000') throw e;
  }
}

/**
 * Memanggil stored function PostgreSQL yang mengembalikan SETOF record,
 * yang dapat mencakup refcursor (portal tak bernama), dan melakukan streaming
 * hasilnya sebagai NDJSON (Newline Delimited JSON) ke objek respons.
 *
 * @param {string} procName - Nama stored function (dipanggil via SELECT * FROM funcName(...)).
 * @param {Array} params - Parameter yang akan diteruskan ke stored function.
 * @param {Object} res - Objek respons HTTP untuk streaming NDJSON.
 */
async function callProcedurePgNdjson(procName, params, res, signal) {
  let client;
  activeResponses.add(res);
  if (procName === "accurate.settingdatabase_manage") params.push(fbkey);
  // Header NDJSON harus dikirim segera
  if (!res.aborted)
    res.cork(() => {
      res.writeHeader("Content-Type", "application/x-ndjson");
    });

  if (signal?.aborted) {
    activeResponses.delete(res);
    console.log("Client disconnected, aborting processes...");
  }

  try {
    client = await pgPool.connect();
    await client.query('BEGIN');

    const placeholders = params.map((_, i) => `$${i + 1}`).join(', ');
    const sqltext = `SELECT * FROM ${procName}(${placeholders})`;
    const mainRes = await client.query({ text: sqltext, values: params, rowMode: "array" });

    if (res.aborted) {
      await client.query('ROLLBACK');
      return;
    }

    const firstRow = mainRes.rows[0];
    const mainHeaders = mainRes.fields.map(f => f.name);

    let outvar = [];
    let outvarheadernoportal = [];
    let cursorNames = [];

    if (firstRow) {
      for (let i = 0; i < firstRow.length; i++) {
        let value = firstRow[i];
        if (typeof value === 'string' && value.includes('unnamed portal')) {
          cursorNames.push(value);
        } else {
          outvar.push(value);
          outvarheadernoportal.push(mainHeaders[i]);
        }
      }
    }

    // --- 1. Streaming data non-cursor (Output Variables) ---
    if (outvarheadernoportal.length > 0) {
      //if(!aborted) writeNdjsonRow(res, outvarheadernoportal);
      writeNdjsonRow(res, ["__BEGIN_SET__", "PG", 'OUTVAR']);
      writeNdjsonRow(res, ["__HEADER__", ...outvarheadernoportal]);
      writeNdjsonRow(res, outvar);
      // Penanda akhir untuk Output Variables Set
      writeNdjsonRow(res, ["__END_SET__"]);
    }

    // --- 2. Proses Setiap Refcursor (Streaming Data dari Cursor) ---

    for (const cursorName of cursorNames) {
      const batchSize = 1000;
      let hasMore = true;
      let isHeaderSent = false;
      let currentCursorHeaders = null;

      // Loop untuk FETCH berulang kali
      while (hasMore) {
        if (res.aborted) {
          await client.query('ROLLBACK');
          return;
        }

        const cursorRes = await client.query({
          text: `FETCH ${batchSize} FROM "${cursorName}"`,
          rowMode: "array"
        });

        if (!isHeaderSent) {
          // Ambil Header di kueri FETCH pertama (meskipun kosong)
          currentCursorHeaders = cursorRes.fields.map(f => f.name);
          //if(!aborted) writeNdjsonRow(res, currentCursorHeaders);
          writeNdjsonRow(res, ["__BEGIN_SET__", "PG", cursorName]);
          writeNdjsonRow(res, ["__HEADER__", ...currentCursorHeaders]);
          isHeaderSent = true;
        }

        if (cursorRes.rows.length === 0) {
          hasMore = false;
        } else {
          // Streaming setiap baris data
          for (const row of cursorRes.rows) {
            if (res.aborted) {
              await client.query('ROLLBACK');
              return;
            }
            writeNdjsonRow(res, row);
          }

          if (cursorRes.rows.length < batchSize) {
            hasMore = false;
          }
        }
      }

      // PENTING: Selalu kirim penanda __END_SET__ setelah setiap kursor.
      writeNdjsonRow(res, ["__END_SET__"]);

      await safeCloseCursor(client, cursorName);
    }

    // --- 3. Selesaikan Transaksi dan Respons ---
    await client.query('COMMIT');

  } catch (e) {
    console.log('catch log callProcedurePgNdjson pg client rollback', e)
    if (client) {
      try {
        await client.query('ROLLBACK');
      } catch (rbError) {
        console.error("Rollback failed:", rbError);
      }
    }
    if (!res.aborted) {
      writeNdjsonRow(res, e.message || e.error, true);
    }
    //throw e;
  } finally {
    if (client) {
      try {
        client.release();
      } catch (rlsError) {
        console.error("Release failed:", rlsError);
      }
    }

    if (!res.aborted) {
      activeResponses.delete(res);
      res.cork(() => {
        res.end();
      });
    }
  }
}

/**
 * Memanggil stored procedure PostgreSQL dengan dukungan multiple refcursor.
 * 
 * @param {string} procedureName - Nama stored procedure.
 * @param {Array} params - Parameter yang dikirim, termasuk nama-nama cursor.
 * @param {Array} refcursorNames - Array nama refcursor, misalnya ['cur1', 'cur2'].
 * 
 * @returns {Promise<Object>} - Object berisi setiap refcursor dalam bentuk array 2D.
 */
async function callProcedurePg2(procedureName, params = [], refcursorNames = []) {
  const output = {}; // Akan diisi { cur1: [...], cur2: [...] }

  try {
    await pgPool.query('BEGIN');

    // Gabungkan parameter dengan nama-nama cursor
    const callParams = [...params, ...refcursorNames];
    const placeholders = callParams.map((_, i) => `$${i + 1}`).join(', ');
    const callSQL = `CALL ${procedureName}(${placeholders})`;

    // Eksekusi stored procedure
    await pgPool.query(callSQL, callParams);

    for (const cursorName of refcursorNames) {
      const res = await pgPool.query(`FETCH ALL FROM ${cursorName}`);
      const rows = res.rows;

      if (rows.length > 0) {
        const headers = Object.keys(rows[0]);
        const data = [headers];
        for (const row of rows) {
          data.push(headers.map(h => row[h]));
        }
        output[cursorName] = data;
      } else {
        output[cursorName] = []; // Kosong
      }

      await pgPool.query(`CLOSE ${cursorName}`);
    }

    await pgPool.query('COMMIT');
    return output;

  } catch (err) {
    await pgPool.query('ROLLBACK');
    throw err;
  } finally {
    await pgPool.end();
  }
}

async function callProcedurePg(procName, params) {
  let hasil = []
  const placeholders = params.map((_, i) => `$${i + 1}`).join(', ');
  const sqltext = `SELECT * FROM ${procName}(${placeholders})`;
  const client = await pgPool.connect()
  try {
    await client.query('BEGIN')
    const res = await client.query({ text: sqltext, values: params, rowMode: "array" })
    let outvarheader = [];
    let outvarheadernoportal = [];
    for (let i in res.fields)
      outvarheader.push(res.fields[i].name)
    let outvar = []
    for (let i = 0; i < res.rows[0].length; i++) {
      let value = res.rows[0][i];

      if (typeof value !== 'string' || !value.includes('unnamed portal')) {
        outvar.push(value);
        outvarheadernoportal.push(outvarheader[i]);
      } else {
        outcursor = await client.query({ text: 'FETCH ALL from "' + value + '"', rowMode: "array" })
        outcurcols = []
        for (let i in outcursor.fields)
          outcurcols.push(outcursor.fields[i].name);
        curhasil = outcursor.rows;
        curhasil.unshift(outcurcols);
        hasil.push(curhasil);
      }
    }
    if (outvarheadernoportal.length !== 0)
      hasil.unshift([outvarheadernoportal, outvar]);
    await client.query('COMMIT')
    return hasil
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

// ===== EXPORTS =====
module.exports = {
  callProcedurePg,   // PostgreSQL
  callProcedurePgNdjson
};
