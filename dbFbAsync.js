import Firebird from "node-firebird";
import { getFirebirdPool } from "./dbFbPoolManager.js"
import { callProcedurePg } from "./dbPg.js"

function isProcedureExists(db, procName, callback) {
  const upperName = procName.toUpperCase();

  callProcedurePg('accurate.getddlspfb_version', ['6def3da0-ec48-4c49-8f40-be96201367f4', procName])
    .then(result => {
      const versionFromPg = String(result[0][1][0]);

      // Gunakan transaksi agar 'statement' handle terjaga di socket Debian
      db.transaction(Firebird.ISOLATION_READ_COMMITTED, (err, transaction) => {
        if (err) return callback(err);

        const sql = `
          SELECT 1 FROM RDB$PROCEDURES 
          WHERE RDB$PROCEDURE_NAME = ? 
          AND CAST(RDB$DESCRIPTION AS VARCHAR(255)) = ?
        `;

        transaction.query(sql, [upperName, versionFromPg], (err, rows) => {
          if (err) {
            transaction.rollback();
            return callback(err);
          }

          transaction.commit((commitErr) => {
            if (commitErr) return callback(commitErr);
            callback(null, Array.isArray(rows) && rows.length > 0);
          });
        });
      });
    })
    .catch(err => {
      callback(err);
    });
}

function getDbAsync(fbPool) {
  return new Promise((resolve, reject) => {
    fbPool.get((err, db) => {
      if (err) reject(err);
      else resolve(db);
    });
  });
}

function isProcedureExistsAsync(db, procName) {
  return new Promise((resolve, reject) => {
    isProcedureExists(db, procName, (err, exists) => {
      if (err) reject(err);
      else resolve(exists);
    });
  });
}

function getDdlFromPostgresAsync(procName) {
  return new Promise((resolve, reject) => {
    getDdlFromPostgres(procName, (err, ddl, version) => {
      if (err) reject(err);
      else resolve({ ddl, version });
    });
  });
}

function createProcedureAsync(db, procName, ddl, version) {
  return new Promise((resolve, reject) => {
    createProcedure(db, procName, ddl, version, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

function sequentiallyAsync(db, sql, params, onRow) {
  return new Promise((resolve, reject) => {
    db.sequentially(
      sql,
      params,
      (row, index) => {
        onRow(row, index);
      },
      (err) => {
        if (err) reject(err);
        else resolve();
      }
    );
  });
}

function detachAsync(db) {
  return new Promise((resolve) => {
    db.detach(() => resolve());
  });
}

export async function callDbProcedure(
  dbName,
  procName,
  params = [],
  hasResult = false,
  res
) {
  if (!/^[A-Z0-9_]+$/i.test(procName)) {
    if (!res.aborted) {
      res.cork(() => {
        res.write(
          JSON.stringify([
            "__ERROR__",
            "Invalid procedure name : " + procName
          ]) + "\n"
        );
      });
    }
    return;
  }

  const fbPool = getFirebirdPool(dbName);

  let db;

  try {
    db = await getDbAsync(fbPool);

    if (res.aborted) {
      await detachAsync(db);
      return;
    }

    // DEV MODE
    if (fbPool.isdev === "Y") {
      const exists = await isProcedureExistsAsync(db, procName);

      if (!exists) {
        const { ddl, version } =
          await getDdlFromPostgresAsync(procName);

        await createProcedureAsync(
          db,
          procName,
          ddl,
          version
        );
      }
    }

    await executeProcedureAsync(
      db,
      procName,
      params,
      hasResult,
      res
    );

  } catch (err) {
    if (!res.aborted) {
      res.cork(() => {
        res.write(
          JSON.stringify([
            "__ERROR__",
            err.message
          ]) + "\n"
        );
      });
    }
  } finally {
    if (db) {
      await detachAsync(db);
    }
  }
}

async function executeProcedureAsync(
  db,
  procName,
  params,
  hasResult,
  res
) {
  if (res.aborted) return;

  function safeWrite(fn) {
    if (!res.aborted) {
      res.cork(fn);
    }
  }

  const sql = hasResult
    ? `SELECT * FROM ${procName}(${params.map(() => '?').join(', ')})`
    : `EXECUTE PROCEDURE ${procName}(${params.map(() => '?').join(', ')})`;

  let headerAdded = false;
  let headers = [];
  let chunks = [];

  await sequentiallyAsync(
    db,
    sql,
    params,
    (row) => {
      if (!headerAdded) {
        headers = Object.keys(row);
        headerAdded = true;

        chunks.push(
          JSON.stringify([
            "__BEGIN_SET__",
            "FB",
            procName
          ])
        );

        chunks.push(
          JSON.stringify([
            "__HEADER__",
            ...headers
          ])
        );
      }

      const rowArray =
        headers.map(col => row[col]);

      chunks.push(JSON.stringify(rowArray));

      if (chunks.length > 200) {
        safeWrite(() => {
          res.write(
            chunks.join("\n") + "\n"
          );
        });

        chunks = [];
      }
    }
  );

  if (chunks.length > 0) {
    safeWrite(() => {
      res.write(
        chunks.join("\n") + "\n"
      );
    });
  }

  safeWrite(() => {
    res.write(
      JSON.stringify(["__END_SET__"]) + "\n"
    );
  });
}

