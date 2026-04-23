const Firebird = require('node-firebird');
const { getFirebirdPool } = require('./dbFbPoolManager');
const { callProcedurePg } = require("./dbPg");
const { pgPool } = require('./configPg');

// Ambil DDL dari PostgreSQL (callback)
function getDdlFromPostgres(procName, callback) {
  callProcedurePg('accurate.getDdlSpFirebird', ['6def3da0-ec48-4c49-8f40-be96201367f4', procName])
    .then(result => {
      //dari sp pasti ada result jika tidak ada langsung di raise exeption
      callback(null, result[0][1][0], result[0][1][1]);
    })
    .catch(err => {
      console.error('getDdlFromPostgres error:', err);
      return callback(err);
    });
}
// Cek apakah procedure ada di Firebird
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

// Buat procedure di Firebird dari DDL
function createProcedure(db, procname, ddl, version, callback) {
  db.query(ddl, (err) => {
    if (err) return callback(err);
    if (version) {
      const sqlcomment = `COMMENT ON PROCEDURE ${procname} IS '${version}'`;
      console.log(sqlcomment, "sqlcomment")
      db.query(sqlcomment, (err) => {
        if (err) return callback(err);
        callback(null);
      });
    } else callback(null);
  });
}
/**
 * Menjalankan stored procedure Firebird dengan dukungan streaming.
 * 
 * @param {string} dbName - Nama database config
 * @param {string} procName - Nama stored procedure
 * @param {array} params - Parameter prosedur
 * @param {boolean} hasResult - Apakah prosedur mengembalikan result set
 * @param {object|null} res - (Opsional) uWebSockets.js response object, untuk streaming
 * @param {function|null} callback - Callback jika tidak pakai streaming
 */
function callDbProcedure(dbName, procName, params = [], hasResult = false, res) {
  if (!/^[A-Z0-9_]+$/i.test(procName)) {
    if (!res.aborted) {
      res.cork(() => {
        res.end(JSON.stringify(["__ERROR__", "Invalid procedure name : " + procName]) + "\n");
      });
    }
    return;
  }
  const fbPool = getFirebirdPool(dbName);
  fbPool.get((err, db) => {
    if (err) {
      if (!res.aborted) {
        res.cork(() => {
          res.end(JSON.stringify(["__ERROR__", "Database connection failed: " + err.message]) + "\n");
        });
      }
      return;
    }

    if (res.aborted) {
      db.detach();
      return;
    }

    if (fbPool.isdev === "Y") {
      isProcedureExists(db, procName, (err, exists) => {
        if (err) {
          if (!res.aborted) {
            res.cork(() => {
              res.end(JSON.stringify(["__ERROR__", "Failed to check procedure: " + err.message]) + "\n");
            });
          }
          db.detach();
          return;
        }

        if (exists) {
          executeProcedure();
        } else {
          getDdlFromPostgres(procName, (err, ddl, version) => {
            if (err) {
              if (!res.aborted) {
                res.cork(() => {
                  res.end(JSON.stringify(["__ERROR__", "getDdlFromPostgres error: " + err.message]) + "\n");
                });
              }
              db.detach();
              return;
            }
            createProcedure(db, procName, ddl, version, (err) => {
              if (err) {
                if (!res.aborted) {
                  res.cork(() => {
                    res.end(JSON.stringify(["__ERROR__", "Failed to create procedure: " + err.message]) + "\n");
                  });
                }
                db.detach();
                return;
              }
              executeProcedure();
            });
          });
        }
      });
    } else {
      executeProcedure();
    }

    function executeProcedure() {
      if (res.aborted) {
        db.detach();
        return;
      }

      if (!res.aborted) {
        res.cork(() => {
          res.writeHeader("Content-Type", "application/x-ndjson");
        });
      }
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

      db.sequentially(sql, params, (row, index) => {
        if (!headerAdded) {
          headers = Object.keys(row);
          headerAdded = true;
          chunks.push(JSON.stringify(["__BEGIN_SET__", "FB", procName]));
          chunks.push(JSON.stringify(["__HEADER__", ...headers]));
        }

        const rowArray = headers.map(col => row[col]);
        chunks.push(JSON.stringify(rowArray));

        if (chunks.length > 200) {
          safeWrite(() => {
            res.write(chunks.join("\n") + "\n");
          });
          chunks = [];
        }
      }, (err) => {
        if (chunks.length > 0) {
          safeWrite(() => {
            res.write(chunks.join("\n") + "\n");
          });
        }

        if (err) {
          safeWrite(() => {
            res.write(JSON.stringify(["__ERROR__", err.message]) + "\n");
            res.end();
          });
        } else {
          safeWrite(() => {
            res.write(JSON.stringify(["__END_SET__"]) + "\n");
            res.end();
          });
        }

        db.detach();
      });
    }
  });
}

async function FbThenPg(params) {
  const fbDbName = params[2];
  const prefix = "FbThenPg";
  let fbProcName = params[3];
  if (params[3].startsWith(prefix)) {
    fbProcName = params[3].slice(prefix.length);
  }
  const pgProcName = params[5];
  const spparams = params.slice(6);
  const fbPool = getFirebirdPool(fbDbName);
  let pgClient = null;

  return new Promise((resolve, reject) => {
    fbPool.get((err, db) => {
      if (err) return reject(new Error(`Koneksi FB Gagal: ${err.message}`));
      fbConn = db;

      if (fbPool.isdev === "Y")
        isProcedureExists(fbConn, fbProcName, (err, exists) => {
          if (err) {
            fbConn.detach();
            return reject(new Error(`Cek FB Proc is exist gagal : ${err.message}`));
          }

          if (exists) {
            executeProcedure();
          } else {
            getDdlFromPostgres(fbProcName, (err, ddl, version) => {
              if (err) {
                fbConn.detach();
                return reject(new Error(`getDdlFromPostgres error : ${err.message}`));
              }
              createProcedure(fbConn, fbProcName, ddl, version, (err) => {
                if (err) {
                  fbConn.detach();
                  return reject(new Error(`Failed to create procedure: ${err.message}`));
                }

                executeProcedure();
              });
            });
          }
        });
      else
        executeProcedure();

      function executeProcedure() {
        fbConn.transaction(Firebird.ISOLATION_READ_COMMITTED, async (err, fbTx) => {
          if (err) {
            fbConn.detach();
            return reject(err);
          }

          try {
            const sql = `SELECT * FROM ${fbProcName}(${spparams.map(() => '?').join(', ')})`;
            const fbResult = await new Promise((res, rej) => {
              fbTx.query(sql, spparams, (err, result) => {
                if (err) return rej(err);
                res(result ? result : null);
              });
            });

            if (!fbResult) {
              throw new Error('Data tidak ditemukan atau SP Firebird tidak mengembalikan hasil.');
            }
            spparams.pop(); //keluarkan parameter iduser accurate
            spparams.push(JSON.stringify(fbResult));
            spparams.unshift(params[0], params[1], params[2]);
            pgClient = await pgPool.connect();
            await pgClient.query('BEGIN');

            try {
              const pgSql = `SELECT * FROM ${pgProcName}(${spparams.map((_, i) => `$${i + 1}`).join(', ')})`;
              const pgRes = await pgClient.query({ text: pgSql, values: spparams, rowMode: "array" });

              // 6. COMMIT PostgreSQL Terlebih Dahulu
              await pgClient.query('COMMIT');

              // 7. COMMIT Firebird (Hanya jika PG berhasil)
              await new Promise((res, rej) => {
                fbTx.commit((err) => (err ? rej(err) : res()));
              });

              //console.log('✅ Transaksi Berhasil di kedua sisi.');
              const header = pgRes.fields.map(f => f.name);

              resolve({ header, rows: pgRes.rows });

            } catch (pgError) {
              // Jika PG gagal, Rollback PG
              await pgClient.query('ROLLBACK');
              throw pgError;
            }

          } catch (error) {
            // 8. Jika terjadi error apapun (FB atau PG), Rollback Firebird
            fbTx.rollback();
            console.error('❌ Transaksi Dibatalkan:', error.message);
            reject(error);
          } finally {
            // 9. Kembalikan koneksi ke masing-masing Pool
            if (fbConn) fbConn.detach();
            if (pgClient) pgClient.release();
          }
        });
      }
    });
  });
}

function initDbFirebird(params = [], res) {

  const dbName = params[2];
  const fbPool = getFirebirdPool(dbName);

  fbPool.get((err, db) => {
    if (err) {
      if (!res.aborted) {
        res.cork(() => {
          res.end(
            JSON.stringify(["__ERROR__", "Database connection failed: " + err.message]) + "\n"
          );
        });
      }
      return;
    }

    if (res.aborted) {
      db.detach();
      return;
    }

    if (!res.aborted) {
      res.cork(() => {
        res.writeHeader("Content-Type", "application/json");
      });
    }

    if (fbPool.isdev === "Y") {
      callProcedurePg('accurate.getDdlDbInitFirebird', ['6def3da0-ec48-4c49-8f40-be96201367f4', params[5]])
        .then(result => {

          const ddlList = result[0].slice(1).map(row => row[0]); // ambil semua SQL

          db.transaction((err, transaction) => {
            if (err) {
              db.detach();
              if (!res.aborted) {
                res.cork(() => {
                  res.end(JSON.stringify(["__ERROR__", err.message]));
                });
              }
              return;
            }

            const runQuery = (index) => {
              if (index >= ddlList.length) {
                // ✅ semua sukses → commit
                transaction.commit((err) => {
                  db.detach();

                  if (err) {
                    if (!res.aborted) {
                      res.cork(() => {
                        res.end(JSON.stringify(["__ERROR__", "Commit failed"]));
                      });
                    }
                    return;
                  }
                  if (!res.aborted) {
                    res.cork(() => {
                      res.end(JSON.stringify(["SUCCESS"]));
                    });
                  }
                  return;
                });
                return;
              }

              const sql = ddlList[index];

              transaction.query(sql, (err) => {
                if (err) {
                  console.error("DDL error:", sql, err);

                  // ❌ kalau gagal → rollback semua
                  return transaction.rollback(() => {
                    db.detach();
                    if (!res.aborted) {
                      res.cork(() => {
                        res.end(JSON.stringify([
                          "__ERROR__",
                          "DDL failed at index " + index + ": " + err.message
                        ]));
                      });
                    }
                  });
                }

                // lanjut ke query berikutnya
                runQuery(index + 1);
              });
            };

            runQuery(0);
          });

        })
        .catch(err => {
          console.error('getDdlDbInitFirebird error:', err);
          if (!res.aborted) {
            res.cork(() => {
              res.end(JSON.stringify(["__ERROR__", "getDdlDbInitFirebird error"]));
            });
          }
        });
    } else
      if (!res.aborted) {
        res.cork(() => {
          res.end(
            JSON.stringify(["__ERROR__", "Status database bukan development"])
          );
        });
      }
  });
}
// ===== EXPORTS =====
module.exports = {
  callDbProcedure,
  FbThenPg,
  initDbFirebird
};
