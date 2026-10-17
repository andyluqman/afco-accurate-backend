import Firebird from "node-firebird";
import { getFirebirdPool } from "./dbFbPoolManager.js"
import { callProcedurePg } from "./dbPg.js"
import { loadAllDbConfigs } from "./configFb.js";
import { pgPool } from "./configPg.js";

function getDbAsync(fbPool) {
    return new Promise((resolve, reject) => {
        fbPool.get((err, db) => {
            if (err) reject(err);
            else resolve(db);
        });
    });
}

function getTrxAsync(db) {
    const transaction_options = {
        isolation: Firebird.ISOLATION_READ_COMMITTED,
        readOnly: false,
        wait: false,
    };
    return new Promise((resolve, reject) => {
        db.transaction(
            transaction_options,
            (err, tx) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(tx);
                }
            }
        );
    });
}

function detachAsync(db) {
    return new Promise((resolve) => {
        db.detach(() => resolve());
    });
}

function commitAsync(trx) {
    return new Promise((resolve) => {
        trx.commit(() => resolve());
    });
}

function rollbackAsync(trx) {
    return new Promise((resolve) => {
        trx.rollback(() => resolve());
    });
}

export async function callFbSp(proc, sessionId, pPath, params, hasResult, res, signal) {
    let dbName;
    const is2D = Array.isArray(params[0]) && params.every(p => Array.isArray(p));
    const params2D = is2D ? params : [params];
    for (let idx = 0; idx < params2D.length; idx++) {
        if (!dbName)
            dbName = params2D[idx][0];
        else
            if (dbName !== params2D[idx][0])
                throw new Error(`Parameter database harus sama.`);
    }
    if (!dbName) throw new Error(`Database selection tidak boleh kosong.`)
    if (proc === "accurate.verify_actions_confirm" && dbName === "applydb") {
        const result = await callProcedurePg("accurate.verify_actions_confirm", [
            sessionId,
            pPath,
            params[0],
            params[1],
            params[2],
            params[3],
        ]);
        if (result?.[0]?.[0]?.[0] === "callsp") {
            if (result?.[0]?.[1]?.[0] === "loadAllDbConfigs") {
                const jml = await loadAllDbConfigs();
                console.log(jml + " konfigurasi db fb berhasil dimuat.");
                if (!signal.aborted)
                    res.cork(() => {
                        res.writeHeader("Content-Type", "application/json");
                        res.write(JSON.stringify(["success", jml + " Konfigurasi db fb berhasil dimuat."]));
                    });
            } else {
                if (!signal.aborted)
                    res.cork(() => {
                        res.writeHeader("Content-Type", "application/json");
                        res.write(JSON.stringify(["__ERROR__", "Procedure " + result?.[0]?.[1]?.[0] + " tidak ditemukan."]));
                    });
            }
        } else
            throw new Error(`Output callsp tidak ditemukan`);
        return;
    }

    const fbPool = getFirebirdPool(dbName);
    let db;
    let trx;
    try {
        db = await getDbAsync(fbPool);
        trx = await getTrxAsync(db);
        let isWriteHeader = false;
        for (let idx = 0; idx < params2D.length; idx++) {
            const myParams = params2D[idx];
            let result;
            if (proc === "accurate.verify_actions") {
                result = await callProcedurePg("accurate.verify_actions", [
                    sessionId,
                    pPath,
                    myParams[0],
                    myParams[1],
                    myParams[2],
                ]);
            } else if (proc === "accurate.verify_actions_confirm") {
                result = await callProcedurePg("accurate.verify_actions_confirm", [
                    sessionId,
                    pPath,
                    myParams[0],
                    myParams[1],
                    myParams[2],
                    myParams[3],
                ]);
                myParams.splice(3, 1); //hapus param password confirm
            } else
                throw new Error(`Procedure ${proc} tidak dalam list.`)
            if (!result) throw new Error(`Output procedure ${proc} tidak ditemukan`);
            if (result?.[0]?.[0]?.[0] === "callsp") {
                if (result?.[0]?.[1]?.[0] === "callDbProcedure") {
                    if (!isWriteHeader) {
                        res.cork(() => {
                            res.writeHeader("Content-Type", "application/x-ndjson");
                        });
                        isWriteHeader = true;
                    }
                    const dbFbName = myParams.shift(); //ambil dan hapus param dbname
                    const fbSp = myParams.shift(); //ambil dan hapus param nama store procedure
                    myParams.push(result[0][1][2]); // insertkan userid accurate
                    await executeProcedureAsync(trx, fbSp, myParams, hasResult, res, signal);
                } else if (result?.[0]?.[1]?.[0] === "initdbfirebird") {
                    await initDbFirebird(params, res, signal);
                } else if (result?.[0]?.[1]?.[0] === "syncdbfirebird") {
                    await syncDbFirebird(params, res, signal);
                } else if (result?.[0]?.[1]?.[0] === "FbThenPg") {
                    params.push(result[0][1][2]); // insertkan userid accurate
                    await FbThenPg(sessionId, pPath, params, res, signal);
                } else {
                    if (!signal.aborted)
                        res.cork(() => {
                            res.writeHeader("Content-Type", "application/json");
                            res.write(JSON.stringify(["__ERROR__", "Procedure " + result?.[0]?.[1]?.[0] + " tidak ditemukan."]));
                        });
                }
            } else
                throw new Error(`Output callsp tidak ditemukan`);
        }
        await commitAsync(trx);
    } catch (err) {
        if (trx) {
            try {
                await rollbackAsync(trx);
            } catch (_) { }
        }

        throw new Error(`${err.message} [CallFbSp]`, {
            cause: err
        });
    } finally {
        if (db) {
            await detachAsync(db);
        }
    }
}

async function executeProcedureAsync(
    trx,
    procName,
    params,
    hasResult,
    res,
    signal
) {
    if (signal.aborted) return;

    if (!/^[A-Z0-9_]+$/i.test(procName)) {
        throw new Error(`ExecuteProcedureAsync -> Invalid procedure name : ${procName}`);
    }

    function safeWrite(data) {
        try {
            if (!signal.aborted) {
                res.cork(() => {
                    res.write(data);
                });
            }
        } catch (_) { }
    }

    const sql = hasResult
        ? `SELECT * FROM ${procName}(${params.map(() => '?').join(', ')})`
        : `EXECUTE PROCEDURE ${procName}(${params.map(() => '?').join(', ')})`;

    let headerAdded = false;
    let headers = [];
    let chunks = [];

    try {
        await new Promise((resolve, reject) => {
            trx.sequentially(
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
                        safeWrite(chunks.join("\n") + "\n");
                        chunks = [];
                    }
                },
                (err) => {
                    if (err) {
                        const isLockError = err.message && (
                            err.message.includes("335544508") ||
                            err.message.includes("335544336") ||
                            err.message.toLowerCase().includes("lock") ||
                            err.message.toLowerCase().includes("deadlock") ||
                            err.message.toLowerCase().includes("update conflict")
                        );

                        if (isLockError) {
                            reject(new Error("Data sedang diedit user lain, silakan coba lagi nanti"));
                        } else {
                            reject(err);
                        }
                    } else
                        resolve();
                }
            );
        });

        if (chunks.length > 0) {
            safeWrite(chunks.join("\n") + "\n");
        }

        safeWrite(JSON.stringify(["__END_SET__"]) + "\n");
    } catch (err) {
        throw new Error(`ExecuteProcedureAsync -> ${err.message}`);
    }
}

async function initDbFirebird(params = [], res, signal) {
    const dbName = params[0];
    const fbPool = getFirebirdPool(dbName);

    let db;

    try {
        // promisify fbPool.get
        db = await new Promise((resolve, reject) => {
            fbPool.get((err, connection) => {
                if (err) return reject(err);
                resolve(connection);
            });
        });

        if (signal.aborted) {
            db.detach();
            return;
        }

        if (!signal.aborted) {
            res.cork(() => {
                res.writeHeader("Content-Type", "application/json");
            });
        }

        if (fbPool.isdev !== "Y") {
            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify([
                            "__ERROR__",
                            "Status database bukan development"
                        ]) + "\n"
                    );
                });
            }

            db.detach();
            return;
        }

        // ambil ddl
        const result = await callProcedurePg(
            'accurate.getDdlDbInitFirebird',
            ['6def3da0-ec48-4c49-8f40-be96201367f4', params[3]]
        );

        const ddlList = result[0].slice(1).map(row => row[0]);

        // promisify transaction
        const transaction = await new Promise((resolve, reject) => {
            db.transaction((err, trx) => {
                if (err) return reject(err);
                resolve(trx);
            });
        });

        try {

            // execute semua ddl satu per satu
            for (let i = 0; i < ddlList.length; i++) {
                const sql = ddlList[i];

                await new Promise((resolve, reject) => {
                    transaction.query(sql, (err) => {
                        if (err) {
                            console.error("DDL error:", sql, err);

                            return reject(
                                new Error(
                                    `DDL failed at index ${i}: ${err.message}`
                                )
                            );
                        }

                        resolve();
                    });
                });
            }

            // commit
            await new Promise((resolve, reject) => {
                transaction.commit((err) => {
                    if (err) return reject(err);
                    resolve();
                });
            });

            db.detach();

            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify(["SUCCESS"]) + "\n"
                    );
                });
            }

        } catch (err) {

            // rollback kalau ada error
            await new Promise((resolve) => {
                transaction.rollback(() => resolve());
            });

            db.detach();

            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify([
                            "__ERROR__",
                            err.message
                        ]) + "\n"
                    );
                });
            }
        }

    } catch (err) {

        if (db) {
            try {
                db.detach();
            } catch { }
        }

        console.error("initDbFirebird error:", err);

        if (!signal.aborted) {
            res.cork(() => {
                res.write(
                    JSON.stringify([
                        "__ERROR__",
                        err.message || "Unknown error"
                    ]) + "\n"
                );
            });
        }
    }
}

export async function FbThenPg(sessionid, pPath, params, res, signal) {
    console.log(params);
    const fbthenpg = params[1].split("|");
    const fbDbName = params[0];
    let fbProcName = fbthenpg[1];
    const pgProcName = fbthenpg[2];
    const spparams = params.slice(2);
    //console.log(spparams, "spparams");
    const fbPool = getFirebirdPool(fbDbName);
    let pgClient = null;

    return new Promise((resolve, reject) => {
        fbPool.get((err, db) => {
            if (err) return reject(new Error(`Koneksi FB Gagal: ${err.message}`));
            const fbConn = db;

            const transaction_options = {
                isolation: Firebird.ISOLATION_READ_COMMITTED,
                readOnly: false,
                wait: false,
            };

            fbConn.transaction(transaction_options, async (err, fbTx) => {
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
                    if (signal.aborted) {
                        throw new Error('Request aborted.');
                    }
                    spparams.pop(); //keluarkan parameter iduser accurate
                    spparams.push(JSON.stringify(fbResult));
                    spparams.unshift(sessionid, pPath);
                    pgClient = await pgPool.connect();
                    await pgClient.query('BEGIN');

                    try {
                        const pgSql = `SELECT * FROM "${params[0]}".${pgProcName}(${spparams.map((_, i) => `$${i + 1}`).join(', ')})`;
                        const pgRes = await pgClient.query({ text: pgSql, values: spparams, rowMode: "array" });

                        if (!signal.aborted) {
                            res.cork(() => {
                                res.writeHeader("Content-Type", "application/x-ndjson");
                            });
                        }

                        const mainHeaders = pgRes.fields.map(f => f.name);
                        let outvar = [];
                        let outvarheadernoportal = [];
                        let cursorNames = [];

                        if (pgRes.rows.length > 0) {
                            for (let i = 0; i < pgRes.rows[0].length; i++) {
                                let value = pgRes.rows[0][i];
                                if (typeof value !== 'string' || !value.includes('unnamed portal')) {
                                    outvar.push(value);
                                    outvarheadernoportal.push(mainHeaders[i]);
                                } else {
                                    cursorNames.push(value);
                                }
                            }
                        }

                        function writeNdjsonRow(data) {
                            if (signal.aborted) return;
                            try {
                                res.cork(() => {
                                    res.write(JSON.stringify(data) + "\n");
                                });
                            } catch (_) {}
                        }

                        if (outvarheadernoportal.length > 0) {
                            writeNdjsonRow(["__BEGIN_SET__", "PG", "OUTVAR"]);
                            writeNdjsonRow(["__HEADER__", ...outvarheadernoportal]);
                            writeNdjsonRow(outvar);
                            writeNdjsonRow(["__END_SET__"]);
                        }

                        for (const cursorName of cursorNames) {
                            const batchSize = 1000;
                            let hasMore = true;
                            let isHeaderSent = false;
                            let currentCursorHeaders = null;

                            while (hasMore) {
                                if (signal.aborted) {
                                    await pgClient.query('ROLLBACK');
                                    return;
                                }

                                const cursorRes = await pgClient.query({
                                    text: `FETCH ${batchSize} FROM "${cursorName}"`,
                                    rowMode: "array"
                                });

                                if (!isHeaderSent) {
                                    currentCursorHeaders = cursorRes.fields.map(f => f.name);
                                    writeNdjsonRow(["__BEGIN_SET__", "PG", cursorName]);
                                    writeNdjsonRow(["__HEADER__", ...currentCursorHeaders]);
                                    isHeaderSent = true;
                                }

                                if (cursorRes.rows.length === 0) {
                                    hasMore = false;
                                } else {
                                    for (const row of cursorRes.rows) {
                                        if (signal.aborted) {
                                            await pgClient.query('ROLLBACK');
                                            return;
                                        }
                                        writeNdjsonRow(row);
                                    }
                                    if (cursorRes.rows.length < batchSize) {
                                        hasMore = false;
                                    }
                                }
                            }
                            writeNdjsonRow(["__END_SET__"]);

                            try {
                                await pgClient.query(`CLOSE "${cursorName}"`);
                            } catch (e) {
                                if (e.code !== '34000') throw e;
                            }
                        }

                        await pgClient.query('COMMIT');

                        await new Promise((res, rej) => {
                            fbTx.commit((err) => (err ? rej(err) : res()));
                        });

                        resolve();

                    } catch (pgError) {
                        await pgClient.query('ROLLBACK');
                        throw pgError;
                    }

                } catch (error) {
                    fbTx.rollback();
                    console.error('❌ Transaksi Dibatalkan:', error.message);
                    reject(error);
                } finally {
                    if (fbConn) fbConn.detach();
                    if (pgClient) pgClient.release();
                }
            });
        });
    });
}

async function syncDbFirebird(params = [], res, signal) {
    const dbName = params[0];
    const fbPool = getFirebirdPool(dbName);

    let db;

    try {
        // promisify fbPool.get
        db = await new Promise((resolve, reject) => {
            fbPool.get((err, connection) => {
                if (err) return reject(err);
                resolve(connection);
            });
        });

        if (signal.aborted) {
            db.detach();
            return;
        }

        if (!signal.aborted) {
            res.cork(() => {
                res.writeHeader("Content-Type", "application/json");
            });
        }

        if (fbPool.isdev !== "Y") {
            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify([
                            "__ERROR__",
                            "Status database bukan development"
                        ]) + "\n"
                    );
                });
            }

            db.detach();
            return;
        }

        // ambil ddl
        let result;
        if (params[6]) {
            result = await callProcedurePg(
                'accurate.getDdl_cursor',
                ['6def3da0-ec48-4c49-8f40-be96201367f4', '/ddl-compare', params[3], params[4], params[5], params[6]]
            );
        } else {
            result = await callProcedurePg(
                'accurate.getDdl_cursor',
                ['6def3da0-ec48-4c49-8f40-be96201367f4', '/ddl-compare', params[3], params[4], params[5]]
            );
        }
        const ddlList = result[0].slice(1).map(row => row[0]);

        // promisify transaction
        const transaction = await new Promise((resolve, reject) => {
            db.transaction((err, trx) => {
                if (err) return reject(err);
                resolve(trx);
            });
        });

        try {

            // execute semua ddl satu per satu
            for (let i = 0; i < ddlList.length; i++) {
                const sql = ddlList[i];

                await new Promise((resolve, reject) => {
                    transaction.query(sql, (err) => {
                        if (err) {
                            console.error("DDL error:", sql, err);

                            return reject(
                                new Error(
                                    `DDL failed at index ${i}: ${err.message}`
                                )
                            );
                        }

                        resolve();
                    });
                });
            }

            // commit
            await new Promise((resolve, reject) => {
                transaction.commit((err) => {
                    if (err) return reject(err);
                    resolve();
                });
            });

            db.detach();

            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify(["SUCCESS"]) + "\n"
                    );
                });
            }

        } catch (err) {

            // rollback kalau ada error
            await new Promise((resolve) => {
                transaction.rollback(() => resolve());
            });

            db.detach();

            if (!signal.aborted) {
                res.cork(() => {
                    res.write(
                        JSON.stringify([
                            "__ERROR__",
                            err.message
                        ]) + "\n"
                    );
                });
            }
        }

    } catch (err) {

        if (db) {
            try {
                db.detach();
            } catch { }
        }

        console.error("initDbFirebird error:", err);

        if (!signal.aborted) {
            res.cork(() => {
                res.write(
                    JSON.stringify([
                        "__ERROR__",
                        err.message || "Unknown error"
                    ]) + "\n"
                );
            });
        }
    }
}
