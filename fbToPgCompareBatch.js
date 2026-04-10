'use strict';

const Firebird = require('node-firebird');
const { getFirebirdPool } = require('./dbFbPoolManager');
const { executeQueryStream } = require('./executeQueryStreamFbBatch');
const { tbToimport } = require('./fbToPgTables');

// Helper functions
function formatDateForFirebird(date) {
    const pad = (n, size = 2) => {
        let s = n.toString();
        while (s.length < size) s = '0' + s;
        return s;
    };
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`;
}

// Simple writer untuk NDJSON array 2D
function createNDJSONWriter(res) {
    return {
        // Fungsi untuk menulis array ke client
        async writeArray(dataArray) {
            if (res.aborted) {
                throw new Error('Client disconnected');
            }
            
            const ndjsonLine = JSON.stringify(dataArray) + '\n';
            
            return new Promise((resolve, reject) => {
                // PASTIKAN SEMUA WRITE DALAM cork()
                res.cork(() => {
                    const ok = res.write(ndjsonLine);
                    if (ok) {
                        resolve();
                    } else {
                        // Buffer penuh, tunggu sampai writable
                        const writableHandler = () => {
                            res.onWritable(null); // Hapus handler
                            res.cork(() => {
                                const ok = res.write(ndjsonLine);
                                if (ok) {
                                    resolve();
                                } else {
                                    reject(new Error('Failed to write after waiting'));
                                }
                            });
                            return false; // Hanya handle sekali
                        };
                        
                        res.onWritable(writableHandler);
                        
                        // Timeout
                        setTimeout(() => {
                            res.onWritable(null);
                            reject(new Error('Write timeout after 10s'));
                        }, 10000);
                    }
                });
            });
        }
        
        // Catatan: end() method dihapus - penutupan response ditangani oleh fungsi utama
    };
}

// Class untuk memproses batch dari Firebird
class FirebirdDataProcessor {
    constructor(ndjsonWriter, schemaName, tbName, pkColumns, pgClient) {
        this.ndjsonWriter = ndjsonWriter;
        this.schemaName = schemaName;
        this.tbName = tbName;
        this.pkColumns = pkColumns;
        this.pgClient = pgClient;
        
        this.rowCount = 0;
        this.header = null;
        this.pkIndices = null;
        this.fbBeginSent = false;
        this.pgBeginSent = false;
        this.batchNumber = 0;
    }

    getPkValuesFromBatch(batchArray) {
        if (!this.pkIndices || !batchArray.length) {
            return [];
        }

        const pkValues = [];
        for (const row of batchArray) {
            const pkVals = this.pkIndices.map(i => row[i]);
            pkValues.push(pkVals);
        }
        return pkValues;
    }

    async processBatch(batchArray, header, headertype, isLast) {
        this.batchNumber++;
        console.log(`   Processing batch ${this.batchNumber} (${batchArray.length} rows)`);
        
        const messages = [];
        
        // 1. BEGIN_SET dan HEADER FB (hanya batch pertama)
        if (!this.fbBeginSent) {
            this.header = header;
            
            // Calculate PK indices
            if (this.pkColumns && Array.isArray(this.pkColumns) && this.pkColumns.length > 0) {
                this.pkIndices = this.pkColumns.map(col => {
                    const colUpper = col.toUpperCase();
                    const idx = this.header.indexOf(colUpper);
                    if (idx === -1) {
                        throw new Error(`PK column "${colUpper}" not found in table ${this.tbName}`);
                    }
                    return idx;
                });
            }
            
            messages.push(["__BEGIN_SET__", "FB", this.tbName, this.pkIndices]);
            messages.push(["__HEADER__", "FB", this.tbName, ...header]);
            this.fbBeginSent = true;
        }
        
        // 2. Kirim data FB
        messages.push(["__DATA__", "FB", this.tbName, batchArray]);
        this.rowCount += batchArray.length;
        
        // 3. BEGIN_SET dan HEADER PG (hanya batch pertama yang ada PG data)
        if (!this.pgBeginSent) {
            messages.push(["__BEGIN_SET__", "PG", this.tbName]);
            messages.push(["__HEADER__", "PG", this.tbName, ...this.header]);
            this.pgBeginSent = true;
        }
        
        // 4. Query PostgreSQL untuk PK batch ini
        const pkValues = this.getPkValuesFromBatch(batchArray);
        if (pkValues.length > 0) {
            //console.log('JUMLAH PK VALUES ADALAH SEBAGAI BERIKUT :',pkValues.length)
            const pgRows = await this.queryPostgresBatch(pkValues);
            messages.push(["__DATA__", "PG", this.tbName, pgRows]);
        } else {
            // Kirim empty array jika tidak ada PK
            messages.push(["__DATA__", "PG", this.tbName, []]);
        }
        
        // 5. Jika ini batch terakhir FB
        if (isLast) {
            messages.push(["__END_SET__", "FB", this.tbName]);
            messages.push(["__END_SET__", "PG", this.tbName]);
            console.log(`   ✅ Table ${this.tbName} complete: ${this.rowCount} rows in ${this.batchNumber} batches`);
        }
        
        // 6. Kirim semua messages
        await this.ndjsonWriter.writeArray(messages);
        
        return {
            rowCount: this.rowCount,
            batchCount: this.batchNumber
        };
    }
    
    async queryPostgresBatch(pkValues) {
        try {
            console.log(`   🔍 Querying PG for ${pkValues.length} PK values...`);
            await this.pgClient.query({
                text: 'CALL accurate.compare_getdata_by_pkvalues($1, $2, $3)',
                values: [
                    this.schemaName, 
                    this.tbName, 
                    JSON.stringify([this.pkColumns,...pkValues])
                ]
            });
            const pgRows = [];
            let hasMore = true;
            let i =0;
            while (hasMore) {
                const result = await this.pgClient.query({
                    text: `FETCH 1000 FROM compare_cur`,
                    rowMode: "array"
                });
                pgRows.push(...result.rows);
                if (result.rows.length < 1000) {
                    hasMore = false;
                }
                i++;
            }
            console.log('CLOSE compare_cur')
            await this.pgClient.query('CLOSE compare_cur');
            
            console.log(`   📤 Got ${pgRows.length} rows from PG`);
            return pgRows;
            
        } catch (error) {
            try {
                await this.pgClient.query('CLOSE compare_cur');
            } catch (_) {}
            
            if (error.message.includes('too many')) {
                console.log(`   ⚠️ Batch too large (${pkValues.length}), splitting...`);
                
                // Split menjadi 2
                const mid = Math.floor(pkValues.length / 2);
                const batch1 = pkValues.slice(0, mid);
                const batch2 = pkValues.slice(mid);
                
                const results1 = await this.queryPostgresBatch(batch1);
                const results2 = await this.queryPostgresBatch(batch2);
                
                return [...results1, ...results2];
            }
            
            throw error;
        }
    }
}

// Fungsi utama untuk proses compare per tabel
async function processTableCompare(ndjsonWriter, pgClient, schemaName, tbName, lastSync, pkColumns, signal) {
    console.log(`\n=== Processing: ${tbName} ===`);
    
    if (!lastSync) {
        console.log(`   ⏭️ Skipping - no lastSync available`);
        //await ndjsonWriter.writeArray(["__TABLE_SKIP__", tbName, "no lastSync"]);
        return { tableName: tbName, skipped: true, reason: "no lastSync" };
    }
    
    if (!pkColumns || pkColumns.length === 0) {
        console.log(`   ⚠️ Skipping - no PK columns defined`);
        //await ndjsonWriter.writeArray(["__TABLE_SKIP__", tbName, "no PK columns"]);
        return { tableName: tbName, skipped: true, reason: "no PK columns" };
    }
    
    const fbPool = getFirebirdPool(schemaName);
    const fbDb = await new Promise((resolve, reject) => {
        fbPool.get((err, db) => err ? reject(err) : resolve(db));
    });

    try {
        const formattedLastSync = formatDateForFirebird(lastSync);
        //const sql = `SELECT * FROM ${tbName} WHERE updated_at > '${formattedLastSync}'`;
        const sql = `SELECT * FROM ${tbName}`
        console.log(`📋 FB Query: ${sql}`);
        
        // Kirim TABLE_START ke client
        //await ndjsonWriter.writeArray(["__TABLE_START__", tbName]);
        
        // Buat processor
        const fbProcessor = new FirebirdDataProcessor(
            ndjsonWriter,
            schemaName,
            tbName,
            pkColumns,
            pgClient
        );
        
        await new Promise((resolve, reject) => {
            fbDb.transaction(Firebird.ISOLATION_READ_COMMITTED_RECORD_VERSION, async (err, trx) => {
                if (err) {
                    fbDb.detach();
                    return reject(err);
                }

                try {
                    // Execute stream
                    await executeQueryStream(
                        trx,
                        sql,
                        [],
                        500,
                        async (batchArray, header, headertype, isLast) => {
                            if (signal && signal.aborted) {
                                throw new Error('Operation aborted');
                            }
                            
                            console.log(`   📥 Received batch of ${batchArray.length} rows from Firebird`);
                            
                            // Proses dan kirim data ke client
                            const processResult = await fbProcessor.processBatch(batchArray, header, headertype, isLast);
                            console.log(`   📥 SELESAI PROSES batch of ${batchArray.length} rows DI POSTGRESQL`);
                            
                        },
                        {
                            signal: signal,
                            MAX_QUEUE_SIZE: 1,
                            MAX_CONCURRENT_PROCESSING: 1
                        }
                    );

                    await new Promise((commitResolve, commitReject) => {
                        trx.commit((err) => {
                            if (err) {
                                commitReject(err);
                            } else {
                                commitResolve();
                            }
                        });
                    });

                    fbDb.detach();
                    
                    
                    resolve();
                } catch (error) {
                    try {
                        await new Promise((rollbackResolve) => {
                            trx.rollback(() => {
                                rollbackResolve();
                            });
                        });
                    } catch (rollbackErr) {
                        console.error('Rollback error:', rollbackErr);
                    }

                    fbDb.detach();
                    reject(error);
                }
            });
        });

        
        return {
            tableName: tbName,
            skipped: false
        };

    } catch (error) {
        try {
            fbDb.detach();
        } catch (_) {}
        
        // Kirim error ke client
        //await ndjsonWriter.writeArray(["__ERROR__", tbName, error.message]);
        
        throw error;
    }
}

// Fungsi utama untuk compare semua tabel
async function compareDataPgFromFb(pgClient, schemaName, signal, options = {}) {
    const { 
        ndjsonWriter,
        sendProgress = null
    } = options;

    if (!ndjsonWriter) {
        throw new Error('ndjsonWriter is required');
    }

    function getLastSync(data, tableName) {
        const row = data.find(r => r[0] === tableName);
        return row ? row[1] : null;
    }

    function getPkColumns(data, tableName) {
        const row = data.find(r => r[0] === tableName);
        return row ? row[2] : null;
    }

    const results = [];
    let lastSuccessfulTable = null;
    const startTime = Date.now();

    try {
        // Kirim START_COMPARE ke client
        await ndjsonWriter.writeArray(["__START_COMPARE__", schemaName, tbToimport.length]);
        console.log(`📤 Sent START_COMPARE to client`);

        // Ambil last sync info
        const resTbLast = await pgClient.query({
            text: 'SELECT * FROM accurate.compare_get_lastupdated($1)',
            values: [schemaName],
            rowMode: 'array'
        });
        const tbLastSync = resTbLast.rows;

        console.log(`\n🔍 Starting compare for ${tbToimport.length} tables`);

        // Proses tabel
        for (let i = 0; i < tbToimport.length; i++) {
            const tableName = tbToimport[i];
            
            if (signal && signal.aborted) {
                console.log('Compare operation aborted by signal');
                await ndjsonWriter.writeArray(["__ABORTED__", "Signal aborted"]);
                break;
            }

            console.log(`\n[${i + 1}/${tbToimport.length}] ${tableName}`);

            try {
                const tableStartTime = Date.now();

                if (sendProgress) {
                    sendProgress({
                        type: 'progress',
                        table: tableName,
                        index: i + 1,
                        total: tbToimport.length,
                        status: 'processing'
                    });
                }

                // Proses tabel
                const tableResult = await processTableCompare(
                    ndjsonWriter,
                    pgClient,
                    schemaName,
                    tableName,
                    getLastSync(tbLastSync, tableName),
                    getPkColumns(tbLastSync, tableName),
                    signal
                );

                const tableTime = Date.now() - tableStartTime;
                results.push({
                    ...tableResult,
                    processingTime: tableTime
                });

                lastSuccessfulTable = tableName;

                console.log(`   ✓ Completed in ${(tableTime / 1000).toFixed(2)}s`);

            } catch (error) {
                const totalTime = (Date.now() - startTime) / 1000;
                console.error(`\n❌ Compare stopped at table ${tableName}:`, error.message);
                
                throw error;
            }
        }

        const totalTime = Date.now() - startTime;
        const tablesProcessed = results.filter(r => !r.skipped).length;
        const tablesWithChanges = results.filter(r => r.hasChanges && !r.skipped).length;
        const totalFbRows = results.reduce((sum, r) => sum + (r.rowCount || 0), 0);
        const totalPgRows = results.reduce((sum, r) => sum + (r.totalPgRows || 0), 0);

        // Kirim SUMMARY ke client
        await ndjsonWriter.writeArray([
            "__SUMMARY__", 
            results.length,
            tablesProcessed,
            tablesWithChanges,
            totalFbRows,
            totalPgRows,
            Math.round(totalTime / 1000)
        ]);
        console.log(`📤 Sent SUMMARY to client`);

        console.log(`
✅ Compare completed!
├─ Total tables: ${results.length}
├─ Tables processed: ${tablesProcessed}
├─ Tables with changes: ${tablesWithChanges}
├─ Total FB rows: ${totalFbRows.toLocaleString()}
├─ Total PG rows: ${totalPgRows.toLocaleString()}
├─ Total time: ${(totalTime / 1000).toFixed(2)}s
└─ Memory: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)}MB
        `);

        // Kirim COMPLETE ke client
        await ndjsonWriter.writeArray(["__COMPLETE__"]);
        console.log(`📤 Sent COMPLETE to client`);

        return {
            success: true,
            results: results,
            summary: {
                totalTables: results.length,
                tablesProcessed: tablesProcessed,
                tablesWithChanges: tablesWithChanges,
                totalFbRows: totalFbRows,
                totalPgRows: totalPgRows,
                totalTime: totalTime / 1000
            }
        };

    } catch (error) {
        const totalTime = Date.now() - startTime;
        
        console.error(`
❌ Compare failed!
├─ Last successful table: ${lastSuccessfulTable || 'None'}
├─ Processed: ${results.length}/${tbToimport.length} tables
├─ Total time: ${(totalTime / 1000).toFixed(2)}s
└─ Error: ${error.message}
        `);

        // Kirim FINAL_ERROR ke client
        await ndjsonWriter.writeArray([
            "__FINAL_ERROR__", 
            error.message, 
            lastSuccessfulTable || "None",
            results.length,
            Math.round(totalTime / 1000)
        ]);

        throw {
            success: false,
            error: error.message,
            lastSuccessfulTable: lastSuccessfulTable,
            processedTables: results,
            processedCount: results.length,
            totalTables: tbToimport.length,
            totalTime: totalTime / 1000
        };
    }
}

// Handler untuk uWebSockets.js - VERSI SEDERHANA
async function handleComparePostRequest(res, pgClient, schemaName, signal) {
    // Setup headers
    res.writeHeader('Content-Type', 'application/x-ndjson');
    res.writeHeader('Transfer-Encoding', 'chunked');
    
    console.log(`📥 Compare request for schema: ${schemaName}`);
    
    // Buat NDJSON writer
    const ndjsonWriter = createNDJSONWriter(res);
    
    // Return promise dari compare process
    // Semua error akan di-throw ke caller (fungsi utama)
    return compareDataPgFromFb(pgClient, schemaName, signal, {
        ndjsonWriter: ndjsonWriter
    });
}

// Export functions
module.exports = { 
    compareDataPgFromFb,
    processTableCompare,
    handleComparePostRequest,
    createNDJSONWriter,
    FirebirdDataProcessor
};