'use strict';

const Firebird = require('node-firebird');
const { getFirebirdPool } = require('./dbFbPoolManager');
const { executeQueryStream } = require('./executeQueryStreamFbBatch');
const { tbToimport } = require('./fbToPgTables');

// Simple Semaphore untuk concurrency control
class Semaphore {
    constructor(max) {
        this.max = max;
        this.count = 0;
        this.waiting = [];
    }

    acquire() {
        return new Promise((resolve) => {
            if (this.count < this.max) {
                this.count++;
                resolve();
            } else {
                this.waiting.push(resolve);
            }
        });
    }

    release() {
        this.count--;
        if (this.waiting.length > 0) {
            const next = this.waiting.shift();
            this.count++;
            next();
        }
    }
}

// Helper functions
function formatDateForFirebird(date) {
    const pad = (n, size = 2) => {
        let s = n.toString();
        while (s.length < size) s = '0' + s;
        return s;
    };
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)}`;
}
// Smart processor dengan adaptive concurrency
class SmartBatchProcessor {
    constructor(pgClient, schemaName, tbName, writeLog) {
        this.pgClient = pgClient;
        this.schemaName = schemaName;
        this.tbName = tbName;
        this.writeLog = writeLog;

        this.rowCount = 0;
        this.batchCount = 0;
        this.header = null;
        this.startTime = Date.now();
        this.lastLogTime = Date.now();

        // Adaptive concurrency
        this.semaphore = new Semaphore(1); // Mulai dengan 1 concurrent
        this.processingTimes = [];
        this.concurrencyHistory = [];
        this.batchSizes = []; // Untuk tracking ukuran batch
    }

    log(type, data = {}) {
        if (this.writeLog) {
            this.writeLog({
                type,
                table: this.tbName,
                timestamp: new Date().toISOString(),
                ...data
            });
        }
    }

    async processBatch(batchArray, header, headertype, pkColumns, isLast) {
        // Acquire semaphore untuk limit concurrency
        await this.semaphore.acquire();

        const processStart = Date.now();
        const batchSize = batchArray.length; // Simpan ukuran batch

        try {
            // Execute PostgreSQL query
            await this.pgClient.query(
                'CALL accurate.database_sync_batch_arrays($1,$2,$3,$4,$5,$6,$7)',
                [this.schemaName, this.tbName, JSON.stringify(header), JSON.stringify(headertype),JSON.stringify(pkColumns), isLast, JSON.stringify(batchArray)]
            );

            // PERBAIKAN 1: Tambah dengan batch size, bukan 1
            this.rowCount += batchSize;
            this.batchCount++;
            this.batchSizes.push(batchSize);

            // Calculate processing time untuk BATCH ini
            const processTime = Date.now() - processStart;
            this.processingTimes.push(processTime);

            // Keep only last 100 samples
            if (this.processingTimes.length > 100) {
                this.processingTimes.shift();
            }
            if (this.batchSizes.length > 100) {
                this.batchSizes.shift();
            }

            // Adaptive concurrency adjustment - per 100 BARIS, bukan batch
            if (this.rowCount % 100 === 0) {
                this.adjustConcurrency();
            }

            // Progress logging - per 1000 BARIS, bukan batch
            const now = Date.now();
            if (this.rowCount % 1000 === 0 || now - this.lastLogTime > 5000) {
                const elapsed = (now - this.startTime) / 1000;
                const rowsPerSec = (this.rowCount / elapsed).toFixed(1);
                
                // PERBAIKAN 2: Hitung avg time per batch, bukan per row
                // Karena kita process batch, bukan individual rows
                const avgBatchTime = this.processingTimes.length > 0
                    ? (this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length).toFixed(1)
                    : 0;
                    
                // Hitung rata-rata ukuran batch
                const avgBatchSize = this.batchSizes.length > 0
                    ? Math.round(this.batchSizes.reduce((a, b) => a + b, 0) / this.batchSizes.length)
                    : batchSize;

                console.log(`  ${this.tbName}: ${this.rowCount.toLocaleString()} rows (${this.batchCount} batches)`);
                console.log(`    Speed: ${rowsPerSec} rows/sec | Batch: ${avgBatchTime}ms (avg ${avgBatchSize} rows/batch)`);

                this.log('progress', {
                    processed: this.rowCount,
                    batches: this.batchCount,
                    rowsPerSec,
                    avgBatchTime,
                    avgBatchSize,
                    concurrency: this.semaphore.max
                });

                this.lastLogTime = now;
            }

            return true;
        } catch (error) {
            // PERBAIKAN 3: Update error message
            console.error(`❌ Error processing batch #${this.batchCount} (${batchSize} rows) in ${this.tbName}:`, error.message);
            throw error;
        } finally {
            this.semaphore.release();
        }
    }

    adjustConcurrency() {
        if (this.processingTimes.length < 5) return; // Kurangi minimal sample

        const avgTime = this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length;
        const currentConcurrency = this.semaphore.max;

        // Adaptive algorithm - lebih konservatif
        if (avgTime < 100 && currentConcurrency < 3) { // Ubah threshold
            // Processing cepat, tingkatkan concurrency
            this.semaphore.max = Math.min(3, currentConcurrency + 1); // Maksimal 3
            console.log(`⬆️ ${this.tbName}: Increasing concurrency to ${this.semaphore.max} (avg batch time: ${avgTime.toFixed(1)}ms)`);
        } else if (avgTime > 500 && currentConcurrency > 1) { // Ubah threshold
            // Processing lambat, kurangi concurrency
            this.semaphore.max = Math.max(1, currentConcurrency - 1);
            console.log(`⬇️ ${this.tbName}: Decreasing concurrency to ${this.semaphore.max} (avg batch time: ${avgTime.toFixed(1)}ms)`);
        }

        this.concurrencyHistory.push({
            time: Date.now(),
            concurrency: this.semaphore.max,
            avgTime: avgTime
        });
    }

    getMetrics() {
        const totalTime = Date.now() - this.startTime;
        const avgBatchTime = this.processingTimes.length > 0
            ? this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length
            : 0;
            
        const avgBatchSize = this.batchSizes.length > 0
            ? this.batchSizes.reduce((a, b) => a + b, 0) / this.batchSizes.length
            : 0;

        return {
            table: this.tbName,
            rowsProcessed: this.rowCount,
            batchesProcessed: this.batchCount,
            totalTime: totalTime / 1000,
            rowsPerSecond: this.rowCount / (totalTime / 1000),
            avgBatchTime: avgBatchTime,
            avgBatchSize: avgBatchSize,
            finalConcurrency: this.semaphore.max,
            concurrencyHistory: this.concurrencyHistory
        };
    }
}

// Fungsi utama import satu tabel
async function syncFirebirdTbToPg(pgClient, schemaName, tbName, lastSync, pkColumns, signal, writeLog) {
    const fbPool = getFirebirdPool(schemaName);

    const logToClient = (type, data = {}) => {
        if (writeLog) {
            writeLog({
                type,
                table: tbName,
                timestamp: new Date().toISOString(),
                ...data
            });
        }
    };

    logToClient('log', { message: `START` });
    console.log(`\n🚀 Starting sync for: ${tbName}`);

    const fbDb = await new Promise((res, rej) =>
        fbPool.get((e, db) => e ? rej(e) : res(db))
    );

    try {
        let sql = "";
        
        if (lastSync && pkColumns) {
            const formattedLastSync = formatDateForFirebird(lastSync);
            if (tbName === "glaccnt") {
                sql = `select * from glaccnt where updated_at > '${formattedLastSync}' order by indentlevel`;
            } else if (tbName === "arinvdet") {
                sql = `select * from arinvdet where updated_at > '${formattedLastSync}' order by doid`;
            } else if (tbName === "item") {
                sql = `select * from item where updated_at > '${formattedLastSync}' order by parentitem`;
            } else {
                sql = `select * from ${tbName} where updated_at > '${formattedLastSync}'`;
            }
        } else {
            // Query untuk initial sync
            if (tbName === "glaccnt") {
                sql = "select * from glaccnt order by indentlevel";
            } else if (tbName === "arinvdet") {
                sql = "select * from arinvdet order by doid";
            } else if (tbName === "item") {
                sql = "select * from item order by parentitem";
            } else {
                sql = `select * from ${tbName}`;
            }
        }
        console.log(`📋 Query: ${sql}`);

        // Buat processor instance
        const processor = new SmartBatchProcessor(pgClient, schemaName, tbName, writeLog);

        await new Promise((resolve, reject) => {
            fbDb.transaction(Firebird.ISOLATION_SNAPSHOT, async (err, trx) => {
                if (err) {
                    fbDb.detach();
                    return reject(err);
                }

                try {
                    // Execute stream dengan backpressure control
                    await executeQueryStream(
                        trx,
                        sql,
                        [],
                        300, // fetchSize - dimulai dengan ukuran sedang
                        async (batchArray, header, headertype, isLast) => {
                            // Process batch
                            await processor.processBatch(batchArray, header, headertype, pkColumns, isLast);
                        },
                        {
                            signal: signal,
                            MAX_QUEUE_SIZE: 10,
                            MAX_CONCURRENT_PROCESSING: 5
                        }
                    );

                    // Commit transaction
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

                    // Log completion metrics
                    const metrics = processor.getMetrics();
                    console.log(`
✅ Completed: ${tbName}
├─ Rows: ${metrics.rowsProcessed.toLocaleString()}
├─ Batches: ${metrics.batchesProcessed}
├─ Time: ${metrics.totalTime.toFixed(2)}s
├─ Speed: ${metrics.rowsPerSecond.toFixed(1)} rows/sec
├─ Avg batch time: ${metrics.avgBatchTime.toFixed(1)}ms
├─ Avg batch size: ${metrics.avgBatchSize.toFixed(0)} rows
└─ Final concurrency: ${metrics.finalConcurrency}
                    `);

                    logToClient('log', {
                        message: `END`,
                        data: metrics.rowsProcessed,
                        metrics: {
                            time: metrics.totalTime,
                            speed: metrics.rowsPerSecond,
                            batches: metrics.batchesProcessed,
                            concurrency: metrics.finalConcurrency
                        }
                    });

                    resolve();
                } catch (err) {
                    // Rollback transaction
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
                    reject(err);
                }
            });
        });

        return {
            "tbname": tbName,
            "data": processor.rowCount,
            "batches": processor.batchCount,
            "metrics": processor.getMetrics()
        };

    } catch (err) {
        try {
            fbDb.detach();
        } catch (_) { }

        console.error(`❌ Error syncing ${tbName}:`, err.message);

        logToClient('error', {
            message: `Sync error: ${err.message}`,
            stack: err.stack
        });

        throw err;
    }
}

// Fungsi ekspor utama
async function syncDataPgFromFb(pgClient, schemaName, signal, writeLog) {
    if (writeLog) {
        writeLog({ "type": "meta", "totalTable": tbToimport.length });
    }

    function getLastSync(data, tableName) {
        const row = data.find(r => r[0] === tableName);
        return row ? row[1] : null;
    }

    function getPkColumns(data, tableName) {
        const row = data.find(r => r[0] === tableName);
        return row ? row[2] : null;
    }

    let hasil = [];
    let lastSuccessfulTable = null;
    let totalStartTime = Date.now();

    try {
        const resTbLast = await pgClient.query({
            text: 'select * from accurate.database_get_sync_log($1)',
            values: [schemaName],
            rowMode: 'array'
        });
        const tbLastSync = resTbLast.rows;

        console.log(`\n📈 Starting sync for ${tbToimport.length} tables`);

        for (let i = 0; i < tbToimport.length; i++) {
            const tableName = tbToimport[i];
            console.log(`\n=== [${i + 1}/${tbToimport.length}] Processing: ${tableName} ===`);

            try {
                const tableStartTime = Date.now();
                const res = await syncFirebirdTbToPg(
                    pgClient,
                    schemaName,
                    tableName,
                    getLastSync(tbLastSync, tableName),
                    getPkColumns(tbLastSync, tableName),
                    signal,
                    writeLog
                );

                const tableTime = Date.now() - tableStartTime;
                hasil.push(res);
                lastSuccessfulTable = tableName;

                console.log(`✓ ${tableName}: ${res.data} rows in ${(tableTime / 1000).toFixed(2)}s (${res.batches} batches)`);

            } catch (err) {
                const totalTime = (Date.now() - totalStartTime) / 1000;

                console.error(`\n💥 Sync stopped at table ${tableName}`);
                console.error(`   Last successful: ${lastSuccessfulTable || 'None'}`);
                console.error(`   Processed: ${hasil.length}/${tbToimport.length} tables`);
                console.error(`   Total time: ${totalTime.toFixed(2)}s`);
                console.error(`   Error: ${err.message}`);

                throw err;
            }
        }

        const totalTime = (Date.now() - totalStartTime) / 1000;
        const totalRows = hasil.reduce((sum, res) => sum + res.data, 0);
        const totalBatches = hasil.reduce((sum, res) => sum + res.batches, 0);

        console.log(`
🎉 Sync completed successfully!
├─ Tables: ${hasil.length}/${tbToimport.length}
├─ Total rows: ${totalRows.toLocaleString()}
├─ Total batches: ${totalBatches}
├─ Total time: ${totalTime.toFixed(2)}s
├─ Average speed: ${(totalRows / totalTime).toFixed(1)} rows/sec
├─ Average batch size: ${(totalRows / totalBatches).toFixed(1)} rows/batch
└─ Memory usage: ${(process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1)}MB
        `);

        return {
            success: true,
            tables: hasil,
            summary: {
                totalTables: hasil.length,
                totalRows: totalRows,
                totalBatches: totalBatches,
                totalTime: totalTime,
                averageSpeed: totalRows / totalTime,
                averageBatchSize: totalRows / totalBatches
            }
        };

    } catch (err) {
        const totalTime = (Date.now() - totalStartTime) / 1000;

        console.error(`
❌ Sync failed!
├─ Last successful table: ${lastSuccessfulTable || 'None'}
├─ Processed: ${hasil.length}/${tbToimport.length} tables
├─ Total time: ${totalTime.toFixed(2)}s
└─ Error: ${err.message}
        `);

        throw {
            success: false,
            error: err.message,
            lastSuccessfulTable: lastSuccessfulTable,
            processedTables: hasil,
            processedCount: hasil.length,
            totalTables: tbToimport.length,
            totalTime: totalTime
        };
    }
}

module.exports = { syncDataPgFromFb };