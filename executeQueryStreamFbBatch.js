import 'dotenv/config';
const isDev = process.env.NODE_ENV !== "production";
const nodeEnv = process.env.NODE_ENV;
export function executeQueryStream(transaction, query, params, fetchSize, onBatch, options = {}) {
    return new Promise((resolve, reject) => {
        const connection = transaction.connection;
        const originalFetchAll = connection.fetchAll;

        // Configuration dengan options tambahan
        const CONFIG = {
            MAX_QUEUE_SIZE: options.MAX_QUEUE_SIZE || 5,
            MAX_CONCURRENT_PROCESSING: options.MAX_CONCURRENT_PROCESSING || 2,
            INITIAL_FETCH_SIZE: fetchSize || 500,
            MEMORY_CHECK_INTERVAL: 1000,
            TIMEOUT_MS: 3600000, // 60 menit
            BATCH_TIMEOUT_MS: options.batchTimeout || 300000, // 5 menit default untuk batch processing
            SIGNAL: options.signal || null // AbortSignal opsional
        };

        // State management
        let streamActive = true;
        let pendingError = null;
        let processingQueue = [];
        let activeBatchProcesses = 0;
        let totalRowsFetched = 0;
        let totalBatchesProcessed = 0;
        let isPausedByBackpressure = false;
        let lastBatchFetched = false;
        let allDataFetched = false;

        // Header dari statement
        let header = null;
        let headertype = null;
        let statementRef = null;

        // Batch timeout tracking
        const batchTimeouts = new Map();

        // Metrics
        const metrics = {
            startTime: Date.now(),
            fetchCount: 0,
            pauseCount: 0,
            memoryPeak: 0,
            batchProcessingTimes: [],
            timedOutBatches: 0,
            abortedBySignal: false
        };

        let resumeFetching = () => {
            console.log('resumeFetching not initialized yet');
        };

        // Function cleanup yang bisa diakses di semua scope
        const cleanup = () => {
            if (isDev)
                console.log('🧹 Cleaning up stream resources');

            // Hentikan memory monitor
            //if (memoryMonitor) {
            //    clearInterval(memoryMonitor);
            //}

            // Restore original fetchAll
            if (connection && originalFetchAll) {
                connection.fetchAll = originalFetchAll;
            }

            // Hapus semua timeout batch
            batchTimeouts.forEach(timeoutId => {
                if (timeoutId) clearTimeout(timeoutId);
            });
            batchTimeouts.clear();
        };

        // Handler untuk abort signal
        const handleAbortSignal = () => {
            if (CONFIG.SIGNAL && CONFIG.SIGNAL.aborted) {
                console.log('🚨 Stream aborted by signal (handleAbortSignal)');
                streamActive = false;
                metrics.abortedBySignal = true;

                // Hentikan semua batch timeout
                batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                batchTimeouts.clear();

                // Bersihkan dan reject
                cleanup();
                reject(new DOMException('Stream aborted by signal', 'AbortError'));
                return true;
            }
            return false;
        };

        // Memory monitor dengan pengecekan signal
        /* saya tutup dulu, harus nya flag sendiri tidak paka isPauseBybackpressure, karena jika memory masih kurang dari 300 maka jadi false, ini akah membuka fecthnext sehingga bisa lebih dari queue max
        const memoryMonitor = setInterval(() => {
            if (handleAbortSignal()) return;

            const memoryUsage = process.memoryUsage().heapUsed / 1024 / 1024;
            metrics.memoryPeak = Math.max(metrics.memoryPeak, memoryUsage);

            if (memoryUsage > 500 && !isPausedByBackpressure) {
                console.log(`⚠️ High memory: ${memoryUsage.toFixed(1)}MB, pausing fetch`);
                //isPausedByBackpressure = true;
                //metrics.pauseCount++;
            } else if (memoryUsage < 300 && isPausedByBackpressure) {
                //console.log(`✅ Memory normalized: ${memoryUsage.toFixed(1)}MB, resuming fetch`);
                //isPausedByBackpressure = false;
                //resumeFetching();
            }
        }, CONFIG.MEMORY_CHECK_INTERVAL);
        */
        // Monkey patch dengan flag batch terakhir
        connection.fetchAll = function (statement, transaction, callback) {
            if (!streamActive) {
                cleanup();
                return callback(pendingError || new Error('Stream stopped'));
            }

            // Cek abort signal
            if (handleAbortSignal()) return;

            // Simpan statement untuk ambil header
            statementRef = statement;

            // Extract header dari statement metadata
            if (!header && statement && statement.output) {
                const cols = statement.output || [];
                header = cols.map(col => (col.alias || col.field || '').trim());
                headertype = cols.map(col => (col.type));
                if (isDev)
                    console.log(`📋 Header extracted: ${header.length} columns`);

                if (statement.custom && statement.custom.onBatch) {
                    statement.custom.header = header;
                    statement.custom.headertype = headertype;
                }
            }

            const self = this;
            let currentFetchSize = CONFIG.INITIAL_FETCH_SIZE;
            let isFetching = false;
            let batchCounter = 0;

            // Function untuk membersihkan timeout batch
            const cleanupBatchTimeout = (batchId) => {
                if (batchTimeouts.has(batchId)) {
                    clearTimeout(batchTimeouts.get(batchId));
                    batchTimeouts.delete(batchId);
                }
            };

            // Local cleanup function untuk scope ini
            const localCleanup = () => {
                cleanupBatchTimeout('all');
                cleanup();
            };

            // Process queue dengan timeout batch
            const processQueue = async () => {
                while (streamActive &&
                    processingQueue.length > 0 &&
                    activeBatchProcesses < CONFIG.MAX_CONCURRENT_PROCESSING) {

                    if (handleAbortSignal()) return;

                    const batchItem = processingQueue.shift();
                    const batchId = `batch-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
                    activeBatchProcesses++;

                    try {
                        const processStart = Date.now();
                        batchCounter++;

                        // Setup timeout untuk batch processing
                        const batchTimeoutId = setTimeout(() => {
                            console.log(`⏰ Batch timeout after ${CONFIG.BATCH_TIMEOUT_MS}ms`);
                            metrics.timedOutBatches++;

                            // Hentikan stream jika batch timeout
                            streamActive = false;
                            pendingError = new Error(`Batch processing timeout after ${CONFIG.BATCH_TIMEOUT_MS}ms`);

                            // Bersihkan timeout lainnya
                            batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                            batchTimeouts.clear();

                            cleanup();
                            callback(pendingError);
                        }, CONFIG.BATCH_TIMEOUT_MS);

                        batchTimeouts.set(batchId, batchTimeoutId);

                        try {
                            const isLastBatch = batchItem.isLast &&
                                processingQueue.length === 0 &&
                                allDataFetched;

                            // Execute batch processing dengan timeout protection
                            const batchPromise = Promise.resolve(onBatch(
                                batchItem.data,
                                header,
                                headertype,
                                isLastBatch
                            ));

                            // Tambahkan race condition dengan timeout
                            await Promise.race([
                                batchPromise,
                                new Promise((_, reject) =>
                                    setTimeout(() => reject(new Error('Batch processing timeout')), CONFIG.BATCH_TIMEOUT_MS)
                                )
                            ]);

                            totalBatchesProcessed++;

                            // Log untuk batch terakhir
                            if (isLastBatch) {
                                if (isDev)
                                    console.log(`🎯 Processing LAST batch (#${batchCounter}, ${batchItem.data.length} rows)`);
                            }

                            // Adaptive backpressure
                            const processTime = Date.now() - processStart;
                            metrics.batchProcessingTimes.push(processTime);

                            if (processTime > 5000) {
                                currentFetchSize = Math.max(100, Math.floor(currentFetchSize * 0.7));
                                if (isDev)
                                    console.log(`🐌 Slow batch: ${processTime}ms for ${batchItem.data.length} rows, reducing fetch to ${currentFetchSize}`);
                            } else if (processTime < 3000 && currentFetchSize < 10000) {
                                currentFetchSize = Math.min(10000, Math.floor(currentFetchSize * 1.2));
                            }

                        } catch (batchError) {
                            streamActive = false;
                            pendingError = batchError;

                            // Hapus semua timeout
                            batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                            batchTimeouts.clear();

                            cleanup();
                            callback(batchError);
                            return;
                        } finally {
                            // Selalu bersihkan timeout batch ini
                            cleanupBatchTimeout(batchId);
                        }
                    } finally {
                        activeBatchProcesses--;
                        //if (processingQueue.length > 0) {
                        //    setImmediate(processQueue);  // Process next batch
                        //}
                    }

                    // Backpressure control
                    if (processingQueue.length > CONFIG.MAX_QUEUE_SIZE && !isPausedByBackpressure) {
                        if (isDev)
                            console.log(`⏸️ Queue full (${processingQueue.length} batches), pausing fetch`);
                        isPausedByBackpressure = true;
                        metrics.pauseCount++;
                    }

                    if (isPausedByBackpressure &&
                        processingQueue.length < CONFIG.MAX_QUEUE_SIZE) {
                        if (isDev)
                            console.log(`▶️ Queue normalized (${processingQueue.length} batches), resuming fetch`);
                        isPausedByBackpressure = false;
                        resumeFetching();
                    }

                    // Check jika ini batch terakhir dan semua sudah diproses
                    if (batchItem.isLast &&
                        processingQueue.length === 0 &&
                        activeBatchProcesses === 0 &&
                        allDataFetched) {
                        finishStream();
                    }
                }

                // Completion check
                if (!streamActive && processingQueue.length === 0 && activeBatchProcesses === 0) {
                    finishStream();
                }
            };

            resumeFetching = () => {
                if (!isFetching && streamActive && !isPausedByBackpressure && !allDataFetched) {
                    fetchNextBatch();
                }
            };

            const fetchNextBatch = () => {
                /*
                if (processingQueue.length >= CONFIG.MAX_QUEUE_SIZE) {
                    console.log(`⏸️ Queue already at limit (${processingQueue.length}), not fetching`);
                    isPausedByBackpressure = true;
                    return;
                }
                */
                if (handleAbortSignal()) return;

                if (!streamActive || isFetching || isPausedByBackpressure || allDataFetched) {
                    return;
                }

                isFetching = true;
                metrics.fetchCount++;

                self.fetch(statement, transaction, currentFetchSize, (err, ret) => {
                    isFetching = false;

                    if (!streamActive) return;
                    if (handleAbortSignal()) return;

                    if (err) {
                        streamActive = false;
                        pendingError = err;

                        // Hapus semua timeout batch
                        batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                        batchTimeouts.clear();

                        cleanup();
                        callback(err);
                        return;
                    }

                    if (ret && ret.data && ret.data.length) {
                        totalRowsFetched += ret.data.length;

                        const isLast = !!ret.fetched;
                        if (isLast) {
                            allDataFetched = true;
                            lastBatchFetched = true;
                            if (isDev)
                            console.log(`🔚 Last batch fetched (${ret.data.length} rows)`);
                        }

                        processingQueue.push({
                            data: ret.data,
                            isLast: isLast,
                            batchNumber: metrics.fetchCount
                        });

                        setImmediate(processQueue);

                        if (processingQueue.length >= CONFIG.MAX_QUEUE_SIZE && !isPausedByBackpressure) {
                            if (isDev)
                                console.log(`⏸️ Queue reaching limit (${processingQueue.length}), pausing fetch, current fetchsize : ${currentFetchSize}`);
                            isPausedByBackpressure = true;
                            metrics.pauseCount++;
                        }

                        if (!ret.fetched) {
                            if (isPausedByBackpressure) {
                                const checkResume = () => {
                                    if (!streamActive) return;
                                    if (handleAbortSignal()) return;

                                    if (isPausedByBackpressure) {
                                        setTimeout(checkResume, 100);
                                    } else {
                                        fetchNextBatch();
                                    }
                                };
                                checkResume();
                            } else {
                                setTimeout(fetchNextBatch, 0);
                            }
                        } else {
                            const waitForCompletion = () => {
                                if (!streamActive) return;
                                if (handleAbortSignal()) return;

                                if (processingQueue.length > 0 || activeBatchProcesses > 0) {
                                    setTimeout(waitForCompletion, 100);
                                } else {
                                    finishStream();
                                }
                            };
                            waitForCompletion();
                        }
                    } else if (ret && ret.fetched) {
                        allDataFetched = true;
                        lastBatchFetched = true;
                        console.log(`📭 No data fetched for this query`);
                        finishStream();
                    } else {
                        setTimeout(fetchNextBatch, 0);
                    }
                });
            };

            const finishStream = () => {
                if (!streamActive) return;

                streamActive = false;

                // Hapus semua timeout batch
                batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                batchTimeouts.clear();

                cleanup();

                const totalTime = Date.now() - metrics.startTime;
                const avgBatchTime = metrics.batchProcessingTimes.length > 0
                    ? metrics.batchProcessingTimes.reduce((a, b) => a + b, 0) / metrics.batchProcessingTimes.length
                    : 0;
                    
                if (isDev)
                    console.log(`
📊 Stream completed:
├─ Rows: ${totalRowsFetched.toLocaleString()}
├─ Batches: ${totalBatchesProcessed}
├─ Timed out batches: ${metrics.timedOutBatches}
├─ Aborted by signal: ${metrics.abortedBySignal ? 'Yes' : 'No'}
├─ Time: ${(totalTime / 1000).toFixed(2)}s
├─ Rows/sec: ${(totalRowsFetched / (totalTime / 1000)).toFixed(1)}
├─ Header: ${header ? header.length : 0} columns
├─ Last batch flagged: ${lastBatchFetched ? 'Yes' : 'No'}
└─ Memory: ${metrics.memoryPeak.toFixed(1)}MB
                `);

                callback(undefined, []);
            };

            fetchNextBatch();
        };

        // Setup global timeout
        const timeoutId = setTimeout(() => {
            if (streamActive) {
                console.log(`⏰ Global stream timeout after ${CONFIG.TIMEOUT_MS}ms`);
                streamActive = false;
                pendingError = new Error(`Stream timeout after ${CONFIG.TIMEOUT_MS}ms`);

                // Hapus semua timeout batch
                batchTimeouts.forEach(timeoutId => clearTimeout(timeoutId));
                batchTimeouts.clear();

                cleanup();
                reject(pendingError);
            }
        }, CONFIG.TIMEOUT_MS);

        // Setup abort signal listener
        let abortListenerCleanup = null;
        if (CONFIG.SIGNAL) {
            const abortHandler = () => {
                if (streamActive) {
                    console.log('🚨 Abort signal received (event listener)');
                    streamActive = false;
                    metrics.abortedBySignal = true;

                    // Hapus timeout global
                    if (timeoutId) {
                        clearTimeout(timeoutId);
                    }

                    // Hapus semua timeout batch
                    batchTimeouts.forEach(timeoutId => {
                        if (timeoutId) clearTimeout(timeoutId);
                    });
                    batchTimeouts.clear();

                    // Bersihkan semua resources
                    cleanup();

                    // Reject dengan AbortError
                    reject(new DOMException('Aborted', 'AbortError'));
                }
            };

            CONFIG.SIGNAL.addEventListener('abort', abortHandler);

            // Store cleanup function
            abortListenerCleanup = () => {
                CONFIG.SIGNAL.removeEventListener('abort', abortHandler);
            };

            // Cek jika signal sudah aborted sebelum kita mulai
            if (CONFIG.SIGNAL.aborted) {
                console.log('⚠️ Signal already aborted before stream started');
                streamActive = false;
                metrics.abortedBySignal = true;
                cleanup();
                reject(new DOMException('Aborted before stream started', 'AbortError'));
                return;
            }
        }

        // Custom properties
        const customProps = {
            asStream: true,
            onBatch: null,
            header: null,
            headertype: null,
            on: function (batch) {
                return Promise.resolve();
            }
        };

        // Execute query
        transaction.sequentially(
            query,
            params || [],
            () => { },
            (err) => {
                // Cleanup timeouts dan intervals
                if (timeoutId) {
                    clearTimeout(timeoutId);
                }

                //if (memoryMonitor) {
                //    clearInterval(memoryMonitor);
                //}

                // Cleanup abort listener jika ada
                if (abortListenerCleanup) {
                    abortListenerCleanup();
                }

                // Hapus semua timeout batch
                batchTimeouts.forEach(timeoutId => {
                    if (timeoutId) clearTimeout(timeoutId);
                });
                batchTimeouts.clear();

                // Restore original fetchAll
                connection.fetchAll = originalFetchAll;

                // Cek abort signal terakhir
                if (CONFIG.SIGNAL && CONFIG.SIGNAL.aborted) {
                    console.log('⚠️ Stream completed but signal was aborted');
                    reject(new DOMException('Aborted', 'AbortError'));
                    return;
                }

                if (pendingError) {
                    reject(pendingError);
                } else if (err) {
                    reject(err);
                } else {
                    resolve({
                        totalRows: totalRowsFetched,
                        totalBatches: totalBatchesProcessed,
                        header: customProps.header,
                        headertype: customProps.headertype,
                        lastBatchProcessed: lastBatchFetched,
                        metrics: {
                            fetched: totalRowsFetched,
                            batches: totalBatchesProcessed,
                            pauses: metrics.pauseCount,
                            timedOutBatches: metrics.timedOutBatches,
                            abortedBySignal: metrics.abortedBySignal,
                            lastBatchFlagged: lastBatchFetched
                        }
                    });
                }
            },
            true,
            customProps
        );

        // Set callback
        customProps.onBatch = onBatch;
    });
}