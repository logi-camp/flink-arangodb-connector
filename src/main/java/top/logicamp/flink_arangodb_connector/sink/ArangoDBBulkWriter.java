/*
 * MIT License
 *
 * Copyright (c) "2023" Logicamp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package top.logicamp.flink_arangodb_connector.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.OverwriteMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.logicamp.flink_arangodb_connector.config.ArangoDBConnectorOptions;
import top.logicamp.flink_arangodb_connector.internal.connection.ArangoDBClientProvider;
import top.logicamp.flink_arangodb_connector.serde.CDCDocument;
import top.logicamp.flink_arangodb_connector.serde.DocumentSerializer;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/** Writer for ArangoDB sink. */
public class ArangoDBBulkWriter<IN> implements SinkWriter<IN> {
    private final ArangoDBClientProvider collectionProvider;

    private transient ArangoCollection collection;
    private transient ArangoDatabase database;

    private final ConcurrentLinkedQueue<CDCDocument> currentBulk = new ConcurrentLinkedQueue<CDCDocument>();

    private final ArrayBlockingQueue<DocumentBulk> pendingBulks;

    private DocumentSerializer<IN> serializer;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private transient volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private final transient ArangoDBConnectorOptions options;

    private transient volatile boolean initialized = false;

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(ArangoDBBulkWriter.class);

    private final boolean upsertEnable;

    public ArangoDBBulkWriter(
            ArangoDBClientProvider collectionProvider,
            DocumentSerializer<IN> serializer,
            ArangoDBConnectorOptions options) {
        this.upsertEnable = options.isUpsertEnable();
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
        this.maxSize = options.getFlushSize();
        this.pendingBulks = new ArrayBlockingQueue<>(options.getMaxInFlightFlushes());
        this.flushOnCheckpoint = options.getFlushOnCheckpoint();
        this.options = options;
        if (!flushOnCheckpoint && this.options.getFlushInterval() > 0) {
            this.scheduler = Executors.newScheduledThreadPool(
                    1, new ExecutorThreadFactory("arangodb-bulk-writer"));
            this.scheduledFuture = scheduler.scheduleAtFixedRate(
                    () -> {
                        synchronized (ArangoDBBulkWriter.this) {
                            if (initialized && !closed) {
                                try {
                                    rollBulkIfNeeded(true);
                                    flush();
                                } catch (Exception e) {
                                    flushException = e;
                                }
                            }
                        }
                    },
                    options.getFlushInterval(),
                    options.getFlushInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    public void initializeState() {
        collection = collectionProvider.getDefaultCollection();
        database = collectionProvider.getDefaultDatabase();
        /*
         * for (DocumentBulk bulk : recoveredBulks) {
         * for (CDCDocument document : bulk.getDocuments()) {
         * currentBulk.add(document);
         * rollBulkIfNeeded();
         * }
         * }
         */
        initialized = true;
    }

    @Override
    public void write(IN o, Context context) throws IOException {
        checkFlushException();
        currentBulk.add(serializer.serialize(o));
        rollBulkIfNeeded();
    }

    @Override
    public void close() throws Exception {
        // flush all cache data before close in non-transaction mode
        if (!flushOnCheckpoint) {
            synchronized (this) {
                if (!closed) {
                    try {
                        rollBulkIfNeeded(true);
                        flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }

        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }

        if (collectionProvider != null) {
            collectionProvider.close();
        }
    }

    /**
     * Flush by non-transactional bulk write, which may result in data duplicates
     * after multiple
     * tries. There may be concurrent flushes when concurrent checkpoints are
     * enabled.
     *
     * <p>
     * We manually retry write operations, because the driver doesn't support
     * automatic retries
     * for some ArangoDB setups (e.g. standalone instances). TODO: This should be
     * configurable in
     * the future.
     */
    private synchronized void flush() throws IOException {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            if (this.upsertEnable) {
                flushUpsert(iterator);
            } else {
                while (iterator.hasNext()) {
                    DocumentBulk bulk = iterator.next();
                    do {
                        try {
                            // ordered, non-bypass mode
                            if (bulk.size() > 0) {
                                collection.deleteDocuments(
                                        bulk.getDeletes().collect(Collectors.toList()));
                                collection.insertDocuments(bulk.getRepserts().collect(Collectors.toList()),
                                        new DocumentCreateOptions()
                                                .overwriteMode(OverwriteMode.replace)
                                                .waitForSync(true)
                                                .silent(true));
                            }
                            iterator.remove();
                            break;
                        } catch (Exception e) {
                            // maybe partial failure
                            LOGGER.error("Failed to flush data to ArangoDB", e);
                            throw new IOException(e);
                        }
                    } while (!closed && retryPolicy.shouldBackoffRetry());
                }
            }
        }
    }

    private void flushUpsert(Iterator<DocumentBulk> iterator) throws IOException {
        while (iterator.hasNext()) {
            DocumentBulk bulk = iterator.next();
            do {
                try {
                    if (bulk.size() > 0) {
                        collection.deleteDocuments(bulk.getDeletes().collect(Collectors.toList()));
                        collection.insertDocuments(bulk.getRepserts().collect(Collectors.toList()),
                                new DocumentCreateOptions()
                                        .overwriteMode(OverwriteMode.replace)
                                        .waitForSync(true)
                                        .silent(true));
                    }
                    iterator.remove();
                    break;
                } catch (ArangoDBException e) {
                    LOGGER.error("Failed to flush data to ArangoDB ", e);
                    throw new IOException(e);
                }
            } while (!closed && retryPolicy.shouldBackoffRetry());
        }
    }

    private void ensureConnection() {
        try {
            if (!collection.exists()) {
                collection = collectionProvider.recreateCollectionAccess();
                LOGGER.warn("Collection not exists. Try to create it.");
            }
            ;
        } catch (Exception e) {
            LOGGER.warn("Connection is not available. Try to reconnect", e);
            collection = collectionProvider.recreateCollectionAccess();
        }
    }

    private void rollBulkIfNeeded() throws IOException {
        rollBulkIfNeeded(false);
    }

    private synchronized void rollBulkIfNeeded(boolean force) throws IOException {
        int size = currentBulk.size();
        if (force || size >= maxSize) {
            DocumentBulk bulk = new DocumentBulk(maxSize);
            for (int i = 0; i < size; i++) {
                if (bulk.size() >= maxSize) {
                    enqueuePendingBulk(bulk);
                    bulk = new DocumentBulk(maxSize);
                }
                bulk.add(currentBulk.poll());
            }
            enqueuePendingBulk(bulk);
        }
    }

    private synchronized void enqueuePendingBulk(DocumentBulk bulk) throws IOException {
        boolean isEnqueued = pendingBulks.offer(bulk);
        if (!isEnqueued) {
            // pending queue is full, thus block processing if it's transactional sink,
            // or else trigger a flush
            if (options.getFlushOnCheckpoint()) {
                do {
                    try {
                        Thread.sleep(1_000L);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    isEnqueued = pendingBulks.offer(bulk);
                } while (!isEnqueued || closed);
            } else {
                flush();
                isEnqueued = pendingBulks.offer(bulk);
                if (!isEnqueued) {
                    throw new IllegalStateException(
                            "Pending queue still full after flushes, this is a bug, "
                                    + "please file an issue.");
                }
            }
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to ArangoDB", flushException);
        }
    }

    @NotThreadSafe
    class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        flush();
    }
}
