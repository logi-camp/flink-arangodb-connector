package top.logicamp.flink_arangodb_connector.sink;

import top.logicamp.flink_arangodb_connector.config.ArangoDBConnectorOptions;
import top.logicamp.flink_arangodb_connector.internal.connection.ArangoDBClientProvider;
import top.logicamp.flink_arangodb_connector.serde.CDCDocument;
import top.logicamp.flink_arangodb_connector.serde.DocumentSerializer;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Writer for ArangoDB sink.
 */
public class ArangoDBBulkWriter<IN> implements SinkWriter<IN> {
    private final ArangoDBClientProvider collectionProvider;

    private transient ArangoCollection collection;

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
        if (!flushOnCheckpoint && this.options.getFlushInterval().getSeconds() > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("arangodb-bulk-writer"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
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
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            TimeUnit.SECONDS);
        }
    }

    public void initializeState() {
        collection = collectionProvider.getDefaultCollection();
        /*for (DocumentBulk bulk : recoveredBulks) {
            for (CDCDocument document : bulk.getDocuments()) {
                currentBulk.add(document);
                rollBulkIfNeeded();
            }
        }*/
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
     * Flush by non-transactional bulk write, which may result in data duplicates after multiple
     * tries. There may be concurrent flushes when concurrent checkpoints are enabled.
     *
     * <p>We manually retry write operations, because the driver doesn't support automatic retries
     * for some ArangoDB setups (e.g. standalone instances). TODO: This should be configurable in
     * the future.
     */
    private synchronized void flush() {
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
                                collection.insertDocuments(bulk.getInserts().collect(Collectors.toList()));
                                collection.replaceDocuments(bulk.getUpdates().collect(Collectors.toList()));
                                collection.deleteDocuments(bulk.getDeletes().collect(Collectors.toList()));
                            }
                            iterator.remove();
                            break;
                        } catch (Exception e) {
                            // maybe partial failure
                            LOGGER.error("Failed to flush data to ArangoDB", e);
                        }
                    } while (!closed && retryPolicy.shouldBackoffRetry());
                }
            }
        }
    }

    private void flushUpsert(Iterator<DocumentBulk> iterator) {
        while (iterator.hasNext()) {
            DocumentBulk bulk = iterator.next();
            do {
                try {
                    if (bulk.size() > 0) {
                        collection.insertDocuments(bulk.getInserts().collect(Collectors.toList()));
                        collection.replaceDocuments(bulk.getUpdates().collect(Collectors.toList()));
                        collection.deleteDocuments(bulk.getDeletes().collect(Collectors.toList()));
                    }
                    iterator.remove();
                    break;
                } catch (ArangoDBException e) {
                    LOGGER.error("Failed to flush data to ArangoDB ", e);
                }
            } while (!closed && retryPolicy.shouldBackoffRetry());
        }
    }

    private void ensureConnection() {
        try {
            collection.getInfo();
        } catch (Exception e) {
            LOGGER.warn("Connection is not available, try to reconnect", e);
            collectionProvider.recreateClient();
        }
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
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

    private synchronized void enqueuePendingBulk(DocumentBulk bulk) {
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
