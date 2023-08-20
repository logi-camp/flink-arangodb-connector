package top.logicamp.flink_arangodb_connector.config;

import top.logicamp.flink_arangodb_connector.sink.ArangoDBSink;

import java.io.Serializable;

/**
 * Configuration for {@link ArangoDBSink}.
 *
 * <p>Deprecated. Please use {@link ArangoDBConnectorOptions} instead.
 */
@Deprecated
public class SinkConfiguration implements Serializable {

    private boolean isTransactional;

    private boolean isFlushOnCheckpoint;

    private long bulkFlushSize;

    private long bulkFlushInterval;

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean transactional) {
        isTransactional = transactional;
    }

    public boolean isFlushOnCheckpoint() {
        return isFlushOnCheckpoint;
    }

    public void setFlushOnCheckpoint(boolean flushOnCheckpoint) {
        isFlushOnCheckpoint = flushOnCheckpoint;
    }

    public long getBulkFlushSize() {
        return bulkFlushSize;
    }

    public void setBulkFlushSize(long bulkFlushSize) {
        this.bulkFlushSize = bulkFlushSize;
    }

    public long getBulkFlushInterval() {
        return bulkFlushInterval;
    }

    public void setBulkFlushInterval(long bulkFlushInterval) {
        this.bulkFlushInterval = bulkFlushInterval;
    }
}
