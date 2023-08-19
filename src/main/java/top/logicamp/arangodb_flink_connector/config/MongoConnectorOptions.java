package top.logicamp.arangodb_flink_connector.config;

import java.io.Serializable;
import java.time.Duration;

/** MongoDB connector options. */
public class MongoConnectorOptions implements Serializable {

    private static final long serialVersionUID = 1;
    protected final String host;
    protected final String database;
    protected final String collection;

    protected final boolean transactionEnable;
    protected final boolean flushOnCheckpoint;
    protected final int flushSize;
    protected final Duration flushInterval;
    protected final int maxInFlightFlushes;
    protected final boolean upsertEnable;
    protected final String[] upsertKey;
    protected final Integer port;

    public MongoConnectorOptions(
            String host,
            Integer port,
            String database,
            String collection,
            boolean transactionEnable,
            boolean flushOnCheckpoint,
            int flushSize,
            Duration flushInterval,
            int maxInFlightFlushes,
            boolean upsertEnable,
            String[] upsertKey) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.collection = collection;
        this.transactionEnable = transactionEnable;
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.flushSize = flushSize;
        this.flushInterval = flushInterval;
        this.maxInFlightFlushes = maxInFlightFlushes;
        this.upsertEnable = upsertEnable;
        this.upsertKey = upsertKey;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public boolean isTransactionEnable() {
        return transactionEnable;
    }

    public boolean getFlushOnCheckpoint() {
        return flushOnCheckpoint;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public int getMaxInFlightFlushes() {
        return maxInFlightFlushes;
    }

    public boolean isUpsertEnable() {
        return upsertEnable;
    }

    public String[] getUpsertKey() {
        return upsertKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder For {@link MongoConnectorOptions}. */
    public static class Builder {
        protected String host;
        protected Integer port;
        protected String database;
        protected String collection;

        protected boolean transactionEnable;
        protected boolean flushOnCheckpoint;
        protected int flushSize;
        protected Duration flushInterval;
        protected int maxInFlightFlushes = 5;
        protected boolean upsertEnable;
        protected String[] upsertKey;

        public Builder withConnectString(String host, Integer port) {
            this.host = host;
            this.port = port;
            return this;
        }

        public Builder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder withCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder withTransactionEnable(boolean transactionEnable) {
            this.transactionEnable = transactionEnable;
            return this;
        }

        public Builder withFlushOnCheckpoint(boolean flushOnCheckpoint) {
            this.flushOnCheckpoint = flushOnCheckpoint;
            return this;
        }

        public Builder withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withMaxInFlightFlushes(int maxInFlightFlushes) {
            this.maxInFlightFlushes = maxInFlightFlushes;
            return this;
        }

        public Builder withUpsertEnable(boolean upsertEnable) {
            this.upsertEnable = upsertEnable;
            return this;
        }

        public Builder withUpsertKey(String[] upsertKey) {
            this.upsertKey = upsertKey;
            return this;
        }

        public MongoConnectorOptions build() {
            return new MongoConnectorOptions(
                    host,
                    port,
                    database,
                    collection,
                    transactionEnable,
                    flushOnCheckpoint,
                    flushSize,
                    flushInterval,
                    maxInFlightFlushes,
                    upsertEnable,
                    upsertKey);
        }
    }
}
