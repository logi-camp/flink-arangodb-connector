package top.logicamp.arangodb_flink_connector.internal.connection;

import org.apache.flink.util.Preconditions;

/** A builder class for creating {@link MongoClientProvider}. */
public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String host;
        private Integer port;

        private String database;

        private String collection;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(Integer port) {
            this.port = port;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public MongoClientProvider build() {
            Preconditions.checkNotNull(host, "Connection string must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            return new MongoSingleCollectionProvider(host, port, database, collection);
        }
    }
}
