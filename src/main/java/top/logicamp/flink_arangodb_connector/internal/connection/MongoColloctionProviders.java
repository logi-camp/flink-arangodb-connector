package top.logicamp.flink_arangodb_connector.internal.connection;

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
        private String user;
        private String password;
        private Boolean useSsl;


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

        public Builder user(String user){
            this.user = user;
            return this;
        }

        public Builder password(String password){
            this.password = password;
            return this;
        }

        public Builder useSsl(Boolean useSsl){
            this.useSsl = useSsl;
            return this;
        }

        public MongoClientProvider build() {
            Preconditions.checkNotNull(host, "Host must not be null");
            Preconditions.checkNotNull(port, "Port must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            return new MongoSingleCollectionProvider(host, port, database, collection, password, user, useSsl);
        }
    }
}
