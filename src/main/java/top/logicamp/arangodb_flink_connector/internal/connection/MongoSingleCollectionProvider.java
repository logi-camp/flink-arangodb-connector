package top.logicamp.arangodb_flink_connector.internal.connection;

import org.apache.flink.util.Preconditions;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple implementation of {@link MongoClientProvider}. */
public class MongoSingleCollectionProvider implements MongoClientProvider {

    /** Connection string to MongoDB standalone instances, replica sets or sharded clusters. */
    private final String host;

    private final Integer port;

    /** The MongoDB defaultDatabase to write to. */
    private final String defaultDatabase;

    /**
     * The defaultCollection to write to. Must be a existing defaultCollection for MongoDB 4.2 and
     * earlier versions.
     */
    private final String defaultCollection;

    private transient ArangoDB client;

    private transient ArangoDatabase database;

    private transient ArangoCollection collection;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(MongoSingleCollectionProvider.class);

    public MongoSingleCollectionProvider(
            String host, Integer port, String defaultDatabase, String defaultCollection) {
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(port);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.host = host;
        this.port = port;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
    }

    @Override
    public ArangoDB getClient() {
        synchronized (this) {
            if (client == null) {
                client = new ArangoDB.Builder().host(host, port).build();
            }
        }
        return client;
    }

    @Override
    public ArangoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (database == null) {
                database = getClient().db(defaultDatabase);
            }
        }
        return database;
    }

    @Override
    public ArangoCollection getDefaultCollection() {
        synchronized (this) {
            if (collection == null) {
                collection = getDefaultDatabase().collection(defaultCollection);
            }
        }
        return collection;
    }

    @Override
    public String getDefaultCollectionName() {
        return defaultCollection;
    }

    @Override
    public ArangoDB recreateClient() {
        close();
        return getClient();
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close Mongo client", e);
        } finally {
            client = null;
        }
    }
}
