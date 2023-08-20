package top.logicamp.flink_arangodb_connector.internal.connection;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

import java.io.Serializable;

/** Provided for initiate and recreate {@link ArangoDB}. */
public interface ArangoDBClientProvider extends Serializable {

    /**
     * Create one or get the current {@link ArangoDB}.
     *
     * @return Current {@link ArangoDB}.
     */
    ArangoDB getClient();

    /**
     * Get the default database.
     *
     * @return Current {@link ArangoDatabase}.
     */
    ArangoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     *
     * @return Current {@link ArangoCollection}.
     */
    ArangoCollection getDefaultCollection();

    /**
     * Get the default collection name.
     *
     * @return Current {@link String}.
     */
    String getDefaultCollectionName();

    /**
     * Recreate a client. Used typically when a connection is timed out or lost.
     *
     * @return A new {@link ArangoDB}.
     */
    void recreateClient();

    /** Close the underlying ArangoDB connection. */
    void close();
}
