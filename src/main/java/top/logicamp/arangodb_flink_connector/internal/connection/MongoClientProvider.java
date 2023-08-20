package top.logicamp.arangodb_flink_connector.internal.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;

import java.io.Serializable;

/** Provided for initiate and recreate {@link MongoClient}. */
public interface MongoClientProvider extends Serializable {

    /**
     * Create one or get the current {@link MongoClient}.
     *
     * @return Current {@link MongoClient}.
     */
    ArangoDB getClient();

    /**
     * Get the default database.
     *
     * @return Current {@link MongoDatabase}.
     */
    ArangoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     *
     * @return Current {@link MongoCollection}.
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
     * @return A new {@link MongoClient}.
     */
    ArangoDB recreateClient();

    /** Close the underlying MongoDB connection. */
    void close();
}
