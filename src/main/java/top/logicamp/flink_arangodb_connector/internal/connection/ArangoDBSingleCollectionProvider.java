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

package top.logicamp.flink_arangodb_connector.internal.connection;

import org.apache.flink.util.Preconditions;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A simple implementation of {@link ArangoDBClientProvider}. */
public class ArangoDBSingleCollectionProvider implements ArangoDBClientProvider {

    /** Connection string to ArangoDB standalone instances, replica sets or sharded clusters. */
    private final String host;

    private final Integer port;

    private final String password;

    private final String user;
    private final Boolean useSsl;

    /** The ArangoDB defaultDatabase to write to. */
    private final String defaultDatabase;

    /** The defaultCollection to write to. */
    private final String defaultCollection;

    private transient ArangoDB client;

    private transient ArangoDatabase database;

    private transient ArangoCollection collection;

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ArangoDBSingleCollectionProvider.class);

    public ArangoDBSingleCollectionProvider(
            String host,
            Integer port,
            String defaultDatabase,
            String defaultCollection,
            String password,
            String user,
            Boolean useSsl) {
        Preconditions.checkNotNull(host);
        Preconditions.checkNotNull(port);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.host = host;
        this.port = port;
        this.useSsl = useSsl;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
        this.user = user;
        this.password = password;
    }

    @Override
    public ArangoDB getClient() {
        synchronized (this) {
            if (client == null) {
                var builder = new ArangoDB.Builder().host(host, port);
                if (password != null) {
                    builder.password(password);
                }
                if (user != null) {
                    builder.user(user);
                }
                if (useSsl != null) {
                    builder.useSsl(useSsl);
                }
                client = builder.build();
            }
        }
        return client;
    }

    @Override
    public ArangoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (database == null) {
                database = getClient().db(defaultDatabase);
                if (!database.exists()) {
                    database.create();
                }
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
            if (!collection.exists()) {
                collection.create();
            }
        }
        return collection;
    }

    @Override
    public String getDefaultCollectionName() {
        return defaultCollection;
    }

    @Override
    public ArangoCollection recreateCollectionAccess() {
        return getDefaultCollection();
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to close ArangoDB client", e);
        } finally {
            client = null;
        }
    }
}
