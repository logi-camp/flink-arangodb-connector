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
    ArangoCollection recreateCollectionAccess();

    /** Close the underlying ArangoDB connection. */
    void close();
}
