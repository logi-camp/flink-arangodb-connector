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

/** A builder class for creating {@link ArangoDBClientProvider}. */
public class ArangoDBColloctionProviders {

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

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder useSsl(Boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        public ArangoDBClientProvider build() {
            Preconditions.checkNotNull(host, "Host must not be null");
            Preconditions.checkNotNull(port, "Port must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            return new ArangoDBSingleCollectionProvider(
                    host, port, database, collection, password, user, useSsl);
        }
    }
}
