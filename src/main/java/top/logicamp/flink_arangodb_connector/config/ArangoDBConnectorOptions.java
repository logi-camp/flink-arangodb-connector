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

package top.logicamp.flink_arangodb_connector.config;

import java.io.Serializable;
import java.time.Duration;

/** ArangoDB connector options. */
public class ArangoDBConnectorOptions implements Serializable {

    private static final long serialVersionUID = 1;
    protected final String host;
    protected final Integer port;
    protected final String database;
    protected final String collection;
    protected final String user;
    protected final String password;
    protected final Boolean useSsl;
    protected final boolean transactionEnable;
    protected final boolean flushOnCheckpoint;
    protected final int flushSize;
    protected final Duration flushInterval;
    protected final int maxInFlightFlushes;
    protected final boolean upsertEnable;
    protected final String[] upsertKey;

    public ArangoDBConnectorOptions(
            String host,
            Integer port,
            String database,
            String collection,
            String password,
            String user,
            Boolean useSsl,
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
        this.password = password;
        this.user = user;
        this.useSsl = useSsl;
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

    public String getPassword() {
        return password;
    }

    public String getUser() {
        return user;
    }

    public Boolean getUseSsl() {
        return useSsl;
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

    /** Builder For {@link ArangoDBConnectorOptions}. */
    public static class Builder {
        protected String host;
        protected Integer port;
        protected String database;
        protected String collection;
        protected String password;
        protected String user;
        protected Boolean useSsl;

        protected boolean transactionEnable;
        protected boolean flushOnCheckpoint;
        protected int flushSize;
        protected Duration flushInterval;
        protected int maxInFlightFlushes = 5;
        protected boolean upsertEnable;
        protected String[] upsertKey;

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(Integer port) {
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

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withUseSsl(Boolean useSsl) {
            this.useSsl = useSsl;
            return this;
        }

        public ArangoDBConnectorOptions build() {
            return new ArangoDBConnectorOptions(
                    host,
                    port,
                    database,
                    collection,
                    password,
                    user,
                    useSsl,
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
