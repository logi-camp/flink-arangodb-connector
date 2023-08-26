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

import top.logicamp.flink_arangodb_connector.sink.ArangoDBSink;

import java.io.Serializable;

/**
 * Configuration for {@link ArangoDBSink}.
 *
 * <p>Deprecated. Please use {@link ArangoDBConnectorOptions} instead.
 */
@Deprecated
public class SinkConfiguration implements Serializable {

    private boolean isTransactional;

    private boolean isFlushOnCheckpoint;

    private long bulkFlushSize;

    private long bulkFlushInterval;

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean transactional) {
        isTransactional = transactional;
    }

    public boolean isFlushOnCheckpoint() {
        return isFlushOnCheckpoint;
    }

    public void setFlushOnCheckpoint(boolean flushOnCheckpoint) {
        isFlushOnCheckpoint = flushOnCheckpoint;
    }

    public long getBulkFlushSize() {
        return bulkFlushSize;
    }

    public void setBulkFlushSize(long bulkFlushSize) {
        this.bulkFlushSize = bulkFlushSize;
    }

    public long getBulkFlushInterval() {
        return bulkFlushInterval;
    }

    public void setBulkFlushInterval(long bulkFlushInterval) {
        this.bulkFlushInterval = bulkFlushInterval;
    }
}
