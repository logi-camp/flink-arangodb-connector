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

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

/**
 * Simple factory for {@link SinkConfiguration}.
 *
 * <p>Deprecated. Please use {@link ArangoDBConnectorOptions} instead.
 */
@Deprecated
public class SinkConfigurationFactory {

    public static SinkConfiguration fromProperties(Properties properties) {
        SinkConfiguration configuration = new SinkConfiguration();
        configuration.setTransactional(
                PropertiesUtil.getBoolean(
                        properties, ArangoDBOptions.SINK_TRANSACTION_ENABLED, false));
        configuration.setFlushOnCheckpoint(
                PropertiesUtil.getBoolean(
                        properties, ArangoDBOptions.SINK_FLUSH_ON_CHECKPOINT, false));
        configuration.setBulkFlushSize(
                PropertiesUtil.getLong(properties, ArangoDBOptions.SINK_FLUSH_SIZE, 100L));
        configuration.setBulkFlushInterval(
                PropertiesUtil.getLong(properties, ArangoDBOptions.SINK_FLUSH_INTERVAL, 1_000L));

        // validate config
        if (configuration.isTransactional()) {
            Preconditions.checkArgument(
                    configuration.isFlushOnCheckpoint(),
                    "`%s` must be true when the transactional sink is enabled",
                    ArangoDBOptions.SINK_FLUSH_ON_CHECKPOINT);
        }
        Preconditions.checkArgument(
                configuration.getBulkFlushSize() > 0,
                "`%s` must be greater than 0",
                ArangoDBOptions.SINK_FLUSH_SIZE);

        return configuration;
    }
}
