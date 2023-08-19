package top.logicamp.arangodb_flink_connector.config;

import top.logicamp.arangodb_flink_connector.sink.MongoSink;

import java.io.Serializable;

/**
 * Config options for {@link MongoSink}.
 *
 * <p>Deprecated. Please use {@link MongoConnectorOptions} instead.
 */
@Deprecated
public class MongoOptions implements Serializable {

    public static final String SINK_TRANSACTION_ENABLED = "sink.transaction.enable";

    public static final String SINK_FLUSH_ON_CHECKPOINT = "sink.flush.on-checkpoint";

    public static final String SINK_FLUSH_SIZE = "sink.flush.size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush.interval";
}
