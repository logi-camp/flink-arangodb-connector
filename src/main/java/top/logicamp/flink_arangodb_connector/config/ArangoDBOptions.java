package top.logicamp.flink_arangodb_connector.config;

import top.logicamp.flink_arangodb_connector.sink.ArangoDBSink;

import java.io.Serializable;

/**
 * Config options for {@link ArangoDBSink}.
 *
 * <p>Deprecated. Please use {@link ArangoDBConnectorOptions} instead.
 */
@Deprecated
public class ArangoDBOptions implements Serializable {

    public static final String SINK_TRANSACTION_ENABLED = "sink.transaction.enable";

    public static final String SINK_FLUSH_ON_CHECKPOINT = "sink.flush.on-checkpoint";

    public static final String SINK_FLUSH_SIZE = "sink.flush.size";

    public static final String SINK_FLUSH_INTERVAL = "sink.flush.interval";
}
