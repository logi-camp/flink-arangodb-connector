package top.logicamp.arangodb_flink_connector.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import top.logicamp.arangodb_flink_connector.config.MongoConnectorOptions;
import top.logicamp.arangodb_flink_connector.config.SinkConfiguration;
import top.logicamp.arangodb_flink_connector.config.SinkConfigurationFactory;
import top.logicamp.arangodb_flink_connector.internal.connection.MongoClientProvider;
import top.logicamp.arangodb_flink_connector.internal.connection.MongoColloctionProviders;
import top.logicamp.arangodb_flink_connector.serde.DocumentSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Flink sink connector for MongoDB. MongoSink supports transaction mode for MongoDB 4.2+ and
 * non-transaction mode for Mongo 3.0+.
 *
 * <p>In transaction mode, all writes will be buffered in memory and committed to MongoDB in
 * per-taskmanager transactions on successful checkpoints, which ensures exactly-once semantics.
 *
 * <p>In non-transaction mode, writes would be periodically flushed to MongoDB, which provides
 * at-least-once semantics.
 */
public class MongoSink<IN> implements Sink<IN> {

    private final MongoClientProvider clientProvider;

    private final DocumentSerializer<IN> serializer;

    private final MongoConnectorOptions options;

    @Deprecated
    public MongoSink(
            String host,
            Integer port,
            String database,
            String collection,
            DocumentSerializer<IN> serializer,
            Properties properties) {
        SinkConfiguration sinkConfiguration = SinkConfigurationFactory.fromProperties(properties);
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders.getBuilder()
                        .host(host)
                        .port(port)
                        .database(database)
                        .collection(collection)
                        .build();
        this.options =
                MongoConnectorOptions.builder()
                        .withDatabase(database)
                        .withCollection(collection)
                        .withConnectString(host, port)
                        .withTransactionEnable(sinkConfiguration.isTransactional())
                        .withFlushOnCheckpoint(sinkConfiguration.isFlushOnCheckpoint())
                        .withFlushSize((int) sinkConfiguration.getBulkFlushSize())
                        .withFlushInterval(
                                Duration.ofMillis(sinkConfiguration.getBulkFlushInterval()))
                        .build();
    }

    public MongoSink(DocumentSerializer<IN> serializer, MongoConnectorOptions options) {
        this.options = options;
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders.getBuilder()
                        .host(this.options.getHost())
                        .port(this.options.getPort())
                        .database(this.options.getDatabase())
                        .collection(this.options.getCollection())
                        .build();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<>(clientProvider, serializer, options);
        writer.initializeState();
        return writer;
    }
}
