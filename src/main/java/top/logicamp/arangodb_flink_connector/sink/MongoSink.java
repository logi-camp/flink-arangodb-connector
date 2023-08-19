package top.logicamp.arangodb_flink_connector.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import top.logicamp.arangodb_flink_connector.config.MongoConnectorOptions;
import top.logicamp.arangodb_flink_connector.config.SinkConfiguration;
import top.logicamp.arangodb_flink_connector.config.SinkConfigurationFactory;
import top.logicamp.arangodb_flink_connector.internal.connection.MongoClientProvider;
import top.logicamp.arangodb_flink_connector.internal.connection.MongoColloctionProviders;
import top.logicamp.arangodb_flink_connector.serde.DocumentSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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
public class MongoSink<IN> implements Sink<IN, DocumentBulk, DocumentBulk, Void> {

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
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(
            InitContext initContext, List<DocumentBulk> states) throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<>(clientProvider, serializer, options);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (options.isTransactionEnable()) {
            if (options.isUpsertEnable()) {
                String[] upsertKeys = options.getUpsertKey();
                return Optional.of(
                        new MongoCommitter(clientProvider, options.isUpsertEnable(), upsertKeys));
            }
            return Optional.of(new MongoCommitter(clientProvider));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        if (options.isTransactionEnable()) {
            return Optional.of(DocumentBulkSerializer.INSTANCE);
        }
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        if (options.isTransactionEnable()) {
            return Optional.of(DocumentBulkSerializer.INSTANCE);
        }
        return Optional.empty();
    }
}
