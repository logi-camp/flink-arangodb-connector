package top.logicamp.flink_arangodb_connector.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import top.logicamp.flink_arangodb_connector.config.ArangoDBConnectorOptions;
import top.logicamp.flink_arangodb_connector.config.SinkConfiguration;
import top.logicamp.flink_arangodb_connector.config.SinkConfigurationFactory;
import top.logicamp.flink_arangodb_connector.internal.connection.ArangoDBClientProvider;
import top.logicamp.flink_arangodb_connector.internal.connection.ArangoDBColloctionProviders;
import top.logicamp.flink_arangodb_connector.serde.DocumentSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Flink sink connector for ArangoDB.
 */
public class ArangoDBSink<IN> implements Sink<IN> {

    private final ArangoDBClientProvider clientProvider;

    private final DocumentSerializer<IN> serializer;

    private final ArangoDBConnectorOptions options;

    @Deprecated
    public ArangoDBSink(
            String host,
            Integer port,
            String database,
            String collection,
            String user,
            String password,
            Boolean useSsl,
            DocumentSerializer<IN> serializer,
            Properties properties) {
        SinkConfiguration sinkConfiguration = SinkConfigurationFactory.fromProperties(properties);
        this.serializer = serializer;
        this.clientProvider =
                ArangoDBColloctionProviders.getBuilder()
                        .host(host)
                        .port(port)
                        .database(database)
                        .collection(collection)
                        .user(user)
                        .password(password)
                        .useSsl(useSsl)
                        .build();
        this.options =
                ArangoDBConnectorOptions.builder()
                        .withDatabase(database)
                        .withCollection(collection)
                        .withHost(host)
                        .withPort(port)
                        .withPassword(password)
                        .withUser(user)
                        .withUseSsl(useSsl)
                        .withTransactionEnable(sinkConfiguration.isTransactional())
                        .withFlushOnCheckpoint(sinkConfiguration.isFlushOnCheckpoint())
                        .withFlushSize((int) sinkConfiguration.getBulkFlushSize())
                        .withFlushInterval(
                                Duration.ofMillis(sinkConfiguration.getBulkFlushInterval()))
                        .build();
    }

    public ArangoDBSink(DocumentSerializer<IN> serializer, ArangoDBConnectorOptions options) {
        this.options = options;
        this.serializer = serializer;
        this.clientProvider =
                ArangoDBColloctionProviders.getBuilder()
                        .host(this.options.getHost())
                        .port(this.options.getPort())
                        .user(this.options.getUser())
                        .password(this.options.getPassword())
                        .useSsl(this.options.getUseSsl())
                        .database(this.options.getDatabase())
                        .collection(this.options.getCollection())
                        .build();
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        ArangoDBBulkWriter<IN> writer = new ArangoDBBulkWriter<>(clientProvider, serializer, options);
        writer.initializeState();
        return writer;
    }
}
