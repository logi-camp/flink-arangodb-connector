package top.logicamp.arangodb_flink_connector.table;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import top.logicamp.arangodb_flink_connector.config.MongoConnectorOptions;
import top.logicamp.arangodb_flink_connector.serde.DocumentSerializer;
import top.logicamp.arangodb_flink_connector.serde.RowDataDocumentSerializer;
import top.logicamp.arangodb_flink_connector.sink.MongoSink;

public class MongoDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema tableSchema;

    private final DocumentSerializer<RowData> serializer;
    private final MongoConnectorOptions options;

    public MongoDynamicTableSink(MongoConnectorOptions options, ResolvedSchema resolvedSchema) {
        this.tableSchema = resolvedSchema;
        serializer = new RowDataDocumentSerializer();
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new MongoSink<>(serializer, options));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongoDynamicTableSink(options, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Table Sink";
    }
}
