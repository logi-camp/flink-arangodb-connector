package top.logicamp.flink_arangodb_connector.table;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import top.logicamp.flink_arangodb_connector.config.ArangoDBConnectorOptions;
import top.logicamp.flink_arangodb_connector.serde.DocumentSerializer;
import top.logicamp.flink_arangodb_connector.serde.RowDataDocumentSerializer;
import top.logicamp.flink_arangodb_connector.sink.ArangoDBSink;

public class ArangoDBDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema tableSchema;

    private final DocumentSerializer<RowData> serializer;
    private final ArangoDBConnectorOptions options;

    public ArangoDBDynamicTableSink(ArangoDBConnectorOptions options, ResolvedSchema resolvedSchema) {
        this.tableSchema = resolvedSchema;
        serializer = new RowDataDocumentSerializer(resolvedSchema.toPhysicalRowDataType().getLogicalType(), resolvedSchema.getPrimaryKey().get().getColumns().get(0));
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
        return SinkV2Provider.of(new ArangoDBSink<>(serializer, options));
    }

    @Override
    public DynamicTableSink copy() {
        return new ArangoDBDynamicTableSink(options, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "ArangoDB Table Sink";
    }
}
