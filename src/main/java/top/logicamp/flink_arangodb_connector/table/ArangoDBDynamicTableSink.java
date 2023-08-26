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

    public ArangoDBDynamicTableSink(
            ArangoDBConnectorOptions options, ResolvedSchema resolvedSchema) {
        this.tableSchema = resolvedSchema;
        serializer =
                new RowDataDocumentSerializer(
                        resolvedSchema.toPhysicalRowDataType().getLogicalType(),
                        resolvedSchema.getPrimaryKey().get().getColumns().get(0));
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
