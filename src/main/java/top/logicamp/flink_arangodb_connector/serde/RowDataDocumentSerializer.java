package top.logicamp.flink_arangodb_connector.serde;

import com.arangodb.entity.BaseDocument;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;

import org.apache.flink.table.types.logical.LogicalType;
import org.bson.BsonDocument;


import java.util.HashMap;

/**
 * convert rowdata to document.
 */

/**
 * convert rowdata to document.
 */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    private final RowDataToBsonConverters.RowDataToBsonConverter bsonConverter;

    private final String primaryKey;

    private transient BsonDocument node;

    public RowDataDocumentSerializer(LogicalType logicalType, String primaryKey) {
        this.bsonConverter = new RowDataToBsonConverters().createConverter(logicalType);
        this.primaryKey = primaryKey;
    }

    @Override
    public BaseDocument serialize(RowData row) {
        if (node == null) {
            node = new BsonDocument();
        }
        try {
            bsonConverter.convert(node, row);
            row.getRowKind();
            var doc = new BaseDocument();
            doc.setProperties(new ObjectMapper().readValue(node.toJson(), HashMap.class));
            doc.setKey(doc.getAttribute(primaryKey).toString());
            return doc;
        } catch (Exception e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}