package top.logicamp.arangodb_flink_connector.serde;

import org.apache.flink.table.data.RowData;

import com.arangodb.entity.BaseDocument;

/** convert rowdata to document. */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    @Override
    public BaseDocument serialize(RowData row) {
        try {
            BaseDocument doc = new BaseDocument();
            doc.setProperties(doc.getProperties());
            return doc;
        } catch (Exception e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}
