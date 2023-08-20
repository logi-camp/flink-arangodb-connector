package top.logicamp.flink_arangodb_connector.serde;

import com.arangodb.entity.BaseDocument;
import org.apache.flink.types.RowKind;

public class CDCDocument {
    RowKind rowKind;
    BaseDocument document;

    CDCDocument(BaseDocument document, RowKind rowKind){
        this.rowKind = rowKind;
        this.document = document;
    }

    CDCDocument(BaseDocument document){
        this.document = document;
    }

    public RowKind getRowKind() {
        return rowKind;
    }

    public BaseDocument getDocument() {
        return document;
    }
}
