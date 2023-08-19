package top.logicamp.arangodb_flink_connector.sink;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.MultiDocumentEntity;
import com.mongodb.client.TransactionBody;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A simple implementation of Mongo transaction body. */
public class CommittableTransaction implements TransactionBody<Integer>, Serializable {

    protected final ArangoCollection collection;

    protected List<BaseDocument> bufferedDocuments = new ArrayList<>(BUFFER_INIT_SIZE);

    private static final int BUFFER_INIT_SIZE = 1024;

    public CommittableTransaction(ArangoCollection collection, List<BaseDocument> documents) {
        this.collection = collection;
        this.bufferedDocuments.addAll(documents);
    }

    @Override
    public Integer execute() {
        MultiDocumentEntity result = collection.insertDocuments(bufferedDocuments);
        //TODO Verify that is act like original Mongo connector
        return result.getDocuments().size();
    }
}
