package top.logicamp.arangodb_flink_connector.sink;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;


import org.bson.Document;
import org.bson.conversions.Bson;
import top.logicamp.arangodb_flink_connector.bson.CdcDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple implementation of Mongo transaction body, which supports insertions,
 * upserts, and
 * deletes.
 */
public class CommittableCdcTransaction extends CommittableTransaction {

    private final String[] upsertKeys;
    private final UpdateOptions updateOptions = new UpdateOptions();
    private final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();

    public CommittableCdcTransaction(
            ArangoCollection collection, List<BaseDocument> documents, String[] upsertKeys) {
        super(collection, documents);
        this.upsertKeys = upsertKeys;
        updateOptions.upsert(true);
        bulkWriteOptions.ordered(true);
    }

    @Override
    public Integer execute() {
        BulkWriteResult bulkWriteResult =
                collection.replaceDocument( (
                        getWrites(bufferedDocuments, upsertKeys, updateOptions));

        return bulkWriteResult.getUpserts().size() + bulkWriteResult.getInsertedCount();
    }

    public static List<BaseDocument> getWrites(
            List<BaseDocument> bufferedDocuments, String[] upsertKeys, UpdateOptions updateOptions) {
        List<Document> writes = new ArrayList<>(); // interleave upserts and deletes to preserve order of
                                                               // operations


        for (BaseDocument document : bufferedDocuments) {
            List<Bson> filters = new ArrayList<>(upsertKeys.length);
            for (String upsertKey : upsertKeys) {
                Object o = document.get(upsertKey);
                Bson eq = Filters.eq(upsertKey, o);
                filters.add(eq);
            }
            Bson filter = Filters.and(filters);
            BaseDocument model;
            if (((CdcDocument) document).isDelete()) {
                model = new DeleteOneModel<>(filter);
            } else {
                Document update = new Document();
                update.append("$set", document);
                model = new UpdateOneModel<>(filter, update, updateOptions);
            }
            writes.add(model);
        }
        return writes;
    }
}
