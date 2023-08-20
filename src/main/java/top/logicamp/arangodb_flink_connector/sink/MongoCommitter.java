package top.logicamp.arangodb_flink_connector.sink;

import org.apache.flink.api.connector.sink2.Committer;

import com.mongodb.*;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.StreamTransactionEntity;
import com.arangodb.entity.StreamTransactionStatus;
import com.arangodb.model.StreamTransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.logicamp.arangodb_flink_connector.internal.connection.MongoClientProvider;

import java.io.IOException;
import java.util.Collection;

/**
 * MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a
 * transaction is not recommended to be large.
 */
public class MongoCommitter implements Committer<DocumentBulk> {

    private final ArangoDB client;
    private final ArangoCollection collection;
    private final ArangoDatabase database;

    private final StreamTransactionEntity session;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoCommitter.class);

    private TransactionOptions txnOptions =
            TransactionOptions.builder()
                    .readPreference(ReadPreference.primary())
                    .readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();

    private final boolean enableUpsert;
    private final String[] upsertKeys;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

    public MongoCommitter(MongoClientProvider clientProvider) {
        this(clientProvider, false, new String[] {});
    }

    public MongoCommitter(
            MongoClientProvider clientProvider, boolean enableUpsert, String[] upsertKeys) {
        this.client = clientProvider.getClient();
        this.database = clientProvider.getDefaultDatabase();
        this.session = clientProvider.getDefaultDatabase().beginStreamTransaction(null);
        StreamTransactionEntity tx =
                this.database.beginStreamTransaction(
                        new StreamTransactionOptions()
                                .readCollections(clientProvider.getDefaultCollectionName())
                                .writeCollections(clientProvider.getDefaultCollectionName()));

        this.collection = this.database.collection(clientProvider.getDefaultCollectionName());
        this.enableUpsert = enableUpsert;
        this.upsertKeys = upsertKeys;
    }

    // @Override
    // public List<DocumentBulk> commit(List<DocumentBulk> committables) throws IOException {
    //     List<DocumentBulk> failedBulk = new ArrayList<>();

    //     for (DocumentBulk bulk : committables) {
    //         if (bulk.getDocuments().size() > 0) {
    //             CommittableTransaction transaction;
    //             if (enableUpsert) {
    //                 transaction = new CommittableCdcTransaction(
    //                         collection, bulk.getDocuments(), upsertKeys);
    //             } else {
    //                 transaction = new CommittableTransaction(collection, bulk.getDocuments());
    //             }
    //             try {
    //                 //TODO fix it
    //                 //int insertedDocs = session.withTransaction(transaction, txnOptions);

    //                 // LOGGER.info(
    //                 //         "Inserted {} documents into collection {}.",
    //                 //         insertedDocs,
    //                 //         collection.getNamespace());
    //             } catch (MongoBulkWriteException e) {
    //                 // for insertions, ignore duplicate key errors in case txn was already
    // committed
    //                 // but client was not aware of it.

    //                 // NOTE: upserts and deletes are idempotent in mongo, so no specific
    // exceptions
    //                 // need to be caught/ignored for them.
    //                 for (WriteError err : e.getWriteErrors()) {
    //                     if (err.getCode() != DUPLICATE_KEY_ERROR_CODE) {
    //                         // for now, simply requeue records when a write error
    //                         // other than duplicate key is encountered. In some cases, this may
    //                         // result in exception looping, but should not cause data loss. This
    //                         // will slow down the sink though.
    //                         // TODO: handle other write errors as necessary
    //                         LOGGER.error(
    //                                 String.format(
    //                                         "Mongo write error – requeueing %d records",
    //                                         bulk.size()),
    //                                 e);
    //                         failedBulk.add(bulk);
    //                         break;
    //                     }
    //                 }
    //                 if (failedBulk.isEmpty()) {
    //                     LOGGER.warn("Ignoring duplicate records");
    //                 }
    //             } catch (Exception e) {
    //                 // save to a new list that would be retried
    //                 LOGGER.error(
    //                         String.format(
    //                                 "Failed to commit with Mongo transaction – requeueing %d
    // records",
    //                                 bulk.size()),
    //                         e);
    //                 failedBulk.add(bulk);
    //             }
    //         }
    //     }
    //     return failedBulk;
    // }

    @Override
    public void close() throws Exception {
        long deadline = System.currentTimeMillis() + TRANSACTION_TIMEOUT_MS;
        while (database.getStreamTransaction(session.getId()).getStatus()
                        == StreamTransactionStatus.running
                && System.currentTimeMillis() < deadline) {
            // wait for active transaction to finish or timeout
            Thread.sleep(5_000L);
        }
        database.abortStreamTransaction(session.getId());
        client.shutdown();
    }

    @Override
    public void commit(Collection<CommitRequest<DocumentBulk>> committables)
            throws IOException, InterruptedException {
        committables.forEach(
                (item) -> {
                    item.getCommittable()
                            .getDocuments()
                            .forEach(
                                    (document) -> {
                                        this.collection.insertDocument(document);
                                    });
                });
    }
}
