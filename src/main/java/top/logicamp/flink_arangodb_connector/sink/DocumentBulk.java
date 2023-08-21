package top.logicamp.flink_arangodb_connector.sink;

import com.arangodb.entity.BaseDocument;
import org.apache.flink.types.RowKind;
import top.logicamp.flink_arangodb_connector.serde.CDCDocument;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * DocumentBulk is buffered {@link BaseDocument} in memory, which would be written to ArangoDB in a
 * single transaction. Due to execution efficiency, each DocumentBulk maybe be limited to a maximum
 * size, typically 1,000 documents. But for the transactional mode, the maximum size should not be
 * respected because all that data must be written in one transaction.
 */
@NotThreadSafe
class DocumentBulk implements Serializable {

    private final List<CDCDocument> bufferedDocuments;

    private final long maxSize;

    private static final int BUFFER_INIT_SIZE = Integer.MAX_VALUE;

    DocumentBulk(long maxSize) {
        this.maxSize = maxSize;
        bufferedDocuments = new ArrayList<>(1024);
    }

    DocumentBulk() {
        this(BUFFER_INIT_SIZE);
    }

    int add(CDCDocument document) {
        if (bufferedDocuments.size() == maxSize) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
        return bufferedDocuments.size();
    }

    int size() {
        return bufferedDocuments.size();
    }

    boolean isFull() {
        return bufferedDocuments.size() >= maxSize;
    }

    Collection<CDCDocument> getDocuments() {
        return bufferedDocuments;
    }

    Stream<BaseDocument> getInserts() {
        return bufferedDocuments.stream().filter((i) -> i.getRowKind() == RowKind.INSERT).map(CDCDocument::getDocument);
    }

    Stream<BaseDocument> getUpdates() {
        return bufferedDocuments.stream().filter((i) -> i.getRowKind() == RowKind.UPDATE_AFTER).map((i)-> {
            var doc = i.getDocument();
            return doc;
        });
    }

    Stream<String> getDeletes() {
        return bufferedDocuments.stream().filter((i) -> { return i.getRowKind() == RowKind.DELETE;}).map((i) -> i.getDocument().getKey());
    }

    @Override
    public String toString() {
        return "DocumentBulk{"
                + "bufferedDocuments="
                + bufferedDocuments
                + ", maxSize="
                + maxSize
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentBulk)) {
            return false;
        }
        DocumentBulk bulk = (DocumentBulk) o;
        return maxSize == bulk.maxSize && Objects.equals(bufferedDocuments, bulk.bufferedDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferedDocuments, maxSize);
    }
}
