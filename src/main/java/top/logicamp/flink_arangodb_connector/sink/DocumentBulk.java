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

package top.logicamp.flink_arangodb_connector.sink;

import org.apache.flink.types.RowKind;

import com.arangodb.entity.BaseDocument;
import top.logicamp.flink_arangodb_connector.serde.CDCDocument;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;

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

    Collection<List<CDCDocument>> groupedByKey() {
        return bufferedDocuments.stream()
                .collect(groupingBy(document -> document.getDocument().getKey()))
                .values();
    }

    Stream<CDCDocument> singles() {
        return groupedByKey().stream()
                .filter((list) -> list.size() <= 1)
                .map((list) -> list.get(0));
    }

    Stream<CDCDocument> groupsEndWith_LastItem(RowKind rowKind) {
        return groupedByKey().stream()
                .filter((list) -> list.size() > 1) // Not singles
                .filter((list) -> list.get(list.size() - 1).getRowKind() == rowKind) // Last item
                .map((list) -> list.get(list.size() - 1));
    }

    Stream<BaseDocument> getInserts() {
        return singles()
                .filter((i) -> i.getRowKind() == RowKind.INSERT)
                .map(CDCDocument::getDocument);
    }

    Stream<BaseDocument> getUpdates() {
        return singles()
                .filter((i) -> i.getRowKind() == RowKind.UPDATE_AFTER)
                .map(
                        (i) -> {
                            var doc = i.getDocument();
                            return doc;
                        });
    }

    Stream<String> getDeletes() {
        return Stream.concat(
                singles()
                        .filter((i) -> i.getRowKind() == RowKind.DELETE)
                        .map((i) -> i.getDocument().getKey()), // Single group deletes
                groupsEndWith_LastItem(RowKind.DELETE)
                        .map((doc) -> doc.getDocument().getKey()) // Groups end with delete
                );
    }

    Stream<Map<String, Object>> getUpserts() {
        return groupedByKey().stream()
                .filter((list) -> list.size() > 1) // Not singles
                .filter(
                        (list) -> {
                            var lastRowKind = list.get(list.size() - 1).getRowKind();
                            return lastRowKind == RowKind.INSERT
                                    || lastRowKind == RowKind.UPDATE_AFTER;
                        }) // Last item
                .map((list) -> list.get(list.size() - 1).getDocument().getProperties());
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
