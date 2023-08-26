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

package top.logicamp.flink_arangodb_connector.serde;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.arangodb.entity.BaseDocument;
import org.bson.BsonDocument;

import java.util.HashMap;

/** convert rowdata to document. */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    private final RowDataToBsonConverters.RowDataToBsonConverter bsonConverter;

    private final String primaryKey;

    private transient BsonDocument node;

    public RowDataDocumentSerializer(LogicalType logicalType, String primaryKey) {
        this.bsonConverter = new RowDataToBsonConverters().createConverter(logicalType);
        this.primaryKey = primaryKey;
    }

    @Override
    public CDCDocument serialize(RowData row) {
        if (node == null) {
            node = new BsonDocument();
        }
        try {
            bsonConverter.convert(node, row);
            var doc = new BaseDocument();
            doc.setProperties(new ObjectMapper().readValue(node.toJson(), HashMap.class));
            doc.setKey(doc.getAttribute(primaryKey).toString());
            return new CDCDocument(doc, row.getRowKind());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}
