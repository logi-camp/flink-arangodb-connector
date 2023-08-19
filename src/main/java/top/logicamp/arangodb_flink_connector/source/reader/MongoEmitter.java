package top.logicamp.arangodb_flink_connector.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import org.bson.Document;
import top.logicamp.arangodb_flink_connector.serde.DocumentDeserializer;
import top.logicamp.arangodb_flink_connector.source.split.MongoSplitState;

/** A {@link RecordEmitter} that transforms a Mongo document into records fo the required type. */
public class MongoEmitter<E> implements RecordEmitter<Document, E, MongoSplitState> {

    private final DocumentDeserializer<E> deserializer;

    MongoEmitter(DocumentDeserializer<E> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(Document element, SourceOutput<E> output, MongoSplitState splitState)
            throws Exception {
        output.collect(deserializer.deserialize(element));
        splitState.increaseOffset(1);
    }
}
