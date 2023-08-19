package top.logicamp.bson.codecs;

import org.bson.BsonReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import top.logicamp.arangodb_flink_connector.bson.*;
import top.logicamp.arangodb_flink_connector.bson.CdcDocument;

/**
 * CdcDocumentCodec is a DocumentCodec with an overriden decode method that serializes bson to a
 * CdcDocument.
 */
public class CdcDocumentCodec extends DocumentCodec {

    public CdcDocumentCodec() {
        super();
    }

    @Override
    public Document decode(final BsonReader reader, final DecoderContext decoderContext) {
        Document doc = super.decode(reader, decoderContext);
        return new CdcDocument(doc);
    }
}
