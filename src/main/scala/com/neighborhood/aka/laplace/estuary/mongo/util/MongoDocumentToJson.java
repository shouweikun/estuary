package com.neighborhood.aka.laplace.estuary.mongo.util;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.StringWriter;

/**
 * Created by duming on 17/1/15.
 */
public class MongoDocumentToJson {
    private final JsonWriterSettings jsonWriterSettings;
    private final DocumentCodec documentCodec;

    public MongoDocumentToJson() {
        jsonWriterSettings = new JsonWriterSettings();
        documentCodec = new DocumentCodec();
    }

    public String docToJson(Document doc) {
        JsonWriter writer = new MongoOplogJsonWriter(new StringWriter(), jsonWriterSettings);
        documentCodec.encode(writer, doc, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }
    public String docToBson(Document doc) {
        JsonWriter writer = new MongoOplogBsonWriter(new StringWriter(), jsonWriterSettings);
        documentCodec.encode(writer, doc, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }
}
