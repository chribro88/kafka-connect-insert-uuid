package com.github.chribro88.kafka.connect.smt;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;

public class BsonToBinary {

    private static final Codec<Document> DOCUMENT_CODEC = new DocumentCodec();

    public static byte[] toBytes(Document document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        byte[] bytes = buffer.toByteArray();
        buffer.close();
    
        return bytes;
    }

    public static Document toDocument(byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        Document document = DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        reader.close();
        return document;
    }
}
