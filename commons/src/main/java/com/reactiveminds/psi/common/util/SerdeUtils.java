package com.reactiveminds.psi.common.util;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class SerdeUtils {
    private static final Serializer<Long> LONGSER = Serdes.Long().serializer();
    private static final Serializer<String> STRINGSER = Serdes.String().serializer();
    private static final Serializer<Integer> INTSER = Serdes.Integer().serializer();

    private static final Deserializer<Long> LONGDE = Serdes.Long().deserializer();
    private static final Deserializer<String> STRINGDE = Serdes.String().deserializer();
    private static final Deserializer<Integer> INTDE = Serdes.Integer().deserializer();

    public static byte[] stringToBytes(String s){
        return STRINGSER.serialize(null, s);
    }
    public static byte[] intToBytes(int s){
        return INTSER.serialize(null, s);
    }
    public static byte[] longToBytes(long s){
        return LONGSER.serialize(null, s);
    }

    public static String bytesToString(byte[] b){
        return STRINGDE.deserialize(null, b);
    }
    public static int bytesToInt(byte[] b){
        return INTDE.deserialize(null, b);
    }
    public static long bytesToLong(byte[] b){
        return LONGDE.deserialize(null, b);
    }

    @Deprecated
    /**
     *
     * @param avro
     * @param <T>
     * @return
     */
    public static <T extends SpecificRecord> byte[] toAvroBytes(T avro){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<T> writer = new SpecificDatumWriter(avro.getClass());
        try {
            writer.write(avro, encoder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }
    @Deprecated
    public static <T extends SpecificRecord> T toAvroObject(byte[] b, Class<T> ofType){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(b, null);
        DatumReader<T> reader = new SpecificDatumReader<>(ofType);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
