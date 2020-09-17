package com.reactiveminds.psi.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyAccessor;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Properties;

public class JsonUtils {
    static abstract class AvroMixin {
        @JsonIgnore
        abstract Schema getSchema();
    }

    private JsonUtils() {
    }

    public static ObjectMapper mapper() {
        return mapper;
    }

    private static final ObjectMapper mapper = new ObjectMapper();


    static {
        configureMapper();
    }

    private static void configureMapper() {
        mapper
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
                .addMixInAnnotations(SpecificRecordBase.class, AvroMixin.class);
                // either of these *mixin methods depending on Jackson version
                //.addMixIn(SpecificRecordBase.class, AvroMixin.class);

    }

    public static <T> String toJson(T object, boolean pretty) {
        try {
            ObjectWriter writer = mapper().writerFor(object.getClass());
            if(pretty)
                writer.withDefaultPrettyPrinter();
            return writer.writeValueAsString(object);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    public static String toPrettyJson(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T extends SpecificRecord> T jsonToSpecificAvroRecord(Class<T> ofType, String input) {
        try {
            return mapper.readerFor(ofType).readValue(input);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    public static <T extends SpecificRecord> String specificAvroRecordToJson(T input) {
        try {
            return mapper.writerFor(input.getClass()).writeValueAsString(input);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void copyBeanProperties(Object target, Properties props, String propPrefix){
        PropertyAccessor myAccessor = PropertyAccessorFactory.forBeanPropertyAccess(target);
        props.forEach((k,v) -> {
            String beanProp = k.toString();
            if(propPrefix != null && k.toString().startsWith(propPrefix)){
                beanProp = beanProp.substring(propPrefix.length());
            }
            try {
                myAccessor.setPropertyValue(beanProp, v);
            } catch (BeansException e) {
                e.printStackTrace();
            }
        });

    }


}
