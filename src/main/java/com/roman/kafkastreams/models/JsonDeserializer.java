package com.roman.kafkastreams.models;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper;
    private final TypeReference<T> typeReference;


    public JsonDeserializer(ObjectMapper mapper, TypeReference<T> typeReference) {
        this.mapper = mapper;
        this.typeReference = typeReference;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return mapper.readValue(bytes, typeReference);
        } catch (final IOException ex) {
            throw new SerializationException("Can't deserialize data [" + Arrays.toString(bytes) + "]", ex);
        }
    }

    private Class<T> deserializeFrom() {
        return null;
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, typeReference);
        } catch (final IOException ex) {
            throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) + "] from topic [" + topic + "]", ex);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, ByteBuffer data) {
        return null;
    }

    @Override
    public void close() {

    }
}
