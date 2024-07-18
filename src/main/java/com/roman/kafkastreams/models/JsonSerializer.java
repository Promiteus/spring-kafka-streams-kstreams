package com.roman.kafkastreams.models;

import com.google.gson.Gson;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }


    @Override
    public void close() {

    }
}
