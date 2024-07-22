package com.roman.kafkastreams.componets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import com.roman.kafkastreams.models.JsonDeserializer;
import com.roman.kafkastreams.models.JsonSerializer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

@Profile("json-select-key-value")
@Component
public class KafkaSteamsPurchaseSelectKeyTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC = "json-select-key-topic";
    private KafkaStreams kafkaStreams;
    private final Properties kafkaStreamsProps;
    private final Producer<String, String> producer;

    public KafkaSteamsPurchaseSelectKeyTranslation(Properties kafkaStreamsProps, Producer<String, String> producer) {
        this.kafkaStreamsProps = kafkaStreamsProps;
        this.producer = producer;
    }

    @Override
    public void exec() {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Purchase> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), purchaseSerde));

        sourceStream.print(Printed.<String, Purchase>toSysOut().withLabel("input-data"));
        KStream<String, Purchase> transformStream = sourceStream.mapValues(v ->
                Purchase.builder()
                        .id(v.getId())
                        .name(v.getName().toUpperCase())
                        .price(v.getPrice())
                        .timestamp(v.getTimestamp())
                        .build()
        );

        KeyValueMapper<String, Purchase, String> keyValueMapper = (k, v) -> {
            return v.getId();
        };
        transformStream.print(Printed.<String, Purchase>toSysOut().withLabel("output-data-without-key"));
        transformStream.selectKey((k, p) -> (keyValueMapper));
        transformStream.to("output-topic", Produced.with(Serdes.String(), purchaseSerde));
        transformStream.print(Printed.<String, Purchase>toSysOut().withLabel("output-data-with-key"));

        this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), this.kafkaStreamsProps);
        this.kafkaStreams.start();
    }

    @Override
    public void toTopic() {
        String key = null;
        Purchase purchase = Purchase.builder().id(UUID.randomUUID().toString()).name("cola").price(100).timestamp(new Date().getTime()).build();
        Gson gson = new Gson();
        String value = gson.toJson(purchase);

        this.send(this.producer, INP_TOPIC, key, value);
    }

    @PreDestroy
    public void destroy() {
        this.kafkaStreams.close();
    }
}
