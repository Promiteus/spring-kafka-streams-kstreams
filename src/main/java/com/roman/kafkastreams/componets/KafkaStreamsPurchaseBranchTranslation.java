package com.roman.kafkastreams.componets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import com.roman.kafkastreams.models.JsonDeserializer;
import com.roman.kafkastreams.models.JsonSerializer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Profile("json-branch-value")
@Component
public class KafkaStreamsPurchaseBranchTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC = "json-branch-topic";
    private final static int LESS50 = 0;
    private final static int ABOVE50 = 1;
    private KafkaStreams kafkaStreams;
    private final Properties kafkaStreamsProps;
    private final Producer<String, String> producer;

    public KafkaStreamsPurchaseBranchTranslation(Properties kafkaStreamsProps, Producer<String, String> producer) {
        this.kafkaStreamsProps = kafkaStreamsProps;
        this.producer = producer;
    }

    /**
     * Случайна цена из диапазона
     * @param min double
     * @param max double
     * @return String
     */
    private String getRandomPrice(double min, double max) {
        double random = new Random().nextDouble();
        double val = min + (random * (max - min));
        return String.valueOf(val);
    }

    @Override
    public void exec() {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Purchase> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), purchaseSerde));

        Predicate<String, Purchase> isLess50 = (key, purchase) -> purchase.getPrice() < 50;
        Predicate<String, Purchase> isAbove50 = (key, purchase) -> purchase.getPrice() > 50;

        sourceStream.print(Printed.<String, Purchase>toSysOut().withLabel("input-data"));
        KStream<String, Purchase>[] branchStream = sourceStream.branch(isLess50, isAbove50);

        branchStream[LESS50].to("output-less-50-topic", Produced.with(Serdes.String(), purchaseSerde));
        branchStream[LESS50].print(Printed.<String, Purchase>toSysOut().withLabel("output-less-50-data"));

        branchStream[ABOVE50].to("output-above-50-topic", Produced.with(Serdes.String(), purchaseSerde));
        branchStream[ABOVE50].print(Printed.<String, Purchase>toSysOut().withLabel("output-above-50-data"));

        this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), this.kafkaStreamsProps);
        this.kafkaStreams.start();
    }

    @Override
    public void toTopic() {
        String key = null;
        Purchase purchase = Purchase.builder().id(UUID.randomUUID().toString()).name("cola").price(Double.parseDouble(this.getRandomPrice(35, 180))).timestamp(new Date().getTime()).build();
        Gson gson = new Gson();
        String value = gson.toJson(purchase);

        this.send(this.producer, INP_TOPIC, key, value);
    }

    @PreDestroy
    public void destroy() {
        this.kafkaStreams.close();
        this.producer.close();
    }
}
