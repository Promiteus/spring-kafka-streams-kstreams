package com.roman.kafkastreams.componets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import com.roman.kafkastreams.mappers.PurchaseJoiner;
import com.roman.kafkastreams.models.CorrelatePurchase;
import com.roman.kafkastreams.models.JsonDeserializer;
import com.roman.kafkastreams.models.JsonSerializer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Profile("json-join-values")
@Component
public class KafkaStreamsPurchaseJoinTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC_JOIN_1 = "input-topic-join-1";
    private final static String INP_TOPIC_JOIN_2 = "input-topic-join-2";
    private KafkaStreams kafkaStreams1;
    private KafkaStreams kafkaStreams2;
    private final Properties kafkaStreamsProps;
    private final Producer<String, String> producer;

    public KafkaStreamsPurchaseJoinTranslation(Properties kafkaStreamsProps, Producer<String, String> producer) {
        this.kafkaStreamsProps = kafkaStreamsProps;
        this.producer = producer;
    }

    @Override
    public void exec() {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        StreamsBuilder streamsBuilder1 = new StreamsBuilder();
        KStream<String, Purchase> sourceStreams1 = streamsBuilder1.stream(INP_TOPIC_JOIN_1, Consumed.with(Serdes.String(), purchaseSerde));

        StreamsBuilder streamsBuilder2 = new  StreamsBuilder();
        KStream<String, Purchase> sourceStreams2 = streamsBuilder2.stream(INP_TOPIC_JOIN_2, Consumed.with(Serdes.String(), purchaseSerde));

        PurchaseJoiner purchaseJoiner = new PurchaseJoiner();

        KStream<String, CorrelatePurchase> joinedKStream = sourceStreams1.join(sourceStreams2, purchaseJoiner, JoinWindows.of(Duration.ofSeconds(1)));

        joinedKStream.print(Printed.<String, CorrelatePurchase>toSysOut().withLabel("joined-data"));

        this.kafkaStreams1 = new KafkaStreams(streamsBuilder1.build(), this.kafkaStreamsProps);
        this.kafkaStreams1.start();
        this.kafkaStreams2 = new KafkaStreams(streamsBuilder2.build(), this.kafkaStreamsProps);
        this.kafkaStreams2.start();
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
    public void toTopic() {
        String key = null;
        Gson gson = new Gson();

        try {
            Purchase purchase1 = Purchase.builder().id(UUID.randomUUID().toString()).name("cola").price(Double.parseDouble(this.getRandomPrice(45, 200))).timestamp(new Date().getTime()).build();
            String value1 = gson.toJson(purchase1);
            this.send(this.producer, INP_TOPIC_JOIN_1, key, value1);
            Thread.sleep(2000);
            Purchase purchase2 = Purchase.builder().id(UUID.randomUUID().toString()).name("Smartphone").price(Double.parseDouble(this.getRandomPrice(8000, 200000))).timestamp(new Date().getTime()).build();
            String value2 = gson.toJson(purchase2);
            this.send(this.producer, INP_TOPIC_JOIN_2, key, value2);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        this.kafkaStreams2.close();
        this.kafkaStreams1.close();
        this.producer.close();
    }
}
