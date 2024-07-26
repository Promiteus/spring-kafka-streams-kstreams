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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Profile("json-join-values")
@Component
public class KafkaStreamsPurchaseJoinTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC_JOIN_1 = "input-topic-join-1";
    private final static String INP_TOPIC_JOIN_2 = "input-topic-join-2";
    private KafkaStreams kafkaStreams;
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

        JsonDeserializer<CorrelatePurchase> correlatePurchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), CorrelatePurchase.class);
        JsonSerializer<CorrelatePurchase> correlatePurchaseJsonSerializer = new JsonSerializer<>();
        Serde<CorrelatePurchase> correlatePurchaseSerde = Serdes.serdeFrom(correlatePurchaseJsonSerializer, correlatePurchaseJsonDeserializer);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Purchase> sourceStreams1 = streamsBuilder.stream(INP_TOPIC_JOIN_1, Consumed.with(Serdes.String(), purchaseSerde));
        KStream<String, Purchase> sourceStreams2 = streamsBuilder.stream(INP_TOPIC_JOIN_2, Consumed.with(Serdes.String(), purchaseSerde));

       /* sourceStreams1.print(Printed.<String, Purchase>toSysOut().withLabel("input-source-1"));
        sourceStreams2.print(Printed.<String, Purchase>toSysOut().withLabel("input-source-2"));*/

        PurchaseJoiner purchaseJoiner = new PurchaseJoiner();

        //Сообщения отправляются раз в несколько секунд по две штуки и между ними интервал 1 сек. Если JoinWindows.of() более > 1 сек, то join выдаст результат слияния в joined-data, если ниже, то не выдаст.
        //Слияние происходит только у сообщений с одинаковым ключом и разницой во временных метках не менее 1 сек - Thread.sleep(1000); в методе toTopic()
        KStream<String, CorrelatePurchase> joinedKStream = sourceStreams1.join(sourceStreams2, purchaseJoiner, JoinWindows.of(Duration.ofMillis(10000)), StreamJoined.with(Serdes.String(), purchaseSerde, purchaseSerde));

        joinedKStream.to("output-topic-join", Produced.with(Serdes.String(), correlatePurchaseSerde));
        joinedKStream.print(Printed.<String, CorrelatePurchase>toSysOut().withLabel("joined-data"));

        this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), this.kafkaStreamsProps);
        this.kafkaStreams.start();
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
        Gson gson = new Gson();
        String key = "FIXED"; // КЛЮЧ У СООБЩЕНИЙ ПОДЛЕЖЩИХ JOIN ДОЛЖЕН БЫТЬ ОДИНАКОВЫЙ

        try {
            String key1 = UUID.randomUUID().toString();
            Purchase purchase1 = Purchase.builder().id(key1).name("cola").price(Double.parseDouble(this.getRandomPrice(45, 200))).timestamp(new Date().getTime()).build();
            String value1 = gson.toJson(purchase1);
            this.send(this.producer, INP_TOPIC_JOIN_1, key, value1);
            Thread.sleep(1000);
            String key2 = UUID.randomUUID().toString();
            Purchase purchase2 = Purchase.builder().id(key2).name("Smartphone").price(Double.parseDouble(this.getRandomPrice(8000, 200000))).timestamp(new Date().getTime()).build();
            String value2 = gson.toJson(purchase2);
            this.send(this.producer, INP_TOPIC_JOIN_2, key, value2);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        this.kafkaStreams.close();
        this.producer.close();
    }
}
