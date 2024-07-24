package com.roman.kafkastreams.componets;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import com.roman.kafkastreams.models.Purchase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

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
            this.send(this.producer, INP_TOPIC_JOIN_1, key, value2);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
}
