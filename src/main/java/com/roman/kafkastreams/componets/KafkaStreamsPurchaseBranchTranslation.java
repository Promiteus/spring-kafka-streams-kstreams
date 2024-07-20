package com.roman.kafkastreams.componets;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

@Profile("json-branch-value")
@Component
public class KafkaStreamsPurchaseBranchTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC = "json-branch-topic";
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
        return new DecimalFormat("#,##").format(val);
    }

    @Override
    public void exec() {

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
    }
}
