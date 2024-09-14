package com.roman.kafkastreams.componets.producers;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.KafkaStreamsPurchaseBranchTranslation;
import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

@Profile("json-branch-value")
@Component
public class KafkaProducerPurchaseBranch implements IPermProducer {
    private final Producer<String, String> producer;

    public KafkaProducerPurchaseBranch(Producer<String, String> producer) {
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
    public void toTopic() {
        String key = null;
        Purchase purchase = Purchase.builder()
                .id(UUID.randomUUID().toString())
                .name("cola")
                .price(Double.parseDouble(this.getRandomPrice(35, 180)))
                .timestamp(new Date().getTime())
                .build();
        Gson gson = new Gson();
        String value = gson.toJson(purchase);

        this.send(this.producer, KafkaStreamsPurchaseBranchTranslation.INP_TOPIC, key, value);
    }

    @PreDestroy
    public void destroy() {
        this.producer.close();
    }
}
