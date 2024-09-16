package com.roman.kafkastreams.componets.producers;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.KafkaStreamsPurchaseTopology;
import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.Random;


@Profile("json-value")
@Component
public class KafkaProducerPurchaseData implements IPermProducer {
    private final Producer<String, String> producer;

    public KafkaProducerPurchaseData(Producer<String, String> producer) {
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
    public void toTopic() {
        String key = null;
        Purchase purchase = Purchase.builder()
                .id(java.util.UUID.randomUUID().toString())
                .name("pencil")
                .price(Double.parseDouble(this.getRandomPrice(10, 100)))
                .timestamp(new Date().getTime())
                .build();
        Gson gson = new Gson();
        String value = gson.toJson(purchase);

        this.send(this.producer, KafkaStreamsPurchaseTopology.INP_TOPIC, key, value);
    }

    @PreDestroy
    public void destroy() {
        this.producer.close();
    }
}
