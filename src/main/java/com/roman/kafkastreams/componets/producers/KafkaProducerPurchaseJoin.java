package com.roman.kafkastreams.componets.producers;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.KafkaStreamsPurchaseJoinTopology;
import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import com.roman.kafkastreams.models.Purchase;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Profile("json-join-values")
@Component
public class KafkaProducerPurchaseJoin implements IPermProducer {
    private final Producer<String, String> producer;

    public KafkaProducerPurchaseJoin(Producer<String, String> producer) {
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
        Gson gson = new Gson();
        String key = "purchase"; // КЛЮЧ У СООБЩЕНИЙ ПОДЛЕЖЩИХ JOIN ДОЛЖЕН БЫТЬ ОДИНАКОВЫЙ

        try {
            String key1 = UUID.randomUUID().toString();
            Purchase purchase1 = Purchase.builder().id(key1).name("cola").price(Double.parseDouble(this.getRandomPrice(45, 200))).timestamp(new Date().getTime()).build();
            String value1 = gson.toJson(purchase1);
            this.send(this.producer, KafkaStreamsPurchaseJoinTopology.INP_TOPIC_JOIN_1, key, value1);
            Thread.sleep(1000);
            String key2 = UUID.randomUUID().toString();
            Purchase purchase2 = Purchase.builder().id(key2).name("Smartphone").price(Double.parseDouble(this.getRandomPrice(8000, 200000))).timestamp(new Date().getTime()).build();
            String value2 = gson.toJson(purchase2);
            this.send(this.producer, KafkaStreamsPurchaseJoinTopology.INP_TOPIC_JOIN_2, key, value2);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        this.producer.close();
    }
}
