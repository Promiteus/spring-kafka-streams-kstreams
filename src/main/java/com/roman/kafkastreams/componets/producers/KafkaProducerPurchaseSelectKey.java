package com.roman.kafkastreams.componets.producers;

import com.google.gson.Gson;
import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import com.roman.kafkastreams.models.Purchase;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

import static com.roman.kafkastreams.componets.KafkaSteamsPurchaseSelectKeyTranslation.INP_TOPIC;

@Profile("json-select-key-value")
@Component
public class KafkaProducerPurchaseSelectKey implements IPermProducer {
    private final Producer<String, String> producer;

    public KafkaProducerPurchaseSelectKey(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void toTopic() {
        String key = null;
        Purchase purchase = Purchase.builder().id(UUID.randomUUID().toString()).name("cola").price(100).timestamp(new Date().getTime()).build();
        Gson gson = new Gson();
        String value = gson.toJson(purchase);

        this.send(this.producer, INP_TOPIC, key, value);
    }

    public void destroy() {
        this.producer.close();
    }
}
