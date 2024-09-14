package com.roman.kafkastreams.componets.producers;

import com.roman.kafkastreams.componets.KafkaStreamsStringTransformTranslation;
import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Profile(value = {"default", "string-value"})
@Component
public class KafkaProducerStringTransform implements IPermProducer {
    private final Producer<String, String> producer;

    public KafkaProducerStringTransform(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void toTopic() {
        String key = UUID.randomUUID().toString();
        String value = "Hello, Kafka! important";

        this.send(this.producer, KafkaStreamsStringTransformTranslation.INP_TOPIC, key, value);
    }

    @PreDestroy
    public void destroy() {
        this.producer.close();
    }
}
