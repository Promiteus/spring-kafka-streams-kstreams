package com.roman.kafkastreams.componets;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Properties;

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

    @Override
    public void exec() {

    }

    @Override
    public void toTopic() {

    }
}
