package com.roman.kafkastreams.componets;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

@Profile(value = {"default", "string-value"})
@Component
public class KafkaStreamsStringTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC = "string-topic";
    private KafkaStreams kafkaStreams;
    private final Properties kafkaStreamsProps;
    private final Producer<String, String> producer;

    public KafkaStreamsStringTranslation(Properties kafkaStreamsProps, Producer<String, String> producer) {
        this.kafkaStreamsProps = kafkaStreamsProps;
        this.producer = producer;
    }

    @Override
    public void exec() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("input-data"));
        KStream<String, String> transformStream = sourceStream.filter((k, v) -> v.contains("important")).mapValues(v -> v.toUpperCase());

        transformStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        transformStream.print(Printed.<String, String>toSysOut().withLabel("output-data"));

        this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), this.kafkaStreamsProps);
        this.kafkaStreams.start();
    }

    @Override
    public void toTopic() {
        String key = UUID.randomUUID().toString();
        String value = "Hello, Kafka! important";

        this.send(this.producer, INP_TOPIC, key, value);
    }


    @PreDestroy
    public void destroy() {
        this.kafkaStreams.close();
        this.producer.close();
    }
}
