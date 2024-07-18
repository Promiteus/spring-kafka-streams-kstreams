package com.roman.kafkastreams.componets;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaStreamsStringTranslation implements IKafkaStreamsValueTranslation {
    private final static String INP_TOPIC = "input-topic";
    private KafkaStreams kafkaStreams;
    private final Properties kafkaStreamsProps;

    public KafkaStreamsStringTranslation(Properties kafkaStreamsProps) {
        this.kafkaStreamsProps = kafkaStreamsProps;
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


    @PreDestroy
    public void destroy() {
        System.out.println(" > close App");
        this.kafkaStreams.close();
    }
}
