package com.roman.kafkastreams.componets;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamTopology;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


@Profile(value = {"default", "string-value"})
@Component
public class KafkaStreamsStringTransformTranslation implements IKafkaStreamTopology {
    public final static String INP_TOPIC = "string-topic";

    @Override
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, String> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("input-data"));
        KStream<String, String> transformStream = sourceStream.filter((k, v) -> v.contains("important")).mapValues(v -> v.toUpperCase());

        transformStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        transformStream.print(Printed.<String, String>toSysOut().withLabel("output-data"));
    }

}
