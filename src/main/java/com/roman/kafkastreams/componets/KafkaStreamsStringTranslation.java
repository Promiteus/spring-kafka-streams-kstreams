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
public class KafkaStreamsStringTranslation implements IKafkaStreamTopology {
    private final static String INP_TOPIC = "string-topic";
    private final Producer<String, String> producer;

    public KafkaStreamsStringTranslation(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, String> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("input-data"));
        KStream<String, String> transformStream = sourceStream.filter((k, v) -> v.contains("important")).mapValues(v -> v.toUpperCase());

        transformStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
        transformStream.print(Printed.<String, String>toSysOut().withLabel("output-data"));
    }

  /*  @Override
    public void toTopic() {
        String key = UUID.randomUUID().toString();
        String value = "Hello, Kafka! important";

        this.send(this.producer, INP_TOPIC, key, value);
    }


    @PreDestroy
    public void destroy() {
        this.producer.close();
    }*/
}
