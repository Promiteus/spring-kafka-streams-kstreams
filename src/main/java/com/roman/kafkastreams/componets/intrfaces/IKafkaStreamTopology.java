package com.roman.kafkastreams.componets.intrfaces;

import org.apache.kafka.streams.StreamsBuilder;

public interface IKafkaStreamTopology {
    void process(StreamsBuilder streamsBuilder);
}
