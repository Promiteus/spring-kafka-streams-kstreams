package com.roman.kafkastreams.componets.intrfaces;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.StreamsBuilder;

public interface IKafkaStreamTopology {
    void process(StreamsBuilder streamsBuilder);
    void toTopic();

    default void send(Producer<String, String> producer, String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Message sent successfully. Offset: " + recordMetadata.offset());
                } else {
                    System.err.println("Error sending message: " + e.getMessage());
                }
            }
        });
    }
}
