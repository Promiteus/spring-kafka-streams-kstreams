package com.roman.kafkastreams;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Properties;
import java.util.UUID;

@EnableScheduling
@SpringBootApplication
public class KafkaStreamsApplication {

    private final static String INP_TOPIC = "input-topic";
    @Autowired
    private Producer<String, String> producer;
    @Autowired
    private IKafkaStreamsValueTranslation kafkaStreamsValueTranslation;

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner() {
        return args -> {
            topicGenerator();
            this.kafkaStreamsValueTranslation.exec();
        };
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void topicGenerator() {
        String key = UUID.randomUUID().toString();
        String value = "Hello, Kafka! important";

        producer.send(new ProducerRecord<>(INP_TOPIC, key, value), new Callback() {
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
