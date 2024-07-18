package com.roman.kafkastreams;

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
    private KafkaStreams kafkaStreams;
    @Autowired
    private Producer<String, String> producer;


    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner() {
        return args -> {
            System.out.println("runner");

            Properties properties = new Properties();
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-streams-app");
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            topicGenerator();

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, String> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

            sourceStream.print(Printed.<String, String>toSysOut().withLabel("input-data"));
            KStream<String, String> transformStream = sourceStream.filter((k, v) -> v.contains("important")).mapValues(v -> v.toUpperCase());

            transformStream.to("output-topic", Produced.with(Serdes.String(), Serdes.String()));
            transformStream.print(Printed.<String, String>toSysOut().withLabel("output-data"));

            kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
            kafkaStreams.start();
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

    @PreDestroy
    public void destroy() {
        System.out.println(" > close App");
        this.kafkaStreams.close();
    }
}
