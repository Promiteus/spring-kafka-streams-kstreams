package com.roman.kafkastreams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Properties;

@SpringBootApplication
public class KafkaStreamsApplication {
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

            StreamsBuilder streamsBuilder = new StreamsBuilder();
            KStream<String, String> sourceStream = streamsBuilder.stream("input-topic");

            sourceStream.print(Printed.<String, String>toSysOut().withLabel("input-data"));
            KStream<String, String> transformStream = sourceStream.filter((k, v) -> v.contains("important")).mapValues(v -> v.toUpperCase());

            transformStream.to("output-topic");
            transformStream.print(Printed.<String, String>toSysOut().withLabel("output-data"));

            KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
            kafkaStreams.start();
        };
    }
}
