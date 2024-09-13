package com.roman.kafkastreams;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@EnableScheduling
@SpringBootApplication
public class KafkaStreamsApplication {
    @Autowired
    private IKafkaStreamTopology kafkaStreamsValueTranslation;
    @Autowired
    private StreamsBuilder streamsBuilder;

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner() {
        return args -> {
            topicGenerator();
            this.kafkaStreamsValueTranslation.process(streamsBuilder);
        };
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void topicGenerator() {
        this.kafkaStreamsValueTranslation.toTopic();
    }
}
