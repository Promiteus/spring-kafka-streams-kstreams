package com.roman.kafkastreams;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;


@EnableScheduling
@SpringBootApplication
public class KafkaStreamsApplication {
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

    @Profile(value = {"default", "string-value", "json-value", "json-branch-value", "json-select-key-value"})
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void topicGenerator() {
        this.kafkaStreamsValueTranslation.toTopic();
    }


    @Profile(value = {"json-join-values"})
    @Scheduled(fixedDelay = 100)
    public void topicGenerator2() {
        this.kafkaStreamsValueTranslation.toTopic();
    }
}
