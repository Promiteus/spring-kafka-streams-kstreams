package com.roman.kafkastreams;

import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamsValueTranslation;
import lombok.extern.slf4j.Slf4j;
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
        this.kafkaStreamsValueTranslation.toTopic();
    }
}
