package com.roman.kafkastreams;

import com.roman.kafkastreams.componets.intrfaces.IPermProducer;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@EnableScheduling
@SpringBootApplication
public class KafkaStreamsApplication {
    @Autowired
    private IPermProducer permProducer;
    @Autowired
    private KafkaStreams kafkaStreams;
    private ExecutorService executorService;

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }


    private Runnable startStream() {
        return () -> {
            log.warn("Kafka Streams starting...");
            this.kafkaStreams.start();
        };
    }

    @Bean
    public CommandLineRunner runner() {
        return args -> {
            topicGenerator();
            log.warn("Starting service single pool...");
            this.executorService = Executors.newSingleThreadExecutor();
            this.executorService.submit(this.startStream());
        };
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    public void topicGenerator() {
        this.permProducer.toTopic();
    }

    @PreDestroy
    public void destroy() {
        log.warn("Stopping service single pool...");
        this.executorService.shutdownNow();
    }
}
