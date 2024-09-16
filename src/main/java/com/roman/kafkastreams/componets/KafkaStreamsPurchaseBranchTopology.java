package com.roman.kafkastreams.componets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamTopology;
import com.roman.kafkastreams.models.JsonDeserializer;
import com.roman.kafkastreams.models.JsonSerializer;
import com.roman.kafkastreams.models.Purchase;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;


@Profile("json-branch-value")
@Component
public class KafkaStreamsPurchaseBranchTopology implements IKafkaStreamTopology {
    public final static String INP_TOPIC = "json-branch-topic";
    public final static int LESS50 = 0;
    public final static int ABOVE50 = 1;


    @Override
    public void process(StreamsBuilder streamsBuilder) {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        KStream<String, Purchase> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), purchaseSerde));

        Predicate<String, Purchase> isLess50 = (key, purchase) -> purchase.getPrice() < 50;
        Predicate<String, Purchase> isAbove50 = (key, purchase) -> purchase.getPrice() > 50;

        sourceStream.print(Printed.<String, Purchase>toSysOut().withLabel("input-data"));
        KStream<String, Purchase>[] branchStream = sourceStream.branch(isLess50, isAbove50);

        branchStream[LESS50].to("output-less-50-topic", Produced.with(Serdes.String(), purchaseSerde));
        branchStream[LESS50].print(Printed.<String, Purchase>toSysOut().withLabel("output-less-50-data"));

        branchStream[ABOVE50].to("output-above-50-topic", Produced.with(Serdes.String(), purchaseSerde));
        branchStream[ABOVE50].print(Printed.<String, Purchase>toSysOut().withLabel("output-above-50-data"));
    }

}
