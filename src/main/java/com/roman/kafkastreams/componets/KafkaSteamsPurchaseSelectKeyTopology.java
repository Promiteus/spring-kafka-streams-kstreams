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

@Profile("json-select-key-value")
@Component
public class KafkaSteamsPurchaseSelectKeyTopology implements IKafkaStreamTopology {
    public final static String INP_TOPIC = "json-select-key-topic";

    @Override
    public void process(StreamsBuilder streamsBuilder) {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        KStream<String, Purchase> sourceStream = streamsBuilder.stream(INP_TOPIC, Consumed.with(Serdes.String(), purchaseSerde));

        KeyValueMapper<String, Purchase, String> purchaseIdMapper = (k, v) -> v.getId();
        sourceStream.print(Printed.<String, Purchase>toSysOut().withLabel("input-data"));
        KStream<String, Purchase> transformStream = sourceStream.mapValues(v ->
                Purchase.builder()
                        .id(v.getId())
                        .name(v.getName().toUpperCase())
                        .price(v.getPrice())
                        .timestamp(v.getTimestamp())
                        .build()
        ).selectKey(purchaseIdMapper);;

        transformStream.to("output-topic", Produced.with(Serdes.String(), purchaseSerde));
        transformStream.print(Printed.<String, Purchase>toSysOut().withLabel("output-data-with-key"));
    }
}
