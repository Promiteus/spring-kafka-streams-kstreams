package com.roman.kafkastreams.componets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.roman.kafkastreams.componets.intrfaces.IKafkaStreamTopology;
import com.roman.kafkastreams.mappers.PurchaseJoiner;
import com.roman.kafkastreams.models.CorrelatePurchase;
import com.roman.kafkastreams.models.JsonDeserializer;
import com.roman.kafkastreams.models.JsonSerializer;
import com.roman.kafkastreams.models.Purchase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Duration;


@Slf4j
@Profile("json-join-values")
@Component
public class KafkaStreamsPurchaseJoinTranslation implements IKafkaStreamTopology {
    public final static String INP_TOPIC_JOIN_1 = "input-topic-join-1";
    public final static String INP_TOPIC_JOIN_2 = "input-topic-join-2";

    @Override
    public void process(StreamsBuilder streamsBuilder) {
        JsonDeserializer<Purchase> purchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), Purchase.class);
        JsonSerializer<Purchase> purchaseJsonSerializer = new JsonSerializer<>();
        Serde<Purchase> purchaseSerde = Serdes.serdeFrom(purchaseJsonSerializer, purchaseJsonDeserializer);

        JsonDeserializer<CorrelatePurchase> correlatePurchaseJsonDeserializer = new JsonDeserializer<>(new ObjectMapper(), CorrelatePurchase.class);
        JsonSerializer<CorrelatePurchase> correlatePurchaseJsonSerializer = new JsonSerializer<>();
        Serde<CorrelatePurchase> correlatePurchaseSerde = Serdes.serdeFrom(correlatePurchaseJsonSerializer, correlatePurchaseJsonDeserializer);

        KStream<String, Purchase> sourceStreams1 = streamsBuilder.stream(INP_TOPIC_JOIN_1, Consumed.with(Serdes.String(), purchaseSerde));
        KStream<String, Purchase> sourceStreams2 = streamsBuilder.stream(INP_TOPIC_JOIN_2, Consumed.with(Serdes.String(), purchaseSerde));

       /* sourceStreams1.print(Printed.<String, Purchase>toSysOut().withLabel("input-source-1"));
        sourceStreams2.print(Printed.<String, Purchase>toSysOut().withLabel("input-source-2"));*/

        PurchaseJoiner purchaseJoiner = new PurchaseJoiner();

        //JoinWindows.of() задет предельное врменное окно между сообщениями.
        //Сообщения отправляются раз в несколько секунд по две штуки и между ними интервал 1 сек. Если JoinWindows.of() более > 1 сек, то join выдаст результат слияния в joined-data, если ниже, то не выдаст.
        //Слияние происходит только у сообщений с одинаковым ключом и разницой во временных метках не менее 1 сек - Thread.sleep(1000); в методе toTopic()
        KStream<String, CorrelatePurchase> joinedKStream = sourceStreams1.join(sourceStreams2, purchaseJoiner, JoinWindows.of(Duration.ofMillis(100)), StreamJoined.with(Serdes.String(), purchaseSerde, purchaseSerde));

        joinedKStream.to("output-topic-join", Produced.with(Serdes.String(), correlatePurchaseSerde));
        joinedKStream.print(Printed.<String, CorrelatePurchase>toSysOut().withLabel("joined-data"));
    }




}
