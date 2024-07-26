package com.roman.kafkastreams.mappers;

import com.roman.kafkastreams.models.CorrelatePurchase;
import com.roman.kafkastreams.models.Purchase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class PurchaseJoiner implements ValueJoiner<Purchase, Purchase, CorrelatePurchase> {
    @Override
    public CorrelatePurchase apply(Purchase purchase, Purchase purchase2) {
        CorrelatePurchase.CorrelatePurchaseBuilder correlatePurchase = CorrelatePurchase.builder();

        List<Purchase> purchaseList = new ArrayList<>();
        Optional.ofNullable(purchase).ifPresent(p -> {
            correlatePurchase.firstPurchaseDateTime(p.getTimestamp());
            purchaseList.add(p);
        });

        Optional.ofNullable(purchase2).ifPresent(p -> {
            correlatePurchase.secondPurchaseDateTime(p.getTimestamp());
            purchaseList.add(p);
        });

        double totalPrice = purchaseList.size() > 0 ? purchaseList.stream().map(Purchase::getPrice).reduce(Double::sum).get() : 0;
        correlatePurchase.totalPrice(totalPrice);
        correlatePurchase.purchases(purchaseList);

        log.warn(">>>>> totalPrice: "+totalPrice);

        return correlatePurchase.build();
    }
}
