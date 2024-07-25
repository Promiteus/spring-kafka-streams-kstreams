package com.roman.kafkastreams.models;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CorrelatePurchase implements Serializable {
    private long firstPurchaseDateTime = 0;
    private long secondPurchaseDateTime = 0;
    private double totalPrice = 0;
    private List<Purchase> purchases = new ArrayList<>();
}
