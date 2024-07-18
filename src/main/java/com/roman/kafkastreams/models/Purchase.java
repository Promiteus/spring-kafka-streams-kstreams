package com.roman.kafkastreams.models;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@Builder
public class Purchase implements Serializable {
    private String id;
    private String name;
    private double price;
    private long timestamp;
}
