package com.jyoti.streamlab.producer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One "an order was placed" fact. This is the message VALUE we send to Kafka.
 * The message KEY is {@link #orderId()}.
 *
 * A Java 17 record: immutable, with constructor, getters, equals/hashCode and toString generated.
 */
public record OrderEvent(
        String orderId,
        String customerId,
        String product,
        int quantity,
        BigDecimal amount,
        Instant createdAt) {
}
