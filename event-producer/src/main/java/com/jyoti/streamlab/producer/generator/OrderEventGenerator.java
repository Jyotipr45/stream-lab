package com.jyoti.streamlab.producer.generator;

import com.jyoti.streamlab.producer.model.OrderEvent;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates fake but realistic order events.
 *
 * Order ids are sequential (ORD-00001, ORD-00002, ...) so the key of every message
 * is predictable. Kafka picks the partition from the key, so predictable keys give
 * predictable partitions - useful for checking your understanding.
 */
public class OrderEventGenerator {

    static final int CUSTOMER_COUNT = 5;

    private static final List<Product> CATALOG = List.of(
            new Product("Laptop", new BigDecimal("55000.00")),
            new Product("Phone", new BigDecimal("20000.00")),
            new Product("Headphones", new BigDecimal("2500.00")),
            new Product("Keyboard", new BigDecimal("1500.00")),
            new Product("Mouse", new BigDecimal("800.00")));

    private final Clock clock;
    private final Random random;
    private final AtomicLong sequence = new AtomicLong();

    public OrderEventGenerator(Clock clock, Random random) {
        this.clock = clock;
        this.random = random;
    }

    public OrderEvent next() {
        long number = sequence.incrementAndGet();
        Product product = CATALOG.get(random.nextInt(CATALOG.size()));
        int quantity = 1 + random.nextInt(3);

        return new OrderEvent(
                "ORD-%05d".formatted(number),
                "CUST-" + (1 + random.nextInt(CUSTOMER_COUNT)),
                product.name(),
                quantity,
                product.price().multiply(BigDecimal.valueOf(quantity)),
                Instant.now(clock));
    }

    private record Product(String name, BigDecimal price) {
    }
}
