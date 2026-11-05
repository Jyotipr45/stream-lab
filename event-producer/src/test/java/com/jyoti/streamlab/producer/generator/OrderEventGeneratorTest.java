package com.jyoti.streamlab.producer.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.jyoti.streamlab.producer.model.OrderEvent;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Random;
import org.junit.jupiter.api.Test;

class OrderEventGeneratorTest {

    private static final Instant NOW = Instant.parse("2026-11-05T10:00:00Z");
    private final OrderEventGenerator generator =
            new OrderEventGenerator(Clock.fixed(NOW, ZoneOffset.UTC), new Random(42));

    @Test
    void orderIdsAreSequentialAndZeroPadded() {
        assertThat(generator.next().orderId()).isEqualTo("ORD-00001");
        assertThat(generator.next().orderId()).isEqualTo("ORD-00002");
        assertThat(generator.next().orderId()).isEqualTo("ORD-00003");
    }

    @Test
    void eventsAreValid() {
        for (int i = 0; i < 100; i++) {
            OrderEvent event = generator.next();

            assertThat(event.customerId()).matches("CUST-[1-5]");
            assertThat(event.quantity()).isBetween(1, 3);
            assertThat(event.amount()).isGreaterThan(BigDecimal.ZERO);
            assertThat(event.createdAt()).isEqualTo(NOW);
        }
    }

    @Test
    void sameSeedGivesSameEvents() {
        OrderEventGenerator other = new OrderEventGenerator(Clock.fixed(NOW, ZoneOffset.UTC), new Random(42));

        for (int i = 0; i < 20; i++) {
            assertThat(other.next()).isEqualTo(generator.next());
        }
    }
}
