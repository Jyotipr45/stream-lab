package com.jyoti.streamlab.producer.publisher;

import static org.mockito.Mockito.verify;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.model.OrderEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

/** Unit test: no Kafka needed. Mockito replaces KafkaTemplate with a fake we can inspect. */
@ExtendWith(MockitoExtension.class)
class OrderEventPublisherTest {

    private static final String TOPIC = "orders.order.placed.v1";

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Test
    void sendsToConfiguredTopicUsingOrderIdAsKey() {
        var props = new OrdersTopicProperties(TOPIC, 3, (short) 1, Duration.ofDays(7));
        var publisher = new OrderEventPublisher(kafkaTemplate, props);
        var event = new OrderEvent("ORD-00007", "CUST-2", "Mouse", 2,
                new BigDecimal("1600.00"), Instant.parse("2026-11-05T10:00:00Z"));

        publisher.publish(event);

        // The topic name comes from configuration, and the key is the orderId.
        verify(kafkaTemplate).send(TOPIC, "ORD-00007", event);
    }
}
