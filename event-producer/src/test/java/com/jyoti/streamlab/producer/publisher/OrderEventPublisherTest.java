package com.jyoti.streamlab.producer.publisher;

import static org.mockito.Mockito.verify;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.model.OrderEvent;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

/** Unit test: no Kafka needed. Mockito replaces KafkaTemplate with a fake we can inspect. */
@ExtendWith(MockitoExtension.class)
class OrderEventPublisherTest {

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Test
    void sendsToConfiguredTopicUsingOrderIdAsKey() {
        var publisher = new OrderEventPublisher(kafkaTemplate, new OrdersTopicProperties("orders", 3, (short) 1));
        var event = new OrderEvent("ORD-00007", "CUST-2", "Mouse", 2,
                new BigDecimal("1600.00"), Instant.parse("2026-11-05T10:00:00Z"));

        publisher.publish(event);

        verify(kafkaTemplate).send("orders", "ORD-00007", event);
    }
}
