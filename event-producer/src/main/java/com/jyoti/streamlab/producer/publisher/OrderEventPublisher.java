package com.jyoti.streamlab.producer.publisher;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.model.OrderEvent;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * Publishes {@link OrderEvent}s to the order-placed topic ({@code orders.order.placed.v1}).
 *
 * <p><b>Asynchronous send:</b> {@code KafkaTemplate.send()} does not wait for Kafka. It puts
 * the record in an in-memory buffer and returns a future at once. A background sender thread
 * ships the buffer to the broker in batches; when the broker answers, the future completes
 * with the partition and offset (or fails with the error).
 *
 * <p><b>Key choice:</b> the key is the orderId, so every event of one order lands in the same
 * partition and is read in the order it was written. Choose the key by "which events must stay
 * in order?", not by convenience. A low-variety key (e.g. country) creates hot partitions.
 *
 * <p>The topic name is injected from configuration, never hard-coded, so renaming or
 * versioning the topic is a config change.
 */
@Service
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final String topic;

    public OrderEventPublisher(KafkaTemplate<String, OrderEvent> kafkaTemplate, OrdersTopicProperties topicProps) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topicProps.name();
    }

    public CompletableFuture<SendResult<String, OrderEvent>> publish(OrderEvent event) {
        // key = orderId -> Kafka hashes the key to choose the partition.
        return kafkaTemplate.send(topic, event.orderId(), event);
    }
}
