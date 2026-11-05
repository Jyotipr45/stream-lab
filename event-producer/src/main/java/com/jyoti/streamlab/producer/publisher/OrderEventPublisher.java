package com.jyoti.streamlab.producer.publisher;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.model.OrderEvent;
import java.util.concurrent.CompletableFuture;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * Sends order events to the orders topic.
 *
 * KafkaTemplate.send() does not wait for Kafka. It puts the message in an in-memory
 * buffer and returns a future. A background thread sends the buffer to the broker.
 * When the broker answers, the future completes with the partition and offset.
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
