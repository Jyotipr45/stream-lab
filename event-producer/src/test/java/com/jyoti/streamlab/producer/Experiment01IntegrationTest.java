package com.jyoti.streamlab.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.jyoti.streamlab.producer.generator.OrderEventGenerator;
import com.jyoti.streamlab.producer.model.OrderEvent;
import com.jyoti.streamlab.producer.publisher.OrderEventPublisher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Integration test: starts a real Kafka (same image as docker-compose) in Docker,
 * runs the real Spring context against it, and checks partitions and offsets.
 * Needs Docker running.
 */
@Testcontainers
@SpringBootTest(properties = "streamlab.experiment=none") // don't auto-run the experiment; the test drives it
class Experiment01IntegrationTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Autowired
    private OrderEventGenerator generator;

    @Autowired
    private OrderEventPublisher publisher;

    @Test
    void topicIsCreatedWithThreePartitions() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            TopicDescription orders = admin.describeTopics(List.of("orders"))
                    .allTopicNames().get().get("orders");

            assertThat(orders.partitions()).hasSize(3);
        }
    }

    @Test
    void messagesAreSpreadOverPartitionsAndOffsetsCountUpPerPartition() {
        List<CompletableFuture<SendResult<String, OrderEvent>>> futures = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            futures.add(publisher.publish(generator.next()));
        }

        Map<Integer, List<Long>> offsetsByPartition = new HashMap<>();
        for (var future : futures) {
            RecordMetadata meta = future.join().getRecordMetadata();
            offsetsByPartition.computeIfAbsent(meta.partition(), p -> new ArrayList<>()).add(meta.offset());
        }

        // 30 different keys -> all 3 partitions are used.
        assertThat(offsetsByPartition).containsOnlyKeys(0, 1, 2);

        // Offsets belong to a partition, not the topic: each partition counts 0, 1, 2, ... on its own.
        offsetsByPartition.values().forEach(offsets -> {
            assertThat(offsets).isSorted();
            assertThat(offsets.get(offsets.size() - 1) - offsets.get(0)).isEqualTo(offsets.size() - 1);
        });
    }

    @Test
    void sameKeyAlwaysGoesToSamePartition() {
        OrderEvent first = generator.next();
        OrderEvent again = new OrderEvent(first.orderId(), "CUST-9", "Phone", 1, first.amount(), first.createdAt());

        int p1 = publisher.publish(first).join().getRecordMetadata().partition();
        int p2 = publisher.publish(again).join().getRecordMetadata().partition();

        assertThat(p2).isEqualTo(p1);
    }
}
