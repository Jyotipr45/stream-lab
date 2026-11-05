package com.jyoti.streamlab.producer;

import static org.assertj.core.api.Assertions.assertThat;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.generator.OrderEventGenerator;
import com.jyoti.streamlab.producer.model.OrderEvent;
import com.jyoti.streamlab.producer.publisher.OrderEventPublisher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
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

    @Autowired
    private OrdersTopicProperties topicProps;

    @Test
    void topicFollowsNamingConvention() {
        // <domain>.<entity>.<event>.v<version>, lower case, dots between parts, hyphens inside a part.
        assertThat(topicProps.name())
                .isEqualTo("orders.order.placed.v1")
                .matches("[a-z0-9-]+\\.[a-z0-9-]+\\.[a-z0-9-]+\\.v[0-9]+");
    }

    @Test
    void topicIsCreatedWithThreePartitionsAndSevenDayRetention() throws Exception {
        String topic = topicProps.name();
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            TopicDescription description = admin.describeTopics(List.of(topic))
                    .allTopicNames().get().get(topic);
            assertThat(description.partitions()).hasSize(3);

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            assertThat(config.get(TopicConfig.RETENTION_MS_CONFIG).value())
                    .isEqualTo(String.valueOf(Duration.ofDays(7).toMillis()));
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
