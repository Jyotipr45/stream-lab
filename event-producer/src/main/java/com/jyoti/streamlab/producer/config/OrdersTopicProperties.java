package com.jyoti.streamlab.producer.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings of the order-placed topic, bound from {@code streamlab.orders-topic.*} in application.yml.
 *
 * <p>Keeping topic settings in configuration (not hard-coded) is the small version of
 * "topics as code": the topic's shape is reviewed in a pull request like any other change.
 * See {@code docs/topic-management.md}.
 *
 * @param name       topic name, following {@code <domain>.<entity>.<event>.<version>},
 *                   e.g. {@code orders.order.placed.v1}
 * @param partitions number of partitions (parallel lanes). Can be increased later but never
 *                   decreased, and increasing it moves keys to other partitions - choose up front
 * @param replicas   copies of each partition on different brokers; must be &lt;= number of brokers
 *                   (1 in this lab, 3 in production)
 * @param retention  how long Kafka keeps a record before deleting it, e.g. {@code 7d};
 *                   applied as the topic's {@code retention.ms}
 */
@ConfigurationProperties(prefix = "streamlab.orders-topic")
public record OrdersTopicProperties(String name, int partitions, short replicas, Duration retention) {
}
