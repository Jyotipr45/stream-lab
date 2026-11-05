package com.jyoti.streamlab.producer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings of the "orders" topic, read from application.yml (streamlab.orders-topic.*).
 *
 * @param name       topic name
 * @param partitions how many partitions (parallel lanes) the topic has
 * @param replicas   how many copies of each partition exist (must be &lt;= number of brokers)
 */
@ConfigurationProperties(prefix = "streamlab.orders-topic")
public record OrdersTopicProperties(String name, int partitions, short replicas) {
}
