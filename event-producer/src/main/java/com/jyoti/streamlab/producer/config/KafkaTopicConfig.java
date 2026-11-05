package com.jyoti.streamlab.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic this app needs.
 *
 * Spring's KafkaAdmin sees every NewTopic bean at startup and asks the broker to create it.
 * If the topic already exists with the same or more partitions, nothing happens.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic ordersTopic(OrdersTopicProperties props) {
        return TopicBuilder.name(props.name())
                .partitions(props.partitions())
                .replicas(props.replicas())
                .build();
    }
}
