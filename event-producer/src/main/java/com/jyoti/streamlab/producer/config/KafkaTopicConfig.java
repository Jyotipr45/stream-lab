package com.jyoti.streamlab.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics this application owns.
 *
 * <p><b>Ownership rule:</b> the service that <i>produces</i> an event owns its topic. This
 * producer owns {@code orders.order.placed.v1}, so the topic is declared here; consumers
 * never create it.
 *
 * <p><b>How it works:</b> at startup Spring's {@code KafkaAdmin} collects every {@link NewTopic}
 * bean and asks the broker to create the missing ones. If a topic already exists, its
 * partition count is only ever increased, never decreased, and other settings are left
 * as they are.
 *
 * <p>In a company this declaration usually lives in a separate git repo (Terraform or a
 * YAML catalogue) applied by a pipeline. The principle is the same: topics are created from
 * reviewed code, never by hand, which is why the broker runs with
 * {@code auto.create.topics.enable=false}.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic ordersTopic(OrdersTopicProperties props) {
        return TopicBuilder.name(props.name())
                .partitions(props.partitions())
                .replicas(props.replicas())
                // Delete records older than the retention period (an event stream, not a state table).
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(props.retention().toMillis()))
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}
