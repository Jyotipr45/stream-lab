package com.jyoti.streamlab.producer.experiment;

import com.jyoti.streamlab.producer.config.OrdersTopicProperties;
import com.jyoti.streamlab.producer.generator.OrderEventGenerator;
import com.jyoti.streamlab.producer.model.OrderEvent;
import com.jyoti.streamlab.producer.publisher.OrderEventPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Experiment 1: publish a few messages and print where each one landed.
 *
 * Runs only when streamlab.experiment=1 (the default in application.yml).
 */
@Component
@ConditionalOnProperty(name = "streamlab.experiment", havingValue = "1")
public class Experiment01Runner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(Experiment01Runner.class);

    private final OrderEventGenerator generator;
    private final OrderEventPublisher publisher;
    private final OrdersTopicProperties topicProps;
    private final int messageCount;

    public Experiment01Runner(OrderEventGenerator generator,
                              OrderEventPublisher publisher,
                              OrdersTopicProperties topicProps,
                              @Value("${streamlab.experiment1.message-count:10}") int messageCount) {
        this.generator = generator;
        this.publisher = publisher;
        this.topicProps = topicProps;
        this.messageCount = messageCount;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("=== Experiment 1: sending {} messages to topic '{}' ({} partitions) ===",
                messageCount, topicProps.name(), topicProps.partitions());

        // 1. Send everything without waiting. Sends happen in the background, in batches.
        List<CompletableFuture<SendResult<String, OrderEvent>>> futures = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            futures.add(publisher.publish(generator.next()));
        }

        // 2. Now wait for every broker answer and print where each message was stored.
        Map<Integer, Integer> perPartition = new TreeMap<>();
        for (CompletableFuture<SendResult<String, OrderEvent>> future : futures) {
            SendResult<String, OrderEvent> result = future.join();
            RecordMetadata meta = result.getRecordMetadata();
            OrderEvent event = result.getProducerRecord().value();

            log.info("key={}  customer={}  ->  partition={}  offset={}",
                    event.orderId(), event.customerId(), meta.partition(), meta.offset());
            perPartition.merge(meta.partition(), 1, Integer::sum);
        }

        log.info("=== Messages per partition: {} ===", perPartition);
    }
}
