package com.jyoti.streamlab.producer.config;

import com.jyoti.streamlab.producer.generator.OrderEventGenerator;
import java.time.Clock;
import java.util.Random;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GeneratorConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    OrderEventGenerator orderEventGenerator(Clock clock) {
        // Fixed seed: customers, products and quantities are the same on every run,
        // which makes experiment output repeatable.
        return new OrderEventGenerator(clock, new Random(42));
    }
}
