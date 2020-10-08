package com.example.kafka.metrics;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Configuration
public class KafkaMetricsConfiguration {
    @Bean
    public Counter receivedMessagesCounter(MeterRegistry registry) {
        return Counter
            .builder("api.kafka.received.messages")
            .description("Amount of messages received")
            .register(registry);
    }
}
