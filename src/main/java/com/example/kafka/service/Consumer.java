package com.example.kafka.service;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class Consumer {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    @Autowired
    protected MeterRegistry meterRegistry;
    private Counter receivedMessagesCounter;

    @PostConstruct
    public void init() {
        receivedMessagesCounter = meterRegistry.counter("api.kafka.received.messages");
    }

    @KafkaListener(topics = "${com.example.kafka.topic}", groupId = "${com.example.kafka.consumer-group}")
    public void receive(String message) {
        long current = System.currentTimeMillis();
        receivedMessagesCounter.increment();
        long data = Long.parseLong(message);
        LOG.info("received message is {}, diff: {}msec", message, current - data);
    }

    @KafkaListener(topics = "${com.example.kafka.topic}", groupId = "${com.example.kafka.consumer-group}")
    public void receive(ConsumerRecord<?, ?> consumerRecord, Acknowledgment ack) {
        long current = System.currentTimeMillis();
        receivedMessagesCounter.increment();
        String message = consumerRecord.value().toString();
        long data = Long.parseLong(message);
        LOG.info("received message is {}, diff: {}msec", message, current - data);

        ack.acknowledge();
        LOG.info("ack");
    }

    @KafkaListener(topics = "${com.example.kafka.topic}")
    public void receive(List<String> records) {
        long current = System.currentTimeMillis();
        records.forEach(record -> {
            receivedMessagesCounter.increment();
            long data = Long.parseLong(record);
            LOG.info("received message is {}, diff: {}msec", record, current - data);
        });

        LOG.info("total: {}", records.size());
    }

    @KafkaListener(topics = "${com.example.kafka.topic}")
    public void receive(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
        long current = System.currentTimeMillis();
        records.forEach(record -> {
            receivedMessagesCounter.increment();
            String message = record.value().toString();
            long data = Long.parseLong(message);
            LOG.info("received message is {}, diff: {}msec", message, current - data);
        });

        ack.acknowledge();
        LOG.info("ack, total: {}", records.size());
    }
}
