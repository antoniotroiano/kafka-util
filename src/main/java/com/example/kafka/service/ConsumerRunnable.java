package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerRunnable implements Runnable {

    private final CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch, String topic, Map<String, Object> consumerProperties) {
        this.latch = latch;
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            consumer.poll(Duration.ofMillis(100));
        } catch (WakeupException e) {
            log.info("Received shutdown signal!");
        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}