package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class StartStopConsumer {

    //private CountDownLatch latch = new CountDownLatch(1);
    //private ConsumerRunnable consumerRunnable;
    private KafkaConsumer<String, Object> consumer = null;

    public KafkaConsumer<String, Object> startConsumer(String topic, Map<String, Object> consumerProps) {
        log.info("Create and start consumer on topic {}", topic);
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public void stopConsumer() {
        log.info("Stop consumer");
        consumer.unsubscribe();
        consumer.close();
    }

    /*public void consumerRun(String topic, Map<String, Object> consumerProperties) {
        log.info("Create the consumer thread");
        consumerRunnable = new ConsumerRunnable(latch, topic, consumerProperties);
        Thread thread = new Thread(consumerRunnable);
        thread.start();
    }

    public void consumerStop() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (Exception e) {
                log.error("Application got interrupted {}", e.getMessage());
            }
            log.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (Exception e) {
            log.error("Application got interrupted {}", e.getMessage());
        } finally {
            log.info("Application is closing");
        }
    }*/
}