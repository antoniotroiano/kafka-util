package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerUtil {

    private CountDownLatch latch = new CountDownLatch(1);
    private ConsumerRunnable consumerRunnable;
    private Thread thread;

    public void consumerRun(String topic, Map<String, Object> consumerProperties) {
        log.info("create the consumer thread");
        consumerRunnable = new ConsumerRunnable(latch, topic, consumerProperties);
        thread = new Thread(consumerRunnable);
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
    }
}