/*
package com.example.springkafka.service;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerSpring {

    @KafkaListener(containerFactory = "test-listener",
            groupId = "test_group",
            topics = "test_topic")
    public void onMessage(String message) {
        log.info("Received message from Kafka: " + message);
    }
}
*/
