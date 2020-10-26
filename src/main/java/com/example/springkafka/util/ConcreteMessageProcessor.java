package com.example.springkafka.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class ConcreteMessageProcessor implements IMessageProcessor {

    @Override
    public void process(ConsumerRecord<String, Object> record) {
        log.info("Received message: " + record.value() + " from Topic: " + record.topic());
    }
}