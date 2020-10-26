package com.example.springkafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IMessageProcessor {

    void process(ConsumerRecord<String, Object> record);
}