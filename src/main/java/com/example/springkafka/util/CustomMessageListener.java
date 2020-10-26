package com.example.springkafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class CustomMessageListener implements MessageListener<String, Object> {

    private final IMessageProcessor iMessageProcessor;

    public CustomMessageListener(IMessageProcessor iMessageProcessor) {
        this.iMessageProcessor = iMessageProcessor;
    }

    @Override
    public void onMessage(ConsumerRecord<String, Object> record) {
        iMessageProcessor.process(record);
    }
}