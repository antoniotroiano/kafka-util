package com.example.springkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class StartStopSpringConsumer {

    private static final Map<String, ConcurrentMessageListenerContainer<String, String>> consumerMap = new HashMap<>();

    public void startConsumer(String topic, Object messageListener, Map<String, Object> consumerProperties) {
        ConcurrentMessageListenerContainer<String, String> container =
                consumerMap.computeIfAbsent(topic, t -> createContainer(t, messageListener, consumerProperties));

        if (!container.isRunning()) {
            container.start();
            log.info("Consumer exist or created for topic {} and is started", topic);
        }
    }

    private ConcurrentMessageListenerContainer<String, String> createContainer(String topic,
                                                                               Object messageListener,
                                                                               Map<String, Object> consumerProperties) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setPollTimeout(100);

        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(consumerProperties);
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(factory, containerProperties);
        container.setupMessageListener(messageListener);
        return container;
    }

    public void stopConsumer(String topic) {
        ConcurrentMessageListenerContainer<String, String> container = consumerMap.get(topic);
        if (container != null) {
            container.stop();
            log.info("Consumer stopped for topic {}", topic);
        }
    }
}