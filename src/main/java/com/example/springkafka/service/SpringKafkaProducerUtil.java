package com.example.springkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.ProducerListener;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SpringKafkaProducerUtil {

    private static final Map<String, KafkaTemplate<String, String>> producerMap = new HashMap<>();

    public KafkaTemplate<String, String> startSpringProducer(String topic, ProducerListener<String, String> producerListener, Map<String, Object> producerProperties) {
        log.info("Producer exist or created for topic {} and started", topic);
        return producerMap.computeIfAbsent(topic, t -> createProducer(t, producerListener, producerProperties));
    }

    private KafkaTemplate<String, String> createProducer(String topic, ProducerListener<String, String> producerListener, Map<String, Object> producerProperties) {
        ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(producerProperties);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(factory);
        kafkaTemplate.setDefaultTopic(topic);
        //Brauche ich den ProducerListener überhaupt?
        kafkaTemplate.setProducerListener(producerListener);
        return kafkaTemplate;
    }

    public void stopSpringProducer(String topic) {
        KafkaTemplate<String, String> kafkaTemplate = producerMap.get(topic);
        if (kafkaTemplate != null) {
            kafkaTemplate.destroy();
            log.info("Producer stopped for topic {}", topic);
        }
    }
}