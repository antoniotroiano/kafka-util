package com.example.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class ProducerUtil {

    private static KafkaProducer<String, String> producer;

    public void startKafkaProducer(String topic, String key, String message, KafkaProducer<String, String> createProducer) {

        producer = createProducer;
        producer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
            if (exception == null) {
                log.info("Send new message to topic {}", topic);
            } else {
                log.error("Something bad happened: " + exception.getMessage());
            }
        });
        producer.flush();
        log.info("Producer start. Send message to topic {}", topic);
    }

    public void stopKafkaProducer() {
        producer.close();
        log.info("Producer has stopped");
    }
}