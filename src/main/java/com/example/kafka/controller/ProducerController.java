package com.example.kafka.controller;

import com.example.kafka.service.ProducerUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class ProducerController {

    ProducerUtil producerUtil = new ProducerUtil();

    @PostMapping("/producer/{topic}/{key}/{message}")
    public String startProducer(@PathVariable String topic,
                                @PathVariable String key,
                                @PathVariable String message,
                                @RequestBody KafkaProducer<String, String> kafkaProducer) {
        producerUtil.startKafkaProducer(topic, key, message, kafkaProducer);
        return "producer started";
    }

    @DeleteMapping()
    public String stopProducer() {
        producerUtil.stopKafkaProducer();
        return "producer stopped";
    }
}