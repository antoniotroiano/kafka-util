package com.example.springkafka.controller;

import com.example.springkafka.service.StartStopSpringProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Slf4j
public class ProducerSpringController {

    @Autowired
    private StartStopSpringProducer startStopSpringProducer;

    @PostMapping("/producerSpring/{topic}/{producerListener}/{producerProperties}")
    public String startProducer(@PathVariable String topic,
                                @RequestBody ProducerListener<String, String> producerListener, Map<String, Object> producerProperties) {
        startStopSpringProducer.startSpringProducer(topic, producerListener, producerProperties);
        return "started producer";
    }

    @DeleteMapping("/producerSpring/{topic}")
    public String stop(@PathVariable String topic) {
        startStopSpringProducer.stopSpringProducer(topic);
        return "stopped producer";
    }
}