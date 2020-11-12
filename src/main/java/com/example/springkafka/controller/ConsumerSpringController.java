package com.example.springkafka.controller;

import com.example.springkafka.service.StartStopSpringConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@Slf4j
public class ConsumerSpringController {

    @Autowired
    private StartStopSpringConsumer startStopSpringConsumer;

    @PostMapping("/consumerSpring/{topic}")
    public String startProducer(@PathVariable String topic,
                                @RequestBody Map<String, Object> consumerProperties) {
        startStopSpringConsumer.startConsumer(topic, null, consumerProperties);
        return "started listening";
    }

    @DeleteMapping("/consumerSpring/{topic}")
    public String stop(@PathVariable String topic) {
        startStopSpringConsumer.stopConsumer(topic);
        return "stopped listening";
    }
}
