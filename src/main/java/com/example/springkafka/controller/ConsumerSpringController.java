package com.example.springkafka.controller;

import com.example.springkafka.service.SpringKafkaConsumerUtil;
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
    private SpringKafkaConsumerUtil springKafkaConsumerUtil;

    @PostMapping("/consumerSpring/{topic}")
    public String startProducer(@PathVariable String topic,
                                @RequestBody Object messageListener, Map<String, Object> consumerProperties) {
        springKafkaConsumerUtil.startConsumer(topic, messageListener, consumerProperties);
        return "started listening";
    }

    @DeleteMapping("/consumerSpring/{topic}")
    public String stop(@PathVariable String topic) {
        springKafkaConsumerUtil.stopConsumer(topic);
        return "stopped listening";
    }
}