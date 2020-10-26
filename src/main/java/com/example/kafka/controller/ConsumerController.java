package com.example.kafka.controller;

import com.example.kafka.service.StartStopConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Map;

@RestController
@Slf4j
public class ConsumerController {

    private final StartStopConsumer startStopConsumer = new StartStopConsumer();

    @PostMapping("/consumer/{topic}")
    public String startConsumer(@PathVariable String topic,
                                @RequestBody Map<String, Object> consumerProperties) {
        startStopConsumer.consumerRun(topic, consumerProperties);
        return "started consumer";
    }

    @GET
    @Path("/stop")
    public String stopConsumer() {
        startStopConsumer.consumerStop();
        log.info("Close consumer");
        return "stopped consumer";
    }
}