package startstop;

import com.example.springkafka.service.StartStopSpringConsumer;
import com.example.springkafka.util.ConcreteMessageProcessor;
import com.example.springkafka.util.CustomMessageListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

@RunWith(SpringRunner.class)
@DirtiesContext
@Slf4j
@EmbeddedKafka(
        partitions = 1,
        topics = {"testTopic"}
)
public class StartStopSpringConsumerTest {

    private final StartStopSpringConsumer startStopSpringConsumer = new StartStopSpringConsumer();

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void startStopSpringConsumerTest() throws InterruptedException {

        KafkaTemplate<Integer, Object> producerJson = producerJson();

        for (int index = 1; index <= 2000; index++) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("index", index);
            objectNode.put("message", "The index is now: " + index);
            producerJson.send("testTopic", index, objectNode);
        }

        //Consumer props
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", this.embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        //consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        ConcreteMessageProcessor concreteMessageProcessor = new ConcreteMessageProcessor();
        CustomMessageListener customMessageListener = new CustomMessageListener(concreteMessageProcessor);

        startStopSpringConsumer.startConsumer("testTopic", customMessageListener, consumerProps);
        log.info("Consumer start first time");
        Thread.sleep(1000);

        startStopSpringConsumer.stopConsumer("testTopic");
        log.info("Consumer stop first time");

        startStopSpringConsumer.startConsumer("testTopic", customMessageListener, consumerProps);
        log.info("Consumer start second time");
        Thread.sleep(1000);

        startStopSpringConsumer.stopConsumer("testTopic");
        log.info("Consumer stop second time");
    }

    private KafkaTemplate<Integer, Object> producerJson() {
        //Producer props
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        //Create producer
        DefaultKafkaProducerFactory<Integer, Object> pf = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(pf);
    }
}