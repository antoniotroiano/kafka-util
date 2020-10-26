package startstop;

import com.example.kafka.service.StartStopConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringRunner.class)
@DirtiesContext
@Slf4j
@EmbeddedKafka(
        partitions = 1,
        topics = {"testTopic"}
)
public class StartStopConsumerTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final StartStopConsumer startStopConsumer = new StartStopConsumer();

    @Test
    public void startStopConsumer() throws InterruptedException {

        KafkaProducer<String, Object> producer = producer();

        for (int index = 1; index <= 100; index++) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("index", index);
            objectNode.put("message", "The index is now: " + index);
            ProducerRecord<String, Object> record = new ProducerRecord<>("testTopic", "id_" + index, objectNode);
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                log.info(exception.getMessage());
            });
        }
        producer.flush();

        //Consumer props
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", this.embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        KafkaConsumer<String, Object> consumer = startStopConsumer.startConsumer("testTopic", consumerProps);
        log.info("Start consumer first time");

        ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, Object> record : records) {
            log.info("Received message: " + record.value() + " of topic: " + record.topic());
        }
        Thread.sleep(300);

        startStopConsumer.stopConsumer();
        log.info("Stop consumer first time");

        KafkaConsumer<String, Object> consumer2 = startStopConsumer.startConsumer("testTopic", consumerProps);
        log.info("Start consumer second time");

        ConsumerRecords<String, Object> records2 = consumer2.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, Object> record : records2) {
            log.info("Received message: " + record.value() + " of topic: " + record.topic());
        }

        startStopConsumer.stopConsumer();
        log.info("Stop consumer second time");
    }

    private KafkaProducer<String, Object> producer() {
        //Producer props
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new KafkaProducer<>(producerProps);
    }
}