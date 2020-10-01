import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Map;

@RunWith(SpringRunner.class)
@DirtiesContext
@Slf4j
@EmbeddedKafka(
        partitions = 1,
        topics = {"testTopicEmbeddedKafka"}
)
public class SpringKafkaEmbeddedTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    public void kafkaEmbeddedBrokerTest() {

        KafkaTemplate<Integer, Object> producerJson = producerJson();
        KafkaTemplate<Integer, Object> producerString = producerString();

        //Send message to producer
        for (int index = 1; index <= 19; index++) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("index", index);
            objectNode.put("message", "The index is now: " + index);
            producerJson.send("testTopicEmbeddedKafka", index, objectNode);
        }
        producerString.send("testTopicEmbeddedKafka", 20, "Wrong message");

        ConsumerRecords<Integer, Object> records = consumer().poll(Duration.ofMillis(10000));

        log.info("Count records: " + records.count());

        for (ConsumerRecord<Integer, Object> record : records) {
            if (record.value() == null) {
                log.warn("A poison pill record was encountered: " + record);
            } else {
                log.info("Topic: " + record.topic() + ", value: " + record.value());
            }
        }
    }

    private KafkaTemplate<Integer, Object> producerJson() {
        //Producer props
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(this.embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        //Create producer
        DefaultKafkaProducerFactory<Integer, Object> pf = new DefaultKafkaProducerFactory<>(producerProps);
        //kafkaTemplate.setDefaultTopic("testTopicEmbeddedKafka");
        return new KafkaTemplate<>(pf);
    }

    private KafkaTemplate<Integer, Object> producerString() {
        //Producer props
        Map<String, Object> producerPropsString = KafkaTestUtils.producerProps(this.embeddedKafkaBroker);
        producerPropsString.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerPropsString.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        //Create producer
        DefaultKafkaProducerFactory<Integer, Object> pfString = new DefaultKafkaProducerFactory<>(producerPropsString);
        return new KafkaTemplate<>(pfString);
    }

    private Consumer<Integer, Object> consumer() {
        //Consumer props
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", this.embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Die nächsten zwei Zeilen auskommentieren und den Block einkommentieren behebt das Problem
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        /*consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, IntegerDeserializer.class);
        consumerProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);*/
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        //Create consumer
        DefaultKafkaConsumerFactory<Integer, Object> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<Integer, Object> consumer = cf.createConsumer();
        this.embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "testTopicEmbeddedKafka");
        return consumer;
    }
}