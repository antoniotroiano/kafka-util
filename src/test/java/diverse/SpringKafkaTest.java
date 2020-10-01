package diverse;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;

@Slf4j
public class SpringKafkaTest {

    private static final String TOPIC = "spring";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, false, TOPIC);

    @Test
    public void testSendAndConsume() {
        try (Consumer<Integer, String> consumer = new KafkaConsumer<>(
                KafkaTestUtils.consumerProps("spring_consumer_group", "true", embeddedKafkaRule.getEmbeddedKafka()))) {
            KafkaTemplate<Integer, String> kafkaTemplate = new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafkaRule.getEmbeddedKafka())));

            consumer.subscribe(Collections.singleton(TOPIC));

            kafkaTemplate.send(TOPIC, 1, "value one");
            kafkaTemplate.send(TOPIC, 2, "value two");

            ConsumerRecords<Integer, String> records = KafkaTestUtils.getRecords(consumer);
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("Show value of Topic: " + record.topic());
                log.info("Key: " + record.key() + " Value " + record.value());
            }
        }
    }
}