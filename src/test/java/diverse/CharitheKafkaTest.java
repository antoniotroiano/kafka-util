package diverse;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
@Slf4j
class CharitheKafkaTest {

    @Autowired
    private static KafkaHelper kafkaHelper;

    @Autowired
    private static EphemeralKafkaBroker broker;

    @BeforeEach
    public void setUp() throws Exception {
        broker = EphemeralKafkaBroker.create();
        broker.start();
        kafkaHelper = KafkaHelper.createFor(broker);
    }

    @Test
    void testCharitheJUnit() throws Exception {
        ListenableFuture<List<String>> futureMessages = kafkaHelper.consumeStrings("my-test-topic", 5);
        kafkaHelper.produceStrings("my-test-topic", "a", "b", "c", "d", "e");
        List<String> result = futureMessages.get(5, TimeUnit.SECONDS);
        System.out.println("Ausgabe result: " + result.toString());
        assertThat(result).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @Test
    void testCharitheJUnitBuiltIn() {
        //{"name" : "test"}
        KafkaProducer<String, String> producer = kafkaHelper.createStringProducer();
        producer.send(new ProducerRecord<>("my-test-topic", "A1", "Test value one"));
        producer.send(new ProducerRecord<>("my-test-topic", "A2", "test value two"));
        producer.flush();

        KafkaConsumer<String, String> consumer = kafkaHelper.createStringConsumer();
        consumer.subscribe(Lists.newArrayList("my-test-topic"));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
        //ConsumerRecord<String, String> record = records.records("my-test-topic").iterator().next();
        log.info("Records: " + records.count());
        for (ConsumerRecord<String, String> record : records) {
            log.info("Record topic: " + record.topic() + ", record key: " + record.key() + ", record value: " + record.value());
        }
    }

    //batch processing nachricht lesen er quitiert mir das die 19 gesendeten nachrichten....

    @AfterEach
    public void tearDown() throws ExecutionException, InterruptedException {
        broker.stop();
        broker = null;
        kafkaHelper = null;
    }
}