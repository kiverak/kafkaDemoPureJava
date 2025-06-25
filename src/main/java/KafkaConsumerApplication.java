import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class KafkaConsumerApplication {

    public static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    public static void main(String[] args) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(properties)) {
//            consumer.subscribe(Arrays.asList(properties.getProperty("topic")));
            consumer.assign(List.of(
                    new TopicPartition("sandbox", 0),
                    new TopicPartition("sandbox", 1),
                    new TopicPartition("sandbox", 2)
            ));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                StreamSupport.stream(records.spliterator(), false)
                        .forEach(record -> logger.info("Record: {}", record));
            }
        }
    }
}
