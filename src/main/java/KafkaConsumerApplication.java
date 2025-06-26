import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

public class KafkaConsumerApplication {

    public static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    public static void main(String[] args) {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        // если не указать id инстанса группы, он будет генерироваться каждый раз
        // и будет происходить перебалансировка кластера
//        properties.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-group-instance-id");

        getRecords(properties);
//        getRecordsForGroup(properties);
    }

    private static void getRecords(Properties properties) {
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.assign(List.of(
//                    new TopicPartition("sandbox", 0),
                    new TopicPartition("sandbox", 1)
//                    new TopicPartition("sandbox", 2)
            ));

//            consumer.seek(new TopicPartition("sandbox", 1), new OffsetAndMetadata(5));
//            consumer.seekToEnd(List.of(new TopicPartition("sandbox", 1)));
            var offsets = consumer.offsetsForTimes(Map.of(new TopicPartition("sandbox", 1), 1750878518535L));
            consumer.seek(
                    new TopicPartition("sandbox", 1),
                    offsets.get(new TopicPartition("sandbox", 1)).offset()
            );

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                StreamSupport.stream(records.spliterator(), false)
                        .forEach(record -> logger.info("Record: {}", record));
            }
        }
    }

    private static void getRecordsForGroup(Properties properties) {
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.subscribe(Pattern.compile("sandbox"), new MyConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                StreamSupport.stream(records.spliterator(), false)
                        .forEach(record -> logger.info("Record: {}", record));
            }
        }
    }
}

class MyConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(MyConsumerRebalanceListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned: {}", partitions);
    }
}