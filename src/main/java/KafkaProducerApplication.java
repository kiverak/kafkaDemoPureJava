import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerApplication {

    public static final Logger logger = LoggerFactory.getLogger(KafkaProducerApplication.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var properties = new Properties();

        // Адреса для подключения
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:39092,localhost:49092");

        // Сериализатор ключей
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Сериализатор значений
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        createNewTopic(properties, "sandbox",3, (short) 2);
//        createNewTopic(properties, "audit-events",3, (short) 2);

        // ID транзакции общий для кластера
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

//        sendRecords(properties);
//        sendRecordsWithCallback(properties);
        sendRecordsWithTransaction(properties);
    }

    private static void sendRecords(Properties properties) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaProducer<String, String>(properties)) {
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>("sandbox", "Hello World"));
            var metadata = recordMetadataFuture.get();
            logger.info("====================================================================");
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");

            metadata = producer.send(new ProducerRecord<>("sandbox", "my-key", "Hello World with key")).get();
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");

            metadata = producer.send(new ProducerRecord<>("sandbox", 1, "my-key",
                    "Hello World with key and partition")).get();
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");

            metadata = producer.send(new ProducerRecord<>("sandbox", 1, System.currentTimeMillis(),
                    "my-key-with-timestamp",
                    "Hello World with key and partition and timestamp")).get();
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");

            metadata = producer.send(new ProducerRecord<>("sandbox", 1, System.currentTimeMillis(),
                    "my-key-with-timestamp",
                    "Hello World with key and partition and timestamp")).get();
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");

            metadata = producer.send(new ProducerRecord<>("sandbox", 0, "my-key",
                    "Hello World with key, partition and header",
                    List.of(new RecordHeader("Foo", "Bar".getBytes())))).get();
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");
        }
    }

    private static void sendRecordsWithCallback(Properties properties) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaProducer<String, String>(properties)) {
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord<>("sandbox", "Hello World"),
                    (md, exception) -> {
                        logger.info("Metadata: {}, exception == null: {}", md, exception == null);
                    });
            var metadata = recordMetadataFuture.get();
            logger.info("====================================================================");
            logger.info("Metadata: {}", metadata);
            logger.info("====================================================================");
        }
    }

    private static void sendRecordsWithTransaction(Properties properties) {
        try (var producer = new KafkaProducer<String, String>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("sandbox", "Hello World tx1"));
            producer.send(new ProducerRecord<>("sandbox", "Hello World tx2"));
            producer.commitTransaction();
        }
    }

    private static void createNewTopic(Properties properties, String name, int numPartitions, short replicationFactor) {
        try (AdminClient admin = AdminClient.create(properties)) {
            List<NewTopic> topics = List.of(new NewTopic(name, numPartitions, replicationFactor));
            CreateTopicsResult result = admin.createTopics(topics);
            result.all().get();
            logger.info("Topic created successfully:{}", name);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getMessage().contains("TopicExistsException")) {
                logger.info("Topic already exists: {}", name);
            } else {
                e.printStackTrace();
            }
        }
    }
}
