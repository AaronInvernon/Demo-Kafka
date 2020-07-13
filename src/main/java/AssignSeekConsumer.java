import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AssignSeekConsumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mTopic;

    private AssignSeekConsumer(String bootstrapServer, String topic){
        mBootstrapServer = bootstrapServer;
        mTopic = topic;
    }

    private Properties getConsumerProperties(String bootstrapServer) {
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public void run(long offset, int partitionNum, int numOfMesasages) {
        Properties props = getConsumerProperties(mBootstrapServer);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        setupConsumer(consumer, offset, partitionNum);
        fetchMessages(consumer, numOfMesasages);
    }

    private void setupConsumer(KafkaConsumer<String, String> consumer, long offset, int partitionNum) {
        TopicPartition partition = new TopicPartition(mTopic, partitionNum);
        consumer.assign(Collections.singletonList(partition));
        consumer.seek(partition, offset);
    }

    private void fetchMessages(KafkaConsumer<String, String> consumer, int numOfMessages) {
        int numberOfMessagesRead = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead += 1;

                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() +  ", Offset: " + record.offset());

                if (numberOfMessagesRead >= numOfMessages) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }

}
