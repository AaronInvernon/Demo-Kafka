import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private final String bootstrapServer;
    private final String groupId;
    private final String topic;

    public Consumer(String bootstrapServer, String groupId, String topic) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.topic = topic;
    }

    public void run() {
        logger.info("Creating consumer thread");
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, countDownLatch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            await(countDownLatch);
            logger.info("Application has exited");
        }));
        await(countDownLatch);
    }

    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "user_registered";
        new Consumer(server, groupId, topic).run();
    }

    private void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {
        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;

            Properties properties = getConsumerProperties(bootstrapServer, groupId);

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topic));
        }

        private Properties getConsumerProperties(String bootstrapServer, String groupId) {
            String deserializer = StringDeserializer.class.getName();
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            return properties;
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord<String, String> record: records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }

                } while(true);
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }

}
