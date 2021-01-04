package kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class MySyncConsumer {
    public static void main(String[] args) throws IOException {
        Properties kafkaProps = new Properties();
        InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
        kafkaProps.put("enable.auto.commit", "false");
        kafkaProps.load(resourceAsStream);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);

        consumer.subscribe(Arrays.asList("mySecondTopic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("message: partition = " + record.partition() + " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
                }
                // Sync-Commit
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}
