package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class MySeekConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "group1");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, Byte[]> consumer = new KafkaConsumer<String, Byte[]>(properties);

        try {
            TopicPartition seekToEndPartition = new TopicPartition("mySecondTopic", 1);
            consumer.assign(Arrays.asList(seekToEndPartition));
            // seek,begin consume from offset of 10
            consumer.seek(seekToEndPartition, 10);

            ConsumerRecords<String, Byte[]> records = consumer.poll(1000);

            for (ConsumerRecord<String, Byte[]> record : records) {
                System.out.println("partition = " + record.partition() + " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
