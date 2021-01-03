package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class MyFirstConsumer {
    public static void main(String[] args) {
        Properties kafkaProp = new Properties();

        kafkaProp.put("bootstrap.servers", "localhost:9092");
        kafkaProp.put("group.id", "group1");
        kafkaProp.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, Byte[]> consumer = new KafkaConsumer<String, Byte[]>(kafkaProp);
        consumer.subscribe(Arrays.asList("mySecondTopic"));

        try {
            while (true) {
                ConsumerRecords<String, Byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, Byte[]> record : records) {
                    System.out.println("MyFirstConsumer's consumption message: partition = " + record.partition() + ", offset = " + record.offset() + " ,key = " + record.key() + ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
