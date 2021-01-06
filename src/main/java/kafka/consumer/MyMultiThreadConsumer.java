package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

public class MyMultiThreadConsumer implements Runnable {

    private final int id;
    private final List<String> topics;
    private final KafkaConsumer<String, String> consumer;

    public MyMultiThreadConsumer(int id, String groupId, List<String> topics) {
        this.id = id;
        this.topics = topics;

        Properties prop = new Properties();

        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("group.id", groupId);
        prop.put("key.deserializer", StringDeserializer.class.getName());
        prop.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(prop);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumer-" + id + " : partition: " + record.partition() + " ,offset = " + record.offset() + " , key = " + record.key() + ", value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
