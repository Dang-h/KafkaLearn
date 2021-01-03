package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MySecondProducer {
	public static void main(String[] args) {
		Properties kafkaProps = new Properties();

		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

		ProducerRecord<String, String> record = new ProducerRecord<String, String>("mySecondTopic", "message", "test Sync message send");

		try {
			Future<RecordMetadata> future = producer.send(record);
			RecordMetadata recordMetadata = future.get();
			long offset = recordMetadata.offset();
			int partition = recordMetadata.partition();
			System.out.println("offset: " + offset + " partition = " + partition);
		} catch (Exception e) {
			e.printStackTrace();
		}


	}
}
