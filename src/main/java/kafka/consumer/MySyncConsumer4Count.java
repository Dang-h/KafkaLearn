package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author DangHao
 * @version 1.0
 * @Description: 按处理的信息量提交offset
 * @Date 2021/1/5 15:09
 **/
public class MySyncConsumer4Count {
	public static void main(String[] args) throws IOException {
		// 用于存储partition的offset
		HashMap<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>(16);
		int count = 0;

		// 用于记录消息数量
		Properties properties = new Properties();
		InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
		properties.put("enable.auto.commit", "false");
		properties.load(resourceAsStream);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		consumer.subscribe(Collections.singleton("mySecondTopic"));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("message: partition = " + record.partition() +
							" offset = " + record.offset() +
							" key = " + record.key() +
							" value = " + record.value());

					TopicPartition tp = new TopicPartition(record.topic(), record.partition());
					OffsetAndMetadata om = new OffsetAndMetadata(record.offset(), "");
					currentOffsets.put(tp, om);

					// 每10条提交一次
					if (count % 10 == 0) {
						consumer.commitSync(currentOffsets);
						System.out.println("---------------- Commit! ----------------");
					}
					count++;
				}
				consumer.commitSync();
			}
		} finally {
			consumer.close();
		}

	}
}
