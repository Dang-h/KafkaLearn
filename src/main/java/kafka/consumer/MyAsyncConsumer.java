package kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * offset异步提交，可以使consumer有更高的吞吐量。
 */
public class MyAsyncConsumer {
	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		InputStream resourceAsStream = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
		properties.load(resourceAsStream);
		properties.put("enable.auto.commit", "false");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		consumer.subscribe(Arrays.asList("mySecondTopic"));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("message: partition = " + record.partition() + " offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
				}

				// 异步提交offset，使用回调函数onComplete处理异常。如果不需要处理异常，可使用无参的异步方法。
				consumer.commitAsync(new OffsetCommitCallback() {
					@Override
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
						if (e != null) {
							// 处理异常
						}
					}
				});
			}
		} finally {
			consumer.close();
		}
	}
}
