package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author DangHao
 * @version 1.0
 * @Description: 对某个partition的数据进行处理再提交offset
 * @Date 2021/1/5 14:41
 **/
public class MySyncConsumer4Partition {
	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		InputStream resourceAsStream = ClassLoader.getSystemClassLoader()
				.getResourceAsStream("kafkaProps.properties");
		properties.load(resourceAsStream);
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		consumer.subscribe(Arrays.asList("mySecondTopic"));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (TopicPartition partition : records.partitions()) {
					// 获取指定partition中的消息
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

					for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
						// 业务处理
						if (partitionRecord.partition() == 0) {
							System.out.println("partition = " + partitionRecord.partition() +
									" offset = " + partitionRecord.offset() +
									" key = " + partitionRecord.key() +
									" value = " + partitionRecord.value());
						}

					}

					// 获取partition中最后一条记录的offset
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					int lastPartition = partitionRecords.get(partitionRecords.size() - 1).partition();
					String lastValue = partitionRecords.get(partitionRecords.size() - 1).value();
					System.out.println("lastValue = " + lastValue +
							" lastPartition = " + lastPartition +
							" lastOffset = " + lastOffset);

					// 同步提交一个partition中的offset
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}
			}
		} finally {
			consumer.close();
		}

	}
}
