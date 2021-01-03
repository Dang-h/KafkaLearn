package kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @Description: TODO
 * @Date 2021/1/3 13:17
 **/
public class MyPartition implements Partitioner {

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		System.out.println("Customer Partition is running");

		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();

		// 按照key分区
		if (keyBytes == null) {
			throw new InvalidRecordException("key cannot be null");
		}

		// 如果消息的key值为1， 消息发送到第二个分区
		if (((String) key).equals("1")) {
			return 1;
		}

		// key值不为1，使用hash值取模确定分区
		return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions));
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> configs) {

	}
}
