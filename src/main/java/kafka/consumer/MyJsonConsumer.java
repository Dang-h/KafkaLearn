package kafka.consumer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author DangHao
 * @version 1.0
 * @Description: TODO
 * @Date 2021/1/6 22:15
 **/
public class MyJsonConsumer {
	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
		properties.load(is);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Collections.singleton("mySecondTopic"));

		Gson gson = new Gson();

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					String jsonValue = record.value();

					// HashMap jsonData = gson.fromJson(jsonValue, HashMap.class);

					// 优化后
					List dataList = gson.fromJson(jsonValue, List.class);
					System.out.println("message: ");

					for (int i = 0; i < dataList.size(); i++) {
						Map jsonData = (Map) dataList.get(i);

						System.out.println("id:" + jsonData.get("id") +
								",name:" + jsonData.get("name") +
								",sex:" + jsonData.get("sex") +
								",address:" + jsonData.get("address") +
								",profession:" + jsonData.get("profession"));
					}
				}
			}
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
