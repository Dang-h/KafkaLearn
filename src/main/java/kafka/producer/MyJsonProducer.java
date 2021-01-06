package kafka.producer;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author DangHao
 * @version 1.0
 * @Description: TODO
 * @Date 2021/1/6 22:06
 **/
public class MyJsonProducer {
	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
		properties.load(is);

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		Gson gson = new Gson();

		ArrayList<Map<String, String>> dataList = new ArrayList<>();

		// 用于存放dataList对象转换后的json字符串
		String jsonData = "";
		try {
			for (int i = 0; i < 10; i++) {
				HashMap<String, String> dbData = new HashMap<String, String>(16);

				//id 记录主键
				dbData.put("id", i + "");
				dbData.put("name", "zhangsan" + i);
				dbData.put("sex", "male");
				dbData.put("address", "beijing");
				dbData.put("profession", "programer");

				dataList.add(dbData);
			}
			jsonData = gson.toJson(dataList);

			System.out.println("jsonData = " + jsonData);

			ProducerRecord<String, String> record = new ProducerRecord<String, String>("mySecondTopic", "" + 1, jsonData);
			producer.send(record, new DemoProducerCallback());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
