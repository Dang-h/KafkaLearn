package kafka.producer.image;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @version 1.0
 * @Author DangHao
 * @Description: TODO
 * @Date 2021/1/6 22:42
 **/
public class ImageProducer {
	public static void main(String[] args) throws IOException {
		Properties properties = new Properties();
		InputStream is = ClassLoader.getSystemClassLoader().getResourceAsStream("kafkaProps.properties");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.load(is);

		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

		try {
			File file = new File("kafka_logo.png");
			FileInputStream fis = new FileInputStream(file);
			byte[] buffer = new byte[fis.available()];
			fis.read(buffer);

			ProducerRecord<String, byte[]> record = new ProducerRecord<>("mySecondTopic", file.getName(), buffer);
			producer.send(record);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}


	}
}
