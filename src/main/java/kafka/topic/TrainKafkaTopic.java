package kafka.topic;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import scala.collection.Iterable;
import scala.collection.Map;

import java.util.Properties;

public class TrainKafkaTopic {
    public static void main(String[] args) {
        ZkUtils zu = ZkUtils.apply("localhost:2181", 3000, 3000, false);
        Map<String, Properties> map = AdminUtils.fetchAllTopicConfigs(zu);

        Iterable<String> alltopisc = map.keys();

        String topicResult = alltopisc.mkString("&");

        System.out.println("topicResult = " + topicResult);
    }
}
