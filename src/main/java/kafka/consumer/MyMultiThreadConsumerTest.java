package kafka.consumer;

import javassist.bytecode.analysis.Executor;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyMultiThreadConsumerTest {
    public static void main(String[] args) {
        int numConsumeers = 3;
        String groupId = "group2";

        List<String> topic = Arrays.asList("mySecondTopic");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumeers);

        for (int i = 0; i < numConsumeers; i++) {
            MyMultiThreadConsumer consumer = new MyMultiThreadConsumer(i, groupId, topic);
            executor.submit(consumer);
        }
    }
}
