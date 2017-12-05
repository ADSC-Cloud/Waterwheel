package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by billlin on 2017/11/23
 */
public class InputStreamKafkaReceiverBoltServer extends InputStreamKafkaReceiverBolt {

    private int port;

    public InputStreamKafkaReceiverBoltServer(DataSchema schema, int port, TopologyConfig config) {
        super(schema, config);
        this.port = port;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        int numConsumers = 3;
        String groupId = "consumer-group";
        List<String> topics = Arrays.asList("consumer");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ConsumerLoop consumer = new ConsumerLoop(0, groupId, topics);
        executorService.submit(consumer);
//        final List<ConsumerLoop> consumers = new ArrayList<>();
//        for (int i = 0; i < numConsumers; i++) {
//            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
//            consumers.add(consumer);
//            executor.submit(consumer);
//        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
//                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
//                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
