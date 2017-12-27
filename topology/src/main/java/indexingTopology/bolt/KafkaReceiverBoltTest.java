package indexingTopology.bolt;

import com.alibaba.fastjson.JSONObject;
import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import info.batey.kafka.unit.KafkaUnit;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by billlin on 2017/11/23
 */
public class KafkaReceiverBoltTest extends InputStreamReceiverBolt {

    private final DataSchema schema;
    private String topic;
    private int kafkaHost;
    private int ZKhost;
    TopologyConfig config;


    public KafkaReceiverBoltTest(DataSchema schema, TopologyConfig config) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        KafkaUnit kafkaUnit;
        kafkaUnit = new KafkaUnit(config.ZKHost.get(0), config.kafkaHost.get(0));
        kafkaUnit.startup();
        //        props.put("offsets.topic.replication.factor", 1);  // kafka config
        //        props.put("default.replication.factor", 1);

        super.prepare(map, topologyContext, outputCollector);
        System.out.println("config.kafkaHost" + config.kafkaHost);
        String groupId = "consumer-group";
        List<String> topics = Arrays.asList(config.topic);
        final List<ConsumerLoop> consumers = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(1);
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            ConsumerLoop consumer = new ConsumerLoop(0, groupId, topics, config.kafkaHost.get(0));
            executorService.submit(consumer);
//                ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics, config.kafkaHost.get(i));
//                consumers.add(consumer);
//                executor.submit(consumer);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
//                for (ConsumerLoop consumer : consumers) {
                consumer.shutdown();
//                }
                executor.shutdown();
                try {
                    executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public class ConsumerLoop implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final List<String> topics;
        private final int id;

        public ConsumerLoop(int id, String groupId,  List<String> topics, String kafkaHost) {
            this.id = id;
            this.topics = topics;
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
            String idString = "" + id;
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topics);
                System.out.println("topics : " + topics);
                while (true) {
                    // the consumer whill bolck until the records coming
                    System.out.println("topic.vaule:" + consumer.listTopics().get("topic"));
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    System.out.println("records.count : " + records.count());

                    for (ConsumerRecord<String, String> record : records){
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
                        JSONObject jsonFromData = JSONObject.parseObject(record.value());
                        getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
                    }
                }
            } catch (WakeupException e) {
                System.out.println("consumer start failed1!");
                // ignore for shutdown
            } catch (Exception e) {
                System.out.println("consumer start failed2!");
                e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }


    @Override
    public void cleanup() {
        super.cleanup();
    }
}
