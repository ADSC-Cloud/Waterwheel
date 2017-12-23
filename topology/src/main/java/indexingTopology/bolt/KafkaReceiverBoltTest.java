package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.config.TopologyConfig;
import info.batey.kafka.unit.KafkaUnit;
import net.sf.json.JSONObject;
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
        System.out.println("config.kafkaHost" + config.kafkaHost);
//        this.topic = topic;
//        this.kafkaHost = kafkaHost;
//        this.ZKhost = ZKhost;897694224
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        System.out.println("config.kafkaHost" + config.kafkaHost);
        int numConsumers = config.kafkaHost.size();
        String groupId = "consumer-group";
        List<String> topics = Arrays.asList(config.topic);
        final List<ConsumerLoop> consumers = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            System.out.println("host : " + config.ZKHost.get(i) + config.kafkaHost.get(i));
            String partitionSize = "" + config.kafkaHost.size();
            String idString = "" + (i + 1);
            KafkaUnit kafkaUnit = new KafkaUnit(config.ZKHost.get(i), config.kafkaHost.get(i));
//            kafkaUnit.setKafkaBrokerConfig("num.partitions",partitionSize);
             kafkaUnit.startup();
             kafkaUnit.setKafkaBrokerConfig("broker.id", idString);
            kafkaUnit.setKafkaBrokerConfig("advertised.host.name","localhost");
            ExecutorService executorService = Executors.newSingleThreadExecutor();
    //        ConsumerLoop consumer = new ConsumerLoop(0, groupId, topics);
    //        executorService.submit(consumer);
                ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics, config.kafkaHost.get(i));
                consumers.add(consumer);
                executor.submit(consumer);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(1000000, TimeUnit.MILLISECONDS);
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
//            props.put("bootstrap.servers", "localhost:9092");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
//            props.put("MyTest", 123123);
//            props.put("offsets.topic.replication.factor", 1);
//            props.put("default.replication.factor", 1);
            String idString = "" + id;
            props.put("group.id", groupId);
//            props.put("num.partitions", config.kafkaHost.size());
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
                    System.out.println(consumer.listTopics().get("topic"));
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    System.out.println("records.count : " + records.count());

                    for (ConsumerRecord<String, String> record : records){
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
//                        System.out.println(record.value());
                        JSONObject jsonFromData = JSONObject.fromObject(record.value());
//                        String dateValue = (String)jsonFromData.get("locationtime");
//                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                        Date date = dateFormat.parse(dateValue);
//                        jsonFromData.accumulate("timestamp", date.getTime());

                        //template replace
                        long dataValue = (long)jsonFromData.get("locationtime");
//                        jsonFromData.accumulate("timestamp", dataValue);

                        getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
                    }
//                        JsonParser parser = new JsonParser();
//                        JsonObject object = (JsonObject) parser.parse(record.value());
//                        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
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
