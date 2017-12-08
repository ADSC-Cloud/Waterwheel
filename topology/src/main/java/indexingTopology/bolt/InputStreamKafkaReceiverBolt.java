package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by billlin on 2017/11/23
 */
public class InputStreamKafkaReceiverBolt extends InputStreamReceiverBolt {

    private final DataSchema schema;
    BlockingQueue<DataTuple> inputQueue;
    boolean insert = false;
    static int  invokeNum = 0;

    TopologyConfig config;

    public InputStreamKafkaReceiverBolt(DataSchema schema, TopologyConfig config) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
    }

    public void insertTupleTest(){
        if(insert == true){
            return;
        }
        for(int i = 0;i < 10;i++){
            DataTuple tuple = new DataTuple();
            tuple.add(i);
            tuple.add("asd");
            tuple.add("qwe");
//            tuple.add("payload");

            try {
                getInputQueue().put(tuple);
            } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            e.printStackTrace();
            }
        }
    }


    public class ConsumerLoop implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final List<String> topics;
        private final int id;

        public ConsumerLoop(int id, String groupId,  List<String> topics) {
            this.id = id;
            this.topics = topics;
            Properties props = new Properties();
//            props.put("bootstrap.servers", "localhost:9092");
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//            props.put("MyTest", 123123);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topics);

                while (true) {
                    System.out.println("-------------------------------------");
                    // the consumer whill bolck until the records coming
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);


                    for (ConsumerRecord<String, String> record : records){
                        invokeNum++;
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
                        System.out.println(record.value());
                        JSONObject jsonFromData = JSONObject.fromObject(record.value());
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

    public static void main(String[] args) {
        int numConsumers = 3;
        String groupId = "consumer-tutorial-group";
        List<String> topics = Arrays.asList("consumer");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

//        int i = 0;
        DataSchema dataSchema = new DataSchema();
        TopologyConfig config = new TopologyConfig();
        InputStreamKafkaReceiverBolt bolt = new InputStreamKafkaReceiverBoltServer(dataSchema,9092,config);
//        InputStreamKafkaReceiverBolt.ConsumerLoop consumer = bolt.new ConsumerLoop(i, groupId, topics);
        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer;
            consumer = bolt.new ConsumerLoop(i, groupId, topics);
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
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map,topologyContext,outputCollector);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
    public LinkedBlockingQueue<DataTuple> getInputQueue() {
        return super.getInputQueue();
    }

    public int getInvokeNum(){
        System.out.println("invokeNum : " + invokeNum);
        return invokeNum;
    }
}
