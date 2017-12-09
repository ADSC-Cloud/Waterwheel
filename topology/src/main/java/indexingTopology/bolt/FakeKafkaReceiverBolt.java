package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import info.batey.kafka.unit.KafkaUnit;
import net.sf.json.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by billlin on 2017/12/5
 */
public class FakeKafkaReceiverBolt extends InputStreamReceiverBolt {

    private final DataSchema schema;
    static int  invokeNum = 0;
    int total = 100;
    static int meetRequirements = 0;
    transient KafkaUnit kafkaUnitServer;
    List<String> messages;

    TopologyConfig config;

    public FakeKafkaReceiverBolt(DataSchema schema, TopologyConfig config, KafkaUnit kafkaUnitServer, int total) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
        this.kafkaUnitServer = kafkaUnitServer;
        this.total = total;
        setKafkaUnit(kafkaUnitServer);
    }

    public void setKafkaUnit(KafkaUnit kafkaUnitServer){
        System.out.println("kafkaUnitServer.getZkPort() : " + this.kafkaUnitServer.getZkPort());
        System.out.println(this.kafkaUnitServer.getKafkaConnect()+ "  " + this.kafkaUnitServer.getBrokerPort()) ;
        try {
             messages = kafkaUnitServer.readMessages("consumer",total);
             kafkaUnitServer.shutdown();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    public void insertTupleTest(){
        try {
            for (int i = 0; i < messages.size(); i++) {
                JSONObject jsonFromData = JSONObject.fromObject(messages.get(i));
                getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
            }
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getMeetRequirements(){
        return this.meetRequirements;
    }

    public class ConsumerLoop implements Runnable {
        private final int id;

        public ConsumerLoop(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                    // the consumer whill bolck until the records coming
                List<String> messages = kafkaUnitServer.readMessages("consumer",1000);
                for(int i = 0; i < messages.size(); i++) {
                    System.out.println("111" + messages.get(i));
                    JSONObject jsonFromData = JSONObject.fromObject(messages.get(i));
                    getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
                }
//                    Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println("consumer start failed2!");
                e.printStackTrace();
            }
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
        final List<InputStreamKafkaReceiverBolt.ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            InputStreamKafkaReceiverBolt.ConsumerLoop consumer;
            consumer = bolt.new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (InputStreamKafkaReceiverBolt.ConsumerLoop consumer : consumers) {
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
        insertTupleTest();
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
