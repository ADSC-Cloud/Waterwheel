package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
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
    int totalNumber = 0;
    static int meetRequirements = 0;
    double x1;
    double x2;
    double y1;
    double y2;

    TopologyConfig config;

    public FakeKafkaReceiverBolt(DataSchema schema, TopologyConfig config, double x1, double x2, double y1, double y2, int total) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
        this.total = total;
    }

    public void insertTupleTest(){
        for(int i = 0;i < total;i++){
            DataTuple tuple = new DataTuple();
            Double lon = Math.random() * 100;
            Double lat = Math.random() * 100;
            final int id = new Random().nextInt(100);
            final String idString = "" + id;
            totalNumber++;
            tuple.add(lon);
            tuple.add(lat);
            tuple.add(totalNumber);
            tuple.add("asd");
            tuple.add(idString);
            if(lon >= x1 && lon <= x2 && lat >= y1 && lat <= y2){
                meetRequirements++;
            }
//            tuple.add("payload");

            try {
                getInputQueue().put(tuple);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
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
                    insertTupleTest();
                    Thread.sleep(1000);
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
        ConsumerLoop consumer = new ConsumerLoop(0);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(consumer);
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
