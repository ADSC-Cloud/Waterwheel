package indexingTopology.bolt;

import com.alibaba.fastjson.JSONObject;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import info.batey.kafka.unit.KafkaUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import java.text.ParseException;
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
    String brokerString;
    String topic;

    TopologyConfig config;

    public FakeKafkaReceiverBolt(DataSchema schema, TopologyConfig config, String brokerString, String topic, int total) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
        this.brokerString = brokerString;
        this.topic = topic;
//        this.kafkaUnitServer = kafkaUnitServer;
        this.total = total;
//        setKafkaUnit(kafkaUnitServer);
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

    public void insertTupleTest(Properties props){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> topics = Arrays.asList(topic);
        System.out.println("topic : " + topic);
        consumer.subscribe(topics);
        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);


        for (ConsumerRecord<String, String> record : records) {
            Map<String, Object> data = new HashMap<>();
            data.put("partition", record.partition());
            data.put("offset", record.offset());
            data.put("value", record.value());
            System.out.println(record.value());
            JSONObject jsonFromData = JSONObject.parseObject(record.value());
            try {
                getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ParseException e) {
            e.printStackTrace();
            }
        }
//        try {
//            for (int i = 0; i < messages.size(); i++) {
//                JSONObject jsonFromData = JSONObject.fromObject(messages.get(i));
//                getInputQueue().put(schema.getTupleFromJsonObject(jsonFromData));
//            }
//        }catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
    public class ConsumerLoop implements Runnable {
        private final KafkaConsumer<String, String> consumer;
        private final List<String> topics;
        private final int id;

        public ConsumerLoop(int id, String groupId,  List<String> topics) {
            this.id = id;
            this.topics = topics;
            Properties props = new Properties();
//            props.put("key.deserializer", StringDeserializer.class.getCanonicalName());
//            props.put("value.deserializer", StringDeserializer.class.getCanonicalName());
            props.put("offsets.topic.replication.factor", 1);
            props.put("default.replication.factor", 1);


//            insertTupleTest(props);

//            props.put("bootstrap.servers", "localhost:9092");
//            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//            props.put("MyTest", 123123);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerString);
            this.consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topics);

                while (true) {
                    // the consumer whill bolck until the records coming
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);


                    for (ConsumerRecord<String, String> record : records){
                        invokeNum++;
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
                        System.out.println(record.value());
                        JSONObject jsonFromData = JSONObject.parseObject(record.value());//

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
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map,topologyContext,outputCollector);
//        insertTupleTest();
        String groupId = "consumer-group";
        List<String> topics = Arrays.asList(topic);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        ConsumerLoop consumer = new ConsumerLoop(0, groupId, topics);
        executorService.submit(consumer);

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
////            props.put("bootstrap.servers", "localhost:9092");
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
////            props.put("MyTest", 123123);
//        props.put("group.id", groupId);
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
//        this.consumer = new KafkaConsumer<>(props);
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
