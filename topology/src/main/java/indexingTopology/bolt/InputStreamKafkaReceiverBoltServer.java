package indexingTopology.bolt;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.KafkaDataFromPoliceSchema;
import indexingTopology.config.TopologyConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by billlin on 2017/11/23
 */
public class InputStreamKafkaReceiverBoltServer extends InputStreamReceiverBolt {

    private final DataSchema schema;
    private int port;
    private String topic;
    TopologyConfig config;
    KafkaDataFromPoliceSchema kafkaDataSchema;
    int total = 0;

    public InputStreamKafkaReceiverBoltServer(DataSchema schema, int port, TopologyConfig config, String topic) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
        this.port = port;
        this.topic = topic;
        kafkaDataSchema = new KafkaDataFromPoliceSchema();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        int numConsumers = config.kafkaHost.size();
        String groupId = "consumer-1231";
        List<String> topics = Arrays.asList(topic);
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
//        ConsumerLoop consumer = new ConsumerLoop(0, groupId,  topics);
//        executorService.submit(consumer);
        final List<ConsumerLoop> consumers = new ArrayList<>();
        System.out.println("config.kafkaHost" + config.kafkaHost);
        String regEx = "[`~!@#$%^&*()+=|{}';'\\[\\]<>/?~！@#�%……&*（）——+|{}【】‘；：”“’。，、？]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(config.kafkaHost.toString());
        String currentKafkahost = m.replaceAll("").trim();
//        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(0, groupId, topics, currentKafkahost);
            consumers.add(consumer);
            executor.submit(consumer);
//        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
//                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
//                }
                executor.shutdown();
                try {
                    executor.awaitTermination(100000, TimeUnit.MILLISECONDS);
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
//            props.put("MyTest", 123123);
//            props.put("offsets.topic.replication.factor", 1);
//            props.put("default.replication.factor", 1);
            String regEx = "[`~!@#$%^&*()+=|{}';'\\[\\]<>/?~！@#�%……&*（）——+|{}【】‘；：”“’。，、？]";
            Pattern p = Pattern.compile(regEx);
            Matcher m = p.matcher(config.kafkaHost.toString());
            String zkhostList = m.replaceAll("").trim();
//            props.put("zookeeper.connect",zkhostList);



            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
            props.put("group.id", groupId);
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(props);

        }

        @Override
        public void run() {
            while (true) {
                try {
                    consumer.subscribe(topics);
//                    System.out.println("topics : " + topics);
                    // the consumer whill bolck until the records coming
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    //                    System.out.println("records.count : " + records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
//                        if (total <= 100) {
//                            System.out.println(record.value());
//                            total++;
//                        }
                        try{
                            JSONObject jsonFromData = JSONObject.parseObject(record.value());
//                            System.out.println(record.value());
                            boolean checkRecord = kafkaDataSchema.checkDataIntegrity(jsonFromData);
                            if(checkRecord){ // filter incomplete data
                                DataTuple dataTuple = schema.getTupleFromJsonObject(jsonFromData);
                                if(dataTuple != null){
                                    getInputQueue().put(dataTuple);
//                                    System.out.println("Filter datatuple success: " + record.value());
                                }
                            }
                        } catch (ParseException e){
                            e.printStackTrace();
                            continue;
                        } catch (JSONException e){
                            System.out.println("Record error : Json format exception!The json is " + record.value());
//                            e.printStackTrace();
                            continue;
                        } catch (NullPointerException e){
                            System.out.println("Record error : Some json attribute is null!!The json is " + record.value());
//                            e.printStackTrace();
                            continue;
                        } catch (Exception e){
                            System.out.println("Record error : Unexpected Exception,Consumer record wrong!The json is " + record.value());
//                            e.printStackTrace();
                            continue;
                        }

                        //template replace
                        //                        Date date = ""
                        //                        Date dataValue = (Date)jsonFromData.get("locationtime");
                        //                        jsonFromData.remove("locationtime");
                        //                        jsonFromData.put("locationtime", dataValue.getTime());

                    }
                    //                        JsonParser parser = new JsonParser();
                    //                        JsonObject object = (JsonObject) parser.parse(record.value());
                    //                        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                } catch (WakeupException e) {
                    System.out.println("Consumer poll stop!");
                    e.printStackTrace();
                    // break the loop here to avoid the extremely long waiting time in case of shutdown
                    break;
                    // ignore for shutdown
                } catch (Exception e) {
                    System.out.println("Consumer exception!");
                    e.printStackTrace();
                }
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
