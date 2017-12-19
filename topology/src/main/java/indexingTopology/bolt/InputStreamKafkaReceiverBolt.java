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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by billlin on 2017/11/23
 */
public class InputStreamKafkaReceiverBolt extends InputStreamReceiverBolt {

    private final DataSchema schema;
    BlockingQueue<DataTuple> inputQueue;
    static int  invokeNum = 0;

    TopologyConfig config;

    public InputStreamKafkaReceiverBolt(DataSchema schema, TopologyConfig config) {
        super(schema, config);
        this.schema = schema;
        this.config = config;
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

}
