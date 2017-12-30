package indexingTopology.api.client;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import indexingTopology.common.data.DataTuple;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

/**
 * Create by zelin on 17-12-20
 **/
public class IngestionKafkaBatchMode implements IIngestionKafka{

    String servers;
    String topic;
    Producer<String, String> producer = null;

    public IngestionKafkaBatchMode(String kafkalocalhost, String topic) {
        this.servers = kafkalocalhost;
        this.topic = topic;
    }

    @Override
    public void send(int i, String Msg) {
        this.producer.send(new ProducerRecord<String, String>(topic, String.valueOf(i), Msg));
//        System.out.println("?");
    }

    @Override
    public void ingestProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", 0);
        props.put("acks", "all");
        props.put("retries", "0");
        props.put("batch.size", 16384);
        props.put("auto.commit.interval.ms", "1000");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public void close(){
        producer.close();
    }
}
