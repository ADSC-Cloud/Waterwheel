package indexingTopology.spout;

import indexingTopology.streams.Streams;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import indexingTopology.DataSchema;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 7/27/16.
 */
public class TexiTrajectoryGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TexiTrajectoryGenerator.class);

    SpoutOutputCollector collector_;
    File file;
    BufferedReader bufferedReader;
    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;

    private long timestamp;

    public TexiTrajectoryGenerator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city)
            throws FileNotFoundException {
        this.schema = schema;
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");
        declarer.declareStream(Streams.IndexStream, new Fields(fields));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_ = collector;
    }

    public void nextTuple() {
        Car car = generator.generate();
//        final long timestamp = System.currentTimeMillis();
        collector_.emit(Streams.IndexStream, new Values((double)car.id, (double)city.getZCodeForALocation(car.x
        , car.y), new String(new char[payloadSize]), timestamp), new Object());
//        collector_.emit(Streams.IndexStream, new Values(car.id, city.getZCodeForALocation(car.x
//                , car.y), new String(new char[payloadSize]), timestamp), new Object());
        ++timestamp;
    }

    @Override
    public void ack(Object msgId) {
//        LOG.info("tuple ack: " + msgId + "," + System.currentTimeMillis() /1000);
    }
}
