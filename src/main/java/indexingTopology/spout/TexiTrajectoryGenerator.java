package indexingTopology.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 7/27/16.
 */
public class TexiTrajectoryGenerator extends BaseRichSpout {

    SpoutOutputCollector collector_;
    File file;
    BufferedReader bufferedReader;
    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;


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
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, new Fields(fields));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_ = collector;
    }

    public void nextTuple() {
        Car car = generator.generate();
        final long timestamp = System.currentTimeMillis();
        collector_.emit(NormalDistributionIndexingTopology.IndexStream, new Values((double)car.id, (double)city.getZCodeForALocation(car.x
        , car.y), new String(new char[payloadSize]), timestamp), new Object());
    }
}
