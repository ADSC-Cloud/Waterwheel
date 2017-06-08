package indexingTopology.spout;

import indexingTopology.common.data.DataTuple;
import indexingTopology.streams.Streams;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import indexingTopology.common.data.DataSchema;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Random;

/**
 * Created by acelzj on 7/27/16.
 */
public class TaxiTrajectoryGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(TaxiTrajectoryGenerator.class);

    SpoutOutputCollector collector_;
    File file;
    BufferedReader bufferedReader;
    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;

    private long timestamp;

    private ZipfDistribution distribution;

    private Random random;

    public TaxiTrajectoryGenerator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city)
            throws FileNotFoundException {
        this.schema = schema;
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
//        RandomGenerator randomGenerator = new Well19937c();
//        randomGenerator.setSeed(1000);
//        this.keyGenerator = new ZipfKeyGenerator(200048, 0.5, randomGenerator);
//        distribution = new ZipfDistribution(2000048, 0.5);
        random = new Random(1000);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Streams.IndexStream, new Fields("tuple"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_ = collector;
    }

    public void nextTuple() {
        Car car = generator.generate();
//        final long timestamp = System.currentTimeMillis();
//        DataTuple dataTuple = new DataTuple(car.id, city.getZCodeForALocation(car.x
//                , car.y), new String(new char[payloadSize]), timestamp);


//        DataTuple dataTuple = new DataTuple(car.id, distribution.sample(), new String(new char[payloadSize]), timestamp);
        DataTuple dataTuple = new DataTuple(car.id, random.nextDouble(), new String(new char[payloadSize]), timestamp);

//        collector_.emit(Streams.IndexStream, new Values(dataTuple), new Object());
        collector_.emit(Streams.IndexStream, new Values(schema.serializeTuple(dataTuple)), new Object());
//        collector_.emit(Streams.IndexStream, new Values(car.id, city.getZCodeForALocation(car.x
//                , car.y), new String(new char[payloadSize]), timestamp), new Object());
        ++timestamp;
    }

    @Override
    public void ack(Object msgId) {
//        LOG.info("tuple ack: " + msgId + "," + System.currentTimeMillis() /1000);
    }
}
