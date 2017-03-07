package indexingTopology.bolt;

import indexingTopology.DataSchema;
import indexingTopology.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.BackPressure;
import indexingTopology.util.Permutation;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by acelzj on 21/2/17.
 */
public class Generator extends InputStreamReceiver {


    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;

    private long timestamp;

    private ZipfDistribution distribution;

    private Permutation permutation;

    public Generator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city) {
        super(schema);
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        distribution = new ZipfDistribution(200048, 0.5);
        permutation = new Permutation(200048);
        super.prepare(map, topologyContext, outputCollector);
        Thread generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Car car = generator.generate();
                        Integer key = distribution.sample() - 1;
                        final DataTuple dataTuple = new DataTuple(car.id, permutation.get(key).doubleValue(), new String(new char[payloadSize]), timestamp);
                        inputQueue.put(dataTuple);
                        ++timestamp;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        generationThread.start();
    }

}
