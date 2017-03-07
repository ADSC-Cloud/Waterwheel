package indexingTopology.bolt;

import indexingTopology.DataSchema;
import indexingTopology.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.Acker;
import indexingTopology.util.Permutation;
import indexingTopology.util.texi.Car;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by acelzj on 21/2/17.
 */
public class Generator extends BaseRichBolt {

    OutputCollector collector;

    Acker acker;

    Long tupleId;

    private Random random;

    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;

    private long timestamp;

    private int taskId;

//    private ZipfDistribution distribution;
    private NormalDistribution distribution;

    private Permutation permutation;

    private int sleepTimeInSeconds = 30;

    private double offset = 80000;

    private double mean;

    private double sigma;

    public Generator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city, double mean, double sigma) {
        this.schema = schema;
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;

        this.mean = mean;
        this.sigma = sigma;
//        RandomGenerator randomGenerator = new Well19937c();
//        randomGenerator.setSeed(1000);
//        this.keyGenerator = new ZipfKeyGenerator(200048, 0.5, randomGenerator);
//        random = new Random(1000);
    }

    private void createDistributionChangingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(sleepTimeInSeconds * 1000);
//                    permutation.shuffle();
                    if (random.nextDouble() < 0.5) {
                        distribution = new NormalDistribution(mean, sigma - offset);
                    } else {
                        distribution = new NormalDistribution(mean, sigma + offset);
                    }
                }
            }
        }).start();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        acker = new Acker(TopologyConfig.EMIT_NUM);
        tupleId = 0L;
        taskId = topologyContext.getThisTaskId();

        random = new Random(2048);

//        distribution = new ZipfDistribution(200048, 0.5);
        distribution = new NormalDistribution(mean, sigma);
//        permutation = new Permutation(200048);
        Thread generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    if (tupleId < acker.currentCount.get() + TopologyConfig.MAX_PENDING) {
                        Car car = generator.generate();
//                        Integer key = distribution.sample() - 1;
//                        DataTuple dataTuple = new DataTuple(car.id, random.nextDouble(), new String(new char[payloadSize]), timestamp);
                        Integer key = (int) distribution.sample();
//                        System.out.println("sampled key " + key);
//                        key = key % (int) (mean + 3 * sigma);
//                        System.out.println("key " + key);
                        DataTuple dataTuple = new DataTuple(car.id, Math.abs(key) % (int) mean, new String(new char[payloadSize]), timestamp);
//                        DataTuple dataTuple = new DataTuple(car.id, key.doubleValue(), new String(new char[payloadSize]), timestamp);
//                        System.out.println(tupleId + " has been emitted!!!");
                        collector.emit(Streams.IndexStream, new Values(schema.serializeTuple(dataTuple), tupleId, taskId));

                        ++tupleId;

                        ++timestamp;
                    }
                }
            }
        });
        generationThread.start();

        createDistributionChangingThread();
    }


    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.AckStream)) {
            Long tupleId = tuple.getLongByField("tupleId");
            acker.ack(tupleId);
//            System.out.println("tuple id " + tupleId + "has been acked!!!");
//            System.out.println(acker.pendingIds);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));
    }
}
