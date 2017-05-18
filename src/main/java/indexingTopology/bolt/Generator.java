package indexingTopology.bolt;


import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.util.FrequencyRestrictor;
import indexingTopology.util.Permutation;
import indexingTopology.util.taxi.Car;
import indexingTopology.util.taxi.City;
import indexingTopology.util.taxi.TrajectoryGenerator;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;

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

    private int taskId;

//    private ZipfDistribution distribution;
    private NormalDistribution distribution;

    private int sleepTimeInSeconds = 30;

    private double offset = 10000;

    private double mean;

    private double sigma;

    private Permutation permutation;

    private Random random;

    private FrequencyRestrictor frequencyRestrictor;

    private Thread generationThread;

    public Generator(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city) {
        super(schema, new TopologyConfig());
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
    }

    private void createDistributionChangingThread() {
        new Thread(new Runnable() {
            private double lowerBound = 300000;
            private double upperBound = 700000;
            @Override
            public void run() {
                while (true) {
                    Utils.sleep(sleepTimeInSeconds * 1000);
//                    permutation.shuffle();
                    if (random.nextDouble() < 0.5) {
                        mean = mean + offset;
                        if (mean > upperBound) {
                            mean = upperBound - offset;
                        }
                        distribution = new NormalDistribution(mean, sigma);
                    } else {
                        mean  = mean - offset;
                        if (mean < lowerBound) {
                            mean = lowerBound + offset;
                        }
                        distribution = new NormalDistribution(mean, sigma);
                    }
                }
            }
        }).start();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        random = new Random(2048);

//        distribution = new ZipfDistribution(200048, 0.5);
        mean = 500000.0;
        sigma = 5000.0;
        distribution = new NormalDistribution(mean, sigma);
//        frequencyRestrictor = new FrequencyRestrictor(1500000, 50);
//        permutation = new Permutation(200048);
//        distribution = new ZipfDistribution(200048, 0.5);
//        permutation = new Permutation(200048);
        super.prepare(map, topologyContext, outputCollector);

        generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Car car = generator.generate();
//                        Integer key = distribution.sample() - 1;
//                        DataTuple dataTuple = new DataTuple(car.id, random.nextDouble(), new String(new char[payloadSize]), timestamp);
                        Integer key = (int) distribution.sample();

                        while (key > mean + 3 * sigma || key < mean - 3 * sigma) {
                            key = (int) distribution.sample();
                        }
//                        System.out.println("sampled key " + key);
//                        key = key % (int) (mean + 3 * sigma);
//                        System.out.println("key " + key);
                        final DataTuple dataTuple = new DataTuple(car.id, key, new String(new char[payloadSize]), timestamp);
//                        DataTuple dataTuple = new DataTuple(car.id, key.doubleValue(), new String(new char[payloadSize]), timestamp);
//                        System.out.println(tupleId + " has been emitted!!!");
                        inputQueue.put(dataTuple);
                        ++timestamp;
                    } catch (Exception e) {
//                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
        generationThread.start();

//        createDistributionChangingThread();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        generationThread.interrupt();
    }
}
