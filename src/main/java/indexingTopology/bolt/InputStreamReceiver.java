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
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by acelzj on 21/2/17.
 */
public class InputStreamReceiver extends BaseRichBolt {

    OutputCollector collector;

    BackPressure backPressure;

    Long tupleId;

    private Random random;

    private final DataSchema schema;

    private TrajectoryGenerator generator;

    private City city;

    private int payloadSize;

    private long timestamp;

    private int taskId;

    private ZipfDistribution distribution;

    private Permutation permutation;

    private ArrayBlockingQueue<DataTuple> inputQueue;

    public InputStreamReceiver(DataSchema schema, TrajectoryGenerator generator, int payloadSize, City city) {
        this.schema = schema;
        this.generator = generator;
        this.city = city;
        this.payloadSize = payloadSize;
//        RandomGenerator randomGenerator = new Well19937c();
//        randomGenerator.setSeed(1000);
//        this.keyGenerator = new ZipfKeyGenerator(200048, 0.5, randomGenerator);
        distribution = new ZipfDistribution(200048, 0.5);
        permutation = new Permutation(200048);
//        random = new Random(1000);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        backPressure = new BackPressure(TopologyConfig.EMIT_NUM);
        tupleId = 0L;
        taskId = topologyContext.getThisTaskId();
        Thread generationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    if (tupleId < backPressure.currentCount.get() + TopologyConfig.MAX_PENDING) {
                        try {
                            //TODO: dequeue can be optimized by using drainer.
                            final DataTuple dataTuple = inputQueue.take();
                            collector.emit(Streams.IndexStream, new Values(schema.serializeTuple(dataTuple), tupleId, taskId));
                            ++tupleId;
                            ++timestamp;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        generationThread.start();
    }


    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.AckStream)) {
            Long tupleId = tuple.getLongByField("tupleId");
            backPressure.ack(tupleId);
//            System.out.println("tuple id " + tupleId + "has been acked!!!");
//            System.out.println(backPressure.pendingIds);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));
    }
}
