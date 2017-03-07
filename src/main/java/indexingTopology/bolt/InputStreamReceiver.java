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
 * This bolt takes data tuples from its inputQueue and emits the data tuples to downstream bolt.
 * This bolt has backpressure mechanism.
 * Unless data tuples are inserted into the input queue, this bolt does not emit any tuple actually.
 */
public class InputStreamReceiver extends BaseRichBolt {

    OutputCollector collector;

    BackPressure backPressure;

    private final DataSchema schema;

    private int taskId;

    public ArrayBlockingQueue<DataTuple> inputQueue;

    public InputStreamReceiver(DataSchema schema) {
        this.schema = schema;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        inputQueue = new ArrayBlockingQueue<>(10000);
        backPressure = new BackPressure(TopologyConfig.EMIT_NUM, TopologyConfig.MAX_PENDING);
        taskId = topologyContext.getThisTaskId();
        Thread emittingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                        try {
                            //TODO: dequeue can be optimized by using drainer.
                            final long tupleId = backPressure.acquireNextTupleId();
                            final DataTuple dataTuple = inputQueue.take();
                            collector.emit(Streams.IndexStream, new Values(schema.serializeTuple(dataTuple), tupleId, taskId));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                }
            }
        });
        emittingThread.start();
    }


    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.AckStream)) {
            Long tupleId = tuple.getLongByField("tupleId");
            backPressure.ack(tupleId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));
    }
}
