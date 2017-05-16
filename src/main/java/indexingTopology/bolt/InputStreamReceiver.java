package indexingTopology.bolt;

import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.BackPressure;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    public LinkedBlockingQueue<DataTuple> inputQueue;

    private Thread emittingThread;

    public InputStreamReceiver(DataSchema schema) {
        this.schema = schema;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        inputQueue = new LinkedBlockingQueue<>(10000);
        backPressure = new BackPressure(TopologyConfig.EMIT_NUM, TopologyConfig.MAX_PENDING);
        taskId = topologyContext.getThisTaskId();
        emittingThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
                        try {
                            //TODO: dequeue can be optimized by using drainer.
                            final long tupleId = backPressure.acquireNextTupleId();
                            final DataTuple dataTuple = inputQueue.take();
                            collector.emit(Streams.IndexStream, new Values(schema.serializeTuple(dataTuple), tupleId, taskId));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                }
            }
        });
        emittingThread.start();

//        Thread capacityCheckingThread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        Thread.sleep(1 * 1000);
//                        System.out.println("Input queue size " + inputQueue.size());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        });
//        capacityCheckingThread.start();
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

    @Override
    public void cleanup() {
        super.cleanup();
        emittingThread.interrupt();
    }
}
