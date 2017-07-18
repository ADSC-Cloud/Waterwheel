package indexingTopology.server;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.BackPressure;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by robert on 17/7/17.
 */
public class TupleReceiver {
    private Communicator communicator;
    private DataSchema schema;
    private LinkedBlockingQueue<DataTuple> inputQueue;
    private BackPressure backPressure;
    private TopologyConfig config;
    private int id;
    private Thread emittingThread;
    private Thread backPressureDisplayThread;

    public TupleReceiver(Communicator communicator, DataSchema schema, TopologyConfig config) {
        this.communicator = communicator;
        this.schema = schema;
        this.config = config;
    }

    public LinkedBlockingQueue<DataTuple> getInputQueue() {
        return inputQueue;
    }

    public void prepare() {
        inputQueue = new LinkedBlockingQueue<>(10000);
        backPressure = new BackPressure(config.EMIT_NUM, config.MAX_PENDING, config);
        emittingThread = new Thread(() -> {
            List<DataTuple> drainer = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    //TODO: dequeue can be optimized by using drainer.
                    final DataTuple firstTuple = inputQueue.take();

                    drainer.add(firstTuple);

                    inputQueue.drainTo(drainer, 1024);

                    for (DataTuple tuple : drainer) {

                        final long tupleId = backPressure.acquireNextTupleId();
                        communicator.sendShuffle(Streams.IndexStream, new Values(schema.serializeTuple(tuple), tupleId, id));
                    }
                    drainer.clear();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        emittingThread.start();

        backPressureDisplayThread = new Thread(() -> {
            while(true) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
                System.out.println(backPressure);
                if (Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        backPressureDisplayThread.start();
    }

    public void acknowledge(Long tupleID) {
        backPressure.ack(tupleID);
    }

    public void close() {
        emittingThread.interrupt();
        backPressureDisplayThread.interrupt();
    }

    public void setId(int id) {
        this.id = id;
    }
}
