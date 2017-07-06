package indexingTopology.api.server;

import indexingTopology.api.client.*;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.PartialQueryResult;
import indexingTopology.util.FrequencyRestrictor;
import org.apache.storm.metric.internal.RateTracker;

import java.io.IOException;

/**
 * Created by robert on 3/3/17.
 */
public class FakeServerHandle extends ServerHandle implements QueryHandle, AppendRequestHandle, IAppendRequestBatchModeHandle {

    public RateTracker rateTracker;

    private Thread throughputDisplayThread;

    public volatile FrequencyRestrictor restrictor;

    final private Object lock = new Object();

    int currentFrequency = 100000;

    private Thread rateFluctuationThread;

    public FakeServerHandle(int i) {
        System.out.println("Construct function is called!");
        rateTracker = new RateTracker(1000,5);
        restrictor = new FrequencyRestrictor(600000, 5);
        throughputDisplayThread = new Thread(() ->{
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    Thread.currentThread().interrupt();
                    break;
                }
                System.out.println(String.format("Throughput: %4.4f", rateTracker.reportRate()));
            }
        });
        throughputDisplayThread.start();

        rateFluctuationThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(100);
                    currentFrequency = (currentFrequency + 100000) % 600000;
                    synchronized (lock) {
                        restrictor.close();
                        restrictor = new FrequencyRestrictor(Math.max(10000, currentFrequency), 5);
                    }
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                    break;
                }
            }
        });
//        rateFluctuationThread.start();
    }

    public void handle(final QueryRequest clientQueryRequest) throws IOException {
        DataTuple dataTuple = new DataTuple();
        dataTuple.add("ID 1");
        dataTuple.add(100);
        dataTuple.add(3.14);

        DataTuple dataTuple1 = new DataTuple();
        dataTuple1.add("ID 2");
        dataTuple1.add(200);
        dataTuple1.add(6.34);

        PartialQueryResult particalQueryResult = new PartialQueryResult();
        particalQueryResult.add(dataTuple);
        particalQueryResult.add(dataTuple1);
        particalQueryResult.setEOFflag();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        objectOutputStream.writeObject(new QueryResponse(particalQueryResult, 1L));

    }


    @Override
    public void handle(final AppendRequest tuple) throws IOException {
        objectOutputStream.writeObject(new MessageResponse(String.format("Insertion [%s] success!", tuple.dataTuple)));
    }

    @Override
    public void handle(AppendRequestBatchMode tuple) throws IOException {
        tuple.dataTupleBlock.deserialize();
        final int size = tuple.dataTupleBlock.tuples.size();
        rateTracker.notify(size);
        try {
            synchronized (lock) {
                restrictor.getPermission(size);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        rateTracker.close();
        throughputDisplayThread.interrupt();
        rateFluctuationThread.interrupt();
        restrictor.close();
    }
}
