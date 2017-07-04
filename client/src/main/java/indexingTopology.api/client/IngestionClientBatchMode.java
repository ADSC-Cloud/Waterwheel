package indexingTopology.api.client;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.DataTupleBlock;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by robert on 9/3/17.
 */
public class IngestionClientBatchMode extends ClientSkeleton implements IIngestionClient {

    DataSchema schema;
    int batchSize;

    DataTupleBlock dataTupleBlock;
    Semaphore semaphore;

    Thread receivingThread;

    AtomicInteger unackedBatches = new AtomicInteger(0);

    AtomicInteger tuplesAccumulatedInBatches = new AtomicInteger(0);

    public IngestionClientBatchMode(String serverHost, int port, DataSchema schema, int batchSize) {
        this(serverHost, port, schema, batchSize, 100);
    }

    public IngestionClientBatchMode(String serverHost, int port, DataSchema schema, int batchSize, int batchesOnTheFly) {
        super(serverHost, port);
        semaphore = new Semaphore(batchesOnTheFly);
        this.schema = schema;
        this.batchSize = batchSize;
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
    }

    public IResponse append(DataTuple dataTuple) throws IOException {

        appendInBatch(dataTuple);

//        return (Response) objectInputStream.readObject();
        return null;
    }

    public void waitFinish() {
        while(unackedBatches.get() != 0 || tuplesAccumulatedInBatches.get() != 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException {
        tuplesAccumulatedInBatches.incrementAndGet();
        while (!dataTupleBlock.add(tuple)) {
            flush();
        }

    }

    @Override
    public void flush() throws IOException {
        if (dataTupleBlock.tuples.size() == 0)
            return;
        dataTupleBlock.serialize();
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        unackedBatches.incrementAndGet();
        objectOutputStream.writeUnshared(new AppendRequestBatchMode(dataTupleBlock, true));
        objectOutputStream.flush();
        objectOutputStream.reset();
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
        tuplesAccumulatedInBatches.set(0);
    }

    public void close() throws IOException {
        receivingThread.interrupt();
        super.close();
    }

    public void connect() throws IOException {
        super.connect();
        receivingThread = new Thread(() -> {
            while (true) {
                try {
//                    synchronized (client) {
                    objectInputStream.readUnshared();
//                    }
                    semaphore.release();
                    unackedBatches.decrementAndGet();
                } catch (SocketTimeoutException e) {

                } catch (IOException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
//                        e1.printStackTrace();
                        Thread.currentThread().interrupt();
                    }

                    if (isClosed()) {
                        break;
                    }
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                if (Thread.currentThread().isInterrupted())
                    Thread.currentThread().interrupt();
            }

        });
        receivingThread.start();
    }

}
