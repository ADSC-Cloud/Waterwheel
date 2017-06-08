package indexingTopology.api.client;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.data.DataTupleBlock;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.Semaphore;

/**
 * Created by robert on 9/3/17.
 */
public class IngestionClientBatchMode extends ClientSkeleton implements IIngestionClient {

    DataSchema schema;
    int batchSize;

    DataTupleBlock dataTupleBlock;
    Semaphore semaphore = new Semaphore(1000);

    Thread receivingThread;

    public IngestionClientBatchMode(String serverHost, int port, DataSchema schema, int batchSize) {
        super(serverHost, port);
        this.schema = schema;
        this.batchSize = batchSize;
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
    }

    public IResponse append(DataTuple dataTuple) throws IOException {

        appendInBatch(dataTuple);

//        return (Response) objectInputStream.readObject();
        return null;
    }

    @Override
    public void appendInBatch(DataTuple tuple) throws IOException {
        while (!dataTupleBlock.add(tuple)) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (dataTupleBlock.tuples.size() == 0)
            return;
        dataTupleBlock.serialize();
//        try {
//            semaphore.acquire();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            Thread.currentThread().interrupt();
//        }
        objectOutputStream.writeUnshared(new AppendRequestBatchMode(dataTupleBlock, true));
        objectOutputStream.flush();
        objectOutputStream.reset();
        dataTupleBlock = new DataTupleBlock(schema, batchSize);
//        objectInputStream.readUnshared();
    }

    public void close() throws IOException {
        super.close();
        receivingThread.interrupt();
    }

    public void connect() throws IOException {
        super.connect();
        receivingThread = new Thread(() -> {
            while (true) {
                try {
                    synchronized (client) {
                        objectInputStream.readUnshared();
                    }
                    semaphore.release();
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
//        receivingThread.start();
    }

}
