package indexingTopology.api.server;

import indexingTopology.api.client.AppendRequest;
import indexingTopology.api.client.AppendRequestBatchMode;
import indexingTopology.api.client.MessageResponse;
import indexingTopology.common.data.DataTuple;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by robert on 18/7/17.
 */
public class AppendServerHandle extends ServerHandle implements AppendRequestHandle, IAppendRequestBatchModeHandle {

    BlockingQueue<DataTuple> inputQueue;

    public AppendServerHandle(BlockingQueue<DataTuple> inputQueue) {
        this.inputQueue = inputQueue;
    }

    @Override
    public void handle(AppendRequest tuple) throws IOException {
        DataTuple dataTuple = tuple.dataTuple;
        try {
            inputQueue.put(dataTuple);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
            objectOutputStream.writeUnshared(new MessageResponse("Timeout!"));
            objectOutputStream.reset();
        }
    }

    @Override
    public void handle(AppendRequestBatchMode tuple) throws IOException {

        tuple.dataTupleBlock.deserialize();
        tuple.dataTupleBlock.tuples.forEach(t -> {
            try {
                inputQueue.put(t);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        objectOutputStream.writeUnshared("handled!");
        objectOutputStream.reset();
    }
}
