package indexingTopology.bolt;

import indexingTopology.api.client.AppendRequest;
import indexingTopology.api.client.AppendRequestBatchMode;
import indexingTopology.api.client.MessageResponse;
import indexingTopology.api.server.AppendRequestHandle;
import indexingTopology.api.server.IAppendRequestBatchModeHandle;
import indexingTopology.api.server.Server;
import indexingTopology.api.server.ServerHandle;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by robert on 8/3/17.
 */
public class InputStreamReceiverServer extends InputStreamReceiver {

    Server server;
    int port;

    public InputStreamReceiverServer(DataSchema schema, int port, TopologyConfig config) {
        super(schema, config);
        this.port = port;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        server = new Server(port, AppendServerHandle.class, new Class[]{BlockingQueue.class}, inputQueue);
        server.startDaemon();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        server.endDaemon();
    }

    public static class AppendServerHandle extends ServerHandle implements AppendRequestHandle, IAppendRequestBatchModeHandle {

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
//            if (tuple.requireAck) {
//                objectOutputStream.writeUnshared("received");
//                objectOutputStream.reset();
//            }
            tuple.dataTupleBlock.deserialize();
            tuple.dataTupleBlock.tuples.forEach(t -> {
                try {
                    inputQueue.put(t);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
//                    e.printStackTrace();
//                    return;
                }
            });
            objectOutputStream.writeUnshared("handled the block!");
            objectOutputStream.reset();
        }
    }
}
