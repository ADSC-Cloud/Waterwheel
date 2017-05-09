package indexingTopology.bolt;

import indexingTopology.client.*;
import indexingTopology.data.DataSchema;
import indexingTopology.data.DataTuple;
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

    public InputStreamReceiverServer(DataSchema schema, int port) {
        super(schema);
        this.port = port;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        server = new Server(port, AppendServerHandle.class, new Class[]{BlockingQueue.class}, inputQueue);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.startDaemon();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    @Override
    public void cleanup() {
        super.cleanup();
        server.endDaemon();
    }

    public static class AppendServerHandle extends ServerHandle implements AppendRequestHandle {

        BlockingQueue<DataTuple> inputQueue;

        public AppendServerHandle(BlockingQueue<DataTuple> inputQueue) {
            this.inputQueue = inputQueue;
        }

        @Override
        public void handle(AppendRequest tuple) throws IOException {
            DataTuple dataTuple = tuple.dataTuple;
            try {
                inputQueue.put(dataTuple);
                objectOutputStream.writeObject(new MessageResponse("Inserted!"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
                objectOutputStream.writeObject(new MessageResponse("Timeout!"));
            }
        }
    }
}
