package indexingTopology.bolt;

import indexingTopology.api.client.AppendRequest;
import indexingTopology.api.client.AppendRequestBatchMode;
import indexingTopology.api.client.MessageResponse;
import indexingTopology.api.server.*;
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
public class InputStreamReceiverBoltServer extends InputStreamReceiverBolt {

    private Server server;
    private int port;

    public InputStreamReceiverBoltServer(DataSchema schema, int port, TopologyConfig config) {
        super(schema, config);
        this.port = port;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        server = new Server(port, AppendServerHandle.class, new Class[]{BlockingQueue.class}, getInputQueue());
        server.startDaemon();
    }

    @Override
    public void cleanup() {
        super.cleanup();
        server.endDaemon();
    }

}
