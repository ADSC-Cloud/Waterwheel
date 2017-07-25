package indexingTopology.bolt;

import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.DataTuple;
import indexingTopology.config.TopologyConfig;
import indexingTopology.server.StormCommunicator;
import indexingTopology.server.TupleReceiver;
import indexingTopology.streams.Streams;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This bolt takes data tuples from its inputQueue and emits the data tuples to downstream bolt.
 * This bolt has backpressure mechanism.
 * Unless data tuples are inserted into the input queue, this bolt does not emit any tuple actually.
 */
public class InputStreamReceiverBolt extends BaseRichBolt {

    private final DataSchema schema;

    TopologyConfig config;

    private TupleReceiver tupleReceiver;

    public InputStreamReceiverBolt(DataSchema schema, TopologyConfig config) {
        this.schema = schema;
        this.config = config;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        tupleReceiver = new TupleReceiver(new StormCommunicator(outputCollector), schema, config);
        tupleReceiver.setId(topologyContext.getThisTaskId());
        tupleReceiver.prepare();
    }


    @Override
    final public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.AckStream)) {
            Long tupleId = tuple.getLongByField("tupleId");
            tupleReceiver.acknowledge(tupleId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        tupleReceiver.close();
    }

    protected LinkedBlockingQueue<DataTuple> getInputQueue() {
        return tupleReceiver.getInputQueue();
    }
}
