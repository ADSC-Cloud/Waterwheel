package indexingTopology.server;

import org.apache.storm.task.OutputCollector;
import java.util.List;

/**
 * Created by robert on 17/7/17.
 */
public class StormCommunicator implements Communicator {
    OutputCollector collector;
    public StormCommunicator(OutputCollector collector) {
        this.collector = collector;
    }
    @Override
    public void sentDirect(String streamId, int receiverId, Object message) {
        collector.emitDirect(receiverId, streamId, (List<Object>)message);
    }

    @Override
    public void sendShuffle(String streamId, Object message) {
        collector.emit(streamId, (List<Object>)message);
    }
}
