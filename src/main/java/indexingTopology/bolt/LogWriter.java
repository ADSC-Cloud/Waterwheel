package indexingTopology.bolt;

import indexingTopology.spout.TexiTrajectoryGenerator;
import indexingTopology.streams.Streams;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 22/2/17.
 */
public class LogWriter extends BaseRichBolt {

    OutputCollector collector;

    int numReceivedMessages;

    int numDispatchers;

    Double throughput;

    private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);

    int totalReceivedMessages;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        numReceivedMessages = 0;

        totalReceivedMessages = 0;

        throughput = 0.0;

        numDispatchers = topologyContext.getComponentTasks("DispatcherBolt").size();

        Thread throughputRequestThread = new Thread(new ThroughputRequestSendingRunnable());
        throughputRequestThread.start();
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.ThroughputReportStream)) {
            ++numReceivedMessages;
            throughput += tuple.getDoubleByField("throughput");
            if (numReceivedMessages == numDispatchers) {
                ++totalReceivedMessages;
//                if (totalReceivedMessages == 60) {
                LOG.info("Throughput : " + throughput);
                throughput = 0.0;
                numReceivedMessages = 0;
//                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ThroughputRequestStream, new Fields("throughputRequest"));
    }

    class ThroughputRequestSendingRunnable implements Runnable {

        @Override
        public void run() {
            final int sleepTimeInSecond = 1;
            while (true) {
                try {
                    Thread.sleep(sleepTimeInSecond * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                collector.emit(Streams.ThroughputRequestStream, new Values("Throughput Request"));
            }
        }

    }
}
