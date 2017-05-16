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

    private int waitTimeInSecond = 5;

    private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);

    int totalReceivedMessages;

    boolean throughputRequestEnable;

    Thread throughputRequestThread;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        numReceivedMessages = 0;

        totalReceivedMessages = 0;

        throughput = 0.0;

        numDispatchers = topologyContext.getComponentTasks("IndexerBolt").size();

        throughputRequestEnable = true;

        throughputRequestThread = new Thread(new ThroughputRequestSendingRunnable());
        throughputRequestThread.start();
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.ThroughputReportStream)) {
            ++numReceivedMessages;
            Double realTimeThroughput = tuple.getDoubleByField("throughput");
//            System.out.println("task id " + tuple.getSourceTask() + " " + realTimeThroughput);
            throughput += realTimeThroughput;
            if (numReceivedMessages == numDispatchers) {
                ++totalReceivedMessages;
//                if (totalReceivedMessages == 60) {
                LOG.info("Throughput : " + throughput);
                throughput = 0.0;
                numReceivedMessages = 0;
                throughputRequestEnable = true;
//                }
            }
        } else if (tuple.getSourceStreamId().equals(Streams.LoadBalanceStream)) {
            System.out.println("Load balance!!!");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.ThroughputRequestStream, new Fields("throughputRequest"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        throughputRequestThread.interrupt();
    }

    class ThroughputRequestSendingRunnable implements Runnable {

        @Override
        public void run() {
            try {
                Thread.sleep(waitTimeInSecond * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
//                e.printStackTrace();
            }
            final int sleepTimeInSecond = 10;
//            while (true) {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(sleepTimeInSecond * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
//                    e.printStackTrace();
                }


                if (throughputRequestEnable) {
                    collector.emit(Streams.ThroughputRequestStream, new Values("Throughput Request"));
                }
            }
        }

    }
}
