package indexingTopology.topology.others;

import indexingTopology.bolt.HBaseNetworkGenerator;
import indexingTopology.bolt.LogWriter;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by acelzj on 4/4/17.
 */
public class HBaseNetworkTopology {

    private static final String Generator = "IndexerBolt";
    private static final String LogWriter = "LogWriter";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfig config = new TopologyConfig();

        builder.setBolt(Generator, new HBaseNetworkGenerator(config), 24)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream);

        builder.setBolt(LogWriter, new LogWriter(), 1)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(Generator, Streams.ThroughputReportStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(12);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        StormTopology topology = builder.createTopology();

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("T1", conf, builder.createTopology());

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology);
    }
}
