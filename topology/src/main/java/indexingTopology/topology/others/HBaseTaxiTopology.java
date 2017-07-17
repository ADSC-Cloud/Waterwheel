package indexingTopology.topology.others;

import indexingTopology.bolt.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.util.taxi.City;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by acelzj on 4/4/17.
 */
public class HBaseTaxiTopology {

    private static final String Generator = "IndexerBolt";
    private static final String LogWriter = "LogWriter";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        TopologyConfig config = new TopologyConfig();

        final double x1 = 116.2;
        final double x2 = 117.0;
        final double y1 = 39.6;
        final double y2 = 40.6;
        final int partitions = 1024;

        City city = new City(x1, x2, y1, y2, partitions);

        builder.setBolt(Generator, new HBaseTaxiGeneratorBolt(city, config), 24)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream);

        builder.setBolt(LogWriter, new LoggingBolt(), 1)
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
