package indexingTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import indexingTopology.spout.CSVReaderSpout;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class indexTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("TupleGenerator",new CSVReaderSpout(args[0]),1);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("generatorTest", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
