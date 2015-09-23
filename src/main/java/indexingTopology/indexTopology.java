package indexingTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import indexingTopology.bolt.DispatcherBolt;
import indexingTopology.bolt.IndexerBolt;
import indexingTopology.spout.CSVReaderSpout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class indexTopology {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        List<String> fieldNames=new ArrayList<String>(Arrays.asList("user_id","id_1","id_2","ts_epoch",
                "date","time","latitude","longitude","time_elapsed","distance","speed","angel",
                "mrt_station","label_1","label_2","label_3","label_4"));

        List<Class> valueTypes=new ArrayList<Class>(Arrays.asList(Double.class,String.class,String.class,
                Double.class,String.class,String.class,Double.class,Double.class,Double.class,
                Double.class,Double.class,String.class,String.class,Double.class,Double.class,
                Double.class,Double.class));

        DataSchema schema=new DataSchema(fieldNames,valueTypes);
        builder.setSpout("TupleGenerator", new CSVReaderSpout(args[0],schema), 1);
        builder.setBolt("Dispatcher",new DispatcherBolt("Indexer","longitude",schema),1).shuffleGrouping("TupleGenerator");
        builder.setBolt("Indexer",new IndexerBolt(schema),4).directGrouping("Dispatcher");

        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxTaskParallelism(4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("generatorTest", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
