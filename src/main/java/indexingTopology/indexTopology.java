package indexingTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import indexingTopology.bolt.IndexerBolt;
import indexingTopology.bolt.IndexerBolt;
import indexingTopology.spout.CSVReaderSpout;
import indexingTopology.util.Constants;

import java.io.BufferedReader;
import java.io.InputStreamReader;
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
        builder.setSpout("TupleGenerator", new CSVReaderSpout(args[1], schema), 1).setNumTasks(1);
//        builder.setBolt("Dispatcher",new RangeQueryDispatcherBolt("Indexer","longitude",schema),1).shuffleGrouping("TupleGenerator");

        builder.setBolt("Indexer",new IndexerBolt("longitude",schema,4,6500000),1)
                .setNumTasks(1)
                .shuffleGrouping("TupleGenerator");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(4);
        conf.put(Constants.HDFS_CORE_SITE.str, "/Users/parijatmazumdar" +
                "/Desktop/thesis/hadoop-2.7.1/etc/hadoop/core-site.xml");
        conf.put(Constants.HDFS_HDFS_SITE.str,"/Users/parijatmazumdar/" +
                "Desktop/thesis/hadoop-2.7.1/etc/hadoop/hdfs-site.xml");

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("generatorTest", conf, builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
//        System.out.println("Type anything to stop the cluster");
        in.readLine();
//        cluster.shutdown();
    }
}
