package indexingTopology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import indexingTopology.bolt.*;
import indexingTopology.spout.NormalDistributionGenerator;
import indexingTopology.util.Constants;
import indexingTopology.util.RangePartitionGrouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by acelzj on 11/15/16.
 */
public class NormalDistributionIndexingAndRangeQueryTopology {

    public static final String FileSystemQueryStream = "FileSystemQueryStream";
    public static final String BPlusTreeQueryStream = "BPlusTreeQueryStream";
    public static final String FileInformationUpdateStream = "FileInformationUpdateStream";
    public static final String IndexStream = "IndexStream";

    static final String TupleGenerator = "TupleGenerator";
    static final String DispatcherBolt = "DispatcherBolt";
    static final String QueryBolt = "QueryBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String QueryFileBolt = "QueryFileBolt";
    static final String ResultMergeBolt = "ResultMergeBolt";


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
     /*   List<String> fieldNames=new ArrayList<String>(Arrays.asList("user_id","id_1","id_2","ts_epoch",
                "date","time","latitude","longitude","time_elapsed","distance","speed","angel",
                "mrt_station","label_1","label_2","label_3","label_4"));

        List<Class> valueTypes=new ArrayList<Class>(Arrays.asList(Double.class,String.class,String.class,
                Double.class,String.class,String.class,Double.class,Double.class,Double.class,
                Double.class,Double.class,String.class,String.class,Double.class,Double.class,
                Double.class,Double.class));



        DataSchema schema=new DataSchema(fieldNames,valueTypes);*/
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
                "date", "time", "latitude", "longitude"));
        List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
                Double.class, Double.class, Double.class, Double.class, Double.class));
        DataSchema schema = new DataSchema(fieldNames, valueTypes);
        builder.setSpout(TupleGenerator, new NormalDistributionGenerator(schema), 1).setNumTasks(1);
//        builder.setBolt("Dispatcher",new DispatcherBolt("Indexer","longitude",schema),1).shuffleGrouping("TupleGenerator");

        builder.setBolt(DispatcherBolt, new DispatcherBolt(schema)).setNumTasks(1)
                .shuffleGrouping(TupleGenerator, IndexStream)
                .shuffleGrouping(QueryBolt, BPlusTreeQueryStream);

        builder.setBolt(IndexerBolt, new NormalDistributionIndexAndRangeQueryBolt("user_id", schema, 4, 65000000),1)
                .setNumTasks(2)
                .customGrouping(DispatcherBolt, IndexStream, new RangePartitionGrouping())
                .customGrouping(DispatcherBolt, BPlusTreeQueryStream, new RangePartitionGrouping());

        builder.setBolt(QueryBolt, new RangeQueryBolt()).setNumTasks(1).
                allGrouping(IndexerBolt, FileInformationUpdateStream);

        builder.setBolt(QueryFileBolt, new RangeQueryFileBolt()).setNumTasks(2)
                .fieldsGrouping(QueryBolt, FileSystemQueryStream, new Fields("leftKey", "rightKey"));

        builder.setBolt(ResultMergeBolt, new ResultMergeBolt(schema)).setNumTasks(1)
                .allGrouping(QueryFileBolt, FileSystemQueryStream)
                .allGrouping(IndexerBolt, BPlusTreeQueryStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(4);
        conf.put(Constants.HDFS_CORE_SITE.str, "/Users/parijatmazumdar" +
                "/Desktop/thesis/hadoop-2.7.1/etc/hadoop/core-site.xml");
        conf.put(Constants.HDFS_HDFS_SITE.str,"/Users/parijatmazumdar/" +
                "Desktop/thesis/hadoop-2.7.1/etc/hadoop/hdfs-site.xml");

//        LocalCluster cluster = new LocalCluster();
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("generatorTest", conf, builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
//        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
//        System.out.println("Type anything to stop the cluster");
//        in.readLine();
        //   cluster.shutdown();
        //    cluster.shutdown();
    }

}
