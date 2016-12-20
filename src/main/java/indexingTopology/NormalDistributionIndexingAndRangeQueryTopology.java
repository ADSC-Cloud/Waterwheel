package indexingTopology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import indexingTopology.Streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.spout.NormalDistributionGenerator;
import indexingTopology.util.Constants;

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
    public static final String BPlusTreeQueryInformationStream = "BPlusTreeQueryInformationStream";
    public static final String FileSystemQueryInformationStream = "FileSystemQueryInformationStream";
    public static final String NewQueryStream = "NewQueryStream";
    public static final String TimeStampUpdateStream = "TimeStampUpdateStream";
    public static final String QueryGenerateStream = "QueryGenerateStream";
    public static final String FileSubQueryFinishStream = "FileSubQueryFinishStream";
    public static final String StatisticsReportStream = "StatisticsReportStream";
    public static final String IntervalPartitionUpdateStream = "IntervalPartitionUpdateStream";
    public static final String StaticsRequestStream = "StaticsRequestStream";


    static final String TupleGenerator = "TupleGenerator";
    static final String RangeQueryDispatcherBolt = "DispatcherBolt";
    static final String RangeQueryDecompositionBolt = "QueryDeCompositionBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String RangeQueryChunkScannerBolt = "ChunkScannerBolt";
    static final String ResultMergeBolt = "GResultMergeBolt";
    static final String MetadataServer = "MetadataServer";


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
        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

        Double lowerBound = 0.0;

        Double upperBound = 1000.0;

        boolean enableLoadBalance = true;

        String path = "/home/acelzj";

        boolean enableHdfs = false;

        builder.setSpout(TupleGenerator, new NormalDistributionGenerator(schema), 1).setNumTasks(1);

        builder.setBolt(RangeQueryDispatcherBolt, new RangeQueryDispatcherBolt(schema, lowerBound, upperBound, enableLoadBalance)).setNumTasks(1)
                .shuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);

        builder.setBolt(IndexerBolt, new NormalDistributionIndexAndRangeQueryBolt("user_id", schema, indexingTopology.Config.Config.BTREE_OREDER, 65000000, path, enableHdfs),1)
                .setNumTasks(4)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream);

        builder.setBolt(RangeQueryDecompositionBolt, new RangeQueryDeCompositionBolt(lowerBound, upperBound)).setNumTasks(1)
                .shuffleGrouping(ResultMergeBolt, Streams.NewQueryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimeStampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new RangeQueryChunkScannerBolt(path, enableHdfs)).setNumTasks(2)
//                .fieldsGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream, new Fields("fileName"));
                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream);
//                .shuffleGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream);

        builder.setBolt(ResultMergeBolt, new RangeQueryResultMergeBolt(schema)).setNumTasks(1)
                .allGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .allGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServer(lowerBound, upperBound)).setNumTasks(1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimeStampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream);

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
