package indexingTopology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import indexingTopology.bolt.*;
import indexingTopology.spout.NormalDistributionGenerator;
import indexingTopology.util.RangePartitionGrouping;
import org.apache.hadoop.hdfs.DFSClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by acelzj on 7/22/16.
 */
public class NormalDistributionIndexingTopology {
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
    public static final String StatisticsReportStream = "KeyStatisticsStream";
    public static final String IntervalPartitionUpdateStream = "IntervalPartitionUpdateStream";
    public static final String IndexerNumberReportStream = "IndexerNumberReportStream";

    public static final String TimeCostInformationStream = "TimeCostInformationStream";


    static final String TupleGenerator = "TupleGenerator";
    static final String DispatcherBolt = "DispatcherBolt";
    static final String QueryDecompositionBolt = "QueryDecompositionBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String ChunkScannerBolt = "ChunkScannerBolt";
    static final String ResultMergeBolt = "ResultMergeBolt";
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
        builder.setSpout(TupleGenerator, new NormalDistributionGenerator(schema), 1).setNumTasks(1);
//        builder.setBolt("Dispatcher",new RangeQueryDispatcherBolt("Indexer","longitude",schema),1).shuffleGrouping("TupleGenerator");

        builder.setBolt(DispatcherBolt, new DispatcherBolt(schema)).setNumTasks(1)
                .shuffleGrouping(TupleGenerator, IndexStream)
                .shuffleGrouping(MetadataServer, IntervalPartitionUpdateStream);

        builder.setBolt(IndexerBolt, new NormalDistributionIndexerBolt("user_id", schema, indexingTopology.Config.Config.BTREE_OREDER, 65000000),1)
                .setNumTasks(4)
                .directGrouping(DispatcherBolt, IndexStream)
                .directGrouping(QueryDecompositionBolt, BPlusTreeQueryStream);

        builder.setBolt(QueryDecompositionBolt, new QueryDecompositionBolt()).setNumTasks(1)
                .shuffleGrouping(ResultMergeBolt, NewQueryStream)
                .shuffleGrouping(ChunkScannerBolt, FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, TimeStampUpdateStream);

        builder.setBolt(ChunkScannerBolt, new ChunkScannerBolt()).setNumTasks(2)
//                .fieldsGrouping(QueryDecompositionBolt, FileSystemQueryStream, new Fields("fileName"));
                .directGrouping(QueryDecompositionBolt, FileSystemQueryStream);
//                .shuffleGrouping(QueryDecompositionBolt, FileSystemQueryStream);

        builder.setBolt(ResultMergeBolt, new ResultMergeBolt(schema)).setNumTasks(1)
                .allGrouping(ChunkScannerBolt, FileSystemQueryStream)
                .allGrouping(IndexerBolt, BPlusTreeQueryStream)
                .shuffleGrouping(QueryDecompositionBolt, BPlusTreeQueryInformationStream)
//                .shuffleGrouping(ChunkScannerBolt, TimeCostInformationStream)
                .shuffleGrouping(QueryDecompositionBolt, FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServer()).setNumTasks(1)
                .shuffleGrouping(DispatcherBolt, StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, TimeStampUpdateStream)
                .shuffleGrouping(IndexerBolt, FileInformationUpdateStream)
                .shuffleGrouping(DispatcherBolt, IndexerNumberReportStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(4);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}