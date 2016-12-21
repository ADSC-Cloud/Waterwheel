package indexingTopology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import indexingTopology.Config.TopologyConfig;
import indexingTopology.Streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.spout.NormalDistributionGenerator;

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
    public static final String StaticsRequestStream = "StaticsRequestStream";


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

        Double lowerBound = 0.0;

        Double upperBound = 1000.0;

        boolean enableLoadBlance = true;

        builder.setSpout(TupleGenerator, new NormalDistributionGenerator(schema), 1).setNumTasks(1);
//        builder.setBolt("Dispatcher",new RangeQueryDispatcherBolt("Indexer","longitude",schema),1).shuffleGrouping("TupleGenerator");

        builder.setBolt(DispatcherBolt, new DispatcherBolt(schema, lowerBound, upperBound, enableLoadBlance)).setNumTasks(1)
                .shuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);

        builder.setBolt(IndexerBolt, new NormalDistributionIndexerBolt("user_id", schema, TopologyConfig.BTREE_OREDER, 65000000),1)
                .setNumTasks(4)
                .directGrouping(DispatcherBolt, Streams.IndexStream)
                .directGrouping(QueryDecompositionBolt, Streams.BPlusTreeQueryStream);

        builder.setBolt(QueryDecompositionBolt, new QueryDecompositionBolt(lowerBound, upperBound)).setNumTasks(1)
                .shuffleGrouping(ResultMergeBolt, Streams.NewQueryStream)
                .shuffleGrouping(ChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimeStampUpdateStream);

        builder.setBolt(ChunkScannerBolt, new ChunkScannerBolt()).setNumTasks(2)
//                .fieldsGrouping(QueryDecompositionBolt, FileSystemQueryStream, new Fields("fileName"));
                .directGrouping(QueryDecompositionBolt, Streams.FileSystemQueryStream);
//                .shuffleGrouping(QueryDecompositionBolt, FileSystemQueryStream);

        builder.setBolt(ResultMergeBolt, new ResultMergeBolt(schema)).setNumTasks(1)
                .allGrouping(ChunkScannerBolt, Streams.FileSystemQueryStream)
                .allGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(QueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
//                .shuffleGrouping(ChunkScannerBolt, TimeCostInformationStream)
                .shuffleGrouping(QueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServer(lowerBound, upperBound)).setNumTasks(1)
                .shuffleGrouping(DispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimeStampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(4);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}