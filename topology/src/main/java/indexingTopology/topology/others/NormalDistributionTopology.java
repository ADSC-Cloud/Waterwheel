package indexingTopology.topology.others;

import indexingTopology.common.data.DataSchema;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.spout.NormalDistributionGenerator;

/**
 * Created by acelzj on 11/15/16.
 */
public class NormalDistributionTopology {

    static final String TupleGenerator = "TupleGenerator";
    static final String RangeQueryDispatcherBolt = "DispatcherBolt";
    static final String RangeQueryDecompositionBolt = "QueryDeCompositionBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String RangeQueryChunkScannerBolt = "ChunkScannerBolt";
    static final String ResultMergeBolt = "GResultMergeBolt";
    static final String MetadataServer = "MetadataServer";


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfig config = new TopologyConfig();
     /*   List<String> fieldNames=new ArrayList<String>(Arrays.asList("user_id","id_1","id_2","ts_epoch",
                "date","time","latitude","longitude","time_elapsed","distance","speed","angel",
                "mrt_station","label_1","label_2","label_3","label_4"));

        List<Class> valueTypes=new ArrayList<Class>(Arrays.asList(Double.class,String.class,String.class,
                Double.class,String.class,String.class,Double.class,Double.class,Double.class,
                Double.class,Double.class,String.class,String.class,Double.class,Double.class,
                Double.class,Double.class));



        DataSchema schema=new DataSchema(fieldNames,valueTypes);*/
//        List<String> fieldNames = new ArrayList<String>(Arrays.asList("user_id", "id_1", "id_2", "ts_epoch",
//                "date", "time", "latitude", "longitude"));
//        List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, Double.class,
//                Double.class, Double.class, Double.class, Double.class, Double.class));
//        DataSchema schema = new DataSchema(fieldNames, valueTypes, "user_id");

        DataSchema schema = new DataSchema();
        schema.addDoubleField("user_id");
        schema.addDoubleField("id_1");
        schema.addDoubleField("id_2");
        schema.addDoubleField("ts_epoch");
        schema.addDoubleField("date");
        schema.addDoubleField("time");
        schema.addDoubleField("latitude");
        schema.addDoubleField("longitude");
        schema.setPrimaryIndexField("user_id");


        DataSchema schemaWithTimestamp = schema.duplicate();
        schemaWithTimestamp.addLongField("timestamp");




        Double lowerBound = 0.0;

        Double upperBound = 1000.0;

        boolean enableLoadBalance = false;


        config.dataChunkDir = "/home/acelzj";
        config.HDFSFlag = false;

        builder.setSpout(TupleGenerator, new NormalDistributionGenerator(schema), 1);

        builder.setBolt(RangeQueryDispatcherBolt, new DispatcherServerBolt(schemaWithTimestamp, lowerBound, upperBound,
                enableLoadBalance, false, config))
                .shuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);

        builder.setBolt(IndexerBolt, new IndexingServerBolt(schemaWithTimestamp, config),2)
                .setNumTasks(1)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream);

        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinatorWithQueryGeneratorBolt<>(lowerBound, upperBound,
                config, schema), 1)
                .shuffleGrouping(ResultMergeBolt, Streams.QueryFinishedStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new QueryServerBolt(schemaWithTimestamp, config), 4)
//                .fieldsGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream, new Fields("fileName"));
                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream);
//                .shuffleGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream);

        builder.setBolt(ResultMergeBolt, new ResultMergerBolt(schemaWithTimestamp))
                .allGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .allGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServerBolt(lowerBound, upperBound, config))
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimestampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream);

        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }

}
