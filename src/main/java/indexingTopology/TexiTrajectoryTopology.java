package indexingTopology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import indexingTopology.config.TopologyConfig;
import indexingTopology.streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.spout.TexiTrajectoryGenerator;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by acelzj on 11/15/16.
 */
public class TexiTrajectoryTopology {

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



        //

        TopologyBuilder builder = new TopologyBuilder();
        List<String> fieldNames = new ArrayList<String>(Arrays.asList("id", "zcode", "payload"));
        List<Class> valueTypes = new ArrayList<Class>(Arrays.asList(Double.class, Double.class, String.class));
        DataSchema schema = new DataSchema(fieldNames, valueTypes, "zcode");

        final int payloadSize = 10;

        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);


        Double lowerBound = 0.0;

        Double upperBound = (double)city.getMaxZCode();

        String path = "/home/acelzj";

        boolean enableLoadBalance = false;


        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schema, generator, payloadSize, city), 1);

        builder.setBolt(RangeQueryDispatcherBolt, new RangeQueryDispatcherBolt(schema, lowerBound, upperBound, enableLoadBalance), 1)
                .shuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);


        builder.setBolt(IndexerBolt, new NormalDistributionIndexAndRangeQueryBolt(schema.getIndexField(), schema, TopologyConfig.BTREE_OREDER, 65000000), 2)

                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream); // direct grouping should be used.
        // And RangeQueryDecompositionBolt should emit to this stream via directEmit!!!!!

        builder.setBolt(RangeQueryDecompositionBolt, new RangeQueryDeCompositionBolt(lowerBound, upperBound), 1)
                .shuffleGrouping(ResultMergeBolt, Streams.NewQueryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimeStampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new RangeQueryChunkScannerBolt(), 4)
//                .fieldsGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream, new Fields("fileName"));
//                .directGrouping(RangeQueryDecompositionBolt, streams.FileSystemQueryStream);
                .directGrouping(RangeQueryDecompositionBolt, FileSystemQueryStream);

        builder.setBolt(ResultMergeBolt, new RangeQueryResultMergeBolt(schema), 1)
                .allGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .allGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServer(lowerBound, upperBound), 1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimeStampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);
        

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
