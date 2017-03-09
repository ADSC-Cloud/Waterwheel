package indexingTopology;

import indexingTopology.data.DataSchema;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.Bolt;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import indexingTopology.streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.storm.topology.base.BaseBasicBolt;

/**
 * Created by acelzj on 11/15/16.
 */
public class TexiTrajectoryTopology {

    static final String TupleGenerator = "TupleGenerator";
    static final String RangeQueryDispatcherBolt = "DispatcherBolt";
    static final String RangeQueryDecompositionBolt = "QueryDeCompositionBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String RangeQueryChunkScannerBolt = "ChunkScannerBolt";
    static final String ResultMergeBolt = "ResultMergeBolt";
    static final String MetadataServer = "MetadataServer";
    static final String LogWriter = "LogWriter";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        final int payloadSize = 1;
        DataSchema schema = new DataSchema();
//        schema.addDoubleField("id");
        schema.addLongField("id");
        schema.addDoubleField("zcode");
//        schema.addIntField("zcode");
        schema.addVarcharField("payload", payloadSize);
        schema.setPrimaryIndexField("zcode");


        DataSchema schemaWithTimestamp = schema.duplicate();
        schemaWithTimestamp.addLongField("timestamp");


        final double x1 = 0;
        final double x2 = 1000;
        final double y1 = 0;
        final double y2 = 500;
        final int partitions = 100;

        TrajectoryGenerator generator = new TrajectoryUniformGenerator(10000, x1, x2, y1, y2);
        City city = new City(x1, x2, y1, y2, partitions);


//        Double lowerBound = 0.0;

//        Double upperBound = (double)city.getMaxZCode();
//        Double upperBound = 200048.0;
        Double sigma = 100000.0;
        Double mean = 5000.0;
        Double lowerBound = 0.0;
        Double upperBound = 2 * mean;


        final boolean enableLoadBalance = false;


//        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schema, generator, payloadSize, city), 1);
//        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schemaWithTimestamp, generator, payloadSize, city), 16);
//        IRichBolt datasource = new Generator(schemaWithTimestamp, generator, payloadSize, city);

        IRichBolt datasource = new InputStreamReceiverServer(schemaWithTimestamp, 10000);

        builder.setBolt(TupleGenerator, datasource, 1)
                .directGrouping(IndexerBolt, Streams.AckStream);

        builder.setBolt(RangeQueryDispatcherBolt, new IngestionDispatcher(schemaWithTimestamp, lowerBound, upperBound, enableLoadBalance, false), 1)
                .localOrShuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);
//                .allGrouping(LogWriter, Streams.ThroughputRequestStream);

        builder.setBolt(IndexerBolt, new IngestionBolt(schemaWithTimestamp), 1)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream) // direct grouping should be used.
                .directGrouping(RangeQueryDecompositionBolt, Streams.TreeCleanStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream);
        // And RangeQueryDecompositionBolt should emit to this stream via directEmit!!!!!

//        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinatorWithQueryGenerator<>(lowerBound, upperBound), 1)
        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinatorWithQueryReceiverServer<>(lowerBound, upperBound, 10001), 1)
                .shuffleGrouping(ResultMergeBolt, Streams.QueryFinishedStream)
                .shuffleGrouping(ResultMergeBolt, Streams.PartialQueryResultDeliveryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new ChunkScanner(schemaWithTimestamp), 1)
//                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream); //make comparision with our method.

        builder.setBolt(ResultMergeBolt, new ResultMerger(schemaWithTimestamp), 1)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .shuffleGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.PartialQueryResultReceivedStream);

        builder.setBolt(MetadataServer, new MetadataServer(lowerBound, upperBound), 1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimestampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.EnableRepartitionStream);

        builder.setBolt(LogWriter, new LogWriter(), 1)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(IndexerBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(MetadataServer, Streams.LoadBalanceStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }

}
