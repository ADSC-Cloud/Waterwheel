package indexingTopology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import indexingTopology.streams.Streams;
import indexingTopology.bolt.*;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;

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
//        schema.addDoubleField("zcode");
        schema.addIntField("zcode");
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
        Double mean = 500000.0;
        Double lowerBound = 0.0;
        Double upperBound = mean;

        String path = "/home/acelzj";

        boolean enableLoadBalance = true;


//        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schema, generator, payloadSize, city), 1);
//        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schemaWithTimestamp, generator, payloadSize, city), 16);

        builder.setBolt(TupleGenerator, new Generator(schemaWithTimestamp, generator, payloadSize, city), 14)
                .directGrouping(IndexerBolt, Streams.AckStream);

        builder.setBolt(RangeQueryDispatcherBolt, new IngestionDispatcher(schemaWithTimestamp, lowerBound, upperBound, enableLoadBalance, false), 14)
                .localOrShuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);
//                .allGrouping(LogWriter, Streams.ThroughputRequestStream);

        builder.setBolt(IndexerBolt, new IngestionBolt(schemaWithTimestamp), 8)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream) // direct grouping should be used.
                .directGrouping(RangeQueryDecompositionBolt, Streams.TreeCleanStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream);
        // And RangeQueryDecompositionBolt should emit to this stream via directEmit!!!!!

        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinator(lowerBound, upperBound), 1)
                .shuffleGrouping(ResultMergeBolt, Streams.QueryFinishedStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new ChunkScanner(schemaWithTimestamp), 1)
                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream);
//                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream); //make comparision with our method.

        builder.setBolt(ResultMergeBolt, new ResultMerger(schemaWithTimestamp), 1)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .shuffleGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

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
        conf.setNumWorkers(4);

        conf.put(Config.WORKER_CHILDOPTS, "-Xmx2048m");
//        conf.put(Config.SUPERVISOR_CHILDOPTS, "-Xmx2048m");
        conf.put(Config.WORKER_HEAP_MEMORY_MB, 2048);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }

}
