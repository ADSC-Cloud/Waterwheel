package indexingTopology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
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

    static final String TupleGenerator = "TupleGenerator";
    static final String RangeQueryDispatcherBolt = "DispatcherBolt";
    static final String RangeQueryDecompositionBolt = "QueryDeCompositionBolt";
    static final String IndexerBolt = "IndexerBolt";
    static final String RangeQueryChunkScannerBolt = "ChunkScannerBolt";
    static final String ResultMergeBolt = "GResultMergeBolt";
    static final String MetadataServer = "MetadataServer";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        final int payloadSize = 10;
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


        Double lowerBound = 0.0;

        Double upperBound = (double)city.getMaxZCode();

        String path = "/home/acelzj";

        boolean enableLoadBalance = false;


        builder.setSpout(TupleGenerator, new TexiTrajectoryGenerator(schema, generator, payloadSize, city), 1);

        builder.setBolt(RangeQueryDispatcherBolt, new IngestionDispatcher(schemaWithTimestamp, lowerBound, upperBound, enableLoadBalance, true), 1)
                .shuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);


        builder.setBolt(IndexerBolt, new IngestionBolt(schemaWithTimestamp), 8)

                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream) // direct grouping should be used.
                .directGrouping(RangeQueryDecompositionBolt, Streams.TreeCleanStream);
        // And RangeQueryDecompositionBolt should emit to this stream via directEmit!!!!!

        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinator(lowerBound, upperBound), 1)
                .shuffleGrouping(ResultMergeBolt, Streams.NewQueryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream);

        builder.setBolt(RangeQueryChunkScannerBolt, new ChunkScanner(schemaWithTimestamp), 8)
                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream);
//                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream); //make comparision with our method.

        builder.setBolt(ResultMergeBolt, new ResultMerger(schemaWithTimestamp), 1)
                .allGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .allGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream);

        builder.setBolt(MetadataServer, new MetadataServer(lowerBound, upperBound), 1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimestampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.EableRepartitionStream);

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(4);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }

}
