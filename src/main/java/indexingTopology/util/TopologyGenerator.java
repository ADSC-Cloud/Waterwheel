package indexingTopology.util;

import indexingTopology.bolt.*;
import indexingTopology.config.TopologyConfig;
import indexingTopology.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.texi.City;
import indexingTopology.util.texi.TrajectoryGenerator;
import indexingTopology.util.texi.TrajectoryUniformGenerator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by robert on 10/3/17.
 */
public class TopologyGenerator<Key extends Number & Comparable<Key> >{


    private static final String TupleGenerator = "TupleGenerator";
    private static final String RangeQueryDispatcherBolt = "DispatcherBolt";
    private static final String RangeQueryDecompositionBolt = "QueryDeCompositionBolt";
    private static final String IndexerBolt = "IndexerBolt";
    private static final String RangeQueryChunkScannerBolt = "ChunkScannerBolt";
    private static final String ResultMergeBolt = "ResultMergeBolt";
    private static final String MetadataServer = "MetadataServer";
    private static final String LogWriter = "LogWriter";

    public StormTopology generateIndexingTopology(DataSchema dataSchema, Key lowerBound, Key upperBound, boolean enableLoadBalance,
                                                  InputStreamReceiver dataSource, QueryCoordinator<Key> queryCoordinator,
                                                  DataTupleMapper dataTupleMapper) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt(TupleGenerator, dataSource, 6)
                .directGrouping(IndexerBolt, Streams.AckStream);

        builder.setBolt(RangeQueryDispatcherBolt, new IngestionDispatcher<>(dataSchema, lowerBound, upperBound, enableLoadBalance, false, dataTupleMapper), 6)

                .localOrShuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream);
//                .allGrouping(LogWriter, Streams.ThroughputRequestStream);

        builder.setBolt(IndexerBolt, new IngestionBolt(dataSchema), 6)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream) // direct grouping should be used.
                .directGrouping(RangeQueryDecompositionBolt, Streams.TreeCleanStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream);
        // And RangeQueryDecompositionBolt should emit to this stream via directEmit!!!!!

//        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinatorWithQueryGenerator<>(lowerBound, upperBound), 1)
        builder.setBolt(RangeQueryDecompositionBolt, queryCoordinator, 1)
                .shuffleGrouping(ResultMergeBolt, Streams.QueryFinishedStream)
                .shuffleGrouping(ResultMergeBolt, Streams.PartialQueryResultDeliveryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream);


        if (TopologyConfig.SHUFFLE_GROUPING_FLAG) {
            builder.setBolt(RangeQueryChunkScannerBolt, new ChunkScanner<Key>(dataSchema), 6)
//                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                    .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream)
                    .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream); //make comparision with our method.
        } else {
            builder.setBolt(RangeQueryChunkScannerBolt, new ChunkScanner<Key>(dataSchema), 6)
                    .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                    .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream);
        }

        builder.setBolt(ResultMergeBolt, new ResultMerger(dataSchema), 1)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .shuffleGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.PartialQueryResultReceivedStream);

        builder.setBolt(MetadataServer, new MetadataServer<>(lowerBound, upperBound), 1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimestampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.EnableRepartitionStream);

        builder.setBolt(LogWriter, new LogWriter(), 1)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(IndexerBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(MetadataServer, Streams.LoadBalanceStream);

        return builder.createTopology();
    }

    public StormTopology generateIndexingTopology(DataSchema dataSchema, Key lowerBound, Key upperBound, boolean enableLoadBalance,
                                                  InputStreamReceiver dataSource, QueryCoordinator<Key> queryCoordinator) {
        return generateIndexingTopology(dataSchema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinator, null);
    }
}
