package indexingTopology.topology;

import indexingTopology.bolt.*;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataSchema;
import indexingTopology.spout.DummySpout;
import indexingTopology.spout.Spout;
import indexingTopology.streams.Streams;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.List;


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

    private int numberOfNodes = 1;

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public StormTopology generateIndexingTopology(DataSchema dataSchema, Key lowerBound, Key upperBound, boolean enableLoadBalance,
                                                  InputStreamReceiverBolt dataSource, QueryCoordinatorBolt<Key> queryCoordinatorBolt,
                                                  DataTupleMapper dataTupleMapper, List<String> bloomFilterColumns, TopologyConfig config) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("dummy", new DummySpout(), 1)
                .setCPULoad(1);
        builder.setBolt(TupleGenerator, dataSource, 1)
                .directGrouping(IndexerBolt, Streams.AckStream)
                .shuffleGrouping("dummy")
                .setCPULoad(350);

        builder.setBolt(RangeQueryDispatcherBolt, new DispatcherServerBolt<>(dataSchema, lowerBound, upperBound,
                enableLoadBalance, false, dataTupleMapper, config), numberOfNodes)

                .localOrShuffleGrouping(TupleGenerator, Streams.IndexStream)
                .allGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .allGrouping(MetadataServer, Streams.StaticsRequestStream)
                .setCPULoad(100);

        builder.setBolt(IndexerBolt, new IndexingServerBolt(dataSchema, bloomFilterColumns, config), config.INSERTION_SERVER_PER_NODE * numberOfNodes)
                .directGrouping(RangeQueryDispatcherBolt, Streams.IndexStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryStream)
                .directGrouping(RangeQueryDecompositionBolt, Streams.TreeCleanStream)
                .allGrouping(LogWriter, Streams.ThroughputRequestStream)
                .setCPULoad(100 / config.INSERTION_SERVER_PER_NODE);

//        builder.setBolt(RangeQueryDecompositionBolt, new QueryCoordinatorWithQueryGenerator<>(lowerBound, upperBound), 1)
        builder.setBolt(RangeQueryDecompositionBolt, queryCoordinatorBolt, 1)
                .shuffleGrouping(ResultMergeBolt, Streams.QueryFinishedStream)
                .shuffleGrouping(ResultMergeBolt, Streams.PartialQueryResultDeliveryStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSubQueryFinishStream)
                .shuffleGrouping(MetadataServer, Streams.FileInformationUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.OldDataRemoval)
                .shuffleGrouping(MetadataServer, Streams.IntervalPartitionUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.TimestampUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.LocationInfoUpdateStream)
                .shuffleGrouping(MetadataServer, Streams.DDLResponseStream)
                .setCPULoad(350);


        if (config.SHUFFLE_GROUPING_FLAG) {
            builder.setBolt(RangeQueryChunkScannerBolt, new QueryServerBolt<Key>(dataSchema, config), config.CHUNK_SCANNER_PER_NODE * numberOfNodes)
//                .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                    .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream)
                    .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                    .setCPULoad(100 / config.CHUNK_SCANNER_PER_NODE); //make comparision with our method.
        } else {
            builder.setBolt(RangeQueryChunkScannerBolt, new QueryServerBolt<Key>(dataSchema, config), config.CHUNK_SCANNER_PER_NODE * numberOfNodes)
                    .directGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryStream)
                    .directGrouping(ResultMergeBolt, Streams.SubQueryReceivedStream)
                    .setCPULoad(100 / config.CHUNK_SCANNER_PER_NODE);
        }

        builder.setBolt(ResultMergeBolt, new ResultMergerBolt(dataSchema), 1)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.FileSystemQueryStream)
                .shuffleGrouping(IndexerBolt, Streams.BPlusTreeQueryStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.BPlusTreeQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.FileSystemQueryInformationStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.PartialQueryResultReceivedStream)
                .setCPULoad(1);

        builder.setBolt(MetadataServer, new MetadataServerBolt<>(lowerBound, upperBound, dataSchema, config), 1)
                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.StatisticsReportStream)
                .shuffleGrouping(IndexerBolt, Streams.TimestampUpdateStream)
                .shuffleGrouping(IndexerBolt, Streams.FileInformationUpdateStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.EnableRepartitionStream)
                .shuffleGrouping(IndexerBolt, Streams.LocationInfoUpdateStream)
                .shuffleGrouping(RangeQueryChunkScannerBolt, Streams.LocationInfoUpdateStream)
                .shuffleGrouping(RangeQueryDecompositionBolt, Streams.DDLRequestStream)
                .setCPULoad(350);

        builder.setBolt(LogWriter, new LoggingBolt(), 1)
//                .shuffleGrouping(RangeQueryDispatcherBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(IndexerBolt, Streams.ThroughputReportStream)
                .shuffleGrouping(MetadataServer, Streams.LoadBalanceStream)
                .setCPULoad(1);

        return builder.createTopology();
    }

    public StormTopology generateIndexingTopology(DataSchema dataSchema, Key lowerBound, Key upperBound, boolean enableLoadBalance,
                                                  InputStreamReceiverBolt dataSource, QueryCoordinatorBolt<Key> queryCoordinatorBolt, TopologyConfig config) {
        return generateIndexingTopology(dataSchema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinatorBolt, null, null, config);
    }

    public StormTopology generateIndexingTopology(DataSchema dataSchema, Key lowerBound, Key upperBound, boolean enableLoadBalance,
                                                  InputStreamReceiverBolt dataSource, QueryCoordinatorBolt<Key> queryCoordinatorBolt, DataTupleMapper mapper, TopologyConfig config) {
        return generateIndexingTopology(dataSchema, lowerBound, upperBound, enableLoadBalance, dataSource, queryCoordinatorBolt, mapper, null, config);
    }
}
 // direct grouping should be used.