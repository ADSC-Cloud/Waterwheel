package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import indexingTopology.metrics.PerNodeMetrics;
import indexingTopology.util.MonitorUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.common.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.partition.BalancedPartition;
import indexingTopology.common.Histogram;

import java.util.*;


public class DispatcherServerBolt<IndexType extends Number> extends BaseRichBolt {

    OutputCollector collector;

    private List<Integer> targetTasks;

    private boolean enableLoadBalance;

    private int numberOfPartitions;

    private boolean generateTimeStamp;

    private TopologyConfig config;

    //table-specific
    private final DataSchema outputSchema;

    private final DataSchema inputSchema;

    private IndexType lowerBound;

    private IndexType upperBound;

    private BalancedPartition<IndexType> balancedPartition;

    private DataTupleMapper tupleMapper;

    public DispatcherServerBolt(DataSchema dataSchema, IndexType lowerBound, IndexType upperBound, boolean enableLoadBalance,
                                boolean generateTimeStamp, DataTupleMapper tupleMapper, TopologyConfig config) {
        this.tupleMapper = tupleMapper;
        if (tupleMapper == null) {
            this.inputSchema = dataSchema;
            this.outputSchema = dataSchema;
        } else {
            this.inputSchema = tupleMapper.getOriginalSchema();
            this.outputSchema = dataSchema;
        }

        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.enableLoadBalance = enableLoadBalance;
        this.generateTimeStamp = generateTimeStamp;
        this.config = config;
    }

    public DispatcherServerBolt(DataSchema outputSchema, IndexType lowerBound, IndexType upperBound, boolean enableLoadBalance,
                                boolean generateTimeStamp, TopologyConfig config) {
        this(outputSchema, lowerBound, upperBound, enableLoadBalance, generateTimeStamp, null, config);
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        Set<String> componentIds = topologyContext.getThisTargets()
                .get(Streams.IndexStream).keySet();
        targetTasks = new ArrayList<Integer>();
        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        numberOfPartitions = targetTasks.size();

        balancedPartition = new BalancedPartition<>(numberOfPartitions, config.NUMBER_OF_INTERVALS, lowerBound, upperBound, enableLoadBalance);
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.IndexStream)){

//            DataTuple dataTuple = (DataTuple) tuple.getValueByField("tuple");
            byte[] dataTupleBytes = (byte[]) tuple.getValueByField("tuple");
            Long tupleId = tuple.getLongByField("tupleId");
            int sourceTaskId = tuple.getIntegerByField("taskId");
//                updateBound(indexValue);

            DataTuple dataTuple = inputSchema.deserializeToDataTuple(dataTupleBytes);

            if (tupleMapper != null) {
                dataTuple = tupleMapper.map(dataTuple);
            }
//            System.out.println("Schema: " + outputSchema.toString());
//            System.out.println("after map: " + dataTuple.toString());
//
//            System.out.println("Current time: " + System.currentTimeMillis());
            IndexType indexValue = (IndexType) outputSchema.getIndexValue(dataTuple);

            balancedPartition.record(indexValue);

            int partitionId = balancedPartition.getPartitionId(indexValue);

            int taskId = targetTasks.get(partitionId);
            collector.emitDirect(taskId, Streams.IndexStream, new Values(outputSchema.serializeTuple(dataTuple), tupleId, sourceTaskId));
        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            balancedPartition.setIntervalToPartitionMapping(((BalancedPartition) tuple.getValueByField("newIntervalPartition")).getIntervalToPartitionMapping());
        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){
            final Histogram histogram = new Histogram(balancedPartition.getIntervalDistribution().getHistogram(), config.NUMBER_OF_INTERVALS);
            double CPUload = MonitorUtils.getProcessCpuLoad();
            double freeDiskSpace = MonitorUtils.getFreeDiskSpaceInGB(config.HDFSFlag ? null: config.dataChunkDir);
            double totalDiskSpace = MonitorUtils.getTotalDiskSpaceInGB(config.HDFSFlag ? null: config.dataChunkDir);


            PerNodeMetrics perNodeMetrics = new PerNodeMetrics(histogram, CPUload, totalDiskSpace, freeDiskSpace);
            collector.emit(Streams.StatisticsReportStream,
                    new Values(perNodeMetrics));
            balancedPartition.clearHistogram();
        }
    }

    private void updateBound(IndexType indexValue) {
        if (indexValue.doubleValue() > upperBound.doubleValue()) {
            upperBound = indexValue;
        }

        if (indexValue.doubleValue() < lowerBound.doubleValue()) {
            lowerBound = indexValue;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream(Streams.IndexStream, new Fields("tuple"));
        declarer.declareStream(Streams.IndexStream, new Fields("tuple", "tupleId", "taskId"));

        declarer.declareStream(Streams.StatisticsReportStream, new Fields("statistics"));

        declarer.declareStream(Streams.ThroughputReportStream, new Fields("throughput"));
    }
}
