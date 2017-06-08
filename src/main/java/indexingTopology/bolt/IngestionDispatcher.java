package indexingTopology.bolt;

import indexingTopology.config.TopologyConfig;
import indexingTopology.common.data.DataTuple;
import indexingTopology.common.logics.DataTupleMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.common.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.Histogram;

import java.util.*;


/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class IngestionDispatcher<IndexType extends Number> extends BaseRichBolt {
    OutputCollector collector;

    private final DataSchema outputSchema;

    private final DataSchema inputSchema;

    private List<Integer> targetTasks;

    private IndexType lowerBound;

    private IndexType upperBound;

    private BalancedPartition<IndexType> balancedPartition;

    private boolean enableLoadBalance;

    private int numberOfPartitions;

    private boolean generateTimeStamp;

    private DataTupleMapper tupleMapper;

    private TopologyConfig config;

    public IngestionDispatcher(DataSchema dataSchema, IndexType lowerBound, IndexType upperBound, boolean enableLoadBalance,
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

    public IngestionDispatcher(DataSchema outputSchema, IndexType lowerBound, IndexType upperBound, boolean enableLoadBalance,
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

        balancedPartition = new BalancedPartition<>(numberOfPartitions, lowerBound, upperBound, enableLoadBalance, config);
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

            IndexType indexValue = (IndexType) outputSchema.getIndexValue(dataTuple);

            balancedPartition.record(indexValue);

            int partitionId = balancedPartition.getPartitionId(indexValue);

            int taskId = targetTasks.get(partitionId);
            collector.emitDirect(taskId, Streams.IndexStream, new Values(outputSchema.serializeTuple(dataTuple), tupleId, sourceTaskId));
        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            balancedPartition.setIntervalToPartitionMapping(((BalancedPartition) tuple.getValueByField("newIntervalPartition")).getIntervalToPartitionMapping());
        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){
            collector.emit(Streams.StatisticsReportStream,
                    new Values(new Histogram(balancedPartition.getIntervalDistribution().getHistogram(), config)));
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
