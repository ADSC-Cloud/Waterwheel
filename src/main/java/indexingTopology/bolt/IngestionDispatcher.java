package indexingTopology.bolt;

import indexingTopology.DataTuple;
import org.apache.storm.metric.internal.RateTracker;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.Histogram;

import java.util.*;


/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class IngestionDispatcher<IndexType extends Number> extends BaseRichBolt {
    OutputCollector collector;

    private final DataSchema schema;

    private List<Integer> targetTasks;

    private Double lowerBound;

    private Double upperBound;

    private BalancedPartition balancedPartition;

    private boolean enableLoadBalance;

    private int numberOfPartitions;

    private boolean generateTimeStamp;

    private RateTracker rateTracker;

    public IngestionDispatcher(DataSchema schema, Double lowerBound, Double upperBound, boolean enableLoadBalance,
                               boolean generateTimeStamp) {
        this.schema = schema;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.enableLoadBalance = enableLoadBalance;
        this.generateTimeStamp = generateTimeStamp;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        Set<String> componentIds = topologyContext.getThisTargets()
                .get(Streams.IndexStream).keySet();
        targetTasks = new ArrayList<Integer>();
        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

//        System.out.println(targetTasks);

        rateTracker = new RateTracker(5 * 1000, 5);

        numberOfPartitions = targetTasks.size();

        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound, enableLoadBalance);
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.IndexStream)){

//            DataTuple dataTuple = (DataTuple) tuple.getValueByField("tuple");
            byte[] dataTupleBytes = (byte[]) tuple.getValueByField("tuple");
            Long tupleId = tuple.getLongByField("tupleId");
            int sourceTaskId = tuple.getIntegerByField("taskId");
//                updateBound(indexValue);
            DataTuple dataTuple = schema.deserializeToDataTuple(dataTupleBytes);
            IndexType indexValue = (IndexType) schema.getIndexValue(dataTuple);
            balancedPartition.record(indexValue);

            int partitionId = balancedPartition.getPartitionId(indexValue);

//            System.out.println("partition id " + partitionId);

            int taskId = targetTasks.get(partitionId);


//            System.out.println("Task id " + taskId);

            rateTracker.notify(1);

//            collector.emitDirect(taskId, Streams.IndexStream, tuple, new Values(dataTuple));
//            collector.emitDirect(taskId, Streams.IndexStream, tuple, new Values(schema.serializeTuple(dataTuple)));
//            collector.emitDirect(taskId, Streams.IndexStream, tuple, new Values(schema.serializeTuple(dataTuple), tupleId));
            collector.emitDirect(taskId, Streams.IndexStream, new Values(schema.serializeTuple(dataTuple), tupleId, sourceTaskId));
//            collector.ack(tuple);
        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            System.out.println("partition has been updated!!!");
//            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
//            balancedPartition = (BalancedPartition) tuple.getValueByField("newIntervalPartition");
            balancedPartition.setIntervalToPartitionMapping(((BalancedPartition) tuple.getValueByField("newIntervalPartition")).getIntervalToPartitionMapping());
//            System.out.println(intervalToPartitionMapping);
//            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){
            collector.emit(Streams.StatisticsReportStream,
                    new Values(new Histogram(balancedPartition.getIntervalDistribution().getHistogram())));
            balancedPartition.clearHistogram();
        } else if (tuple.getSourceStreamId().equals(Streams.ThroughputRequestStream)) {
            collector.emit(Streams.ThroughputReportStream, new Values(rateTracker.reportRate()));
        }
    }

    private void updateBound(Double indexValue) {
        if (indexValue > upperBound) {
            upperBound = indexValue;
        }

        if (indexValue < lowerBound) {
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
