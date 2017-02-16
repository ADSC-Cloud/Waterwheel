package indexingTopology.bolt;

import indexingTopology.DataTuple;
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

        numberOfPartitions = targetTasks.size();

        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound, enableLoadBalance);

    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.IndexStream)){

            DataTuple dataTuple = (DataTuple) tuple.getValueByField("tuple");

//                updateBound(indexValue);
            IndexType indexValue = (IndexType) schema.getIndexValue(dataTuple);
            balancedPartition.record(indexValue);


            int partitionId = balancedPartition.getPartitionId(indexValue);

            int taskId = targetTasks.get(partitionId);

            collector.emitDirect(taskId, Streams.IndexStream, tuple, new Values(dataTuple));
            collector.ack(tuple);

        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            System.out.println("partition has been updated!!!");
//            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
            balancedPartition = (BalancedPartition) tuple.getValueByField("newIntervalPartition");
//            System.out.println(intervalToPartitionMapping);
//            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){

            collector.emit(Streams.StatisticsReportStream,
                    new Values(new Histogram(balancedPartition.getIntervalDistribution().getHistogram())));

            balancedPartition.clearHistogram();
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
        declarer.declareStream(Streams.IndexStream, new Fields("tuple"));

        declarer.declareStream(Streams.StatisticsReportStream, new Fields("statistics"));

    }


}
