package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingAndRangeQueryTopology;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.MetaData.TaskMetaData;
import indexingTopology.MetaData.TaskPartitionSchemaManager;
import indexingTopology.Streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.Histogram;
import indexingTopology.util.PartitionFunction;
import indexingTopology.util.RangeQuerySubQuery;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class RangeQueryDispatcherBolt extends BaseRichBolt {
    OutputCollector collector;

    private final DataSchema schema;

    private List<Integer> targetTasks;

    private Double lowerBound;

    private Double upperBound;

    private BalancedPartition balancedPartition;

    private boolean enableLoadBlance;

    private int numberOfPartitions;


    public RangeQueryDispatcherBolt(DataSchema schema, Double lowerBound, Double upperBound, boolean enableLoadBlance) {
        this.schema = schema;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.enableLoadBlance = enableLoadBlance;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingAndRangeQueryTopology.IndexStream).keySet();
        targetTasks = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        numberOfPartitions = targetTasks.size();

        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound, enableLoadBlance);

    }

    public void execute(Tuple tuple) {
//        double partitionValue = tuple.getDoubleByField(rangePartitionField);

        if (tuple.getSourceStreamId().equals(Streams.IndexStream)){

            Double indexValue = tuple.getDoubleByField(schema.getIndexField());

//                updateBound(indexValue);
            balancedPartition.record(indexValue);

//            int intervalId = balancedPartition.getIntervalId(indexValue);

            int partitionId = balancedPartition.getPartitionId(indexValue);

            int taskId = targetTasks.get(partitionId);

            Values values = null;
            try {
                values = schema.getValuesObject(tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }

            collector.emitDirect(taskId, Streams.IndexStream, values);

        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){

            System.out.println(balancedPartition.getIntervalDistribution().getHistogram());

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
        declarer.declareStream(Streams.BPlusTreeQueryStream, new Fields("queryId", "leftKey"
                , "rightKey"));

        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");

        declarer.declareStream(Streams.IndexStream, new Fields(fields));

        declarer.declareStream(Streams.StatisticsReportStream, new Fields("statistics"));

    }


}
