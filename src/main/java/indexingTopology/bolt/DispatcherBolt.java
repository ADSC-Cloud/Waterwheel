package indexingTopology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.DataSchema;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.Histogram;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by acelzj on 11/17/16.
 */
public class DispatcherBolt extends BaseRichBolt{

    OutputCollector collector;

    private final DataSchema schema;

    private List<Integer> targetTasks;

    private Map<Integer, Integer> intervalToPartitionMapping;

    private File outputFile;

    private FileOutputStream fop;

    private Double lowerBound;

    private Double upperBound;

    private BalancedPartition balancedPartition;

    private int numberOfPartitions;

    private boolean enableRecord;

    public DispatcherBolt(DataSchema schema, Double lowerBound, Double upperBound, boolean enableRecord) {
        this.schema = schema;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.enableRecord = enableRecord;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
//        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_tasks.txt");
        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingTopology.IndexStream).keySet();
        targetTasks = new ArrayList<>();
        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        numberOfPartitions = topologyContext.getComponentTasks("IndexerBolt").size();

        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound, enableRecord);

        intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();

//        try {
//            fop = new FileOutputStream(outputFile);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.IndexStream)) {

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
            collector.ack(tuple);

        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)){
            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);

        } else if (tuple.getSourceStreamId().equals(Streams.StaticsRequestStream)){

            collector.emit(Streams.StatisticsReportStream,
                    new Values(new Histogram(balancedPartition.getIntervalDistribution().getHistogram())));

            balancedPartition.clearHistogram();
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Streams.BPlusTreeQueryStream, new Fields("queryId", "key"));

        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");

//        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
        declarer.declareStream(Streams.IndexStream, new Fields(fields));

        declarer.declareStream(Streams.StatisticsReportStream, new Fields("statistics"));

    }

    private void updateBound(Double indexValue) {
        if (indexValue > upperBound) {
            upperBound = indexValue;
        }

        if (indexValue < lowerBound) {
            lowerBound = indexValue;
        }
    }


}
