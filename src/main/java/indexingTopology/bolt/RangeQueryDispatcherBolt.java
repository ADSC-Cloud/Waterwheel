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
import indexingTopology.util.RangeQuerySubQuery;
import javafx.util.Pair;

import java.io.IOException;
import java.util.*;

/**
 * Created by parijatmazumdar on 14/09/15.
 */
public class RangeQueryDispatcherBolt extends BaseRichBolt {
    OutputCollector collector;
    /*
    private final String nextComponentID;
    private final DataSchema schema;
    // TODO hard coded for now. make dynamic.
    private final double [] RANGE_BREAKPOINTS = {103.8,103.85,103.90,104.00};
    private List<Integer> nextComponentTasks;
    private String rangePartitionField;
    */

    private final DataSchema schema;

    private List<Integer> targetTasks;

    private Map<Integer, Pair> taskIdToKeyRange;

    private Map<Integer, Long> taskIdToTimeStamp;

    private TaskPartitionSchemaManager taskPartitionSchemaManager;

    private Map<Integer, TaskMetaData> taskIdToTaskMetaData;


    public RangeQueryDispatcherBolt(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
//        this.nextComponentTasks=topologyContext.getComponentTasks(nextComponentID);
//        assert this.nextComponentTasks.size()==RANGE_BREAKPOINTS.length : "its hardcoded for now. lengths should match";
        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingTopology.BPlusTreeQueryStream).keySet();
        targetTasks = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

//        System.out.println("The task id ");
//        System.out.println(targetTasks);
//        scheduleKeyRangeToTask(targetTasks);
//
//        InitializeTimeStamp(targetTasks);
        setInitialKeyRangeAndTimeStampToTasks(targetTasks);
    }

    public void execute(Tuple tuple) {
//        double partitionValue = tuple.getDoubleByField(rangePartitionField);
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.BPlusTreeQueryStream)) {
//            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                    new Values(tuple.getValue(0)));
//            int numberOfTasksToSearch = 0;

            RangeQuerySubQuery subQuery = (RangeQuerySubQuery) tuple.getValue(0);
            /*
            Long queryId = tuple.getLong(0);
            Double leftKey = tuple.getDouble(1);
            Double rightKey = tuple.getDouble(2);
            Long startTime = tuple.getLong(3);
            */
            Long queryId = subQuery.getQueryId();
            Double leftKey = subQuery.getlefKey();
            Double rightKey = subQuery.getRightKey();
            Long startTime = subQuery.getStartTimestamp();

            /*
            for (Integer taskId : taskIdToKeyRange.keySet()) {
                Double minKey = (Double) taskIdToKeyRange.get(taskId).getKey();
                Double maxKey = (Double) taskIdToKeyRange.get(taskId).getValue();
                Long timeStamp = taskIdToTimeStamp.get(taskId);

                if (minKey >= leftKey && maxKey <= rightKey && timeStamp >= startTime) {
                    collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                    new Values(queryId, leftKey, rightKey));
                    ++numberOfTasksToSearch;
                }
            }
            */

//            List<Integer> targetTasks = taskPartitionSchemaManager.search(leftKey, rightKey, startTime, Long.MAX_VALUE);
//            int numberOfTasksToSearch = targetTasks.size();
            int numberOfTasksToSearch = 0;
//            for (Integer taskId : targetTasks) {
//                collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
//                        new Values(queryId, leftKey, rightKey));
//            }

            collector.emit(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryInformationStream
                    , new Values(queryId, numberOfTasksToSearch));

//            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                    new Values(tuple.getValue(0), tuple.getValue(1)));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.IndexStream)){
            try {
                Long timeStamp = System.currentTimeMillis();
                Values values = schema.getValuesObject(tuple);
                values.add(timeStamp);
//                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.IndexStream, schema.getValuesObject(tuple));
                collector.emit(NormalDistributionIndexingTopology.IndexStream, values);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Long timeStamp = tuple.getLong(0);
            int taskId = tuple.getSourceTask();
//            taskIdToTimeStamp.put(taskId, timeStamp);
            TaskMetaData taskMetaData = taskIdToTaskMetaData.get(taskId);
            taskPartitionSchemaManager.setStartTimeOfTask(taskMetaData, timeStamp);
        }
    }

    private void scheduleKeyRangeToTask(List<Integer> targetTasks) {
        int numberOfBolts = targetTasks.size();
        Double minKey = 0.0;
        Double maxKey = 500.0;
        taskIdToKeyRange = new HashMap<Integer, Pair>();
        for (int i = 0; i < numberOfBolts; ++i) {
            taskIdToKeyRange.put(targetTasks.get(i), new Pair(minKey, maxKey));
            minKey = maxKey + 0.00000000000001;
            maxKey += 500.0;
        }
    }

    private void InitializeTimeStamp(List<Integer> targetTasks) {
        taskIdToTimeStamp = new HashMap<Integer, Long>();
        int numberOfTasks = targetTasks.size();
        Long currentTimeStamp = System.currentTimeMillis();
        for (int i = 0; i < numberOfTasks; ++i) {
            taskIdToTimeStamp.put(targetTasks.get(i), currentTimeStamp);
        }
    }

/*
for (int i=0;i<RANGE_BREAKPOINTS.length;i++) {
if (partitionValue<RANGE_BREAKPOINTS[i]) {
try {
collector.emitDirect(nextComponentTasks.get(i),schema.getValuesObject(tuple));
//                    collector.emit(schema.getValuesObject(tuple));
} catch (IOException e) {
e.printStackTrace();
} finally {
break;
}
}
}
*/
//        try {
//            collector.emitDirect(nextComponentTasks.get(0),schema.getValuesObject(tuple));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(schema.getFieldsObject());
//        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("key"));
        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("queryId", "leftKey"
                , "rightKey"));

        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");

//        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, new Fields(fields));

        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryInformationStream
                , new Fields("queryId", "numberOfTasksToSearch"));
//        declarer.declare(new Fields("key"));
    }

    private void setInitialKeyRangeAndTimeStampToTasks(List<Integer> targetTasks) {
        taskPartitionSchemaManager = new TaskPartitionSchemaManager();
        taskIdToTaskMetaData = new HashMap<Integer, TaskMetaData>();
        int numberOfTasks = targetTasks.size();
        Double keyRangeLowerBound = 0.0;
        Double keyRangeUpperBound = 500.0;
        for (int i = 0; i < numberOfTasks; ++i) {
            Long startTime = System.currentTimeMillis();
            Long endTime = Long.MAX_VALUE;
            int taskId = targetTasks.get(i);
            TaskMetaData taskMetaData = new TaskMetaData(taskId, keyRangeLowerBound, keyRangeUpperBound,
                    startTime, endTime);

            keyRangeLowerBound = keyRangeUpperBound + 0.00000000000001;
            keyRangeUpperBound += 500.0;

            taskPartitionSchemaManager.add(taskMetaData);
            taskIdToTaskMetaData.put(taskId, taskMetaData);
        }
    }
}
