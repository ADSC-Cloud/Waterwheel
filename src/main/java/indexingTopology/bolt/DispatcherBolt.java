package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.DataSchema;
import indexingTopology.MetaData.TaskMetaData;
import indexingTopology.MetaData.TaskPartitionSchemaManager;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.util.Histogram;
import indexingTopology.util.IntervalIdMappingFunction;
import indexingTopology.util.SubQuery;
import javafx.util.Pair;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/17/16.
 */
public class DispatcherBolt extends BaseRichBolt{

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

    private TaskPartitionSchemaManager taskPartitionSchemaManager;

    private Map<Integer, TaskMetaData> taskIdToTaskMetaData;

    private Map<Integer, Integer> intervalToTaskMapping;

    private File outputFile;

    private FileOutputStream fop;

    private Histogram histogram;

    private Double lowerBound;

    private Double upperBound;

    private Double newLowerBound;

    private Double newUpperBound;

    private Thread staticsSendingThread;

    private Semaphore staticsSendingRequest;

    public DispatcherBolt(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_tasks.txt");
//        this.nextComponentTasks=topologyContext.getComponentTasks(nextComponentID);
//        assert this.nextComponentTasks.size()==RANGE_BREAKPOINTS.length : "its hardcoded for now. lengths should match";
        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingTopology.BPlusTreeQueryStream).keySet();
        targetTasks = new ArrayList<Integer>();
        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        lowerBound = 0.0;
        upperBound = 1000.0;

        staticsSendingRequest = new Semaphore(1);

        histogram = new Histogram(lowerBound, upperBound, 4);

        intervalToTaskMapping = getInitialPartition(targetTasks);

        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        newLowerBound = lowerBound;
        newUpperBound = upperBound;

//        scheduleKeyRangeToTask(targetTasks);

//        InitializeTimeStamp(targetTasks);
        setInitialKeyRangeAndTimeStampToTasks(targetTasks);

        staticsSendingThread = new Thread(new SendStatisticsRunnable());
        staticsSendingThread.start();
    }

    public void execute(Tuple tuple) {
//        double partitionValue = tuple.getDoubleByField(rangePartitionField);
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.BPlusTreeQueryStream)) {
//            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                    new Values(tuple.getValue(0)));
//            int numberOfTasksToSearch = 0;
            SubQuery subQuery = (SubQuery) tuple.getValue(0);
            /*
            Long queryId = tuple.getLong(0);
            Double key = tuple.getDouble(1);
            Long startTime = tuple.getLong(2);
            */
            Long queryId = subQuery.getQueryId();
            Double key = subQuery.getKey();
            Long startTime = subQuery.getStartTimestamp();
            /*
            for (Integer taskId : taskIdToKeyRange.keySet()) {
//                Double minKey = (Double) taskIdToKeyRangeAndTimeRange.get(taskId).getKey();
                Double minKey = (Double) taskIdToKeyRange.get(taskId).getKey();
//                Double maxKey = (Double) taskIdToKeyRangeAndTimeRange.get(taskId).getValue();
                Double maxKey = (Double) taskIdToKeyRange.get(taskId).getValue();

                Long timeStamp = taskIdToTimeStamp.get(taskId);
                if (minKey <= key && maxKey >= key && timeStamp >= startTime) {
                    collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                            new Values(queryId, key));
                    ++numberOfTasksToSearch;
                }
            }*/


            List<Integer> targetTasks = taskPartitionSchemaManager.search(key, key, startTime, Long.MAX_VALUE);
            int numberOfTasksToSearch = targetTasks.size();

            for (Integer taskId : targetTasks) {
                collector.emitDirect(taskId, NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                        new Values(queryId, key));
            }

            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryInformationStream
                    , new Values(queryId, numberOfTasksToSearch));


            String content = "Query ID " + queryId + " " + numberOfTasksToSearch;
            String newline = System.getProperty("line.separator");
            byte[] contentInBytes = content.getBytes();
            byte[] nextLineInBytes = newline.getBytes();
            try {
                fop.write(contentInBytes);
                fop.write(nextLineInBytes);
            } catch (IOException e) {
                e.printStackTrace();
            }



//            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                    new Values(tuple.getValue(0), tuple.getValue(1)));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.IndexStream)) {
            try {
                Long timeStamp = System.currentTimeMillis();
                Values values = schema.getValuesObject(tuple);

                Double indexValue = tuple.getDoubleByField(schema.getIndexField());

                updateBound(indexValue);

                histogram.record(IntervalIdMappingFunction.getIntervalId(indexValue, lowerBound, upperBound));

                values.add(timeStamp);
//                collector.emit(NormalDistributionIndexingTopology.IndexStream, schema.getValuesObject(tuple));
                collector.emit(NormalDistributionIndexingTopology.IndexStream, values);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.TimeStampUpdateStream)){
            Long timeStamp = tuple.getLong(0);
            int taskId = tuple.getSourceTask();
//            taskIdToTimeStamp.put(taskId, timeStamp);
            TaskMetaData taskMetaData = taskIdToTaskMetaData.get(taskId);
            taskPartitionSchemaManager.setStartTimeOfTask(taskMetaData, timeStamp);
        } else {
            List<Integer> taskToIntervalMapping = (List) tuple.getValue(0);
            repartitionIntervals(taskToIntervalMapping);
            staticsSendingRequest.release();
        }
    }

    private void repartitionIntervals(List<Integer> taskToIntervalMapping) {
        int numberOfTasks = targetTasks.size();

        Double keyRangeLowerBound = Double.NEGATIVE_INFINITY;
        Double keyRangeUpperBound;

        for (int i = 0; i < numberOfTasks; ++i) {
            int taskId = targetTasks.get(i);
            TaskMetaData taskMetaData = taskIdToTaskMetaData.get(taskId);
            int intervalId = taskToIntervalMapping.get(i);
            keyRangeUpperBound = getIntervalUpperBound(intervalId);
            taskPartitionSchemaManager.setBoundsOfTask(taskMetaData, keyRangeLowerBound, keyRangeUpperBound);
//            System.out.println("After repartition ");
//            System.out.println("task id " + targetTasks.get(i));
//            System.out.println("lower bound " + keyRangeLowerBound);
//            System.out.println("upper bound " + keyRangeUpperBound);
            keyRangeLowerBound = keyRangeUpperBound + 0.000000000001;
        }
    }

    /*
    private void scheduleKeyRangeToTask(List<Integer> targetTasks) {
        int numberOfTasks = targetTasks.size();
        Double minKey = 0.0;
        Double maxKey = 500.0;
        taskIdToKeyRange = new HashMap<Integer, Pair>();
        for (int i = 0; i < numberOfTasks; ++i) {
            taskIdToKeyRange.put(targetTasks.get(i), new Pair(minKey, maxKey));
            minKey = maxKey + 0.00000000000001;
            maxKey += 500.0;
        }
    }
    */


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(schema.getFieldsObject());
//        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("key"));
        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Fields("queryId", "key"));

        List<String> fields = schema.getFieldsObject().toList();
        fields.add("timeStamp");

//        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, schema.getFieldsObject());
        declarer.declareStream(NormalDistributionIndexingTopology.IndexStream, new Fields(fields));

        declarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryInformationStream
                , new Fields("queryId", "numberOfTasksToSearch"));

        declarer.declareStream(NormalDistributionIndexingTopology.StatisticsReportStream, new Fields("statistics", "mapping", "lowerBound", "upperBound"));
//        declarer.declare(new Fields("key"));
    }

    private void setInitialKeyRangeAndTimeStampToTasks(List<Integer> targetTasks) {
        taskPartitionSchemaManager = new TaskPartitionSchemaManager();
        taskIdToTaskMetaData = new HashMap<Integer, TaskMetaData>();
        int numberOfTasks = targetTasks.size();
        Integer distance = (int) ((upperBound - lowerBound) / numberOfTasks);
        Double keyRangeLowerBound = Double.NEGATIVE_INFINITY;
        Double keyRangeUpperBound = lowerBound + distance;
        for (int i = 0; i < numberOfTasks; ++i) {
            Long startTime = System.currentTimeMillis();
            Long endTime = Long.MAX_VALUE;
            int taskId = targetTasks.get(i);
            TaskMetaData taskMetaData = new TaskMetaData(taskId, keyRangeLowerBound, keyRangeUpperBound,
                    startTime, endTime);

            keyRangeLowerBound = keyRangeUpperBound + 0.00000000000001;
            keyRangeUpperBound += distance;

            if (i == numberOfTasks - 2) {
                keyRangeUpperBound = Double.MAX_VALUE;
            }


            taskPartitionSchemaManager.add(taskMetaData);
            taskIdToTaskMetaData.put(taskId, taskMetaData);
        }
    }

    private Map<Integer, Integer> getInitialPartition(List<Integer> targetTasks) {
        int numberOfTasks = targetTasks.size();
        Integer distance = (int) ((upperBound - lowerBound) / numberOfTasks);
        Integer miniDistance = (int) ((upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS);
        Double keyRangeUpperBound = lowerBound + distance;
        Double bound = lowerBound + miniDistance;
        Map<Integer, Integer> IntervalIdToTaskId = new HashMap<>();
        Integer intervalId = 0;
        for (int i = 0; i < Config.NUMBER_OF_INTERVALS; ++i) {
            IntervalIdToTaskId.put(i, intervalId);
            bound += miniDistance;
            if (bound > keyRangeUpperBound) {
                keyRangeUpperBound = keyRangeUpperBound + distance;
                ++intervalId;
            }
        }
        return IntervalIdToTaskId;
    }

    private void updateBound(Double indexValue) {
        if (indexValue > newUpperBound) {
            newUpperBound = indexValue;
        }

        if (indexValue < newLowerBound) {
            newLowerBound = indexValue;
        }
    }

    private Double getIntervalUpperBound(int intervalId) {
        int distance = (int) (upperBound - lowerBound) / Config.NUMBER_OF_INTERVALS;
        Double actualUpperBound = lowerBound + distance;
        if (intervalId == 0) {
            return Double.NEGATIVE_INFINITY;
        } else if (intervalId == Config.NUMBER_OF_INTERVALS - 1) {
            return Double.POSITIVE_INFINITY;
        } else {
            return lowerBound + intervalId * distance;
        }
    }


    class SendStatisticsRunnable implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    staticsSendingRequest.acquire();
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                lowerBound = newLowerBound;
                upperBound = newUpperBound;

                collector.emit(NormalDistributionIndexingTopology.StatisticsReportStream,
                        new Values(histogram, intervalToTaskMapping, lowerBound, upperBound));
            }
        }

    }
}
