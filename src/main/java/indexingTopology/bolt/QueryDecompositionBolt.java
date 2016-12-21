package indexingTopology.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.Config.TopologyConfig;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.MetaData.FilePartitionSchemaManager;
import indexingTopology.MetaData.FileMetaData;
import indexingTopology.Streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.FileScanMetrics;
import indexingTopology.util.SubQuery;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/9/16.
 */
public class QueryDecompositionBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Thread QueryThread;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private List<Integer> indexServers;

    private File file;

    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private transient List<Integer> queryServers;

    private transient Map<Integer, ArrayBlockingQueue<SubQuery>> taskIdToTaskQueue;

    private transient Map<Integer, Long> indexTaskToTimestampMapping;

    private BalancedPartition balancedPartition;

    private ArrayBlockingQueue<SubQuery> taskQueue;

    private Double lowerBound;
    private Double upperBound;


    public QueryDecompositionBolt(Double lowerBound, Double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        taskQueue = new ArrayBlockingQueue<SubQuery>(TopologyConfig.TASK_QUEUE_CAPACITY);
//        taskQueue = new LinkedBlockingQueue<SubQuery>(TopologyConfig.FILE_QUERY_TASK_WATINING_QUEUE_CAPACITY);


        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

        Set<String> componentIds = topologyContext.getThisTargets().get(Streams.FileSystemQueryStream).keySet();

        queryServers = new ArrayList<Integer>();


        for (String componentId : componentIds) {
            queryServers.addAll(topologyContext.getComponentTasks(componentId));
        }

        taskIdToTaskQueue = new HashMap<Integer, ArrayBlockingQueue<SubQuery>>();

        createTaskQueues(queryServers);


        componentIds = topologyContext.getThisTargets().get(Streams.BPlusTreeQueryStream).keySet();

        indexServers = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            indexServers.addAll(topologyContext.getComponentTasks(componentId));
        }

        setTimestamps();

        createTaskQueues(indexServers);

        newQueryRequest = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        balancedPartition = new BalancedPartition(indexServers.size(), lowerBound, upperBound);

        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }





//        QueryThread = new Thread(new QueryRunnable());
//        QueryThread.start();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.NewQueryStream)) {
            Long queryId = tuple.getLong(0);

            FileScanMetrics metrics = (FileScanMetrics) tuple.getValue(2);

            int numberOfFilesToScan = tuple.getInteger(3);

            if (metrics != null) {
                Long totalTimeCost = metrics.getTotalTime();
            }

            newQueryRequest.release();

        } else if (tuple.getSourceStreamId().equals(Streams.QueryGenerateStream)) {
            try {
                newQueryRequest.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long queryId = tuple.getLong(0);
            Double key = tuple.getDouble(1);
            Long startTimeStamp = tuple.getLong(2);
            Long endTimeStamp = tuple.getLong(3);

            List<String> fileNames = filePartitionSchemaManager.search(key, key, startTimeStamp, endTimeStamp);

            collector.emit(Streams.BPlusTreeQueryStream,
                    new Values(queryId, key, startTimeStamp, endTimeStamp));

            int numberOfFilesToScan = fileNames.size();
            collector.emit(Streams.FileSystemQueryInformationStream,
                    new Values(queryId, numberOfFilesToScan));


            for (String fileName : fileNames) {
                SubQuery subQuery = new SubQuery(queryId, key, fileName, startTimeStamp, endTimeStamp);
                try {
                    taskQueue.put(subQuery);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Integer taskId : queryServers) {
                SubQuery subQuery = taskQueue.poll();
                if (subQuery != null) {
                    collector.emitDirect(taskId, Streams.FileSystemQueryStream
                            , new Values(subQuery));
                }
            }

        } else if (tuple.getSourceStreamId().equals(Streams.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();

            /*task queue model
            sendSubqueryToTask(taskId);
            */

//            /*our method
            sendSubquery(taskId);
//            */

        } else if (tuple.getSourceStreamId().equals(Streams.TimeStampUpdateStream)) {
            int taskId = tuple.getIntegerByField("taskId");
            Long timestamp = tuple.getLongByField("timestamp");

            indexTaskToTimestampMapping.put(taskId, timestamp);
        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)) {
            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("queryId", "key", "fileName", "startTimestamp", "endTimestamp"));

        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream,
                new Fields("subQuery"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream,
                new Fields("queryId", "key"));

        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryInformationStream,
                new Fields("queryId", "numberOfTasksToSearch"));
    }


    class QueryRunnable implements Runnable {

        public void run() {

            while (true) {

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    newQueryRequest.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String text = null;
                try {
                    text = bufferedReader.readLine();
//                    if (text == null) {
////                        bufferedReader.close();
//                        bufferedReader = new BufferedReader(new FileReader(file));
//                        text = bufferedReader.readLine();
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

                String [] tuple = text.split(" ");

                Double key = Double.parseDouble(tuple[0]);
//                Double key = 499.34001632016685;

                Long startTimeStamp = (long) 0;
//                Long startTimeStamp = System.currentTimeMillis() - 10000;
//                Long endTimeStamp = System.currentTimeMillis();
                Long endTimeStamp = Long.MAX_VALUE;

                generateTreeSubQuery(key, endTimeStamp);

                List<String> fileNames = filePartitionSchemaManager.search(key, key, startTimeStamp, endTimeStamp);

                int numberOfFilesToScan = fileNames.size();

                int numberOfSubqueries = numberOfFilesToScan;

//                if (numberOfFilesToScan < numberOfSubqueries) {
//                    newQueryRequest.release();
//                    continue;
//                }

                collector.emit(Streams.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfSubqueries));

                /* taskQueueModel
                   putSubqueriesToTaskQueue(numberOfSubqueries, key, fileNames, startTimeStamp, endTimeStamp);
                   sendSubqueriesFromTaskQueue();
                */

                /* shuffleGrouping
                   sendSubqueriesByshuffleGrouping(numberOfSubqueries, key, fileNames, startTimeStamp, endTimeStamp);
                 */


                putSubqueriesToTaskQueues(numberOfSubqueries, key, fileNames, startTimeStamp, endTimeStamp);
                sendSubqueriesFromTaskQueues();

             ++queryId;


            }
        }
    }

    private void generateTreeSubQuery(Double key, Long endTimeStamp) {

        int numberOfTasksToSearch = 0;
//        int intervalId = balancedPartition.getIntervalId(key);
        Integer partitionId = balancedPartition.getPartitionId(key);
        int taskId = indexServers.get(partitionId);
        Long timestamp = indexTaskToTimestampMapping.get(taskId);
        if (timestamp <= endTimeStamp) {
            collector.emitDirect(taskId, Streams.BPlusTreeQueryStream,
                    new Values(queryId, key));
            numberOfTasksToSearch = 1;
        }
        collector.emit(Streams.BPlusTreeQueryInformationStream,
                new Values(queryId, numberOfTasksToSearch));
    }

    private void createTaskQueues(List<Integer> targetTasks) {
        for (Integer taskId : targetTasks) {
            ArrayBlockingQueue<SubQuery> taskQueue = new ArrayBlockingQueue<SubQuery>(TopologyConfig.TASK_QUEUE_CAPACITY);
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }


    private void putSubquerisToTaskQueue(int numberOfSubqueries, Double key,
                                         List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            SubQuery subQuery = new SubQuery(queryId, key, fileNames.get(i), startTimeStamp, endTimeStamp);
            try {
                taskQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void sendSubqueriesFromTaskQueue() {
        for (Integer taskId : queryServers) {
            SubQuery subQuery = taskQueue.poll();
            if (subQuery != null) {
                collector.emitDirect(taskId, Streams.FileSystemQueryStream
                        , new Values(subQuery));
            }
        }
    }

    private void sendSubqueriesByshuffleGrouping(int numberOfSubqueries, Double key,
                                                 List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            SubQuery subQuery = new SubQuery(queryId, key, fileNames.get(i), startTimeStamp, endTimeStamp);
            collector.emit(Streams.FileSystemQueryStream
                    , new Values(subQuery));
        }
    }

    private void putSubqueriesToTaskQueues(int numberOfSubqueries, Double key,
                                           List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            String fileName = fileNames.get(i);
            SubQuery subQuery = new SubQuery(queryId, key, fileName, startTimeStamp, endTimeStamp);
            int index = Math.abs(fileName.hashCode()) % queryServers.size();
            Integer taskId = queryServers.get(index);
            ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            if (taskQueue == null) {
                taskQueue = new ArrayBlockingQueue<SubQuery>(TopologyConfig.TASK_QUEUE_CAPACITY);
            }
            try {
                taskQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }

    private void sendSubqueriesFromTaskQueues() {
        for (Integer taskId : queryServers) {
            ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            SubQuery subQuery = taskQueue.poll();
            if (subQuery != null) {
                collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                        , new Values(subQuery));
            }
        }
    }

    private void sendSubqueryToTask(int taskId) {
        SubQuery subQuery = taskQueue.poll();

        if (subQuery != null) {
            collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                    , new Values(subQuery));
        }

    }

    private void sendSubquery(int taskId) {

        ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);

        SubQuery subQuery = taskQueue.poll();

        if (subQuery == null) {

            taskQueue = getLongestQueue();

            subQuery = taskQueue.poll();

        }

        if (subQuery != null) {
            collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                    , new Values(subQuery));
        }
    }

    private ArrayBlockingQueue<SubQuery> getLongestQueue() {
        List<ArrayBlockingQueue<SubQuery>> taskQueues
                = new ArrayList<ArrayBlockingQueue<SubQuery>>(taskIdToTaskQueue.values());

        Collections.sort(taskQueues, (taskQueue1, taskQueue2) -> Integer.compare(taskQueue2.size(), taskQueue1.size()));

        ArrayBlockingQueue<SubQuery> taskQueue = taskQueues.get(0);

        return taskQueue;
    }

    private void setTimestamps() {

        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexServers) {
            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
        }
    }

}
