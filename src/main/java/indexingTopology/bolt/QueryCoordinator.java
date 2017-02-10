package indexingTopology.bolt;

import indexingTopology.util.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.google.common.util.concurrent.AtomicDouble;
import indexingTopology.config.TopologyConfig;
import indexingTopology.metadata.FilePartitionSchemaManager;
import indexingTopology.metadata.FileMetaData;
import indexingTopology.streams.Streams;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/15/16.
 */
public class QueryCoordinator<DataType extends Number> extends BaseRichBolt {

    private OutputCollector collector;

    private Thread QueryThread;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private File file;

    private BufferedReader bufferedReader;

    private Semaphore concurrentQueriesSemaphore;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private transient List<Integer> queryServers;

    private List<Integer> indexServers;

    private transient Map<Integer, Long> indexTaskToTimestampMapping;

    private ArrayBlockingQueue<SubQuery> taskQueue;

    private transient Map<Integer, ArrayBlockingQueue<SubQuery>> taskIdToTaskQueue;

    private BalancedPartition balancedPartition;

    private BalancedPartition staleBalancedPartition;

    private int numberOfPartitions;

    private DataType lowerBound;
    private DataType upperBound;

    private AtomicDouble minIndexValue;
    private AtomicDouble maxIndexValue;

    private BufferedWriter bufferedWriter;

    private LinkedBlockingQueue<Query> pendingQueue;

    public QueryCoordinator(DataType lowerBound, DataType upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
//        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
//
//        try {
//            File file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/shuffle_grouping");
//
//            if (!file.exists()) {
//                file.createNewFile();
//            }

//            FileWriter fileWriter = new FileWriter(file,true);
//
//            bufferedWriter = new BufferedWriter(fileWriter);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        concurrentQueriesSemaphore = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        taskQueue = new ArrayBlockingQueue<>(TopologyConfig.TASK_QUEUE_CAPACITY);
//        taskQueue = new LinkedBlockingQueue<SubQuery>(TopologyConfig.FILE_QUERY_TASK_WATINING_QUEUE_CAPACITY);

        Set<String> componentIds = topologyContext.getThisTargets().get(Streams.FileSystemQueryStream).keySet();

        taskIdToTaskQueue = new HashMap<Integer, ArrayBlockingQueue<SubQuery>>();

        queryServers = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            queryServers.addAll(topologyContext.getComponentTasks(componentId));
        }

        createTaskQueues(queryServers);
        
        componentIds = topologyContext.getThisTargets().get(Streams.BPlusTreeQueryStream).keySet();

        indexServers = new ArrayList<Integer>();
        for (String componentId : componentIds) {
            indexServers.addAll(topologyContext.getComponentTasks(componentId));
        }


        minIndexValue = new AtomicDouble(2000);
        maxIndexValue = new AtomicDouble(0);


//        try {
//            bufferedReader = new BufferedReader(new FileReader(file));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        setTimestamps();

        numberOfPartitions = queryServers.size();
        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound);

        staleBalancedPartition = null;

        pendingQueue = new LinkedBlockingQueue<>();

        QueryThread = new Thread(new QueryRunnable());
        QueryThread.start();

        createQueryHandlingThread();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, ((DataType) keyRange.getKey()).doubleValue(),
                    ((DataType) keyRange.getValue()).doubleValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));
        } else if (tuple.getSourceStreamId().equals(Streams.NewQueryStream)) {
            Long queryId = tuple.getLong(0);

            System.out.println("query id " + queryId + " has been finished!!!");
//            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);

//            FileScanMetrics metrics = (FileScanMetrics) tuple.getValue(2);

//            int numberOfFilesToScan = tuple.getInteger(3);

//            try {
//                writeToFile(metrics, numberOfFilesToScan);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            concurrentQueriesSemaphore.release();
        } else if (tuple.getSourceStreamId().equals(Streams.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();

            /*task queue model
            sendSubqueryToTask(taskId);
            */

//            /*our method
            sendSubquery(taskId);
//            */

        } else if (tuple.getSourceStreamId().equals(Streams.QueryGenerateStream)) {


//            Long queryId = tuple.getLong(0);
//            DataType leftKey = (DataType) tuple.getValue(1);
//            DataType rightKey = (DataType) tuple.getDouble(2);
//            Long startTimestamp = tuple.getLong(3);
//            Long endTimestamp = tuple.getLong(4);

            Query query = (Query) tuple.getValueByField("query");

//            Query query = new Query<>(queryId, leftKey, rightKey, startTimestamp, endTimestamp);
            try {
                pendingQueue.put(query);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

//            List<String> fileNames = filePartitionSchemaManager.search(leftKey, rightKey, startTimestamp, endTimestamp);
//
//            collector.emit(Streams.BPlusTreeQueryStream,
//                    new Values(queryId, leftKey, rightKey, startTimestamp, endTimestamp));
//
//            int numberOfFilesToScan = fileNames.size();
//            collector.emit(Streams.FileSystemQueryInformationStream, new Values(queryId, numberOfFilesToScan));
//
//
//            for (String fileName : fileNames) {
//                SubQuery subQuery = new SubQuery(queryId, leftKey, rightKey, fileName, startTimestamp, endTimestamp);
//                try {
//                    taskQueue.put(subQuery);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            for (Integer taskId : queryServers) {
//                SubQuery subQuery = taskQueue.poll();
//                if (subQuery != null) {
//                    collector.emitDirect(taskId, Streams.FileSystemQueryStream
//                            , new Values(subQuery));
//                }
//            }

        } else if (tuple.getSourceStreamId().equals(Streams.TimestampUpdateStream)) {
            int taskId = tuple.getIntegerByField("taskId");
            Pair timestampRange = (Pair) tuple.getValueByField("timestampRange");
            Pair keyRange = (Pair) tuple.getValueByField("keyRange");
            Long endTimestamp = (Long) timestampRange.getValue();
            indexTaskToTimestampMapping.put(taskId, endTimestamp);

            DataType keyRangeLowerBound = (DataType) keyRange.getKey();
            DataType keyRangeUpperBound = (DataType) keyRange.getValue();

            if (staleBalancedPartition != null) {
                updateStaleBalancedPartition(keyRangeLowerBound, keyRangeUpperBound);
                if (canDeleteStalePartition()) {
                    staleBalancedPartition = null;
                    System.out.println("stale partition has been deleted!!!");
                    collector.emit(new Values("Partition can be enabled!"));
                }
            }

            collector.emitDirect(taskId, Streams.TreeCleanStream, new Values(keyRange, timestampRange));

        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)) {
//            Map<Integer, Integer> intervalToPartitionMapping = (Map) tuple.getValueByField("newIntervalPartition");
            BalancedPartition newBalancedPartition = (BalancedPartition) tuple.getValueByField("newIntervalPartition");
            staleBalancedPartition = balancedPartition;
            balancedPartition = newBalancedPartition;
            System.out.println("partition has been updated!!!");
//            balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
        }
    }

    private boolean canDeleteStalePartition() {
        boolean canBeDeleted = true;
        Set<Integer> intervals = staleBalancedPartition.getIntervalToPartitionMapping().keySet();
        Map<Integer, Integer> staleIntervalIdToPartitionIdMapping = staleBalancedPartition.getIntervalToPartitionMapping();
        Map<Integer, Integer> intervalIdToPartitionIdMapping = balancedPartition.getIntervalToPartitionMapping();

        for (Integer intervalId : intervals) {
            if (staleIntervalIdToPartitionIdMapping.get(intervalId) != intervalIdToPartitionIdMapping.get(intervalId)) {
                canBeDeleted = false;
                break;
            }
        }

        return canBeDeleted;
    }

    private void updateStaleBalancedPartition(DataType keyRangeLowerBound, DataType keyRangeUpperBound) {
        Map<Integer, Integer> intervalIdToPartitionIdMapping = balancedPartition.getIntervalToPartitionMapping();
        Integer startIntervalId = balancedPartition.getPartitionId(keyRangeLowerBound);
        Integer endIntervalId = balancedPartition.getPartitionId(keyRangeUpperBound);


        for (Integer intervalId = startIntervalId; intervalId <= endIntervalId; ++intervalId) {
            staleBalancedPartition.getIntervalToPartitionMapping().put(intervalId, intervalIdToPartitionIdMapping.get(intervalId));
        }
    }

    private void writeToFile(FileScanMetrics metrics, int numberOfFilesToScan) throws IOException {
        Long totalTime = metrics.getTotalTime();
        Long searchTime = metrics.getSearchTime();
        Long fileReadTime = metrics.getFileReadingTime();
        Long leafDeserializationTime = metrics.getLeafDeserializationTime();
        Long treeDeserializationTime = metrics.getTreeDeserializationTime();

        String text = searchTime + " " + fileReadTime + " " + leafDeserializationTime + " "
                + treeDeserializationTime + " " + numberOfFilesToScan + " " + totalTime;

        System.out.println(text);

        bufferedWriter.write(text);

        bufferedWriter.newLine();

        bufferedWriter.flush();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
//        outputFieldsDeclarer.declareStream(NormalDistributionTopology.FileSystemQueryStream,
//                new Fields("queryId", "leftKey", "rightKey", "fileName", "startTimeStamp", "endTimeStamp"));

        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream, new Fields("subquery"));

//        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream,
//                new Fields("queryId", "leftKey", "rightKey"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream, new Fields("subquery"));

        outputFieldsDeclarer.declareStream(
                Streams.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(
                Streams.BPlusTreeQueryInformationStream,
                new Fields("queryId", "numberOfTasksToScan"));

        outputFieldsDeclarer.declareStream(Streams.TreeCleanStream, new Fields("keyRange", "timestampRange"));

        outputFieldsDeclarer.declareStream(Streams.EableRepartitionStream, new Fields("Repartition"));
    }

    private void handleQuery(Query<DataType> query) {
//        generateSubQueriesOnTheInsertionServer(query.leftKey, query.rightKey, query.endTimestamp);
        generateSubQueriesOnTheInsertionServer(query);
//        generateSubqueriesOnTheFileScanner(query.leftKey, query.rightKey, query.startTimestamp, query.endTimestamp);
        generateSubqueriesOnTheFileScanner(query);

//        System.out.println(query.id + " has been handled!!!");
    }

    private void generateSubqueriesOnTheFileScanner(Query<DataType> query) {
        Long queryId = query.id;
        DataType leftKey = query.leftKey;
        DataType rightKey = query.rightKey;
        Long startTimestamp = query.startTimestamp;
        Long endTimestamp = query.endTimestamp;

        List<String> fileNames = filePartitionSchemaManager.search(leftKey.doubleValue(), rightKey.doubleValue(), startTimestamp, endTimestamp);

        for (String fileName : fileNames) {
            SubQuery subQuery = new SubQueryOnFiles(queryId, leftKey, rightKey, fileName, startTimestamp, endTimestamp);
//            /*shuffle grouping
//            sendSubqueriesByshuffleGrouping(subQuery);
//             */
            /*task queue
            try {
                taskQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendSubqueriesFromTaskQueue();
            */
            putSubqueryToTaskQueues(subQuery);
            sendSubqueriesFromTaskQueues();
        }

        collector.emit(Streams.FileSystemQueryInformationStream, new Values(queryId, fileNames.size()));
    }

    private void createQueryHandlingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Query query = pendingQueue.take();
                        concurrentQueriesSemaphore.acquire();
                        handleQuery(query);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    class QueryRunnable implements Runnable {

        public void run() {
            while (true) {
                try {
                    Thread.sleep(1000);
                    //                String text = null;
//                try {
//                    text = bufferedReader.readLine();
//                    if (text == null) {
//                        bufferedReader.close();
//                        bufferedReader = new BufferedReader(new FileReader(file));
//                        text = bufferedReader.readLine();
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }


//                String [] tuple = text.split(" ");
//
                    Integer leftKey = 940;
                    Integer rightKey = 950;

                    Long startTimestamp = (long) 0;

                    Long endTimestamp = Long.MAX_VALUE;

                    pendingQueue.put(new Query(queryId, leftKey, rightKey, startTimestamp, endTimestamp));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                ++queryId;

            }
        }
    }

    private void createTaskQueues(List<Integer> targetTasks) {
        for (Integer taskId : targetTasks) {
            ArrayBlockingQueue<SubQuery> taskQueue = new ArrayBlockingQueue<SubQuery>(TopologyConfig.TASK_QUEUE_CAPACITY);
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }

//    private void putSubqueriesToTaskQueue(SubQueryOnFiles subQuery) {
//        for (int i = 0; i < numberOfSubqueries; ++i) {
//            SubQueryOnFiles subQuery = new SubQueryOnFiles(queryId, leftKey,  rightKey, fileNames.get(i), starTimestamp, endTimestamp);
//            try {
//                taskQueue.put(subQuery);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    private void generateSubqueriesOnTheFileScanner(DataType leftKey, DataType rightKey, Long startTimestamp,
                                                    Long endTimestamp) {
        List<String> fileNames = filePartitionSchemaManager.search(leftKey.doubleValue(), rightKey.doubleValue(),
                startTimestamp, endTimestamp);


//                int numberOfSubqueries = 10;

//                if (fileNames.size() < numberOfSubqueries) {
//                    newQueryRequest.release();
//                    continue;
//                }



        int numberOfFilesToScan = fileNames.size();

        int numberOfSubqueries = numberOfFilesToScan;

                /* taskQueueModel
                   putSubqueriesToTaskQueue(numberOfSubqueries, leftKey, rightKey, fileNames, startTimeStamp, endTimeStamp);
                   sendSubqueriesFromTaskQueue();
                */

                /* shuffleGrouping
                   sendSubqueriesByshuffleGrouping(numberOfSubqueries, leftKey, rightKey, fileNames, startTimeStamp, endTimeStamp);
                 */

//                /* our method
//        putSubquerisToTaskQueues(numberOfSubqueries, leftKey, rightKey, fileNames, startTimestamp, endTimestamp);
//        sendSubqueriesFromTaskQueues();
//                 */

//                System.out.println("query Id " + queryId + " " + numberOfFilesToScan + " files in decompositionbolt");

        collector.emit(Streams.FileSystemQueryInformationStream, new Values(queryId, numberOfSubqueries));
    }

    private void generateSubQueriesOnTheInsertionServer(DataType leftKey, DataType rightKey, Long endTimestamp) {

        int numberOfTasksToSearch = 0;

        List<Integer> partitionIdsInBalancedPartition = getPartitionIds(balancedPartition, leftKey, rightKey);
        List<Integer> partitionIdsInStalePartition = null;

        if (staleBalancedPartition != null) {
            partitionIdsInStalePartition = getPartitionIds(staleBalancedPartition, leftKey, rightKey);
        }

        List<Integer> partitionIds = (partitionIdsInStalePartition == null
                ? partitionIdsInBalancedPartition
                : mergePartitionIds(partitionIdsInBalancedPartition, partitionIdsInStalePartition));



//        Integer startPartitionId = balancedPartition.getPartitionId(leftKey);
//        Integer endPartitionId = balancedPartition.getPartitionId(rightKey);




//        Integer startTaskId = indexServers.get(startPartitionId);
//        Integer endTaskId = indexServers.get(endPartitionId);

//        for (Integer taskId = startTaskId; taskId <= endTaskId; ++taskId) {
//            Long timestamp = indexTaskToTimestampMapping.get(taskId);
//            if (timestamp <= endTimeStamp) {
//                collector.emitDirect(taskId, NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                        new Values(queryId, leftKey, rightKey));
//                ++numberOfTasksToSearch;
//            }
//        }

        for (Integer partitionId : partitionIds) {
            Integer taskId = indexServers.get(partitionId);
            Long timestamp = indexTaskToTimestampMapping.get(taskId);
            if (timestamp <= endTimestamp) {
                collector.emitDirect(taskId, Streams.BPlusTreeQueryStream,
                        new Values(queryId, leftKey, rightKey));
//                System.out.println("query id " + queryId + " has been emitted to bolt " + taskId);
                ++numberOfTasksToSearch;
            }
        }

        collector.emit(Streams.BPlusTreeQueryInformationStream,
                new Values(queryId, numberOfTasksToSearch));

    }


    private void generateSubQueriesOnTheInsertionServer(Query<DataType> query) {

        int numberOfTasksToSearch = 0;

        Long queryId = query.id;

        DataType leftKey = query.leftKey;
        DataType rightKey = query.rightKey;

        Long startTimestamp = query.startTimestamp;
        Long endTimestamp = query.endTimestamp;

        List<Integer> partitionIdsInBalancedPartition = getPartitionIds(balancedPartition, leftKey, rightKey);
        List<Integer> partitionIdsInStalePartition = null;

        if (staleBalancedPartition != null) {
            partitionIdsInStalePartition = getPartitionIds(staleBalancedPartition, leftKey, rightKey);
        }

        List<Integer> partitionIds = (partitionIdsInStalePartition == null
                ? partitionIdsInBalancedPartition
                : mergePartitionIds(partitionIdsInBalancedPartition, partitionIdsInStalePartition));



//        Integer startPartitionId = balancedPartition.getPartitionId(leftKey);
//        Integer endPartitionId = balancedPartition.getPartitionId(rightKey);




//        Integer startTaskId = indexServers.get(startPartitionId);
//        Integer endTaskId = indexServers.get(endPartitionId);

//        for (Integer taskId = startTaskId; taskId <= endTaskId; ++taskId) {
//            Long timestamp = indexTaskToTimestampMapping.get(taskId);
//            if (timestamp <= endTimeStamp) {
//                collector.emitDirect(taskId, NormalDistributionIndexingTopology.BPlusTreeQueryStream,
//                        new Values(queryId, leftKey, rightKey));
//                ++numberOfTasksToSearch;
//            }
//        }

//        for (Integer partitionId : partitionIds) {
//            Integer taskId = indexServers.get(partitionId);
//            Long timestamp = indexTaskToTimestampMapping.get(taskId);
//            if (timestamp <= endTimestamp && timestamp >= startTimestamp) {
//                SubQuery subQuery = new SubQuery(query.id, query.leftKey, query.rightKey, query.startTimestamp, query.endTimestamp);
//                collector.emitDirect(taskId, Streams.BPlusTreeQueryStream, new Values(subQuery));
//                ++numberOfTasksToSearch;
//            }
//        }

//        collector.emit(Streams.BPlusTreeQueryInformationStream, new Values(queryId, numberOfTasksToSearch));
        collector.emit(Streams.BPlusTreeQueryInformationStream, new Values(queryId, 0));

    }

    private List<Integer> mergePartitionIds(List<Integer> partitionIdsInBalancedPartition, List<Integer> partitionIdsInStalePartition) {
        List<Integer> partitionIds = new ArrayList<>();
        partitionIds.addAll(partitionIdsInBalancedPartition);


        for (Integer partitionId : partitionIdsInStalePartition) {
            if (!partitionIds.contains(partitionId)) {
                partitionIds.add(partitionId);
            }
        }

        return partitionIds;
    }

    private List<Integer> getPartitionIds(BalancedPartition balancedPartition, DataType leftKey, DataType rightKey) {
        Integer startPartitionId = balancedPartition.getPartitionId(leftKey);
        Integer endPartitionId = balancedPartition.getPartitionId(rightKey);

        List<Integer> partitionIds = new ArrayList<>();
        for (Integer partitionId = startPartitionId; partitionId <= endPartitionId; ++partitionId) {
            partitionIds.add(partitionId);
        }

        return partitionIds;
    }

    /*
    private void putSubquerisToTaskQueues(int numberOfSubqueries, DataType leftKey
            , DataType rightKey, List<String> fileNames, Long startTimestamp, Long endTimestamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            String fileName = fileNames.get(i);
            int index = Math.abs(fileName.hashCode()) % queryServers.size();
            Integer taskId = queryServers.get(index);
            ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            SubQuery subQuery = new SubQuery(queryId, leftKey,  rightKey, fileName, startTimestamp, endTimestamp);
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
    */

    private void putSubqueryToTaskQueues(SubQuery subQuery) {
        String fileName = ((SubQueryOnFiles) subQuery).getFileName();
        int index = Math.abs(fileName.hashCode()) % queryServers.size();
        Integer taskId = queryServers.get(index);
        ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
//        if (taskQueue == null) {
//            taskQueue = new ArrayBlockingQueue<SubQueryOnFiles>(TopologyConfig.TASK_QUEUE_CAPACITY);
//        }
        try {
            taskQueue.put(subQuery);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        taskIdToTaskQueue.put(taskId, taskQueue);
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


    private void sendSubqueriesFromTaskQueues() {
        for (Integer taskId : queryServers) {
            ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            SubQuery subQuery = taskQueue.poll();
            if (subQuery != null) {
                collector.emitDirect(taskId, Streams.FileSystemQueryStream
                        , new Values(subQuery));
            }
        }
    }

    /*
    private void sendSubqueriesByshuffleGrouping(int numberOfSubqueries, DataType leftKey
            , DataType rightKey, List<String> fileNames, Long startTimestamp, Long endTimestamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            SubQuery subQuery = new SubQuery(queryId, leftKey, rightKey, fileNames.get(i), startTimestamp, endTimestamp);
            collector.emit(Streams.FileSystemQueryStream
                    , new Values(subQuery));
        }
    }
    */

    private void sendSubqueriesByshuffleGrouping(SubQuery subQuery) {
            collector.emit(Streams.FileSystemQueryStream, new Values(subQuery));
    }

    private void sendSubqueryToTask(int taskId) {
        SubQuery subQuery = taskQueue.poll();

        if (subQuery != null) {
            collector.emitDirect(taskId, Streams.FileSystemQueryStream
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
            collector.emitDirect(taskId, Streams.FileSystemQueryStream
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

    /*
    private List<SubQuery> generateSubqueries(Long queryId, DataType leftKey
            , DataType rightKey, List<String> fileNames, Long startTimestamp, Long endTimestamp) {
        List<SubQuery> subQueries = new ArrayList<>();

        SubQuery subQuery = new SubQuery(queryId, leftKey,
                rightKey, null, startTimestamp, endTimestamp);
        subQueries.add(subQuery);

        for (String fileName : fileNames) {
            subQuery = new SubQuery(queryId, leftKey, rightKey, fileName, startTimestamp, endTimestamp);
            subQueries.add(subQuery);
        }

        return subQueries;
    }
    */
    private void setTimestamps() {

        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexServers) {
            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
        }
    }

}
