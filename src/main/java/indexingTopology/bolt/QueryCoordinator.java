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

    private LinkedBlockingQueue<Query> pendingQueue;

    public QueryCoordinator(DataType lowerBound, DataType upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        concurrentQueriesSemaphore = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        taskQueue = new ArrayBlockingQueue<>(TopologyConfig.TASK_QUEUE_CAPACITY);

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
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
//            Pair timestampRange = (Pair) tuple.getValueByField("timestampRange");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
//            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, ((DataType) keyDomain.getLowerBound()).doubleValue(),
                    ((DataType) keyDomain.getUpperBound()).doubleValue(), timeDomain.getStartTimestamp(), timeDomain.getEndTimestamp()));
        } else if (tuple.getSourceStreamId().equals(Streams.NewQueryStream)) {
            Long queryId = tuple.getLong(0);

            System.out.println("query id " + queryId + " has been finished!!!");
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
            Query query = (Query) tuple.getValueByField("query");

            try {
                pendingQueue.put(query);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else if (tuple.getSourceStreamId().equals(Streams.TimestampUpdateStream)) {
            int taskId = tuple.getIntegerByField("taskId");
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
//            Pair timestampRange = (Pair) tuple.getValueByField("timestampRange");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
//            Pair timeStampRange = (Pair) tuple.getValue(2);
            Long endTimestamp = timeDomain.getEndTimestamp();
            indexTaskToTimestampMapping.put(taskId, endTimestamp);

            DataType keyRangeLowerBound = (DataType) keyDomain.getLowerBound();
            DataType keyRangeUpperBound = (DataType) keyDomain.getUpperBound();

            if (staleBalancedPartition != null) {
                updateStaleBalancedPartition(keyRangeLowerBound, keyRangeUpperBound);
                if (canDeleteStalePartition()) {
                    staleBalancedPartition = null;
                    System.out.println("stale partition has been deleted!!!");
                    collector.emit(new Values("Partition can be enabled!"));
                }
            }

            collector.emitDirect(taskId, Streams.TreeCleanStream, new Values(keyDomain, timeDomain));

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

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream, new Fields("subquery"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream, new Fields("subquery"));

        outputFieldsDeclarer.declareStream(
                Streams.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(
                Streams.BPlusTreeQueryInformationStream,
                new Fields("queryId", "numberOfTasksToScan"));

        outputFieldsDeclarer.declareStream(Streams.TreeCleanStream, new Fields("keyDomain", "timeDomain"));

        outputFieldsDeclarer.declareStream(Streams.EableRepartitionStream, new Fields("Repartition"));
    }

    private void handleQuery(Query<DataType> query) {
        generateSubQueriesOnTheInsertionServer(query);
        generateSubqueriesOnTheFileScanner(query);
    }

    private void generateSubqueriesOnTheFileScanner(Query<DataType> query) {
        Long queryId = query.id;
        DataType leftKey = query.leftKey;
        DataType rightKey = query.rightKey;
        Long startTimestamp = query.startTimestamp;
        Long endTimestamp = query.endTimestamp;

        List<String> fileNames = filePartitionSchemaManager.search(leftKey.doubleValue(), rightKey.doubleValue(), startTimestamp, endTimestamp);

        for (String fileName : fileNames) {
            SubQuery subQuery = new SubQueryOnFile(queryId, leftKey, rightKey, fileName, startTimestamp, endTimestamp);
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

                    Integer leftKey = 0;
                    Integer rightKey = 1000;

//                    Long leftKey = 0L;
//                    Long rightKey = 1000L;

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

//        for (Integer partitionId : partitionIds) {
//            Integer taskId = indexServers.get(partitionId);
//            Long timestamp = indexTaskToTimestampMapping.get(taskId);
//            if (timestamp <= endTimestamp && timestamp >= startTimestamp) {
//                SubQuery subQuery = new SubQuery(query.id, query.leftKey, query.rightKey, query.startTimestamp, query.endTimestamp);
//                collector.emitDirect(taskId, Streams.BPlusTreeQueryStream, new Values(subQuery));
//                ++numberOfTasksToSearch;
//            }
//        }

        collector.emit(Streams.BPlusTreeQueryInformationStream, new Values(queryId, numberOfTasksToSearch));
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


    private void putSubqueryToTaskQueues(SubQuery subQuery) {
        String fileName = ((SubQueryOnFile) subQuery).getFileName();
        int index = Math.abs(fileName.hashCode()) % queryServers.size();
        Integer taskId = queryServers.get(index);
        ArrayBlockingQueue<SubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
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
                collector.emitDirect(taskId, Streams.FileSystemQueryStream, new Values(subQuery));
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


    private void sendSubqueriesByshuffleGrouping(SubQuery subQuery) {
        collector.emit(Streams.FileSystemQueryStream, new Values(subQuery));
    }


    private void sendSubqueryToTask(int taskId) {
        SubQuery subQuery = taskQueue.poll();

        if (subQuery != null) {
            collector.emitDirect(taskId, Streams.FileSystemQueryStream, new Values(subQuery));
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

    private void setTimestamps() {
        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexServers) {
            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
        }
    }

}
