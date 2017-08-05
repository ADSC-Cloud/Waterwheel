package indexingTopology.bolt;

import com.google.common.hash.BloomFilter;
import indexingTopology.bloom.BloomFilterStore;
import indexingTopology.bloom.DataChunkBloomFilters;
import indexingTopology.bolt.metrics.LocationInfo;
import indexingTopology.common.*;
import indexingTopology.common.data.DataSchema;
import indexingTopology.common.data.PartialQueryResult;
import indexingTopology.common.logics.DataTuplePredicate;
import indexingTopology.filesystem.HdfsFileSystemHandler;
import indexingTopology.metadata.SchemaManager;
import indexingTopology.metrics.Tags;
import indexingTopology.metrics.TimeMetrics;
import indexingTopology.util.partition.BalancedPartition;
import javafx.util.Pair;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract public class QueryCoordinatorBolt<T extends Number & Comparable<T>> extends BaseRichBolt {

    private OutputCollector collector;

    private FilePartitionSchemaManager filePartitionSchemaManager;

     private Semaphore concurrentQueriesSemaphore;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 1;

//    private long queryId;

    private transient List<Integer> queryServers;

    private List<Integer> indexServers;

    private transient Map<Integer, Long> indexTaskToTimestampMapping;

    private ArrayBlockingQueue<SubQueryOnFile> taskQueue;

    private transient Map<Integer, PriorityBlockingQueue<SubQueryOnFileWithPriority>> taskIdToTaskQueue;

    private HashSet<String> unprocessedSubqueries;

    private BalancedPartition balancedPartition;

    private BalancedPartition balancedPartitionToBeDeleted;

    private int numberOfPartitions;

    private T lowerBound;
    private T upperBound;

    private AtomicDouble minIndexValue;
    private AtomicDouble maxIndexValue;

    protected LinkedBlockingQueue<List<Query<T>>> pendingQueue;

    private Map<Long, Pair> queryIdToStartTime;

    private Thread queryHandlingThread;

//    private Map<String, Map<String, BloomFilter>> columnToChunkToBloomFilter;

    private BloomFilterStore bloomFilterStore;

    TopologyConfig config;

    private Map<Integer, String> ingestionServerIdToLocations;

    private Map<String, Set<Integer>> locationToChunkServerIds;

    private Map<Integer, String> chunkServerIdToLocation;

    protected DataSchema schema;

    private static final Logger LOG = LoggerFactory.getLogger(QueryCoordinatorBolt.class);

    private FileSystem fileSystem;

    private HashMap<String, String[]> chunkNameToPreferredHostsMapping;

    protected SchemaManager schemaManager;

    public QueryCoordinatorBolt(T lowerBound, T upperBound, TopologyConfig config, DataSchema schema) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.config = config;
        this.schema = schema;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {


        collector = outputCollector;

        bloomFilterStore = new BloomFilterStore(config);

        ingestionServerIdToLocations = new HashMap<>();

        locationToChunkServerIds = new HashMap<>();

        chunkServerIdToLocation = new HashMap<>();

        unprocessedSubqueries = new HashSet<>();

        chunkNameToPreferredHostsMapping = new HashMap<>();

        concurrentQueriesSemaphore = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
//        queryId = 0;
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        taskQueue = new ArrayBlockingQueue<>(config.TASK_QUEUE_CAPACITY);

        queryServers = topologyContext.getComponentTasks("ChunkScannerBolt");

        taskIdToTaskQueue = new HashMap<>();
        createTaskQueues(queryServers);

        indexServers = topologyContext.getComponentTasks("IndexerBolt");


        minIndexValue = new AtomicDouble(2000);
        maxIndexValue = new AtomicDouble(0);


        queryIdToStartTime = new HashMap<>();

        setTimestamps();

        numberOfPartitions = indexServers.size();
        balancedPartition = new BalancedPartition(numberOfPartitions, config.NUMBER_OF_INTERVALS, lowerBound, upperBound);

        balancedPartitionToBeDeleted = null;

        pendingQueue = new LinkedBlockingQueue<>();

        try {
            fileSystem = new HdfsFileSystemHandler(config.dataChunkDir, config).getFileSystem();
        } catch (IOException e) {
            e.printStackTrace();
        }

        createQueryHandlingThread();

        schemaManager = new SchemaManager();
        schemaManager.setDefaultSchema(schema);
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");

//            System.out.println("start " + timeDomain.getStartTimestamp());
//            System.out.println("end" + timeDomain.getEndTimestamp());

            filePartitionSchemaManager.add(new FileMetaData(fileName, ((T) keyDomain.getLowerBound()).doubleValue(),
                    ((T) keyDomain.getUpperBound()).doubleValue(), timeDomain.getStartTimestamp(), timeDomain.getEndTimestamp()));

            DataChunkBloomFilters bloomFilters = (DataChunkBloomFilters)tuple.getValueByField("bloomFilters");


            for(String column: bloomFilters.columnToBloomFilter.keySet()) {
//                Map<String, BloomFilter> chunkNameToFilter = columnToChunkToBloomFilter.computeIfAbsent(column, t->
//                    new ConcurrentHashMap<>());
//                chunkNameToFilter.put(fileName, bloomFilters.columnToBloomFilter.get(column));
                try {
                    bloomFilterStore.store(new BloomFilterStore.BloomFilterId(fileName, column), bloomFilters.columnToBloomFilter.get(column));
                    System.out.println(String.format("A bloom filter is added for chunk: %s, column: %s", fileName, column));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else if (tuple.getSourceStreamId().equals(Streams.QueryFinishedStream)) {
            Long queryId = tuple.getLong(0);

//            Long start = queryIdToStartTime.get(queryId);
            Pair pair = queryIdToStartTime.get(queryId);


            if (pair != null) {
                Long start = (Long) pair.getKey();
                int fileSize = (Integer) pair.getValue();
//                LOG.info("query id " + queryId);
//                LOG.info("Query start time " + start);
//                LOG.info("Query end time " + System.currentTimeMillis());
//                LOG.info("Query time " + (System.currentTimeMillis() - start));
//                LOG.info("file size " + fileSize);
                queryIdToStartTime.remove(queryId);
            }

            System.out.println("query id " + queryId + " has been finished!!!");
            concurrentQueriesSemaphore.release();

        } else if (tuple.getSourceStreamId().equals(Streams.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();


            if (config.TASK_QUEUE_MODEL) {
                sendSubqueryToTask(taskId);
            } else {
                sendSubquery(taskId);
            }

        } else if (tuple.getSourceStreamId().equals(Streams.QueryGenerateStream)) {
            Query<T> query = (Query<T>) tuple.getValueByField("query");

            try {
                final List<Query<T>> queryList = new ArrayList();
                queryList.add(query);
                pendingQueue.put(queryList);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        } else if (tuple.getSourceStreamId().equals(Streams.TimestampUpdateStream)) {
            int taskId = tuple.getIntegerByField("taskId");
            TimeDomain timeDomain = (TimeDomain) tuple.getValueByField("timeDomain");
            KeyDomain keyDomain = (KeyDomain) tuple.getValueByField("keyDomain");
            Long endTimestamp = timeDomain.getEndTimestamp();

            indexTaskToTimestampMapping.put(taskId, endTimestamp);

            T keyRangeLowerBound = (T) keyDomain.getLowerBound();
            T keyRangeUpperBound = (T) keyDomain.getUpperBound();

            if (balancedPartitionToBeDeleted != null) {
                updateStaleBalancedPartition(keyRangeLowerBound, keyRangeUpperBound);
                if (isPartitionDeletable()) {
                    balancedPartitionToBeDeleted = null;
                    collector.emit(Streams.EnableRepartitionStream, new Values("Partition can be enabled!"));
                }
            }

            collector.emitDirect(taskId, Streams.TreeCleanStream, new Values(keyDomain, timeDomain));

        } else if (tuple.getSourceStreamId().equals(Streams.IntervalPartitionUpdateStream)) {
            BalancedPartition newBalancedPartition = (BalancedPartition) tuple.getValueByField("newIntervalPartition");
            balancedPartitionToBeDeleted = balancedPartition;
            balancedPartition = newBalancedPartition;
        } else if (tuple.getSourceStreamId().equals(Streams.PartialQueryResultDeliveryStream)) {
            long queryId = tuple.getLong(0);
            PartialQueryResult partialQueryResult = (PartialQueryResult) tuple.getValue(1);

            handlePartialQueryResult(queryId, partialQueryResult);

            // logic for handling results.
//            System.out.println("Received a partialResult for " + queryId + " " + partialQueryResult.dataTuples.size() + " elements.");
            if(!partialQueryResult.getEOFFlag()) {
                collector.emit(Streams.PartialQueryResultReceivedStream, new Values(queryId));
            } else {
//                System.out.println(String.format("All query results are collected for Query[%d] !!!!", queryId));
            }
        } else if (tuple.getSourceStreamId().equals(Streams.LocationInfoUpdateStream)) {
            LocationInfo info = (LocationInfo) tuple.getValue(0);
            switch (info.type) {
                case Ingestion:
                    ingestionServerIdToLocations.put(info.taskId, info.location);
                    break;
                case Query:
                    Set<Integer> taskIds = locationToChunkServerIds.computeIfAbsent(info.location, t -> new HashSet<>());
                    taskIds.add(info.taskId);
                    chunkServerIdToLocation.put(info.taskId, info.location);
                    break;
            }
            // TODO: handle location update logic, e.g., update a location from a value to a different value.
        }
    }

    public abstract void handlePartialQueryResult(Long queryId, PartialQueryResult partialQueryResult);

    private boolean isPartitionDeletable() {
        Map<Integer, Integer> staleIntervalIdToPartitionIdMapping = balancedPartitionToBeDeleted.getIntervalToPartitionMapping();
        Map<Integer, Integer> intervalIdToPartitionIdMapping = balancedPartition.getIntervalToPartitionMapping();

        return staleIntervalIdToPartitionIdMapping.equals(intervalIdToPartitionIdMapping);
    }

    private void updateStaleBalancedPartition(T keyRangeLowerBound, T keyRangeUpperBound) {
        Map<Integer, Integer> intervalIdToPartitionIdMapping = balancedPartition.getIntervalToPartitionMapping();
        Integer startIntervalId = balancedPartition.getIntervalId(keyRangeLowerBound);
        Integer endIntervalId = balancedPartition.getIntervalId(keyRangeUpperBound);

        for (Integer intervalId = startIntervalId; intervalId <= endIntervalId; ++intervalId) {
            balancedPartitionToBeDeleted.getIntervalToPartitionMapping().put(intervalId, intervalIdToPartitionIdMapping.get(intervalId));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.FileSystemQueryStream, new Fields("subquery", "tags"));

        outputFieldsDeclarer.declareStream(Streams.BPlusTreeQueryStream, new Fields("subquery"));

        outputFieldsDeclarer.declareStream(
                Streams.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(
                Streams.BPlusTreeQueryInformationStream,
                new Fields("queryId", "numberOfTasksToScan"));

        outputFieldsDeclarer.declareStream(Streams.TreeCleanStream, new Fields("keyDomain", "timeDomain"));

        outputFieldsDeclarer.declareStream(Streams.EnableRepartitionStream, new Fields("Repartition"));

        outputFieldsDeclarer.declareStream(Streams.PartialQueryResultReceivedStream, new Fields("queryId"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
        queryHandlingThread.interrupt();
    }

    private void handleQuery(List<Query<T>> querylist) {
        TimeMetrics metrics = new TimeMetrics();
        metrics.startEvent("generate 1");
        generateSubQueriesOnTheInsertionServer(querylist);
        metrics.endEvent("generate 1");
        metrics.startEvent("generate 2");
        generateSubqueriesOnTheFileScanner(querylist);
        metrics.endEvent("generate 2");
        System.out.println(metrics);
    }

    private void generateSubqueriesOnTheFileScanner(List<Query<T>> queryList) {
        Query<T> firstQuery = queryList.get(0);
        List<SubQueryOnFile<T>> subQueries = new ArrayList<>();
        int prunedCount = 0;
        for (Query<T> query: queryList) {
            Long queryId = query.getQueryId();
            T leftKey = query.leftKey;
            T rightKey = query.rightKey;
            Long startTimestamp = query.startTimestamp;
            Long endTimestamp = query.endTimestamp;

            final List<String> chunkNames = filePartitionSchemaManager.search(leftKey.doubleValue(), rightKey.doubleValue(), startTimestamp, endTimestamp);
            for (String chunkName: chunkNames) {
                boolean prunedByBloomFilter = false;
//
//                if (query.equivalentPredicate != null) {
//                    System.out.println("equivalentPredicate is passed.");
//                } else {
//                    System.out.println("equivalentPredicate is null.");
//                }

                if (query.equivalentPredicate != null) {
                    BloomFilterStore.BloomFilterId id = new BloomFilterStore.BloomFilterId(chunkName, query.equivalentPredicate.column);
                    long start = System.currentTimeMillis();
                    BloomFilter filter = bloomFilterStore.get(id);
                    System.out.println(String.format("bloom filter fetch time: %d ms.", System.currentTimeMillis() - start));
                    if (filter != null) {
                        prunedByBloomFilter = !bloomFilterStore.get(id).mightContain(query.equivalentPredicate.value);
                    }
                }

//                if (query.equivalentPredicate != null && columnToChunkToBloomFilter.containsKey(query.equivalentPredicate.column)) {
//                    BloomFilter bloomFilter = columnToChunkToBloomFilter.get(query.equivalentPredicate.column).get(chunkName);
//                    if (bloomFilter != null && !bloomFilter.mightContain(query.equivalentPredicate.value)) {
//                        prunedByBloomFilter = true;
//                    }
////                    else {
////                        System.out.println("Failed to prune by bloom filter.");
////                    }
//                }

                if (!prunedByBloomFilter) {
                    subQueries.add(new SubQueryOnFile<>(queryId, leftKey, rightKey, chunkName, startTimestamp, endTimestamp,
                            query.predicate, query.aggregator, query.sorter));
                } else {
                    prunedCount ++;
                }
            }
        }

        System.out.println(String.format("%s out of %s queries have been pruned by bloom filter.", prunedCount,
                prunedCount + subQueries.size()));



        if (subQueries.size() <= 0) {
            System.out.println(String.format("%d subqueries on chunks.", subQueries.size()));
            collector.emit(Streams.FileSystemQueryInformationStream, new Values(firstQuery, 0));
        } else {

            System.out.println(String.format("%d subqueries on chunks are generated!", subQueries.size()));


            int numberOfSubqueryBeforeMerging = subQueries.size();
            subQueries = mergeSubqueriesOnFiles(subQueries);
            int numberOfSubqueryAfterMerging = subQueries.size();

            System.out.println(String.format("%d subqueries have been merged into %d subqueires",
                    numberOfSubqueryBeforeMerging, numberOfSubqueryAfterMerging));

            for (SubQueryOnFile subQuery: subQueries) {
                unprocessedSubqueries.add(subQuery.getFileName());

                if (config.SHUFFLE_GROUPING_FLAG) {
                    sendSubqueriesByshuffleGrouping(subQuery);
                } else if (config.TASK_QUEUE_MODEL) {
                    try {
                        taskQueue.put(subQuery);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    putSubqueryToTaskQueues(subQuery);
                }

            }

            if (config.TASK_QUEUE_MODEL) {
                sendSubqueriesFromTaskQueue();
            } else {
                sendSubqueriesFromTaskQueues();
            }

            /* task queue
            sendSubqueriesFromTaskQueue();
            */

//            /*our method
//                sendSubqueriesFromTaskQueues();
//            */

            collector.emit(Streams.FileSystemQueryInformationStream, new Values(firstQuery, subQueries.size()));
        }
        System.out.println(firstQuery.queryId + " " + subQueries.size());
        queryIdToStartTime.put(firstQuery.queryId, new Pair<>(System.currentTimeMillis(), subQueries.size()));
    }

    List<SubQueryOnFile<T>> mergeSubqueriesOnFiles(List<SubQueryOnFile<T>> subqueries) {
        final DataSchema localSchema = this.schema.duplicate();
        Map<String, List<SubQueryOnFile<T>>> fileNameToSubqueries = new HashMap<>();
        subqueries.stream().forEach(t -> {
            List<SubQueryOnFile<T>> list = fileNameToSubqueries.computeIfAbsent(t.getFileName(), x -> new ArrayList<>());
            list.add(t);
        });

        List<SubQueryOnFile<T>> ret = new ArrayList<>();

        fileNameToSubqueries.forEach((file, queries) -> {
            if (queries.size() > 0) {
                SubQueryOnFile<T> firstQuery = queries.get(0);
                T leftMost = firstQuery.leftKey;
                T rightMost = firstQuery.rightKey;
                DataTuplePredicate predicate = firstQuery.predicate;
                if (predicate == null) {
                    predicate = t -> true;
                }

                DataTuplePredicate additionalPredicate = t -> true;
                for (int i = 1; i < queries.size(); i++) {
                    SubQueryOnFile<T> currentQuery = queries.get(i);
                    leftMost = leftMost.compareTo(currentQuery.leftKey) < 0 ? leftMost : currentQuery.leftKey;
                    rightMost = rightMost.compareTo(currentQuery.rightKey) > 0 ? rightMost : currentQuery.rightKey;
                    DataTuplePredicate finalAdditionalPredicate1 = additionalPredicate;
                    additionalPredicate = t -> finalAdditionalPredicate1.test(t) ||
                            (currentQuery.leftKey.compareTo((T)localSchema.getIndexValue(t)) <= 0 &&
                            currentQuery.rightKey.compareTo((T)localSchema.getIndexValue(t)) >= 0);
                }
                DataTuplePredicate finalAdditionalPredicate = additionalPredicate;
                DataTuplePredicate finalPredicate = predicate;

                DataTuplePredicate composedPredicate = t -> finalPredicate.test(t) && finalAdditionalPredicate.test(t);

                SubQueryOnFile<T> subQuery = new SubQueryOnFile<>(firstQuery.queryId, leftMost,
                        rightMost, firstQuery.getFileName(), firstQuery.startTimestamp,
                        firstQuery.endTimestamp, composedPredicate, firstQuery.aggregator,
                        firstQuery.sorter);
                ret.add(subQuery);
            }
        });

        return ret;
    }

    private Map<Integer, SubQuery<T>> mergeSubqueriesOnInsertionServers(Map<Integer, List<SubQuery<T>>> insertionServerIdToSubqueries) {
        final DataSchema localSchema = this.schema.duplicate();
        Map<Integer, SubQuery<T>> insertionServerIdToCompactedSubquery = new HashMap<>();
        insertionServerIdToSubqueries.forEach((id, subQueries) -> {
            if (subQueries.size() > 0) {
                SubQuery<T> first = subQueries.get(0);
                T leftmostKey = first.leftKey;
                T rightmostKey = first.rightKey;
                DataTuplePredicate predicate = first.predicate;
                if (predicate == null) {
                    predicate = t -> true;
                }

                DataTuplePredicate additionalPredicate = t -> true;
                for (int i = 0; i < subQueries.size(); i++) {
                    SubQuery<T> currentSubquery = subQueries.get(i);
                    leftmostKey = leftmostKey.compareTo(currentSubquery.leftKey) < 0 ? leftmostKey : currentSubquery.leftKey;
                    rightmostKey = rightmostKey.compareTo(currentSubquery.rightKey) > 0 ? rightmostKey : currentSubquery.rightKey;
                    DataTuplePredicate finalAdditionalPredicate1 = additionalPredicate;
                    additionalPredicate = t -> finalAdditionalPredicate1.test(t) ||
                            (currentSubquery.leftKey.compareTo((T)localSchema.getIndexValue(t)) <= 0 &&
                                    currentSubquery.rightKey.compareTo((T)localSchema.getIndexValue(t)) >=0);
                }

                DataTuplePredicate finalAddtionalPredicate = additionalPredicate;
                DataTuplePredicate finalPredicate = predicate;

                DataTuplePredicate composedPredicate = t -> finalPredicate.test(t) && finalAddtionalPredicate.test(t);

                SubQuery<T> subquery = new SubQuery<T>(first.queryId,leftmostKey, rightmostKey, first.startTimestamp,
                        first.endTimestamp, composedPredicate, first.aggregator, first.sorter);
                insertionServerIdToCompactedSubquery.put(id, subquery);
            }
        });
        return insertionServerIdToCompactedSubquery;
    }

    private void createQueryHandlingThread() {
        queryHandlingThread = new Thread(new Runnable() {
            @Override
            public void run() {
//                while (true) {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        List<Query<T>> queryList = pendingQueue.take();
//                        Query query = pendingQueue.take();
                        concurrentQueriesSemaphore.acquire();
                        handleQuery(queryList);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        queryHandlingThread.start();
    }


    private void createTaskQueues(List<Integer> targetTasks) {
        for (Integer taskId : targetTasks) {
            PriorityBlockingQueue<SubQueryOnFileWithPriority> taskQueue = new PriorityBlockingQueue<>(config.TASK_QUEUE_CAPACITY);
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }


    private void generateSubQueriesOnTheInsertionServer(List<Query<T>> queryList) {
        int numberOfTasksToSearch = 0;
        Query firstQuery = queryList.get(0);

        Map<Integer, List<SubQuery<T>>> insertionSeverIdToSubqueries = new HashMap<>();

//        System.out.println("Decompose subquery for " + queryList.get(0).queryId + "(on insertion servers)");
        for(Query<T> query: queryList) {

            Long queryId = query.queryId;

            T leftKey = query.leftKey;
            T rightKey = query.rightKey;
            Long startTimestamp = query.startTimestamp;
            Long endTimestamp = query.endTimestamp;


            List<Integer> partitionIdsInBalancedPartition = getPartitionIds(balancedPartition, leftKey, rightKey);
            List<Integer> partitionIdsInStalePartition = null;


            if (balancedPartitionToBeDeleted != null) {
                partitionIdsInStalePartition = getPartitionIds(balancedPartitionToBeDeleted, leftKey, rightKey);
            }

            List<Integer> partitionIds = (partitionIdsInStalePartition == null
                    ? partitionIdsInBalancedPartition
                    : mergePartitionIds(partitionIdsInBalancedPartition, partitionIdsInStalePartition));

            for (Integer partitionId : partitionIds) {
                Integer taskId = indexServers.get(partitionId);
                Long timestamp = indexTaskToTimestampMapping.get(taskId);
                if ((timestamp <= endTimestamp && timestamp >= startTimestamp) || (timestamp <= startTimestamp)) {
                    SubQuery<T> subQuery = new SubQuery<>(query.getQueryId(), query.leftKey, query.rightKey, query.startTimestamp,
                            query.endTimestamp, query.predicate, query.aggregator, query.sorter);
                    List<SubQuery<T>> subQueries = insertionSeverIdToSubqueries.computeIfAbsent(taskId, t ->new ArrayList<>());
                    subQueries.add(subQuery);
//                    collector.emitDirect(taskId, Streams.BPlusTreeQueryStream, new Values(subQuery));
                    ++numberOfTasksToSearch;
                }
            }
        }

        Map<Integer, SubQuery<T>> insertionServerIdToCompactedSubquery = mergeSubqueriesOnInsertionServers(insertionSeverIdToSubqueries);

        System.out.println(String.format("%d subqueries on B+tree are compacted into %d", numberOfTasksToSearch,
                insertionServerIdToCompactedSubquery.size()));

        for(Integer taskId: insertionServerIdToCompactedSubquery.keySet()) {
            collector.emitDirect(taskId, Streams.BPlusTreeQueryStream, new Values(insertionServerIdToCompactedSubquery.get(taskId)));
        }

        collector.emit(Streams.BPlusTreeQueryInformationStream, new Values(firstQuery.queryId, insertionServerIdToCompactedSubquery.size()));
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

    private List<Integer> getPartitionIds(BalancedPartition balancedPartition, T leftKey, T rightKey) {
        Integer startPartitionId = balancedPartition.getPartitionId(leftKey);
        Integer endPartitionId = balancedPartition.getPartitionId(rightKey);

        List<Integer> partitionIds = new ArrayList<>();
        for (Integer partitionId = startPartitionId; partitionId <= endPartitionId; ++partitionId) {
            partitionIds.add(partitionId);
        }

        return partitionIds;
    }

    private Integer extractInsertionServerId(String fileName) {
        Pattern pattern = Pattern.compile("taskId(\\d+)");
        Matcher matcher = pattern.matcher(fileName);
        if (matcher.find()) {
            String taskid = matcher.group(1);
            return Integer.parseInt(taskid);
        } else {
            System.err.println("Fail to extract task id from " + fileName);
            return null;
        }
    }

    private int getPreferredLocationByHashing(String fileName) {
        int index = Math.abs(fileName.hashCode()) % queryServers.size();
        return queryServers.get(index);
    }

    private List<Integer> getPreferredQueryServerIds(String fileName) {
        List<Integer> ids = new ArrayList<>();
        if (config.HybridStorage) {
            Integer insertionServerId = extractInsertionServerId(fileName);
            if (insertionServerId == null) {
                System.out.println("Fail to extract insertion server id from chunk name," +
                        " using hashing dispatch instead.");
                ids.add(getPreferredLocationByHashing(fileName));
                return ids;
            }

            String location = ingestionServerIdToLocations.get(insertionServerId);
            if (location == null) {
                System.out.println("ingestionServerIdToLocations info is not available," +
                        " using hashing dispatch instead.");
                ids.add(getPreferredLocationByHashing(fileName));
                return ids;
            }
            Set<Integer> candidates = locationToChunkServerIds.get(location);

            if (candidates == null || candidates.size() == 0) {
                System.out.println("locationToChunkServerIds info is not available," +
                        " using hashing dispatch instead.");
                ids.add(getPreferredLocationByHashing(fileName));
                return ids;
            }

            int randomIndex = new Random().nextInt(candidates.size());

//            System.out.println("Select Task " + new ArrayList<>(candidates).get(randomIndex) + " among " + candidates.size() + " as the target query server. ");
            ids.addAll(candidates);
            return ids;
        } else if (config.HDFSFlag && config.HdfsTaskLocality) {
            Set<Integer> candidates = new HashSet<>();
            String[] locations = new String[0];
            if (chunkNameToPreferredHostsMapping.containsKey(fileName)) {
                locations = chunkNameToPreferredHostsMapping.get(fileName);
            } else {


                Path path = new Path(config.dataChunkDir + "/" + fileName);
                Long fileLength = 0L;
                try {
                    fileLength = fileSystem.getFileStatus(path).getLen();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                BlockLocation[] blockLocations;
//            String location = null;
                try {
                    blockLocations = fileSystem.getFileBlockLocations(path, 0, fileLength);
                    BlockLocation blockLocation = blockLocations[new Random().nextInt(blockLocations.length)];
                    locations = blockLocation.getHosts();
                    chunkNameToPreferredHostsMapping.put(fileName, locations);
//                int randomIndex = new Random().nextInt(locations.length);
//                location = locations[randomIndex];
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            for (String location: locations) {
                Set<Integer> serverIds = locationToChunkServerIds.get(location);
                if (serverIds != null)
                    candidates.addAll(serverIds);
            }
            if (candidates.size() == 0) {
                System.out.println("locationToChunkServerIds info is not available," +
                        " using hashing dispatch instead.");
                ids.add(getPreferredLocationByHashing(fileName));
                return ids;
            }

            ids.addAll(candidates);
            return ids;
        } else {
            int index = Math.abs(fileName.hashCode()) % queryServers.size();
            ids.add(queryServers.get(index));
            return ids;
        }
    }



    private void putSubqueryToTaskQueues(SubQueryOnFile subQuery) {
        String fileName = subQuery.getFileName();

        List<Integer> taskIds = getPreferredQueryServerIds(fileName);

        Collections.sort(taskIds);
        Collections.shuffle(taskIds, new Random(subQuery.getFileName().hashCode()));

        for(int i = 0; i < taskIds.size(); i++) {
            PriorityBlockingQueue<SubQueryOnFileWithPriority> taskQueue = taskIdToTaskQueue.get(taskIds.get(i));
                taskQueue.put(new SubQueryOnFileWithPriority(subQuery,i));
        }
    }


    private void sendSubqueriesFromTaskQueue() {
        for (Integer taskId : queryServers) {
            SubQueryOnFile subQuery = taskQueue.poll();
            if (subQuery != null) {
                unprocessedSubqueries.remove(subQuery.getFileName());
                collector.emitDirect(taskId, Streams.FileSystemQueryStream, new Values(subQuery, new Tags()));
            }
        }
    }

    private void sendSubqueriesFromTaskQueues() {
        for (Integer taskId : queryServers) {
            sendSubquery(taskId);
        }
    }


    private void sendSubqueriesByshuffleGrouping(SubQuery subQuery) {
        collector.emit(Streams.FileSystemQueryStream, new Values(subQuery, new Tags()));
    }


    private void sendSubqueryToTask(int taskId) {
        SubQueryOnFile subQuery = taskQueue.poll();

        if (subQuery != null) {
            unprocessedSubqueries.remove(subQuery.getFileName());
            collector.emitDirect(taskId, Streams.FileSystemQueryStream, new Values(subQuery, new Tags()));
        }
    }



    private void sendSubquery(int taskId) {
        SubQueryOnFileWithPriority subQuery = null;

        Boolean preferred = true; // indicate if the subquery is assigned to a preferred query server
        while (subQuery == null) {

            PriorityBlockingQueue<SubQueryOnFileWithPriority> taskQueue = taskIdToTaskQueue.get(taskId);
            subQuery = taskQueue.poll();

            if (subQuery == null) {
                preferred = false;
                taskQueue = getLongestQueue();
                if (taskQueue.size() > 0)
                    subQuery = taskQueue.poll();
                else
                    break;

            }
            if (!unprocessedSubqueries.contains(subQuery.getFileName()))
                subQuery = null;
        }
        if (subQuery != null) {
            Tags tags = new Tags();
            tags.setTag("locality", preferred.toString());
            unprocessedSubqueries.remove(subQuery.getFileName());
            collector.emitDirect(taskId, Streams.FileSystemQueryStream
                    , new Values(subQuery, tags));
            System.out.println(String.format("subquery %d is sent to %s for %s", subQuery.queryId,
                    chunkServerIdToLocation.get(taskId), subQuery.getFileName()));
        }

    }

    private PriorityBlockingQueue<SubQueryOnFileWithPriority> getLongestQueue() {

        List<PriorityBlockingQueue<SubQueryOnFileWithPriority>> taskQueues = new ArrayList<>();
        taskQueues.addAll(taskIdToTaskQueue.values());

        Collections.sort(taskQueues, (taskQueue1, taskQueue2) -> Integer.compare(taskQueue2.size(), taskQueue1.size()));

        PriorityBlockingQueue<SubQueryOnFileWithPriority> taskQueue = taskQueues.get(0);

        return taskQueue;
    }

    private void setTimestamps() {
        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexServers) {
//            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
            indexTaskToTimestampMapping.put(taskId, 0L);
        }
    }

}
