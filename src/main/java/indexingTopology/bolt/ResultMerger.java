package indexingTopology.bolt;

import indexingTopology.aggregator.Aggregator;
import indexingTopology.common.data.PartialQueryResult;
import indexingTopology.util.Query;
import indexingTopology.util.SubQuery;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.common.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.FileScanMetrics;

import java.util.*;

/**
 * Created by acelzj on 11/9/16.
 */
public class ResultMerger extends BaseRichBolt {

    Map<Long, Integer> queryIdToNumberOfTuples;

    Map<Long, Integer> queryIdToCounter;

    Map<Long, Integer> queryIdToNumberOfQueriesOnFileFinished;

    Map<Long, Integer> queryIdToNumberOfFilesToScan;

    Map<Long, Integer> queryIdToNumberOfTasksToSearch;

    Map<Long, FileScanMetrics> queryIdToFileScanMetrics;

    Map<Long, List<PartialQueryResult>> queryIdToPartialQueryResults;

    Map<Long, Map<Integer, List<FileScanMetrics>>> queryIdToTaskIdToTimeMapping;

    DataSchema schema;

    OutputCollector collector;

    public ResultMerger(DataSchema schema) {
        this.schema = schema;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        queryIdToNumberOfTuples = new HashMap<Long, Integer>();
        queryIdToCounter = new HashMap<Long, Integer>();
        queryIdToNumberOfFilesToScan = new HashMap<Long, Integer>();
        queryIdToNumberOfTasksToSearch = new HashMap<Long, Integer>();

        queryIdToFileScanMetrics = new HashMap<Long, FileScanMetrics>();

        queryIdToNumberOfQueriesOnFileFinished = new HashMap<>();

        queryIdToTaskIdToTimeMapping = new HashMap<>();
        queryIdToPartialQueryResults = new HashMap<>();

        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId()
                .equals(Streams.BPlusTreeQueryInformationStream)) {
            int numberOfTasksToSearch = tuple.getInteger(1);
            Long queryId = tuple.getLong(0);

//            System.out.println("queryId " + queryId + " number of tasks to search " + numberOfTasksToSearch);
            queryIdToNumberOfTasksToSearch.put(queryId, numberOfTasksToSearch);

            if (isQueryFinished(queryId)) {
//                printTimeInformation(queryId);
                sendNewQueryPermit(queryId);
//                printTimeInformation(queryId);
                removeQueryIdFromMappings(queryId);
            }

        } else if (tuple.getSourceStreamId()
                .equals(Streams.FileSystemQueryInformationStream)) {
            int numberOfFilesToScan = tuple.getInteger(1);
            Query query = (Query) tuple.getValue(0);
            Long queryId = query.queryId;
//            System.out.println("queryId " + queryId + " number of files to scan " + numberOfFilesToScan);
            queryIdToNumberOfFilesToScan.put(queryId, numberOfFilesToScan);

            // It is possible in a rare case where the query information arrives later than the query result.
            if (isQueryFinished(queryId)) {
//                printTimeInformation(queryId);
                finalizeQuery(query);
            }

        } else if (tuple.getSourceStreamId().equals(Streams.BPlusTreeQueryStream) ||
                tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {
            SubQuery subQuery = (SubQuery)tuple.getValue(0);
            long queryId = subQuery.getQueryId();
//            System.out.println(String.format("A subquery for Query[%d] is completed!", queryId));

            Integer counter = queryIdToCounter.getOrDefault(queryId, 0);
            counter++;
            queryIdToCounter.put(queryId, counter);

//            if (tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {
//                for (int i = 0; i < serializedTuples.size(); ++i) {
//                    DataTuple dataTuple = schema.deserializeToDataTuple(serializedTuples.get(i));
//                    System.out.println(dataTuple);
//                    System.out.println("tuples in query id " + queryId + " " + tuple.getSourceStreamId());
//                }
//            }
            if (tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {
                Integer fileCounter = queryIdToNumberOfQueriesOnFileFinished.getOrDefault(queryId, 0);
                fileCounter++;
                queryIdToNumberOfQueriesOnFileFinished.put(queryId, fileCounter);

                int taskId = tuple.getSourceTask();
                FileScanMetrics fileScanMetrics = (FileScanMetrics) tuple.getValueByField("metrics");
//                        System.out.println(queryId + "has been finished");


                System.out.println("Subquery on file debug: " + fileScanMetrics.debugInfo);

                if (queryIdToTaskIdToTimeMapping.get(queryId) == null) {
                    Map<Integer, List<FileScanMetrics>> taskIdToTimeMapping = new HashMap<>();
                    List<FileScanMetrics> time = new ArrayList<>();
                    time.add(fileScanMetrics);
                    taskIdToTimeMapping.put(taskId, time);
                    queryIdToTaskIdToTimeMapping.put(queryId, taskIdToTimeMapping);
                } else {
                    Map<Integer, List<FileScanMetrics>> taskIdToMetricsMapping = queryIdToTaskIdToTimeMapping.get(queryId);
                    if (taskIdToMetricsMapping.get(taskId) == null) {
                        List<FileScanMetrics> time = new ArrayList<>();
                        time.add(fileScanMetrics);
                        taskIdToMetricsMapping.put(taskId, time);
                        queryIdToTaskIdToTimeMapping.put(queryId, taskIdToMetricsMapping);
                    } else {
                        List<FileScanMetrics> time = taskIdToMetricsMapping.get(taskId);
                        time.add(fileScanMetrics);
                        taskIdToMetricsMapping.put(taskId, time);
                        queryIdToTaskIdToTimeMapping.put(queryId, taskIdToMetricsMapping);
                    }

                }


                collector.emitDirect(taskId, Streams.SubQueryReceivedStream, new Values("received"));
            }

            ArrayList<byte[]> serializedTuples = (ArrayList) tuple.getValue(1);

            handleNewPartialQueryResult(subQuery, serializedTuples);

            if (isQueryFinished(queryId)) {
//                System.out.println(tuple.getSourceStreamId());
//                printTimeInformation(queryId);
                finalizeQuery(subQuery);
            }

        } else if (tuple.getSourceStreamId().equals(Streams.PartialQueryResultReceivedStream)) {
            long queryId = tuple.getLong(0);
            sendAPartialQueryResult(queryId);
        }
    }

    private void handleNewPartialQueryResult(SubQuery subQuery, ArrayList<byte[]> serializedTuples) {
        long queryId = subQuery.getQueryId();
        PartialQueryResult partialQueryResult = new PartialQueryResult(Integer.MAX_VALUE);
        if (subQuery.getAggregator() != null) {
            final DataSchema inputSchema = subQuery.getAggregator().getOutputDataSchema();
            serializedTuples.forEach(r -> partialQueryResult.add(inputSchema.deserializeToDataTuple(r)));
        } else {
            serializedTuples.forEach(r -> partialQueryResult.add(schema.deserializeToDataTuple(r)));
        }

        List<PartialQueryResult> results = queryIdToPartialQueryResults.computeIfAbsent(queryId, k->new ArrayList<>());
        results.add(partialQueryResult);

        Integer numberOfTuples = queryIdToNumberOfTuples.getOrDefault(queryId, 0);
        numberOfTuples += serializedTuples.size();
        queryIdToNumberOfTuples.put(queryId, numberOfTuples);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream(NormalDistributionTopology.NewQueryStream
//                , new Fields("queryId", "New Query"));

        outputFieldsDeclarer.declareStream(Streams.QueryFinishedStream
                , new Fields("queryId", "New Query", "metrics", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(Streams.SubQueryReceivedStream, new Fields("receivedMessage"));

        outputFieldsDeclarer.declareStream(Streams.PartialQueryResultDeliveryStream, new Fields("queryId", "result"));
    }

    private boolean isQueryFinished(Long queryId) {

//        System.out.println(String.format("query: %d, numberOfFilesToScan: %s -> %s, B+ tree to scan: %s, Count: %s", queryId,
//                queryIdToNumberOfFilesToScan.get(queryId),
//                queryIdToNumberOfQueriesOnFileFinished.get(queryId),
//                queryIdToNumberOfTasksToSearch.get(queryId),
//                queryIdToCounter.get(queryId)));
        if (queryIdToNumberOfFilesToScan.get(queryId) != null &&
                queryIdToNumberOfTasksToSearch.get(queryId) != null) {
            int numberOfFilesToScan = queryIdToNumberOfFilesToScan.get(queryId);
            int tasksToSearch = queryIdToNumberOfTasksToSearch.get(queryId);
            if (numberOfFilesToScan == 0 && tasksToSearch == 0) {
                return true;
            } else if (queryIdToCounter.get(queryId) != null) {
                int counter = queryIdToCounter.get(queryId);
                return numberOfFilesToScan + tasksToSearch == counter;
            }
        }
        return false;
    }


    private void finalizeQuery(SubQuery subQuery) {
        final Long queryId = subQuery.getQueryId();
        mergeQueryResults(subQuery);
        sendAPartialQueryResult(queryId);
        sendNewQueryPermit(queryId);
        removeQueryIdFromMappings(queryId);
    }

    private void sendAPartialQueryResult(Long queryId) {
        List<PartialQueryResult> results = queryIdToPartialQueryResults.get(queryId);
        if (results != null && !results.isEmpty()) {
            PartialQueryResult result = results.get(0);
            if (results.size() == 0) {
                result.setEOFflag();
            }
//            System.out.println("A partial query result is sent to coordinator from merger.");
            collector.emit(Streams.PartialQueryResultDeliveryStream, new Values(queryId, result));
            results.remove(0);
            if (results.size() == 0) {
                queryIdToPartialQueryResults.remove(queryId);
            }
        } else {
            PartialQueryResult result = new PartialQueryResult();
            result.setEOFflag();
//            System.out.println("A empty partial query result is sent to coordinator from merger.");
            collector.emit(Streams.PartialQueryResultDeliveryStream, new Values(queryId, result));
        }

    }

    private void mergeQueryResults(SubQuery subQuery) {
        // This is where aggregation happens.

        final long queryId = subQuery.getQueryId();

        final int unitSize = 4 * 1024;

        //we should initialize the PartialQueryResult in case that there is no valid subquery.
        List<PartialQueryResult> queryResults = queryIdToPartialQueryResults.computeIfAbsent(queryId, t -> new ArrayList<>());

        PartialQueryResult allResults = new PartialQueryResult(Integer.MAX_VALUE);

        List<PartialQueryResult> compactedResults = null;

        // perform aggregation if applicable.
        if (subQuery.getAggregator() != null) {
            Aggregator globalAggregator = subQuery.getAggregator().generateGlobalAggregator();
            Aggregator.IntermediateResult intermediateResult = globalAggregator.createIntermediateResult();
            queryResults.forEach(r -> globalAggregator.aggregate(r.dataTuples, intermediateResult));
            allResults.dataTuples.addAll(globalAggregator.getResults(intermediateResult).dataTuples);
        } else {
            queryResults.stream().forEach(t -> allResults.dataTuples.addAll(t.dataTuples));
        }

        // sort if applicable.
        if (subQuery.sorter != null) {
            allResults.dataTuples.sort(subQuery.sorter);
//            System.out.println("Sort is applied!! ##########");
        }

        // compact results into groups with bounded size.
        compactedResults = PartialQueryResult.Compact(allResults, unitSize);
        queryIdToPartialQueryResults.put(queryId, compactedResults);

    }

    private void sendNewQueryPermit(Long queryId) {
        collector.emit(Streams.QueryFinishedStream, new Values(queryId, "New query can be executed",
                        null, 0));
    }

    private void removeQueryIdFromMappings(Long queryId) {
        queryIdToCounter.remove(queryId);
        queryIdToNumberOfFilesToScan.remove(queryId);
        queryIdToNumberOfTasksToSearch.remove(queryId);
        queryIdToFileScanMetrics.remove(queryId);
        queryIdToNumberOfQueriesOnFileFinished.remove(queryId);
    }

    private void putFileScanMetrics(Long queryId, FileScanMetrics metrics) {
        FileScanMetrics fileScanMetrics = queryIdToFileScanMetrics.get(queryId);
        if (fileScanMetrics == null) {
            queryIdToFileScanMetrics.put(queryId, metrics);
        } else {
//            fileScanMetrics.addWithAnotherMetrics(metrics);
            queryIdToFileScanMetrics.put(queryId, fileScanMetrics);
        }
    }


    private void printTimeInformation(Long queryId) {
        Map<Integer, List<FileScanMetrics>> taskIdToTimeMapping = queryIdToTaskIdToTimeMapping.get(queryId);
//        if (queryId < 10) {
        Long totalQueryTime = 0L;

        int numberOfSubqueries = 0;
            if (taskIdToTimeMapping != null) {
//            System.out.println("query id " + queryId + "has been finished ");
                Set<Integer> taskIds = taskIdToTimeMapping.keySet();
//                Long numberOfRecords = 0L;
//                Long totalTime = 0L;
//                for (Integer taskId : taskIds) {
//                    List<FileScanMetrics> records = taskIdToTimeMapping.get(taskId);
//                    for (FileScanMetrics fileScanMetrics : records) {
//                        numberOfRecords += fileScanMetrics.getNumberOfRecords();
//                        totalTime += fileScanMetrics.getTotalTime();
//                    }

                Long keyRangTime = 0L;
                Long timestampRangTime = 0L;
                Long predicationTime = 0L;
                Long aggregationTime = 0L;
                Long fileReadingTime = 0L;

                for (Integer taskId : taskIds) {
                    List<FileScanMetrics> records = taskIdToTimeMapping.get(taskId);
                    for (FileScanMetrics fileScanMetrics : records) {
                        totalQueryTime += fileScanMetrics.getTotalTime();
                        keyRangTime += fileScanMetrics.getKeyRangeTime();
                        timestampRangTime += fileScanMetrics.getTimestampRangeTime();
                        predicationTime += fileScanMetrics.getPredicationTime();
                        aggregationTime += fileScanMetrics.getAggregationTime();
                        fileReadingTime += fileScanMetrics.getFileReadingTime();
                        ++numberOfSubqueries;
                    }

                    /*
                    if (records != null) {
                        Long time = 0L;
                        for (FileScanMetrics fileScanMetrics : records) {
                            System.out.println(fileScanMetrics);
                            time += fileScanMetrics.getTotalTime();
                        }
                        System.out.println("task id " + taskId + " " + time);
                        System.out.println("file size " + records.size());
                    }
                    */
                }

//                System.out.println("Query time " + (totalQueryTime / numberOfSubqueries));
                System.out.println("key range " + keyRangTime);
                System.out.println("timestamp range " + timestampRangTime);
                System.out.println("predicate " + predicationTime);
                System.out.println("aggregation " + aggregationTime);
                System.out.println("file reading " + fileReadingTime);
                System.out.println("total " + totalQueryTime);
//            }
//                System.out.println("query id " + queryId + " " + numberOfRecords);
//            }
        }

        queryIdToTaskIdToTimeMapping.remove(queryId);
    }

}
