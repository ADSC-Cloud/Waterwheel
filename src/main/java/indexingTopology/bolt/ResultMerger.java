package indexingTopology.bolt;

import indexingTopology.data.PartialQueryResult;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import indexingTopology.data.DataSchema;
import indexingTopology.streams.Streams;
import indexingTopology.util.FileScanMetrics;

import java.util.*;

/**
 * Created by acelzj on 11/9/16.
 */
public class ResultMerger extends BaseRichBolt {

    Map<Long, Integer> queryIdToNumberOfTuples;

    Map<Long, Integer> queryIdToCounter;

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

        queryIdToTaskIdToTimeMapping = new HashMap<>();
        queryIdToPartialQueryResults = new HashMap<>();

        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId()
                .equals(Streams.BPlusTreeQueryInformationStream)) {
            int numberOfTasksToSearch = tuple.getInteger(1);
            Long queryId = tuple.getLong(0);

//            System.out.println("queryId" + queryId + " number of tasks to search " + numberOfTasksToSearch);
            queryIdToNumberOfTasksToSearch.put(queryId, numberOfTasksToSearch);

            if (isQueryFinished(queryId)) {
//                printTimeInformation(queryId);
                sendNewQueryPermit(queryId);
                removeQueryIdFromMappings(queryId);
            }

        } else if (tuple.getSourceStreamId()
                .equals(Streams.FileSystemQueryInformationStream)) {
            int numberOfFilesToScan = tuple.getInteger(1);
            Long queryId = tuple.getLong(0);
//            System.out.println("queryId" + queryId + " number of files to scan " + numberOfFilesToScan);
            queryIdToNumberOfFilesToScan.put(queryId, numberOfFilesToScan);

            // It is possible in a rare case where the query information arrives later than the query result.
            if (isQueryFinished(queryId)) {
//                printTimeInformation(queryId);
                finalizeQuery(queryId);
            }

        } else if (tuple.getSourceStreamId().equals(Streams.BPlusTreeQueryStream) ||
                tuple.getSourceStreamId().equals(Streams.FileSystemQueryStream)) {
            long queryId = tuple.getLong(0);
            System.out.println(String.format("A subquery for Query[%d] is completed!", queryId));

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
                int taskId = tuple.getSourceTask();
                FileScanMetrics fileScanMetrics = (FileScanMetrics) tuple.getValueByField("metrics");
//                        System.out.println(queryId + "has been finished");
//                if (queryIdToTaskIdToTimeMapping.get(queryId) == null) {
//                    Map<Integer, List<FileScanMetrics>> taskIdToTimeMapping = new HashMap<>();
//                    List<FileScanMetrics> time = new ArrayList<>();
//                    time.add(fileScanMetrics);
//                    taskIdToTimeMapping.put(taskId, time);
//                    queryIdToTaskIdToTimeMapping.put(queryId, taskIdToTimeMapping);
//                } else {
//                    Map<Integer, List<FileScanMetrics>> taskIdToMetricsMapping = queryIdToTaskIdToTimeMapping.get(queryId);
//                    if (taskIdToMetricsMapping.get(taskId) == null) {
//                        List<FileScanMetrics> time = new ArrayList<>();
//                        time.add(fileScanMetrics);
//                        taskIdToMetricsMapping.put(taskId, time);
//                        queryIdToTaskIdToTimeMapping.put(queryId, taskIdToMetricsMapping);
//                    } else {
//                        List<FileScanMetrics> time = taskIdToMetricsMapping.get(taskId);
//                        time.add(fileScanMetrics);
//                        taskIdToMetricsMapping.put(taskId, time);
//                        queryIdToTaskIdToTimeMapping.put(queryId, taskIdToMetricsMapping);
//                    }
//                }
                collector.emitDirect(taskId, Streams.SubQueryReceivedStream, new Values("received"));
            }

            ArrayList<byte[]> serializedTuples = (ArrayList) tuple.getValue(1);

            handleNewPartialQueryResult(queryId, serializedTuples);

            if (isQueryFinished(queryId)) {
//                System.out.println(tuple.getSourceStreamId());
//                printTimeInformation(queryId);
                finalizeQuery(queryId);
            }

        } else if (tuple.getSourceStreamId().equals(Streams.PartialQueryResultReceivedStream)) {
            long queryId = tuple.getLong(0);
            sendAPartialQueryResult(queryId);
        }
    }

    private void handleNewPartialQueryResult(Long queryId, ArrayList<byte[]> serializedTuples) {
        PartialQueryResult partialQueryResult = new PartialQueryResult(Integer.MAX_VALUE);
        serializedTuples.forEach(r -> partialQueryResult.add(schema.deserializeToDataTuple(r)));

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


    private void finalizeQuery(Long queryId) {

        mergeQueryResults(queryId);
        sendAPartialQueryResult(queryId);
        sendNewQueryPermit(queryId);
        removeQueryIdFromMappings(queryId);
    }

    private void sendAPartialQueryResult(Long queryId) {
        List<PartialQueryResult> results = queryIdToPartialQueryResults.get(queryId);
        if (results != null && !results.isEmpty()) {
            PartialQueryResult result = results.get(0);
            if (results.size() == 1) {
                result.setEOFflag();
            }
            System.out.println("A partial query result is sent to coordinator from merger.");
            collector.emit(Streams.PartialQueryResultDeliveryStream, new Values(queryId, result));
            results.remove(0);
            if (results.size() == 0) {
                queryIdToPartialQueryResults.remove(queryId);
            }
        } else {
            PartialQueryResult result = new PartialQueryResult();
            result.setEOFflag();
            System.out.println("A empty partial query result is sent to coordinator from merger.");
            collector.emit(Streams.PartialQueryResultDeliveryStream, new Values(queryId, result));
        }

    }

    private void mergeQueryResults(long queryId) {
        // This is where aggregation happens.


        final int unitSize = 4 * 1024;
        List<PartialQueryResult> queryResults = queryIdToPartialQueryResults.get(queryId);
        List<PartialQueryResult> compactedResults = PartialQueryResult.Compact(queryResults, unitSize);
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
        if (taskIdToTimeMapping != null) {
            System.out.println("query id " + queryId + "has been finished ");
            Set<Integer> taskIds = taskIdToTimeMapping.keySet();
            for (Integer taskId : taskIds) {
                List<FileScanMetrics> time = taskIdToTimeMapping.get(taskId);
                if (time != null) {
                    System.out.println("" + taskId);
                    for (FileScanMetrics fileScanMetrics : time) {
                        System.out.println(fileScanMetrics);
                    }
//                    Long sum = 0L;
//                    for (Long t : time) {
//                        sum += t;
//                    }
//                    System.out.println("task id " + taskId + " total "  + sum);
                }
            }
        }
    }

}
