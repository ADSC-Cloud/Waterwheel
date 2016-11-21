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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by acelzj on 11/17/16.
 */
public class ResultMergeBolt extends BaseRichBolt {

    Map<Long, Integer> queryIdToNumberOfTuples;

    Map<Long, Integer> queryIdToCounter;

    Map<Long, Integer> queryIdToNumberOfFilesToScan;

    Map<Long, Integer> queryIdToNumberOfTasksToSearch;

    DataSchema schema;

    OutputCollector collector;

    private int numberOfFilesToScan;

    private int numberOfTasksToSearch;

    private int counter;

    public ResultMergeBolt(DataSchema schema) {
        this.schema = schema;
        numberOfFilesToScan = 0;
        numberOfTasksToSearch = 0;
        counter = 0;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        queryIdToNumberOfTuples = new HashMap<Long, Integer>();
        queryIdToCounter = new HashMap<Long, Integer>();
        queryIdToNumberOfFilesToScan = new HashMap<Long, Integer>();
        queryIdToNumberOfTasksToSearch = new HashMap<Long, Integer>();
        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId()
                == NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryInformationStream) {
            numberOfTasksToSearch = tuple.getInteger(1);
            queryIdToNumberOfTasksToSearch.put(tuple.getLong(0), numberOfTasksToSearch);
//            System.out.println("Number of tasks have been updated " + numberOfFilesToScan + " query id " + tuple.getLong(0));
        } else if (tuple.getSourceStreamId()
                == NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream) {
            numberOfFilesToScan = tuple.getInteger(1);
            queryIdToNumberOfFilesToScan.put(tuple.getLong(0), numberOfFilesToScan);
//            System.out.println("Number of files have been updated " + numberOfTasksToSearch + " query id " + tuple.getLong(0));
        } else if (tuple.getSourceStreamId() == NormalDistributionIndexingTopology.BPlusTreeQueryStream ||
                tuple.getSourceStreamId() == NormalDistributionIndexingTopology.FileSystemQueryStream) {
            long queryId = tuple.getLong(0);
            Integer counter = queryIdToCounter.get(queryId);
            if (counter == null) {
                counter = 1;
            } else {
                counter += 1;
            }
//            System.out.println("The counter is " + counter + "The query id is " + queryId);
            ArrayList<byte[]> serializedTuples = (ArrayList) tuple.getValue(1);
            for (int i = 0; i < serializedTuples.size(); ++i) {
                Values deserializedTuple = null;
                try {
                    deserializedTuple = schema.deserialize(serializedTuples.get(i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                System.out.println(deserializedTuple);
            }
            Integer numberOfTuples = queryIdToNumberOfTuples.get(queryId);
            if (numberOfTuples == null)
                numberOfTuples = 0;
            numberOfTuples += serializedTuples.size();
            queryIdToCounter.put(queryId, counter);
            queryIdToNumberOfTuples.put(queryId, numberOfTuples);

            if (queryIdToNumberOfFilesToScan.get(queryId) != null) {
                numberOfFilesToScan = queryIdToNumberOfFilesToScan.get(queryId);
            } else {
                numberOfFilesToScan = 0;
            }
            if (queryIdToNumberOfTasksToSearch.get(queryId) != null) {
                numberOfTasksToSearch = queryIdToNumberOfTasksToSearch.get(queryId);
            } else {
                numberOfTasksToSearch = 0;
            }

//            System.out.println("The query id is " + queryId);
            if (counter == numberOfFilesToScan + numberOfTasksToSearch) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed")));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
            }
            collector.ack(tuple);
        }

//        System.out.println("Key: " + key + "Number of tuples: " + numberOfTuples);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream
                , new Fields("queryId", "New Query"));
    }
}
