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
import indexingTopology.util.DeserializationHelper;
import indexingTopology.util.FileScanMetrics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by acelzj on 11/9/16.
 */
public class RangeQueryResultMergeBolt extends BaseRichBolt {

    Map<Long, Integer> queryIdToNumberOfTuples;

    Map<Long, Integer> queryIdToCounter;

    Map<Long, Integer> queryIdToNumberOfFilesToScan;

    Map<Long, Integer> queryIdToNumberOfTasksToSearch;

    Map<Long, Long> queryIdToTimeCostOfDeserilizationOfATree;

    Map<Long, Long> queryIdToTimeCostOfDeserilizationOfALeaf;

    Map<Long, Long> queryIdToTimeCostOfReadFile;

    DataSchema schema;

    OutputCollector collector;

    private int counter;


    private File outputFile;
    private File outputFile2;
    private File outputFile3;
    private File outputFile4;
    private File outputFile5;

    private FileOutputStream fop;
    private FileOutputStream fop2;
    private FileOutputStream fop3;
    private FileOutputStream fop4;
    private FileOutputStream fop5;



    public RangeQueryResultMergeBolt(DataSchema schema) {
        this.schema = schema;
        counter = 0;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        queryIdToNumberOfTuples = new HashMap<Long, Integer>();
        queryIdToCounter = new HashMap<Long, Integer>();
        queryIdToNumberOfFilesToScan = new HashMap<Long, Integer>();
        queryIdToNumberOfTasksToSearch = new HashMap<Long, Integer>();

        queryIdToTimeCostOfDeserilizationOfATree = new HashMap<Long, Long>();
        queryIdToTimeCostOfDeserilizationOfALeaf = new HashMap<Long, Long>();
        queryIdToTimeCostOfReadFile = new HashMap<Long, Long>();

        /*
        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost.txt");
        outputFile2 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_of_read_file.txt");
        outputFile3 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_tree.txt");
        outputFile4 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_leaf.txt");
        outputFile5 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_searching.txt");


        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile2.exists()) {
                outputFile2.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile3.exists()) {
                outputFile3.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile4.exists()) {
                outputFile4.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop2 = new FileOutputStream(outputFile2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop3 = new FileOutputStream(outputFile3);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop4 = new FileOutputStream(outputFile4);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop5 = new FileOutputStream(outputFile5);
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

        collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId()
                .equals(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryInformationStream)) {
//            numberOfTasksToSearch = tuple.getInteger(1);
            int numberOfTasksToSearch = tuple.getInteger(1);
            Long queryId = tuple.getLong(0);
            queryIdToNumberOfTasksToSearch.put(queryId, numberOfTasksToSearch);
            /*
            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed")));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
            }*/

            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed"),
                                queryIdToTimeCostOfReadFile.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfALeaf.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfATree.get(queryId)));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
                queryIdToTimeCostOfReadFile.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfALeaf.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfATree.remove(queryId);
            }



//            System.out.println("Number of tasks have been updated " + numberOfFilesToScan + " query id" + tuple.getLong(0));
        } else if (tuple.getSourceStreamId()
                .equals(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream)) {
//            numberOfFilesToScan = tuple.getInteger(1);
            int numberOfFilesToScan = tuple.getInteger(1);
            Long queryId = tuple.getLong(0);
            queryIdToNumberOfFilesToScan.put(queryId, numberOfFilesToScan);
            /*
            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed")));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
            }
            */

            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed"),
                                queryIdToTimeCostOfReadFile.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfALeaf.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfATree.get(queryId)));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
                queryIdToTimeCostOfReadFile.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfALeaf.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfATree.remove(queryId);
            }



//            System.out.println("Number of files have been updated " + numberOfTasksToSearch + " query id" + tuple.getLong(0));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream) ||
                tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream)) {
            long queryId = tuple.getLong(0);



            if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream)) {
                FileScanMetrics metrics = (FileScanMetrics) tuple.getValue(2);
                Long timeCostInSearching = metrics.getSearchTime();
                Long timeCostInMillis = metrics.getTotalTime();
                Long timeCostOfReadFile = metrics.getFileReadingTime();
                Long timeCostOfDeserializationALeaf = metrics.getLeafDeserializationTime();
                Long timeCostOfDeserializationATree = metrics.getTreeDeserializationTime();

                /*
                String content = "" + timeCostInMillis;
                String newline = System.getProperty("line.separator");
                byte[] contentInBytes = content.getBytes();
                byte[] nextLineInBytes = newline.getBytes();
                try {
                    fop.write(contentInBytes);
                    fop.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "" + timeCostOfReadFile;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop2.write(contentInBytes);
                    fop2.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "" + timeCostOfDeserializationATree;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop3.write(contentInBytes);
                    fop3.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "" + timeCostOfDeserializationALeaf;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop4.write(contentInBytes);
                    fop4.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                content = "" + timeCostInSearching;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop5.write(contentInBytes);
                    fop5.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                */

                Long timeCostOfReadFileTotal = queryIdToTimeCostOfReadFile.get(queryId);
                Long timeCostOfDeserializationALeafTotal = queryIdToTimeCostOfDeserilizationOfALeaf.get(queryId);
                Long timeCostOfDeserializationATreeTotal = queryIdToTimeCostOfDeserilizationOfATree.get(queryId);
                if (timeCostOfReadFileTotal == null) {
                    queryIdToTimeCostOfReadFile.put(queryId, timeCostOfReadFile);
                } else {
                    queryIdToTimeCostOfReadFile.put(queryId, timeCostOfReadFileTotal + timeCostOfReadFile);
                }
                if (timeCostOfDeserializationALeafTotal == null) {
                    queryIdToTimeCostOfDeserilizationOfALeaf.put(queryId, timeCostOfDeserializationALeaf);
                } else {
                    queryIdToTimeCostOfDeserilizationOfALeaf.put(queryId, timeCostOfDeserializationALeafTotal + timeCostOfDeserializationALeaf);
                }
                if (timeCostOfDeserializationATreeTotal == null) {
                    queryIdToTimeCostOfDeserilizationOfATree.put(queryId, timeCostOfDeserializationATree);
                } else {
                    queryIdToTimeCostOfDeserilizationOfATree.put(queryId, timeCostOfDeserializationATreeTotal + timeCostOfDeserializationATree);
                }
            }






            Integer counter = queryIdToCounter.get(queryId);
            if (counter == null) {
                counter = 1;
            } else {
                counter = counter + 1;
            }
            ArrayList<byte[]> serializedTuples = (ArrayList) tuple.getValue(1);


            for (int i = 0; i < serializedTuples.size(); ++i) {
                Values deserializedTuple = null;
                try {
//                    deserializedTuple = schema.deserialize(serializedTuples.get(i));
                    deserializedTuple = DeserializationHelper.deserialize(serializedTuples.get(i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(deserializedTuple);
            }




            Integer numberOfTuples = queryIdToNumberOfTuples.get(queryId);
            if (numberOfTuples == null)
                numberOfTuples = 0;
            numberOfTuples += serializedTuples.size();
            queryIdToNumberOfTuples.put(queryId, numberOfTuples);
            queryIdToCounter.put(queryId, counter);

//            System.out.println(queryIdToNumberOfFilesToScan.get(queryId));
//            System.out.println(queryIdToNumberOfTasksToSearch.get(queryId));
            /*
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
            */

//            System.out.println("The query id is " + queryId);
            /*
            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed")));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
            }
            */

            if (isQueryFinshed(queryId)) {
                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream,
                        new Values(queryId, new String("New query can be executed"),
                                queryIdToTimeCostOfReadFile.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfALeaf.get(queryId),
                                queryIdToTimeCostOfDeserilizationOfATree.get(queryId)));
                queryIdToCounter.remove(queryId);
                queryIdToNumberOfFilesToScan.remove(queryId);
                queryIdToNumberOfTasksToSearch.remove(queryId);
                queryIdToTimeCostOfReadFile.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfALeaf.remove(queryId);
                queryIdToTimeCostOfDeserilizationOfATree.remove(queryId);
            }



            collector.ack(tuple);
        }

//        System.out.println("Key: " + key + "Number of tuples: " + numberOfTuples);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.NewQueryStream
//                , new Fields("queryId", "New Query"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.NewQueryStream
                , new Fields("queryId", "New Query", "timeCostOfReadFile", "timeCostOfDeserializationALeaf",
                        "timeCostOfDeserializationATree"));


    }

    private boolean isQueryFinshed(Long queryId) {
        if (queryIdToNumberOfFilesToScan.get(queryId) == null) {
            return false;
        } else if (queryIdToNumberOfTasksToSearch.get(queryId) == null) {
            return false;
        } else if (queryIdToCounter.get(queryId) == null) {
            return false;
        } else {
            int counter = queryIdToCounter.get(queryId);
            int numberOfFilesToScan = queryIdToNumberOfFilesToScan.get(queryId);
            int tasksToSearch = queryIdToNumberOfTasksToSearch.get(queryId);
            if (numberOfFilesToScan + tasksToSearch == counter) {
                return true;
            }
        }
        return false;
    }
}
