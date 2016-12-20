package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
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

//    private File file;

//    private File outputFile;
//    private File outputFile1;
//    private File outputFile2;
//    private File outputFile3;
//    private File outputFile4;
//    private File outputFile5;


    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

    private transient List<Integer> queryServers;

    private transient Map<Integer, ArrayBlockingQueue<SubQuery>> taskIdToTaskQueue;

    private transient Map<Integer, Long> indexTaskToTimestampMapping;

    private transient Map<Integer, Integer> intervalToPartitionMapping;

    private BalancedPartition balancedPartition;

//    private LinkedBlockingQueue<SubQuery> taskQueue;
    private ArrayBlockingQueue<SubQuery> taskQueue;


//    private FileOutputStream fop;
//    private FileOutputStream fop1;
//    private FileOutputStream fop2;
//    private FileOutputStream fop3;
//    private FileOutputStream fop4;
//    private FileOutputStream fop5;

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


//        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

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


//        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost.txt");
//        outputFile1 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_files.txt");
//        outputFile2 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_of_read_file.txt");
//        outputFile3 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_tree.txt");
//        outputFile4 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_leaf.txt");
//        outputFile5 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_Searching.txt");


//        try {
//            if (!outputFile.exists()) {
//                outputFile.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (!outputFile1.exists()) {
//                outputFile1.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (!outputFile2.exists()) {
//                outputFile2.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (!outputFile3.exists()) {
//                outputFile3.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (!outputFile4.exists()) {
//                outputFile4.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (!outputFile5.exists()) {
//                outputFile5.createNewFile();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            fop = new FileOutputStream(outputFile);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            fop1 = new FileOutputStream(outputFile1);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        /*
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

        newQueryRequest = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        queryIdToTimeCostInMillis = new HashMap<Long, Long>();
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        balancedPartition = new BalancedPartition(indexServers.size(), lowerBound, upperBound);

        intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();

//        try {
//            bufferedReader = new BufferedReader(new FileReader(file));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


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
            /*
            Long timeCostOfReadFile = tuple.getLong(2);
            Long timeCostOfDeserializationALeaf = tuple.getLong(3);
            Long timeCostOfDeserializationATree = tuple.getLong(4);
            Long timeCostOfSearching = tuple.getLong(5);
            Long totalTimeCost = tuple.getLong(6);
            */


//                queryIdToTimeCostInMillis.remove(queryId);
//                System.out.println("Query ID " + queryId + " " + timeCostInMillis);


//                String content = "Query ID " + queryId + " " + totalTimeCost;
                String content = "" + totalTimeCost;
                String newline = System.getProperty("line.separator");
                byte[] contentInBytes = content.getBytes();
                byte[] nextLineInBytes = newline.getBytes();
//                try {
//                    fop.write(contentInBytes);
//                    fop.write(nextLineInBytes);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }


                content = "" + numberOfFilesToScan;
//                String content = "" + totalTimeCost;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
//                try {
//                    fop1.write(contentInBytes);
//                    fop1.write(nextLineInBytes);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            }



                /*
                content = "Query ID " + queryId + " " + timeCostOfReadFile;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop2.write(contentInBytes);
                    fop2.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "Query ID " + queryId + " " + timeCostOfDeserializationATree;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop3.write(contentInBytes);
                    fop3.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "Query ID " + queryId + " " + timeCostOfDeserializationALeaf;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop4.write(contentInBytes);
                    fop4.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                content = "Query ID " + queryId + " " + timeCostOfSearching;
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
            if (intervalToPartitionMapping.size() > 0) {
                balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
            }
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
        int intervalId = balancedPartition.getIntervalId(key);
        Integer partitionId = intervalToPartitionMapping.get(intervalId);
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
