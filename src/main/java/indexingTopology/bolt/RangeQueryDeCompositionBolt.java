package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.util.concurrent.AtomicDouble;
import indexingTopology.Config.Config;
import indexingTopology.NormalDistributionIndexingAndRangeQueryTopology;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.MetaData.FilePartitionSchemaManager;
import indexingTopology.MetaData.FileMetaData;
import indexingTopology.Streams.Streams;
import indexingTopology.util.BalancedPartition;
import indexingTopology.util.FileScanMetrics;
import indexingTopology.util.PartitionFunction;
import indexingTopology.util.RangeQuerySubQuery;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/15/16.
 */
public class RangeQueryDeCompositionBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Thread QueryThread;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private File file;

    private File outputFile;
    private File outputFile1;
    private File outputFile2;
    private File outputFile3;
    private File outputFile4;


    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

    private transient List<Integer> queryServers;

    private List<Integer> indexServers;

    private transient Map<Integer, Long> indexTaskToTimestampMapping;

    private transient Map<Integer, Integer> intervalToPartitionMapping;

    private ArrayBlockingQueue<RangeQuerySubQuery> taskQueue;

    private transient Map<Integer, ArrayBlockingQueue<RangeQuerySubQuery>> taskIdToTaskQueue;

    private BalancedPartition balancedPartition;

    private int numberOfPartitions;

    private Double lowerBound;
    private Double upperBound;

    private FileOutputStream fop;
    private FileOutputStream fop1;
    private FileOutputStream fop2;
    private FileOutputStream fop3;
    private FileOutputStream fop4;


    private AtomicDouble minIndexValue;
    private AtomicDouble maxIndexValue;

    public RangeQueryDeCompositionBolt(Double lowerBound, Double upperBound) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
        newQueryRequest = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        queryIdToTimeCostInMillis = new HashMap<Long, Long>();
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        taskQueue = new ArrayBlockingQueue<RangeQuerySubQuery>(Config.TASK_QUEUE_CAPACITY);
//        taskQueue = new LinkedBlockingQueue<RangeQuerySubQuery>(Config.FILE_QUERY_TASK_WATINING_QUEUE_CAPACITY);

        Set<String> componentIds = topologyContext.getThisTargets().get(Streams.FileSystemQueryStream).keySet();

        taskIdToTaskQueue = new HashMap<Integer, ArrayBlockingQueue<RangeQuerySubQuery>>();

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


        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost.txt");
        outputFile1 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_files.txt");
        /*
        outputFile2 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_of_read_file.txt");
        outputFile3 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_tree.txt");
        outputFile4 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_leaf.txt");
        */


        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            if (!outputFile.exists()) {
                outputFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (!outputFile1.exists()) {
                outputFile1.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
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
        */
        try {
            fop = new FileOutputStream(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fop1 = new FileOutputStream(outputFile1);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        */


        setTimestamps();

        numberOfPartitions = queryServers.size();
        balancedPartition = new BalancedPartition(numberOfPartitions, lowerBound, upperBound);

        intervalToPartitionMapping = balancedPartition.getIntervalToPartitionMapping();

        QueryThread = new Thread(new QueryRunnable());
        QueryThread.start();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(Streams.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));
        } else if (tuple.getSourceStreamId().equals(Streams.NewQueryStream)) {
            Long queryId = tuple.getLong(0);
//            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);

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


//            if (timeCostOfReadFile != null && timeCostOfDeserializationALeaf != null && timeCostOfDeserializationATree != null) {
//                queryIdToTimeCostInMillis.remove(queryId);
//                System.out.println("Query ID " + queryId + " " + timeCostInMillis);


//                String content = "Query ID " + queryId + " " + totalTimeCost;
                String content = "" + totalTimeCost;
                String newline = System.getProperty("line.separator");
                byte[] contentInBytes = content.getBytes();
                byte[] nextLineInBytes = newline.getBytes();
                try {
                    fop.write(contentInBytes);
                    fop.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                content = "" + numberOfFilesToScan;
//                String content = "" + totalTimeCost;
                newline = System.getProperty("line.separator");
                contentInBytes = content.getBytes();
                nextLineInBytes = newline.getBytes();
                try {
                    fop1.write(contentInBytes);
                    fop1.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//            }
            /*
            if (timeCostOfReadFile != null && timeCostOfDeserializationALeaf != null && timeCostOfDeserializationATree != null) {
                queryIdToTimeCostInMillis.remove(queryId);
                System.out.println("Query ID " + queryId + " " + timeCostInMillis);


                String content = "Query ID " + queryId + " " + timeCostInMillis;
                String newline = System.getProperty("line.separator");
                byte[] contentInBytes = content.getBytes();
                byte[] nextLineInBytes = newline.getBytes();
                try {
                    fop.write(contentInBytes);
                    fop.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }


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
            } else {
                queryIdToTimeCostInMillis.remove(queryId);
            }
            */

            newQueryRequest.release();
        } else if (tuple.getSourceStreamId().equals(Streams.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();

            /*task queue model
            sendSubqueryToTask(taskId);
            */

//            /*our method
            sendSubquery(taskId);
//            */

        } else if (tuple.getSourceStreamId().equals(Streams.QueryGenerateStream)) {

            try {
                newQueryRequest.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Long queryId = tuple.getLong(0);
            Double leftKey = tuple.getDouble(1);
            Double rightKey = tuple.getDouble(2);
            Long startTimeStamp = tuple.getLong(3);
            Long endTimeStamp = tuple.getLong(4);

            List<String> fileNames = filePartitionSchemaManager.search(leftKey, rightKey, startTimeStamp, endTimeStamp);

            collector.emit(Streams.BPlusTreeQueryStream,
                    new Values(queryId, leftKey, rightKey, startTimeStamp, endTimeStamp));

            int numberOfFilesToScan = fileNames.size();
            collector.emit(Streams.FileSystemQueryInformationStream, new Values(queryId, numberOfFilesToScan));


            for (String fileName : fileNames) {
                RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey, rightKey, fileName, startTimeStamp, endTimeStamp);
                try {
                    taskQueue.put(subQuery);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Integer taskId : queryServers) {
                RangeQuerySubQuery subQuery = taskQueue.poll();
                if (subQuery != null) {
                    collector.emitDirect(taskId, Streams.FileSystemQueryStream
                            , new Values(subQuery));
                }
            }

        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.TimeStampUpdateStream)) {
            int taskId = tuple.getIntegerByField("taskId");
            Long timestamp = tuple.getLongByField("timestamp");

            indexTaskToTimestampMapping.put(taskId, timestamp);
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.IntervalPartitionUpdateStream)) {
            Map<Integer, Integer> intervalToTaskMapping = (Map) tuple.getValueByField("newIntervalPartition");
            if (intervalToTaskMapping.size() > 0) {
                balancedPartition.setIntervalToPartitionMapping(intervalToPartitionMapping);
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
//                new Fields("queryId", "leftKey", "rightKey", "fileName", "startTimeStamp", "endTimeStamp"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                new Fields("subQuery"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                new Fields("queryId", "leftKey", "rightKey"));

        outputFieldsDeclarer.declareStream(
                NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));

        outputFieldsDeclarer.declareStream(
                NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryInformationStream,
                new Fields("queryId", "numberOfTasksToScan"));
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
                    if (text == null) {
//                        bufferedReader.close();
                        bufferedReader = new BufferedReader(new FileReader(file));
                        text = bufferedReader.readLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }


//                String [] tuple = text.split(" ");
//
                Double leftKey = 0.0;
                Double rightKey = 1000.0;
                Double min = minIndexValue.get();
                Double max = maxIndexValue.get();
//                while (min > max) {
//                    min = minIndexValue.get();
//                    max = maxIndexValue.get();
//                }
//                Double leftKey = min + ((max - min) * (1 - Config.KER_RANGE_COVERAGE)) / 2;
//                Double rightKey = max - ((max - min) * (1 - Config.KER_RANGE_COVERAGE)) / 2;



//                Long startTimeStamp = System.currentTimeMillis() - 10000;
//                Long endTimeStamp = System.currentTimeMillis();

                Long startTimeStamp = (long) 0;
//                Long endTimeStamp = System.currentTimeMillis();
                Long endTimeStamp = Long.MAX_VALUE;

                List<String> fileNames = filePartitionSchemaManager.search(leftKey, rightKey,
                        startTimeStamp, endTimeStamp);

                generateTreeSubQuery(leftKey, rightKey, endTimeStamp);

                int numberOfSubqueries = fileNames.size();
                


                int numberOfFilesToScan = fileNames.size();

                /* taskQueueModel
                   putSubqueriesToTaskQueue(numberOfSubqueries, leftKey, rightKey, fileNames, startTimeStamp, endTimeStamp);
                   sendSubqueriesFromTaskQueue();
                */

                /* shuffleGrouping
                   sendSubqueriesByshuffleGrouping(numberOfSubqueries, leftKey, rightKey, fileNames, startTimeStamp, endTimeStamp);
                 */


                putSubquerisToTaskQueues(numberOfSubqueries, leftKey, rightKey, fileNames, startTimeStamp, endTimeStamp);
                sendSubqueriesFromTaskQueues();


                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfSubqueries));

                ++queryId;

            }
        }
    }

    private void createTaskQueues(List<Integer> targetTasks) {
        for (Integer taskId : targetTasks) {
            ArrayBlockingQueue<RangeQuerySubQuery> taskQueue = new ArrayBlockingQueue<RangeQuerySubQuery>(Config.TASK_QUEUE_CAPACITY);
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }

    private void putSubquerisToTaskQueue(int numberOfSubqueries, Double leftKey
            , Double rightKey, List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey,  rightKey, fileNames.get(i), startTimeStamp, endTimeStamp);
            try {
                taskQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void generateTreeSubQuery(Double leftKey, Double rightKey, Long endTimeStamp) {

        int numberOfTasksToSearch = 0;


        int leftIntervalId = balancedPartition.getIntervalId(leftKey);
        int rightIntervalId = balancedPartition.getIntervalId(rightKey);

        Integer startPartitionId = intervalToPartitionMapping.get(leftIntervalId);
        Integer endPartitionId = intervalToPartitionMapping.get(rightIntervalId);


        Integer startTaskId = indexServers.get(startPartitionId);
        Integer endTaskId = indexServers.get(endPartitionId);

        for (Integer taskId = startTaskId; taskId <= endTaskId; ++taskId) {
            Long timestamp = indexTaskToTimestampMapping.get(taskId);
            if (timestamp <= endTimeStamp) {
                collector.emitDirect(taskId, NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                        new Values(queryId, leftKey, rightKey));
                ++numberOfTasksToSearch;
            }
        }
        collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryInformationStream,
                new Values(queryId, numberOfTasksToSearch));
    }

    private void putSubquerisToTaskQueues(int numberOfSubqueries, Double leftKey
            , Double rightKey, List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            String fileName = fileNames.get(i);
            int index = Math.abs(fileName.hashCode()) % queryServers.size();
            Integer taskId = queryServers.get(index);
            ArrayBlockingQueue<RangeQuerySubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey,  rightKey, fileName, startTimeStamp, endTimeStamp);
            if (taskQueue == null) {
                taskQueue = new ArrayBlockingQueue<RangeQuerySubQuery>(Config.TASK_QUEUE_CAPACITY);
            }
            try {
                taskQueue.put(subQuery);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            taskIdToTaskQueue.put(taskId, taskQueue);
        }
    }


    private void sendSubqueriesFromTaskQueue() {
        for (Integer taskId : queryServers) {
            RangeQuerySubQuery subQuery = taskQueue.poll();
            if (subQuery != null) {
                collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream
                        , new Values(subQuery));
            }
        }
    }


    private void sendSubqueriesFromTaskQueues() {
        for (Integer taskId : queryServers) {
            ArrayBlockingQueue<RangeQuerySubQuery> taskQueue = taskIdToTaskQueue.get(taskId);
            RangeQuerySubQuery subQuery = taskQueue.poll();
            if (subQuery != null) {
                collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream
                        , new Values(subQuery));
            }
        }
    }

    private void sendSubqueriesByshuffleGrouping(int numberOfSubqueries, Double leftKey
            , Double rightKey, List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        for (int i = 0; i < numberOfSubqueries; ++i) {
            RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey, rightKey, fileNames.get(i), startTimeStamp, endTimeStamp);
            collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream
                    , new Values(subQuery));
        }
    }


    private void sendSubqueryToTask(int taskId) {
        RangeQuerySubQuery subQuery = taskQueue.poll();

        if (subQuery != null) {
            collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                    , new Values(subQuery));
        }

    }



    private void sendSubquery(int taskId) {

        ArrayBlockingQueue<RangeQuerySubQuery> taskQueue = taskIdToTaskQueue.get(taskId);

        RangeQuerySubQuery subQuery = taskQueue.poll();

        if (subQuery == null) {

            taskQueue = getLongestQueue();

            subQuery = taskQueue.poll();

        }
        if (subQuery != null) {
            collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                    , new Values(subQuery));
        }
    }

    private ArrayBlockingQueue<RangeQuerySubQuery> getLongestQueue() {
        List<ArrayBlockingQueue<RangeQuerySubQuery>> taskQueues
                = new ArrayList<ArrayBlockingQueue<RangeQuerySubQuery>>(taskIdToTaskQueue.values());

        Collections.sort(taskQueues, (taskQueue1, taskQueue2) -> Integer.compare(taskQueue2.size(), taskQueue1.size()));

        ArrayBlockingQueue<RangeQuerySubQuery> taskQueue = taskQueues.get(0);

        return taskQueue;
    }

    private List<RangeQuerySubQuery> generateSubqueries(Long queryId, Double leftKey
            , Double rightKey, List<String> fileNames, Long startTimeStamp, Long endTimeStamp) {
        List<RangeQuerySubQuery> subQueries = new ArrayList<>();

        RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey,
                rightKey, null, startTimeStamp, endTimeStamp);
        subQueries.add(subQuery);

        for (String fileName : fileNames) {
            subQuery = new RangeQuerySubQuery(queryId, leftKey, rightKey, fileName, startTimeStamp, endTimeStamp);
            subQueries.add(subQuery);
        }

        return subQueries;
    }

    private void setTimestamps() {

        indexTaskToTimestampMapping = new HashMap<>();

        for (Integer taskId : indexServers) {
            indexTaskToTimestampMapping.put(taskId, System.currentTimeMillis());
        }
    }

}
