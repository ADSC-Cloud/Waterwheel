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
import indexingTopology.util.RangeQuerySubQuery;
import indexingTopology.util.SubQuery;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/15/16.
 */
public class RangeQueryDeCompositionBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Random random;

    private long seed;

    private Thread QueryThread;

    private ConcurrentHashMap<String, Pair> fileNameToKeyRangeOfFile;

    private ConcurrentHashMap<String, Pair> fileNameToTimeStampRangeOfFile;

    private FilePartitionSchemaManager filePartitionSchemaManager;

    private File file;

    /*
    private File outputFile;
    private File outputFile1;
    private File outputFile2;
    private File outputFile3;
    private File outputFile4;
    */

    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

    private transient List<Integer> targetTasks;

    private LinkedBlockingQueue<RangeQuerySubQuery> taskQueue;

    /*
    private FileOutputStream fop;
    private FileOutputStream fop1;
    private FileOutputStream fop2;
    private FileOutputStream fop3;
    private FileOutputStream fop4;
    */

    private AtomicDouble minIndexValue;
    private AtomicDouble maxIndexValue;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        seed = 1000;
        random = new Random(seed);
        fileNameToKeyRangeOfFile = new ConcurrentHashMap<String, Pair>();
        fileNameToTimeStampRangeOfFile= new ConcurrentHashMap<String, Pair>();
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
        newQueryRequest = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        queryIdToTimeCostInMillis = new HashMap<Long, Long>();
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        taskQueue = new LinkedBlockingQueue<RangeQuerySubQuery>(Config.FILE_QUERY_TASK_WATINING_QUEUE_CAPACITY);

        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream).keySet();

        targetTasks = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }

        minIndexValue = new AtomicDouble(2000);
        maxIndexValue = new AtomicDouble(0);

        /*
        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost.txt");
        outputFile1 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_files.txt");
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

        /*
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
            fop1 = new FileOutputStream(outputFile1);
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
        */



        QueryThread = new Thread(new QueryRunnable());
        QueryThread.start();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);

            Double minKey = (Double) keyRange.getKey();
            Double maxKey = (Double) keyRange.getValue();
            if (minKey < minIndexValue.get()) {
                minIndexValue.set(minKey);
            }
            if (maxKey > maxIndexValue.get()) {
                maxIndexValue.set(maxKey);
            }

            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));
//            fileNameToKeyRangeOfFile.put(fileName, keyRange);
//            fileNameToTimeStampRangeOfFile.put(fileName, timeStampRange);
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.NewQueryStream)) {
            Long queryId = tuple.getLong(0);
//            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);

            Long timeCostOfReadFile = tuple.getLong(2);
            Long timeCostOfDeserializationALeaf = tuple.getLong(3);
            Long timeCostOfDeserializationATree = tuple.getLong(4);

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
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();

            RangeQuerySubQuery subQuery = taskQueue.poll();

            if (subQuery != null) {
                collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                        , new Values(subQuery));
            }

        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingAndRangeQueryTopology.QueryGenerateStream)) {

            try {
                newQueryRequest.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            long queryId = tuple.getLong(0);
            Double leftKey = tuple.getDouble(1);
            Double rightKey = tuple.getDouble(2);
            Long startTimeStamp = tuple.getLong(3);
            Long endTimeStamp = tuple.getLong(4);

            List<String> fileNames = filePartitionSchemaManager.search(leftKey, rightKey, startTimeStamp, endTimeStamp);

            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                    new Values(queryId, leftKey, rightKey, startTimeStamp, endTimeStamp));

            int numberOfFilesToScan = fileNames.size();
            collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                    new Values(queryId, numberOfFilesToScan));


            for (String fileName : fileNames) {
                RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey, rightKey, fileName, startTimeStamp, endTimeStamp);
                try {
                    taskQueue.put(subQuery);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Integer taskId : targetTasks) {
                RangeQuerySubQuery subQuery = taskQueue.poll();
                if (subQuery != null) {
                    collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                            , new Values(subQuery));
                }
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
                new Fields("queryId", "leftKey", "rightKey", "startTimeStamp", "endTimeStamp"));

        outputFieldsDeclarer.declareStream(
                NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));
    }



    class QueryRunnable implements Runnable {

        public void run() {
            while (true) {
                try {
                    Thread.sleep(100);
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
//                Double leftKey = 0.0;
//                Double rightKey = 1000.0;
                Double min = minIndexValue.get();
                Double max = maxIndexValue.get();
                while (min > max) {
                    min = minIndexValue.get();
                    max = maxIndexValue.get();
                }
                Double leftKey = min + ((max - min) * (1 - Config.KER_RANGE_COVERAGE)) / 2;
                Double rightKey = max - ((max - min) * (1 - Config.KER_RANGE_COVERAGE)) / 2;

                System.out.println("Left key is " + leftKey.intValue());
                System.out.println("Right key is " + rightKey.intValue());

                Long startTimeStamp = System.currentTimeMillis() - 10000;
                Long endTimeStamp = System.currentTimeMillis();

//                Long startTimeStamp = (long) 0;
//                Long endTimeStamp = System.currentTimeMillis();
//                Long endTimeStamp = Long.MAX_VALUE;

                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                        new Values(queryId, leftKey, rightKey, startTimeStamp, endTimeStamp));

//                Double key = 457.6042636844468;
                /*
                List<String> fileNames = new ArrayList<String>();
                for (String fileName : fileInformation.keySet()) {
                    Pair keyRange = fileInformation.get(fileName);
                    if (leftKey.compareTo((Double) keyRange.getKey()) <= 0
                            && rightKey.compareTo((Double) keyRange.getValue()) >= 0) {
//                        System.out.println(key);
//                        System.out.println(leftKey);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println(rightKey);
//                        System.out.println("max key is " + keyRange.getValue());
                        fileNames.add(fileName);
                    }
                }*/


//                System.out.println("The size of file names is " + fileNames.size());
//                int numberOfFilesToScan = 0;

                /*
                for (String fileName : fileNameToKeyRangeOfFile.keySet()) {

                    Pair keyRange = fileNameToKeyRangeOfFile.get(fileName);
                    Pair timeStampRange = fileNameToTimeStampRangeOfFile.get(fileName);

                    if (leftKey.compareTo((Double) keyRange.getKey()) <= 0
                            && rightKey.compareTo((Double) keyRange.getValue()) >= 0
                            && startTimeStamp.compareTo((Long) timeStampRange.getKey()) <= 0
                            && endTimeStamp.compareTo((Long) timeStampRange.getValue()) >= 0) {
//                        System.out.println(key);
//                        System.out.println(leftKey);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println(rightKey);
//                        System.out.println("max key is " + keyRange.getValue());
                        ++numberOfFilesToScan;
                        collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                                new Values(queryId, leftKey, rightKey, fileName));
                    }
                }*/


                List<String> fileNames = filePartitionSchemaManager.search(leftKey, rightKey,
                        startTimeStamp, endTimeStamp);

                for (String fileName : fileNames) {
                    RangeQuerySubQuery subQuery = new RangeQuerySubQuery(queryId, leftKey,  rightKey, fileName, startTimeStamp, endTimeStamp);
                    try {
                        taskQueue.put(subQuery);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                int numberOfFilesToScan = fileNames.size();


                for (Integer taskId : targetTasks) {
                    RangeQuerySubQuery subQuery = taskQueue.poll();
                    if (subQuery != null) {
                        collector.emitDirect(taskId, NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream
                                , new Values(subQuery));
                    }
                }




//                for (String fileName : fileNames) {
//                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                            new Values(queryId, leftKey, rightKey, fileName, startTimeStamp, endTimeStamp));
//                }

//                queryIdToTimeCostInMillis.put(queryId, System.currentTimeMillis());

                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfFilesToScan));



                ++queryId;

            }
        }
    }

}
