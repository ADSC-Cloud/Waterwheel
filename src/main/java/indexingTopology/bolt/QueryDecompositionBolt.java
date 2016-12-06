package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.Config.Config;
import indexingTopology.NormalDistributionIndexingTopology;
import indexingTopology.MetaData.FilePartitionSchemaManager;
import indexingTopology.MetaData.FileMetaData;
import indexingTopology.util.SubQuery;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Created by acelzj on 11/9/16.
 */
public class QueryDecompositionBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Random random;

    private long seed;

    private Thread QueryThread;

    private ConcurrentHashMap<String, Pair> fileNameToKeyRangeOfFile;

    private ConcurrentHashMap<String, Pair> fileNameToTimeStampRangeOfFile;

    private FilePartitionSchemaManager filePartitionSchemaManager;


    private File file;


    private File outputFile;
    private File outputFile1;
    private File outputFile2;
    private File outputFile3;
    private File outputFile4;
    private File outputFile5;


    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

    private transient List<Integer> targetTasks;

    private LinkedBlockingQueue<SubQuery> taskQueue;


    private FileOutputStream fop;
    private FileOutputStream fop1;
    private FileOutputStream fop2;
    private FileOutputStream fop3;
    private FileOutputStream fop4;
    private FileOutputStream fop5;




    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        seed = 1000;
        random = new Random(seed);
        fileNameToKeyRangeOfFile = new ConcurrentHashMap<String, Pair>();
        fileNameToTimeStampRangeOfFile= new ConcurrentHashMap<String, Pair>();
        taskQueue = new LinkedBlockingQueue<SubQuery>(Config.FILE_QUERY_TASK_WATINING_QUEUE_CAPACITY);

        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

        Set<String> componentIds = topologyContext.getThisTargets()
                .get(NormalDistributionIndexingTopology.FileSystemQueryStream).keySet();

        targetTasks = new ArrayList<Integer>();

        for (String componentId : componentIds) {
            targetTasks.addAll(topologyContext.getComponentTasks(componentId));
        }


        outputFile = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost.txt");
        outputFile1 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/number_of_files.txt");
        outputFile2 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_of_read_file.txt");
        outputFile3 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_tree.txt");
        outputFile4 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_deserialization_a_leaf.txt");
        outputFile5 = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/time_cost_Searching.txt");


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
            if (!outputFile5.exists()) {
                outputFile5.createNewFile();
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
        try {
            fop5 = new FileOutputStream(outputFile5);
        } catch (IOException e) {
            e.printStackTrace();
        }


        newQueryRequest = new Semaphore(MAX_NUMBER_OF_CONCURRENT_QUERIES);
        queryId = 0;
        queryIdToTimeCostInMillis = new HashMap<Long, Long>();
        filePartitionSchemaManager = new FilePartitionSchemaManager();

        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        QueryThread = new Thread(new QueryRunnable());
        QueryThread.start();
    }

    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.FileInformationUpdateStream)) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            Pair timeStampRange = (Pair) tuple.getValue(2);
//            fileNameToKeyRangeOfFile.put(fileName, keyRange);
//            fileNameToTimeStampRangeOfFile.put(fileName, timeStampRange);
            filePartitionSchemaManager.add(new FileMetaData(fileName, (Double) keyRange.getKey(),
                    (Double)keyRange.getValue(), (Long) timeStampRange.getKey(), (Long) timeStampRange.getValue()));
        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.NewQueryStream)) {
            Long queryId = tuple.getLong(0);
//            System.out.println(tuple.getString(1));
//            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);

            Long timeCostOfReadFile = tuple.getLong(2);
            Long timeCostOfDeserializationALeaf = tuple.getLong(3);
            Long timeCostOfDeserializationATree = tuple.getLong(4);
            Long timeCostOfSearching = tuple.getLong(5);
            Long totalTimeCost = tuple.getLong(6);


            if (timeCostOfReadFile != null && timeCostOfDeserializationALeaf != null && timeCostOfDeserializationATree != null) {
                queryIdToTimeCostInMillis.remove(queryId);
//                System.out.println("Query ID " + queryId + " " + timeCostInMillis);


                String content = "Query ID " + queryId + " " + totalTimeCost;
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

            } else {
                queryIdToTimeCostInMillis.remove(queryId);
            }

            newQueryRequest.release();

        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.QueryGenerateStream)) {
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

            collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                    new Values(queryId, key, startTimeStamp, endTimeStamp));

            int numberOfFilesToScan = fileNames.size();
            collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                    new Values(queryId, numberOfFilesToScan));


            for (String fileName : fileNames) {
                SubQuery subQuery = new SubQuery(queryId, key, fileName, startTimeStamp, endTimeStamp);
                try {
                    taskQueue.put(subQuery);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            for (Integer taskId : targetTasks) {
                SubQuery subQuery = taskQueue.poll();
                if (subQuery != null) {
                    collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                            , new Values(subQuery));
                }
            }

        } else if (tuple.getSourceStreamId().equals(NormalDistributionIndexingTopology.FileSubQueryFinishStream)) {

            int taskId = tuple.getSourceTask();

            SubQuery subQuery = taskQueue.poll();

            if (subQuery != null) {
                collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                        , new Values(subQuery));
            }

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
//        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                new Fields("queryId", "key", "fileName", "startTimestamp", "endTimestamp"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("subQuery"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                new Fields("queryId", "key", "startTimeStamp", "endTimeStamp"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                new Fields("queryId", "numberOfFilesToScan"));
    }


    class QueryRunnable implements Runnable {

        public void run() {

            while (true) {

                try {
                    Thread.sleep(1);
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

                String [] tuple = text.split(" ");

                Double key = Double.parseDouble(tuple[0]);

                Long startTimeStamp = (long) 0;
//                Long endTimeStamp = System.currentTimeMillis();
                Long endTimeStamp = Long.MAX_VALUE;

                collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                        new Values(queryId, key, startTimeStamp, endTimeStamp));

                List<String> fileNames = filePartitionSchemaManager.search(key, key, startTimeStamp, endTimeStamp);
                for (String fileName : fileNames) {
                    SubQuery subQuery = new SubQuery(queryId, key, fileName, startTimeStamp, endTimeStamp);
                    try {
                        taskQueue.put(subQuery);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                int numberOfFilesToScan = fileNames.size();
                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfFilesToScan));


                /*
                try {
                    QueryInformation queryInformation = queue.take();
                    long queryId = queryInformation.getQueryId();
                    List<String> fileNames = queryInformation.getFileNames();
                    Double key = queryInformation.getKey();
                    Long startTimeStamp = queryInformation.getStartTimestamp();
                    Long endTimeStamp = queryInformation.getEndTimestamp();
                    int numberOfFilesToScan = fileNames.size();
                    System.out.println("Number of files to scan is " + numberOfFilesToScan);

                    for (String fileName : fileNames) {
                        collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                                new Values(queryId, key, fileName, startTimeStamp, endTimeStamp));
                    }


                    collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                            new Values(queryId, numberOfFilesToScan));


                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/

                for (Integer taskId : targetTasks) {
                    SubQuery subQuery = taskQueue.poll();
                    if (subQuery != null) {
                        collector.emitDirect(taskId, NormalDistributionIndexingTopology.FileSystemQueryStream
                                , new Values(subQuery));
                    }
                }




                /*
                String content = "Query ID " + queryId + " " + numberOfFilesToScan;
                String newline = System.getProperty("line.separator");
                byte[] contentInBytes = content.getBytes();
                byte[] nextLineInBytes = newline.getBytes();
                try {
                    fop1.write(contentInBytes);
                    fop1.write(nextLineInBytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                */




            }
        }
    }

}
