package indexingTopology.bolt;

import backtype.storm.metric.SystemBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.NormalDistributionIndexingTopology;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private File file;

    private BufferedReader bufferedReader;

    private Semaphore newQueryRequest;

    private static final int MAX_NUMBER_OF_CONCURRENT_QUERIES = 5;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

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
            fileNameToKeyRangeOfFile.put(fileName, keyRange);
            fileNameToTimeStampRangeOfFile.put(fileName, timeStampRange);
        } else {
            Long queryId = tuple.getLong(0);
            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);
            newQueryRequest.release();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("queryId", "key", "fileName"));

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
//
                Double key = Double.parseDouble(tuple[0]);

                Long startTimeStamp = System.currentTimeMillis() - 10000;
                Long endTimeStamp = System.currentTimeMillis();
//                Double key = 457.6042636844468;
/*
                List<String> fileNames = new ArrayList<String>();
                for (String fileName : fileInformation.keySet()) {
                    Pair keyRange = fileInformation.get(fileName);
                    if (key.compareTo((Double) keyRange.getKey()) >= 0
                            && key.compareTo((Double) keyRange.getValue()) <= 0) {
//                        System.out.println(key);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println("max key is " + keyRange.getValue());
                        fileNames.add(fileName);
                    }
                }
                */

//                System.out.println("The size of file names is " + fileNames.size());
                int numberOfFilesToScan = 0;
                for (String fileName : fileNameToKeyRangeOfFile.keySet()) {
                    Pair keyRange = fileNameToKeyRangeOfFile.get(fileName);
                    Pair timeStampRange = fileNameToTimeStampRangeOfFile.get(fileName);

                    if (key.compareTo((Double) keyRange.getKey()) >= 0
                            && key.compareTo((Double) keyRange.getValue()) <= 0
                            && startTimeStamp.compareTo((Long) timeStampRange.getKey()) <= 0
                            && endTimeStamp.compareTo((Long) timeStampRange.getValue()) >= 0) {
//                        System.out.println(key);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println("max key is " + keyRange.getValue());
                        ++numberOfFilesToScan;
                        collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                                new Values(queryId, key, fileName));
                    }
                }

                queryIdToTimeCostInMillis.put(queryId, System.currentTimeMillis());

                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfFilesToScan));


//                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                        new Values(key, fileNames));

                collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                        new Values(queryId, key, startTimeStamp, endTimeStamp));

                ++queryId;
            }
        }
    }
}
