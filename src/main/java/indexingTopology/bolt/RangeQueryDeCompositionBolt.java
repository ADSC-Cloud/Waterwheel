package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.NormalDistributionIndexingAndRangeQueryTopology;
import indexingTopology.NormalDistributionIndexingTopology;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

    private File file;

    private BufferedReader bufferedReader;

    private Semaphore numberOfQueries;

    private long queryId;

    private Map<Long, Long> queryIdToTimeCostInMillis;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        seed = 1000;
        random = new Random(seed);
        fileNameToKeyRangeOfFile = new ConcurrentHashMap<String, Pair>();
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
        numberOfQueries = new Semaphore(5);
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
            fileNameToKeyRangeOfFile.put(fileName, keyRange);
        } else {
            System.out.println(tuple.getString(1));
            Long queryId = tuple.getLong(0);
            Long timeCostInMillis = System.currentTimeMillis() - queryIdToTimeCostInMillis.get(queryId);
            numberOfQueries.release();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                new Fields("queryId", "leftKey", "rightKey", "fileName"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                new Fields("queryId", "leftKey", "rightKey"));

        outputFieldsDeclarer.declareStream(
                NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
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
                    numberOfQueries.acquire();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
/*
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
                */

//                String [] tuple = text.split(" ");
//
                Double leftKey = 0.0;
                Double rightKey = 1000.0;

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
                int numberOfFilesToScan = 0;
                for (String fileName : fileNameToKeyRangeOfFile.keySet()) {
                    Pair keyRange = fileNameToKeyRangeOfFile.get(fileName);
                    if (leftKey.compareTo((Double) keyRange.getKey()) <= 0
                            && rightKey.compareTo((Double) keyRange.getValue()) >= 0) {
//                        System.out.println(key);
//                        System.out.println(leftKey);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println(rightKey);
//                        System.out.println("max key is " + keyRange.getValue());
                        ++numberOfFilesToScan;
                        collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryStream,
                                new Values(queryId, leftKey, rightKey, fileName));
                    }
                }

                queryIdToTimeCostInMillis.put(queryId, System.currentTimeMillis());

                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfFilesToScan));

                collector.emit(NormalDistributionIndexingAndRangeQueryTopology.BPlusTreeQueryStream,
                        new Values(queryId, leftKey, rightKey));

                ++queryId;

            }
        }
    }

}
