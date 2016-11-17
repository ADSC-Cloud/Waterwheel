package indexingTopology.bolt;

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

    private ConcurrentHashMap<String, Pair> fileInformation;

    private File file;

    private BufferedReader bufferedReader;

    private Semaphore numberOfQueries;

    private long queryId;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        seed = 1000;
        random = new Random(seed);
        fileInformation = new ConcurrentHashMap<String, Pair>();
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
        numberOfQueries = new Semaphore(5);
        queryId = 0;

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
            fileInformation.put(fileName, keyRange);
        } else {
            numberOfQueries.release();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("queryId", "key", "fileName"));

        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                new Fields("queryId", "key"));

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
                    numberOfQueries.acquire();
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
                for (String fileName : fileInformation.keySet()) {
                    Pair keyRange = fileInformation.get(fileName);
                    if (key.compareTo((Double) keyRange.getKey()) >= 0
                            && key.compareTo((Double) keyRange.getValue()) <= 0) {
//                        System.out.println(key);
//                        System.out.println("min key is " + keyRange.getKey());
//                        System.out.println("max key is " + keyRange.getValue());
                        ++numberOfFilesToScan;
                        collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                                new Values(queryId, key, fileName));
                    }
                }


                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryInformationStream,
                        new Values(queryId, numberOfFilesToScan));


//                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
//                        new Values(key, fileNames));

                collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Values(queryId, key));

                ++queryId;
            }
        }
    }
}
