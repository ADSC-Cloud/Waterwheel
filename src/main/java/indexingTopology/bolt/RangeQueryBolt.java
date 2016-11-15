package indexingTopology.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import indexingTopology.NormalDistributionIndexingTopology;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by acelzj on 11/15/16.
 */
public class RangeQueryBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Random random;

    private long seed;

    private Thread QueryThread;

    private ConcurrentHashMap<String, Pair> fileInformation;

    private File file;

    private BufferedReader bufferedReader;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        seed = 1000;
        random = new Random(seed);
        fileInformation = new ConcurrentHashMap<String, Pair>();
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

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
        if (tuple.getSourceStreamId() == NormalDistributionIndexingTopology.FileInformationUpdateStream) {
            String fileName = tuple.getString(0);
            Pair keyRange = (Pair) tuple.getValue(1);
            fileInformation.put(fileName, keyRange);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("key"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.FileSystemQueryStream,
                new Fields("leftKey", "rightKey", "fileNames"));
        outputFieldsDeclarer.declareStream(NormalDistributionIndexingTopology.BPlusTreeQueryStream,
                new Fields("leftKey", "rightKey"));
    }

    class QueryRunnable implements Runnable {

        public void run() {
            while (true) {
                try {
                    Thread.sleep(1);
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

                List<String> fileNames = new ArrayList<String>();
                for (String fileName : fileInformation.keySet()) {
                    Pair keyRange = fileInformation.get(fileName);
                    if (leftKey.compareTo((Double) keyRange.getKey()) <= 0
                            && rightKey.compareTo((Double) keyRange.getValue()) >= 0) {
//                        System.out.println(key);
                        fileNames.add(fileName);
                    }
                }

//                System.out.println("The size of file names is " + fileNames.size());
                collector.emit(NormalDistributionIndexingTopology.FileSystemQueryStream,
                        new Values(leftKey, rightKey, fileNames));

                collector.emit(NormalDistributionIndexingTopology.BPlusTreeQueryStream, new Values(leftKey, rightKey));
            }
        }
    }

}
