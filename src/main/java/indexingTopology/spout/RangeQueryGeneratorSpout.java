package indexingTopology.spout;

import indexingTopology.streams.Streams;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Created by acelzj on 12/5/16.
 */
public class RangeQueryGeneratorSpout extends BaseRichSpout {

    SpoutOutputCollector collector;

    private Thread QueryThread;

    private File file;

    private BufferedReader bufferedReader;

    private long queryId;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

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

    public void nextTuple() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Streams.QueryGenerateStream,
                new Fields("queryId", "leftKey", "rightKey", "startTimestamp", "endTimestamp"));
    }

    class QueryRunnable implements Runnable {

        public void run() {
            while (true) {
                try {
                    Thread.sleep(1);
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

                /*
                Double min = minIndexValue.get();
                Double max = maxIndexValue.get();
                while (min > max) {
                    min = minIndexValue.get();
                    max = maxIndexValue.get();
                }
                Double leftKey = min + ((max - min) * (1 - TopologyConfig.KER_RANGE_COVERAGE)) / 2;
                Double rightKey = max - ((max - min) * (1 - TopologyConfig.KER_RANGE_COVERAGE)) / 2;
                */

                System.out.println("Left key is " + leftKey.intValue());
                System.out.println("Right key is " + rightKey.intValue());

                Long startTimeStamp = System.currentTimeMillis() - 10000;
                Long endTimeStamp = System.currentTimeMillis();

                collector.emit(Streams.QueryGenerateStream,
                        new Values(queryId, leftKey, rightKey, startTimeStamp, endTimeStamp));

                ++queryId;
            }
        }
    }
}
