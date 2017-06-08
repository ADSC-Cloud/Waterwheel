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
 * Created by acelzj on 12/3/16.
 */
public class QueryGeneratorSpout extends BaseRichSpout{

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
                new Fields("queryId", "key", "startTimestamp", "endTimestamp"));
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

                String [] tuple = text.split(" ");
//
                Double key = Double.parseDouble(tuple[0]);

//                Long startTimeStamp = System.currentTimeMillis() - 10000;
                Long startTimestamp = (long) 0;
//                Long endTimeStamp = System.currentTimeMillis();
                Long endTimestamp = Long.MAX_VALUE;

                collector.emit(Streams.QueryGenerateStream,
                        new Values(queryId, key, startTimestamp, endTimestamp));

                ++queryId;
            }
        }
    }

}
