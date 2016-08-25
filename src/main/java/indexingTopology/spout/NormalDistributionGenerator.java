package indexingTopology.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import indexingTopology.DataSchema;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.io.*;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by acelzj on 7/27/16.
 */
public class NormalDistributionGenerator extends BaseRichSpout {

    double mean;
    double sd;
    SpoutOutputCollector collector_;
    NormalDistribution distribution;
    transient Thread normalDistributionChanger;
    transient Thread ioSpeedTester;
    File file;
    BufferedReader bufferedReader;
    AtomicInteger counter;
    Random random;
    long randomFactor;
    private FileOutputStream fop;


    public NormalDistributionGenerator() throws FileNotFoundException {
//        mean = 500;
//        sd = 20;
//        distribution = new NormalDistribution(mean, sd);
//        randomFactor = 1000;
//        random = new Random(randomFactor);
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("indexValue"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_=collector;
        counter = new AtomicInteger(0);
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

//        try {
//            fop = new FileOutputStream(file);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        normalDistributionChanger = new Thread(new Runnable() {
//            public void run() {
//                while (true) {
//                    Utils.sleep(30000);
//                    mean = random.nextInt(1000);
//                    sd = random.nextInt(100);
//                    while (sd == 0) {
//                        sd = random.nextInt(100);
//                    }
//                    distribution = new NormalDistribution(mean, sd);
//                }
//            }
//        });
//        normalDistributionChanger.start();
//        ioSpeedTester = new Thread((new Runnable() {
//            public void run() {
//                while (true) {
//                    Utils.sleep(10000);
//                    System.out.println(count + "tuples has been emitted in 10 seconds");
//                    count = 0;
//                }
//            }
//        }));
//        ioSpeedTester.start();



    }

    public void nextTuple() {
        String text = null;
        try {
            text = bufferedReader.readLine();
            int msgId = this.counter.getAndIncrement();
//            System.out.println(text);
            double indexValue = Double.parseDouble(text);
            collector_.emit(new Values(indexValue), msgId);
            if (counter.get() == Integer.MAX_VALUE) {
                counter = new AtomicInteger(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        double indexValue = distribution.sample();

//        String content = "" + indexValue;
//        byte[] contentInBytes = content.getBytes();
//        String newline = System.getProperty("line.separator");
//        byte[] nextLineInBytes = newline.getBytes();
//        try {
//            fop.write(contentInBytes);
//            fop.write(nextLineInBytes);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


//        collector_.emit(new Values(indexValue));
    }
}
