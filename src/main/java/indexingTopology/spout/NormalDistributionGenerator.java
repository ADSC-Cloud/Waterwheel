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

/**
 * Created by acelzj on 7/27/16.
 */
public class NormalDistributionGenerator extends BaseRichSpout {

 //   double mean;
 //   double sd;
    SpoutOutputCollector collector_;
 //   NormalDistribution distribution;
 //   transient Thread normalDistributionChanger;
    File file;
    BufferedReader bufferedReader;
 //   Random random;
 //   long randomFactor;

    public NormalDistributionGenerator() throws FileNotFoundException {
      //  mean = 500;
      //  sd = 20;
      //  distribution = new NormalDistribution(mean, sd);
     //   randomFactor = 1000;
     //   random = new Random(randomFactor);
        file = new File("/home/acelzj/IndexTopology_experiment/NormalDistribution/input_data");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("indexValue"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_=collector;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

     /*   normalDistributionChanger = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    Utils.sleep(30000);
                    mean = random.nextInt(1000);
                    sd = random.nextInt(100);
                    distribution = new NormalDistribution(mean, sd);
                }
            }
        });
        normalDistributionChanger.start();*/



    }

    public void nextTuple() {
        String text = null;
        try {
            text = bufferedReader.readLine();
            double indexValue = Double.parseDouble(text);
            collector_.emit(new Values(indexValue));
        } catch (IOException e) {
            e.printStackTrace();
        }

     //   double indexValue = distribution.sample();
     //   collector_.emit(new Values(indexValue));
    }
}
