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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

/**
 * Created by acelzj on 7/27/16.
 */
public class NormalDistributionGenerator extends BaseRichSpout {

    double mean;
    double sd;
    SpoutOutputCollector collector_;
    transient Thread normalDistributionChanger;
    NormalDistribution distribution;

    public NormalDistributionGenerator()
    {
        mean = 500;
        sd = 20;
        distribution = new NormalDistribution(mean, sd);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("indexValue"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        collector_=collector;
        //   Utils.sleep(60000);
        normalDistributionChanger = new Thread(new Runnable() {
            public void run() {
                while (true) {
                    Utils.sleep(30000);
                    Random random = new Random(System.currentTimeMillis());
                    mean = random.nextInt(1000);
                    sd = random.nextInt(100);
                    distribution = new NormalDistribution(mean, sd);
                }
            }
        });
        normalDistributionChanger.start();
    }

    public void nextTuple() {
        double indexValue = distribution.sample();
        collector_.emit(new Values(indexValue));
    }
}
